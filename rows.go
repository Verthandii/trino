package trino

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"time"
)

// driverRows implements driver.Rows
type driverRows struct {
	ctx     context.Context
	stmt    *driverStmt
	nextURI string

	err      error
	rowindex int
	columns  []string
	coltype  []*typeConverter
	data     []queryData
}

var _ driver.Rows = &driverRows{}

func (qr *driverRows) Close() error {
	if qr.nextURI != "" {
		hs := make(http.Header)
		hs.Add(_trinoUserHeader, qr.stmt.user)
		req, err := qr.stmt.conn.newRequest("DELETE", qr.nextURI, nil, hs)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithDeadline(
			context.Background(),
			time.Now().Add(DefaultCancelQueryTimeout),
		)
		defer cancel()
		resp, err := qr.stmt.conn.roundTrip(ctx, req)
		if err != nil {
			qferr, ok := err.(*ErrQueryFailed)
			if ok && qferr.StatusCode == http.StatusNoContent {
				qr.nextURI = ""
				return nil
			}
			return err
		}
		resp.Body.Close()
	}
	return qr.err
}

func (qr *driverRows) Columns() []string {
	if qr.err != nil {
		return []string{}
	}
	if qr.columns == nil {
		if err := qr.fetch(false); err != nil {
			qr.err = err
			return []string{}
		}
	}
	return qr.columns
}

var _coltypeLengthSuffix = regexp.MustCompile(`\(\d+\)$`)

func (qr *driverRows) ColumnTypeDatabaseTypeName(index int) string {
	name := qr.coltype[index].typeName
	if m := _coltypeLengthSuffix.FindStringSubmatch(name); m != nil {
		name = name[0 : len(name)-len(m[0])]
	}
	return name
}

func (qr *driverRows) Next(dest []driver.Value) error {
	if qr.err != nil {
		return qr.err
	}
	if qr.columns == nil || qr.rowindex >= len(qr.data) {
		if qr.nextURI == "" {
			qr.err = io.EOF
			return qr.err
		}
		if err := qr.fetch(true); err != nil {
			qr.err = err
			return err
		}
	}
	if len(qr.coltype) == 0 {
		qr.err = sql.ErrNoRows
		return qr.err
	}
	for i, v := range qr.coltype {
		vv, err := v.ConvertValue(qr.data[qr.rowindex][i])
		if err != nil {
			qr.err = err
			return err
		}
		dest[i] = vv
	}
	qr.rowindex++
	return nil
}

type queryResponse struct {
	ID               string        `json:"id"`
	InfoURI          string        `json:"infoUri"`
	PartialCancelURI string        `json:"partialCancelUri"`
	NextURI          string        `json:"nextUri"`
	Columns          []queryColumn `json:"columns"`
	Data             []queryData   `json:"data"`
	Stats            stmtStats     `json:"stats"`
	Error            stmtError     `json:"error"`
}

type queryColumn struct {
	Name          string        `json:"name"`
	Type          string        `json:"type"`
	TypeSignature typeSignature `json:"typeSignature"`
}

type queryData []interface{}

type typeSignature struct {
	RawType          string        `json:"rawType"`
	TypeArguments    []interface{} `json:"typeArguments"`
	LiteralArguments []interface{} `json:"literalArguments"`
}

type infoResponse struct {
	QueryID string `json:"queryId"`
	State   string `json:"state"`
}

func handleResponseError(status int, respErr stmtError) error {
	switch respErr.ErrorName {
	case "":
		return nil
	case "USER_CANCELLED":
		return ErrQueryCancelled
	default:
		return &ErrQueryFailed{
			StatusCode: status,
			Reason:     &respErr,
		}
	}
}

func (qr *driverRows) fetch(allowEOF bool) error {
	hs := make(http.Header)
	hs.Add(_trinoUserHeader, qr.stmt.user)
	req, err := qr.stmt.conn.newRequest("GET", qr.nextURI, nil, hs)
	if err != nil {
		return err
	}
	resp, err := qr.stmt.conn.roundTrip(qr.ctx, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var qresp queryResponse
	d := json.NewDecoder(resp.Body)
	d.UseNumber()
	err = d.Decode(&qresp)
	if err != nil {
		return fmt.Errorf("trino: %v", err)
	}
	err = handleResponseError(resp.StatusCode, qresp.Error)
	if err != nil {
		return err
	}
	qr.rowindex = 0
	qr.data = qresp.Data
	qr.nextURI = qresp.NextURI

	if qr.stmt.conn.callback != nil {
		qr.stmt.conn.callback.OnUpdated(QueryInfo{
			Id:         qresp.ID,
			QueryStats: qresp.Stats,
			Cancel:     qr.Close,
		})
	}

	if len(qr.data) == 0 {
		if qr.nextURI != "" {
			return qr.fetch(allowEOF)
		}
		if allowEOF {
			return io.EOF
		}
	} else {
		// 有数据之后忽略 next uri
		qr.nextURI = ""
	}
	if qr.columns == nil && len(qresp.Columns) > 0 {
		qr.initColumns(&qresp)
	}
	return nil
}

func (qr *driverRows) initColumns(qresp *queryResponse) {
	qr.columns = make([]string, len(qresp.Columns))
	qr.coltype = make([]*typeConverter, len(qresp.Columns))
	for i, col := range qresp.Columns {
		qr.columns[i] = col.Name
		qr.coltype[i] = newTypeConverter(col.Type)
	}
}

func cancelQuery(req *http.Request, client http.Client) error {
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New(fmt.Sprintf("cancel query error: http status is %s", resp.Status))
	}

	return nil
}
