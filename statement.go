package trino

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// driverStmt implements driver.Stmt & driver.StmtQueryContext
type driverStmt struct {
	conn  *Conn
	query string
	user  string

	callback QueryCallBack
}

var (
	_ driver.Stmt              = &driverStmt{}
	_ driver.StmtQueryContext  = &driverStmt{}
	_ driver.NamedValueChecker = &driverStmt{}
)

func (st *driverStmt) Close() error {
	st.callback = nil
	return nil
}

func (st *driverStmt) NumInput() int {
	return -1
}

func (st *driverStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, ErrOperationNotSupported
}

func (st *driverStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, driver.ErrSkip
}

// CheckNamedValue check if NamedValue is by type assertion & implements driver.NamedValueChecker
func (st *driverStmt) CheckNamedValue(value *driver.NamedValue) error {
	callback, ok := value.Value.(QueryCallBack)
	if ok {
		st.callback = callback
		return driver.ErrRemoveArgument
	}

	return driver.ErrSkip
}

type stmtResponse struct {
	ID      string    `json:"id"`
	InfoURI string    `json:"infoUri"`
	NextURI string    `json:"nextUri"`
	Stats   stmtStats `json:"stats"`
	Error   stmtError `json:"error"`
}

type stmtStats struct {
	State              string    `json:"state"`
	ProgressPercentage float32   `json:"progressPercentage"`
	Scheduled          bool      `json:"scheduled"`
	Nodes              int       `json:"nodes"`
	TotalSplits        int       `json:"totalSplits"`
	QueuesSplits       int       `json:"queuedSplits"`
	RunningSplits      int       `json:"runningSplits"`
	CompletedSplits    int       `json:"completedSplits"`
	UserTimeMillis     int       `json:"userTimeMillis"`
	CPUTimeMillis      int       `json:"cpuTimeMillis"`
	WallTimeMillis     int       `json:"wallTimeMillis"`
	ProcessedRows      int       `json:"processedRows"`
	ProcessedBytes     int       `json:"processedBytes"`
	RootStage          stmtStage `json:"rootStage"`
}

type stmtStage struct {
	StageID         string      `json:"stageId"`
	State           string      `json:"state"`
	Done            bool        `json:"done"`
	Nodes           int         `json:"nodes"`
	TotalSplits     int         `json:"totalSplits"`
	QueuedSplits    int         `json:"queuedSplits"`
	RunningSplits   int         `json:"runningSplits"`
	CompletedSplits int         `json:"completedSplits"`
	UserTimeMillis  int         `json:"userTimeMillis"`
	CPUTimeMillis   int         `json:"cpuTimeMillis"`
	WallTimeMillis  int         `json:"wallTimeMillis"`
	ProcessedRows   int         `json:"processedRows"`
	ProcessedBytes  int         `json:"processedBytes"`
	SubStages       []stmtStage `json:"subStages"`
}

type stmtError struct {
	Message       string               `json:"message"`
	ErrorName     string               `json:"errorName"`
	ErrorCode     int                  `json:"errorCode"`
	ErrorLocation stmtErrorLocation    `json:"errorLocation"`
	FailureInfo   stmtErrorFailureInfo `json:"failureInfo"`
	// Other fields omitted
}

type stmtErrorLocation struct {
	LineNumber   int `json:"lineNumber"`
	ColumnNumber int `json:"columnNumber"`
}

type stmtErrorFailureInfo struct {
	Type string `json:"type"`
	// Other fields omitted
}

func (e stmtError) Error() string {
	return e.FailureInfo.Type + ": " + e.Message
}

func (st *driverStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	query := st.query
	var hs http.Header

	if len(args) > 0 {
		hs = make(http.Header)
		var ss []string
		for _, arg := range args {
			switch arg.Name {
			case _trinoUserHeader:
				st.user = arg.Value.(string)
				hs.Add(_trinoUserHeader, st.user)
			case _trinoQueryCallbackHeader:
				// 正常情况下 sql.driverArgsConnLocked 中过滤掉了这个 case
				err := st.CheckNamedValue(&arg)
				if err != nil {
					return nil, err
				}
			default:
				s, err := Serial(arg.Value)
				if err != nil {
					return nil, err
				}
				if hs.Get(_preparedStatementHeader) == "" {
					hs.Add(_preparedStatementHeader, _preparedStatementName+"="+url.QueryEscape(st.query))
				}
				ss = append(ss, s)
			}
		}
		if len(ss) > 0 {
			query = "EXECUTE " + _preparedStatementName + " USING " + strings.Join(ss, ", ")
		}
	}

	req, err := st.conn.newRequest("POST", st.conn.baseURL+"/v1/statement", strings.NewReader(query), hs)
	if err != nil {
		return nil, err
	}

	resp, err := st.conn.roundTrip(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var sr stmtResponse
	d := json.NewDecoder(resp.Body)
	d.UseNumber()
	err = d.Decode(&sr)
	if err != nil {
		return nil, fmt.Errorf("trino: %v", err)
	}
	err = handleResponseError(resp.StatusCode, sr.Error)
	if err != nil {
		return nil, err
	}
	rows := &driverRows{
		ctx:     ctx,
		stmt:    st,
		nextURI: sr.NextURI,
	}

	// first callback
	if st.callback != nil {
		st.callback.OnUpdated(QueryInfo{
			Id:         sr.ID,
			QueryStats: sr.Stats,
			Cancel: func() error {
				req, err := st.conn.newRequest("DELETE", sr.NextURI, nil, hs)
				if err != nil {
					return err
				}
				return cancelQuery(req, st.conn.httpClient)
			},
		})
	}

	if err = rows.fetch(false); err != nil {
		return nil, err
	}
	return rows, nil
}
