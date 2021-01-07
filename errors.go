package trino

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

var (
	// DefaultQueryTimeout is the default timeout for queries executed without a context.
	DefaultQueryTimeout = 60 * time.Second

	// DefaultCancelQueryTimeout is the timeout for the request to cancel queries in Trino.
	DefaultCancelQueryTimeout = 30 * time.Second

	// ErrOperationNotSupported indicates that a database operation is not supported.
	ErrOperationNotSupported = errors.New("trino: operation not supported")

	// ErrQueryCancelled indicates that a query has been cancelled.
	ErrQueryCancelled = errors.New("trino: query cancelled")
)

// ErrQueryFailed indicates that a query to Trino failed.
type ErrQueryFailed struct {
	StatusCode int
	Reason     error
}

// Error implements the error interface.
func (e *ErrQueryFailed) Error() string {
	return fmt.Sprintf("trino: query failed (%d %s): %q",
		e.StatusCode, http.StatusText(e.StatusCode), e.Reason)
}

func newErrQueryFailedFromResponse(resp *http.Response) *ErrQueryFailed {
	const maxBytes = 8 * 1024
	defer resp.Body.Close()
	qf := &ErrQueryFailed{StatusCode: resp.StatusCode}
	b, err := ioutil.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		qf.Reason = err
		return qf
	}
	reason := string(b)
	if resp.ContentLength > maxBytes {
		reason += "..."
	}
	qf.Reason = errors.New(reason)
	return qf
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
