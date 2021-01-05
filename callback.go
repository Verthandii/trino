package trino

type QueryCallBack interface {
	OnUpdated(QueryInfo)
}

type CancelQuery func() error

type QueryInfo struct {
	Id         string      `json:"id"`
	QueryStats stmtStats   `json:"query_stats"`
	Cancel     CancelQuery `json:"cancel"`
}
