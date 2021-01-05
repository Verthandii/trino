package trino

import (
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("trino", &sqldriver{})
}

// sqldriver implements driver.Driver
type sqldriver struct{}

func (d *sqldriver) Open(name string) (driver.Conn, error) {
	return newConn(name)
}

var _ driver.Driver = &sqldriver{}
