package mysql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-sql-driver/mysql"
)

const (
	sqlRawBytes uint8 = iota
	sqlNullInt64
	goInt64
	mysqlNullTime
)

var (
	refRawBytes  = reflect.TypeOf(sql.RawBytes{})
	refNullInt64 = reflect.TypeOf(sql.NullInt64{})
	refInt64     = reflect.TypeOf(int64(0))
	refNullTime  = reflect.TypeOf(mysql.NullTime{})
)

// ScanType returns the scan type number for
// the given column suggested scan type
func ScanType(col *sql.ColumnType) uint8 {
	switch col.ScanType() {
	case refRawBytes:
		return sqlRawBytes
	case refNullInt64:
		return sqlNullInt64
	case refInt64:
		return goInt64
	case refNullTime:
		return mysqlNullTime
	}

	panic(fmt.Errorf("unhandled scan type of %s", spew.Sdump(col.ScanType())))
}

// ScanInto scans a src bytes into a sql.Scanner
func ScanInto(c Column, s sql.Scanner, src []byte) error {
	if src == nil {
		return s.Scan(nil)
	}

	switch c.ScanType {
	case sqlNullInt64, goInt64:
		v, err := strconv.ParseInt(string(src), 10, 64)
		if err != nil {
			return err
		}
		return s.Scan(v)
	}

	return s.Scan(src)
}
