package mysql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"

	"github.com/davecgh/go-spew/spew"
)

// var sqlNullInt64 = reflect.TypeOf(sql.NullInt64{})

type scanType uint8

const (
	sqlNullInt64 scanType = iota
)

// ScanType returns the scan type number for
// the given column suggested scan type
func ScanType(col *sql.ColumnType) scanType {
	switch col.ScanType() {
	case reflect.TypeOf(sql.NullInt64{}):
		return sqlNullInt64
	}

	panic(fmt.Errorf("unhandled scan type of %s", spew.Sdump(col.ScanType())))
}

// ScanInto scans a src bytes into a sql.Scanner
func ScanInto(scanType scanType, s sql.Scanner, src []byte) error {
	if src == nil {
		return s.Scan(nil)
	}

	switch scanType {
	case sqlNullInt64:
		v, err := strconv.ParseInt(string(src), 10, 64)
		if err != nil {
			return err
		}
		return s.Scan(v)
	}

	return s.Scan(src)
}
