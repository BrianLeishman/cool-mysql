package cool

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"strconv"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
)

const (
	sqlRawBytes uint8 = iota
	sqlNullInt64
	goInt64
	mysqlNullTime
)

var (
	reflectTypeSQLNullInt64 = reflect.TypeOf((*sql.NullInt64)(nil)).Elem()
	reflectTypeInt64        = reflect.TypeOf((*int64)(nil)).Elem()
)

// ScanType returns the scan type number for
// the given column suggested scan type
func ScanType(col *sql.ColumnType) uint8 {
	switch col.ScanType() {
	case reflectTypeSQLNullInt64:
		return sqlNullInt64
	case reflectTypeInt64:
		return goInt64
	}

	return sqlRawBytes
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

var reflectTypeBytes = reflect.TypeOf((*[]byte)(nil)).Elem()
var reflectTypeTime = reflect.TypeOf((*time.Time)(nil)).Elem()

func getScanFunc(c Column, iface interface{}, reflectType reflect.Type, reflectKind reflect.Kind) func(dst reflect.Value, src []byte) error {
	if scanner, ok := iface.(sql.Scanner); ok {
		return func(dst reflect.Value, src []byte) error {
			return ScanInto(c, scanner, src)
		}
	}

	switch reflectType {
	case reflectTypeTime:
		return func(dst reflect.Value, src []byte) error {
			if src == nil {
				dst.Set(reflect.New(reflectTypeTime).Elem())
				return nil
			}

			n, err := time.Parse(time.RFC3339Nano, string(src))
			if err != nil {
				return errors.Wrapf(err, "failed to parse time")
			}
			dst.Set(reflect.ValueOf(n))
			return nil
		}
	case reflectTypeBytes:
		return func(dst reflect.Value, src []byte) error {
			var b []byte

			if src != nil {
				b = make([]byte, len(src))
				copy(b, src)
			}

			dst.SetBytes(b)
			return nil
		}
	}

	switch reflectKind {
	case reflect.String:
		return func(dst reflect.Value, src []byte) error {
			dst.SetString(string(src))
			return nil
		}
	case reflect.Bool:
		return func(dst reflect.Value, src []byte) error {
			if src == nil {
				dst.SetBool(false)
				return nil
			}

			n, err := strconv.ParseBool(string(src))
			if err != nil {
				return err
			}
			dst.SetBool(n)
			return nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		bits := reflectType.Bits()
		return func(dst reflect.Value, src []byte) error {
			if src == nil {
				dst.SetInt(0)
				return nil
			}

			n, err := strconv.ParseInt(string(src), 10, bits)
			if err != nil {
				return err
			}
			dst.SetInt(n)
			return nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		bits := reflectType.Bits()
		return func(dst reflect.Value, src []byte) error {
			if src == nil {
				dst.SetUint(0)
				return nil
			}

			n, err := strconv.ParseUint(string(src), 10, bits)
			if err != nil {
				return err
			}
			dst.SetUint(n)
			return nil
		}
	case reflect.Float32, reflect.Float64:
		bits := reflectType.Bits()
		return func(dst reflect.Value, src []byte) error {
			if src == nil {
				dst.SetFloat(0)
				return nil
			}

			n, err := strconv.ParseFloat(string(src), bits)
			if err != nil {
				return err
			}
			dst.SetFloat(n)
			return nil
		}
	case reflect.Complex64, reflect.Complex128:
		bits := reflectType.Bits()
		return func(dst reflect.Value, src []byte) error {
			if src == nil {
				dst.SetComplex(0)
				return nil
			}

			n, err := strconv.ParseComplex(string(src), bits)
			if err != nil {
				return err
			}
			dst.SetComplex(n)
			return nil
		}
	case reflect.Array, reflect.Slice, reflect.Map, reflect.Struct:
		return func(dst reflect.Value, src []byte) error {
			if src == nil {
				dst.Set(reflect.New(reflectType).Elem())
				return nil
			}

			return errors.Wrapf(json.Unmarshal(src, iface), "failed to parse json")
		}
	case reflect.Ptr:
		switch reflectType.Elem().Kind() {
		case reflect.String:
			return func(dst reflect.Value, src []byte) error {
				v := reflect.New(reflectType).Elem()

				if src != nil {
					r := reflect.New(v.Type().Elem())
					r.Elem().SetString(string(src))
					v.Set(r)
				}

				dst.Set(v)
				return nil
			}
		case reflect.Bool:
			return func(dst reflect.Value, src []byte) error {
				v := reflect.New(reflectType).Elem()

				if src != nil {
					r := reflect.New(v.Type().Elem())
					n, err := strconv.ParseBool(string(src))
					if err != nil {
						return err
					}
					r.Elem().SetBool(n)
					v.Set(r)
				}

				dst.Set(v)
				return nil
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			bits := reflectType.Elem().Bits()
			return func(dst reflect.Value, src []byte) error {
				v := reflect.New(reflectType).Elem()

				if src != nil {
					r := reflect.New(v.Type().Elem())
					n, err := strconv.ParseInt(string(src), 10, bits)
					if err != nil {
						return err
					}
					r.Elem().SetInt(n)
					v.Set(r)
				}

				dst.Set(v)
				return nil
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			bits := reflectType.Elem().Bits()
			return func(dst reflect.Value, src []byte) error {
				v := reflect.New(reflectType).Elem()

				if src != nil {
					r := reflect.New(v.Type().Elem())
					n, err := strconv.ParseUint(string(src), 10, bits)
					if err != nil {
						return err
					}
					r.Elem().SetUint(n)
					v.Set(r)
				}

				dst.Set(v)
				return nil
			}
		case reflect.Float32, reflect.Float64:
			bits := reflectType.Elem().Bits()
			return func(dst reflect.Value, src []byte) error {
				v := reflect.New(reflectType).Elem()

				if src != nil {
					r := reflect.New(v.Type().Elem())
					n, err := strconv.ParseFloat(string(src), bits)
					if err != nil {
						return err
					}
					r.Elem().SetFloat(n)
					v.Set(r)
				}

				dst.Set(v)
				return nil
			}
		case reflect.Complex64, reflect.Complex128:
			bits := reflectType.Elem().Bits()
			return func(dst reflect.Value, src []byte) error {
				v := reflect.New(reflectType).Elem()

				if src != nil {
					r := reflect.New(v.Type().Elem())
					n, err := strconv.ParseComplex(string(src), bits)
					if err != nil {
						return err
					}
					r.Elem().SetComplex(n)
					v.Set(r)
				}

				dst.Set(v)
				return nil
			}
		}
	}

	return func(dst reflect.Value, src []byte) error {
		return errors.Errorf("unhandled scan of %q into type %s", src, spew.Sdump(dst))
	}
}
