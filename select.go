package mysql

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"reflect"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"golang.org/x/sync/singleflight"
)

var selectSingleflight = new(singleflight.Group)

// FastDest is a type that implements all the helper funcs
// to tell us exactly how to store raw mysql data into it
// ideally, these funcs will be generated with `go generate`
type FastDest interface {
	CoolMySQLGetColumns(colTypes []*sql.ColumnType) (cols []Column)
	CoolMySQLRowScan(cols []Column, ptrs []interface{}) error
}

// Select stores the results of the query in the given destination
func (db *Database) Select(dest interface{}, query string, cache time.Duration, params ...Params) error {
	return db.SelectContext(context.Background(), dest, query, cache, params...)
}

// SelectContext stores the results of the query in the given destination
func (db *Database) SelectContext(ctx context.Context, dest interface{}, query string, cache time.Duration, params ...Params) error {
	replacedQuery, mergedParams := ReplaceParams(query, params...)
	if db.die {
		fmt.Println(replacedQuery)
		os.Exit(0)
	}

	rd := reflect.ValueOf(dest)
	switch rd.Kind() {
	case reflect.Chan:
		go func() {
			err := _select(ctx, db, rd, replacedQuery, cache, mergedParams)
			if err != nil {
				panic(err)
			}
			rd.Close()
		}()
		return nil
	case reflect.Ptr:
		return _select(ctx, db, rd, replacedQuery, cache, mergedParams)
	default:
		return errors.New("cool-mysql: select destination must be a channel or a pointer to something")
	}
}

func _select(ctx context.Context, db *Database, rd reflect.Value, replacedQuery string, cache time.Duration, mergedParams Params) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rd = reflect.Indirect(rd)
	rt := getDestType(rd)
	rv := reflect.New(rt)

	rvIface := rv.Interface()
	fd := rvIface.(FastDest)

	var cacheBuf []byte

	k := rd.Kind()
	single := k != reflect.Chan && k != reflect.Slice
	rowsScanned := 0

	readFromDB := func() error {
		rows, err := db.Reads.QueryContext(ctx, replacedQuery)
		if err != nil {
			return err
		}
		defer rows.Close()

		colTypes, err := rows.ColumnTypes()
		if err != nil {
			return err
		}

		ptrs := make([]interface{}, len(colTypes))
		for i := 0; i < len(colTypes); i++ {
			ptrs[i] = new(sql.RawBytes)
		}

		var cols []Column

		for rows.Next() {
			err := rows.Scan(ptrs...)
			if err != nil {
				return err
			}
			rowsScanned++

			if cols == nil {
				if fd, ok := rvIface.(interface {
					CoolMySQLGetColumns(colTypes []*sql.ColumnType) (cols []Column)
				}); ok {
					cols = fd.CoolMySQLGetColumns(colTypes)
				} else {
					cols = getDestCols(rv, colTypes)
				}

				if cache != 0 {
					serializeDestColumns(&cacheBuf, cols)
				}
			}

			if cache != 0 {
				serializeDestRow(&cacheBuf, cols, ptrs)
			}

			err = fd.CoolMySQLRowScan(cols, ptrs)
			if err != nil {
				return err
			}

			switch rd.Kind() {
			case reflect.Chan:
				rd.Send(rv.Elem())
			case reflect.Slice:
				rd.Set(reflect.Append(rd, rv.Elem()))
			default:
				rd.Set(rv.Elem())
				break
			}
		}
		return nil
	}

	if cache == 0 {
		err := readFromDB()
		if err != nil {
			return err
		}
	} else {
		if db.redis == nil {
			return errors.New("cache duration given without redis connection")
		}

		hasher := md5.New()
		gob.NewEncoder(hasher).EncodeValue(rv)
		hasher.Write([]byte(replacedQuery))
		h := base64.RawStdEncoding.EncodeToString(hasher.Sum(nil))

		scanned := false
		cachedBytes, err, _ := selectSingleflight.Do(h, func() (interface{}, error) {
			b, err := db.redis.Get(ctx, h).Bytes()
			if err == redis.Nil {
				err := readFromDB()
				if err != nil {
					return nil, err
				}
				scanned = true

				err = db.redis.Set(ctx, h, cacheBuf, cache).Err()
				if err != nil {
					return nil, err
				}

				return cacheBuf, nil
			} else if err != nil {
				return nil, err
			}

			return b, nil
		})
		if err != nil {
			return err
		}

		if b := cachedBytes.([]byte); len(b) != 0 && !scanned {
			cols, err := deserializeDestColumns(&b)
			if err != nil {
				return err
			}
			for {
				ptrs, err := deserializeDestRow(len(cols), &b)
				if err == io.EOF {
					break
				} else if err != nil {
					return err
				}

				err = fd.CoolMySQLRowScan(cols, ptrs)
				if err != nil {
					return err
				}

				switch rd.Kind() {
				case reflect.Chan:
					rd.Send(rv.Elem())
				case reflect.Slice:
					rd.Set(reflect.Append(rd, rv.Elem()))
				default:
					rd.Set(rv.Elem())
					break
				}
			}
		}
	}

	if single && rowsScanned == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func getDestType(rd reflect.Value) reflect.Type {
	switch rd.Kind() {
	case reflect.Chan, reflect.Slice:
		return rd.Type().Elem()
	default:
		return rd.Type()
	}
}

// Column is the name and scan type of a query column
// used in (de)serialization
type Column struct {
	Name     string
	ScanType uint8
}

func getDestCols(rv reflect.Value, colTypes []*sql.ColumnType) (cols []Column) {
	colTypesMap := make(map[string]int, len(colTypes))
	for i, ct := range colTypes {
		colTypesMap[ct.Name()] = i
	}

	rv = rv.Elem()

	switch rv.Kind() {
	case reflect.Struct:
		numField := rv.NumField()

		colsCap := len(colTypes)
		if numField < colsCap {
			colsCap = numField
		}

		cols = make([]Column, 0, len(colTypes))

		rt := rv.Type()

		for i := 0; i < numField; i++ {
			if !rv.Field(i).CanInterface() {
				continue
			}

			f := rt.Field(i)
			name, ok := f.Tag.Lookup("mysql")
			if !ok {
				name = f.Name
			}

			if colTypeI, ok := colTypesMap[name]; ok {
				cols = append(cols, Column{
					Name:     name,
					ScanType: ScanType(colTypes[colTypeI]),
				})
			}
		}
	}

	return cols
}

func serializeDestColumns(buf *[]byte, cols []Column) {
	totalLen := 1 // first byte for column count
	for _, c := range cols {
		totalLen += 1 + 1 + len(c.Name) // scanType + len(name) + name
	}

	// grow our buf cap if it's not big enough
	// this is the first thing so the buffer *should*
	// be empty but you know, maybe we want to sync.Pool
	// it or something
	if cap(*buf)-len(*buf) < totalLen {
		tmp := make([]byte, len(*buf), 2*cap(*buf)+totalLen)
		copy(tmp, *buf)
		*buf = tmp
	}
	*buf = append(*buf, uint8(len(cols)))
	for _, c := range cols {
		*buf = append(*buf, c.ScanType)
		*buf = append(*buf, uint8(len(c.Name)))
		*buf = append(*buf, []byte(c.Name)...)
	}
}

func serializeDestRow(buf *[]byte, cols []Column, ptrs []interface{}) {
	totalLen := 0
	for i := range cols {
		rb := ptrs[i].(*sql.RawBytes)
		if *rb == nil {
			totalLen++
		} else {
			totalLen += 1 + 8 + len(*rb)
		}
	}

	// grow our buf cap if it's not big enough
	if cap(*buf)-len(*buf) < totalLen {
		tmp := make([]byte, len(*buf), 2*cap(*buf)+totalLen)
		copy(tmp, *buf)
		*buf = tmp
	}
	for i := range cols {
		rb := ptrs[i].(*sql.RawBytes)
		if *rb == nil {
			*buf = append(*buf, 1)
		} else {
			*buf = append(*buf, 0)
			*buf = (*buf)[:len(*buf)+8]
			binary.LittleEndian.PutUint64((*buf)[len(*buf)-8:], uint64(len(*rb)))
			*buf = append(*buf, *rb...)
		}
	}
}

func deserializeDestColumns(buf *[]byte) (cols []Column, err error) {
	if len(*buf) == 0 {
		return nil, io.EOF
	}

	offset := 0

	cols = make([]Column, (*buf)[offset])
	offset++

	for i := range cols {
		scanType := (*buf)[offset]
		offset++

		nameLen := int((*buf)[offset])
		offset++

		name := (*buf)[offset : offset+nameLen]
		offset += nameLen

		cols[i] = Column{
			Name:     string(name),
			ScanType: scanType,
		}
	}

	(*buf) = (*buf)[offset:]

	return cols, nil
}

func deserializeDestRow(colLen int, buf *[]byte) (ptrs []interface{}, err error) {
	ptrs = make([]interface{}, colLen)

	if len(*buf) == 0 {
		return nil, io.EOF
	}
	offset := 0

	for i := range ptrs {
		var src []byte
		null := (*buf)[offset] == 1
		offset++

		if !null {
			size := int(binary.LittleEndian.Uint64((*buf)[offset:]))
			offset += 8

			src = (*buf)[offset : offset+size]
			offset += size
		}

		rb := sql.RawBytes(src)
		ptrs[i] = &rb
		i++
	}

	(*buf) = (*buf)[offset:]

	return ptrs, nil
}
