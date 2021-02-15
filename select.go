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

type fieldCol struct {
	fieldName string
	colName   string
	field     reflect.Value
	col       int
	scanFunc  func(dst reflect.Value, src []byte) error
}

// ColumnGetter is a type that implements CoolMySQLGetColumns
type ColumnGetter interface {
	CoolMySQLGetColumns(colTypes []*sql.ColumnType) (cols []Column)
}

// RowScanner is a type that implements CoolMySQLScanRow
type RowScanner interface {
	CoolMySQLScanRow(cols []Column, ptrs []interface{}) error
}

func _select(ctx context.Context, db *Database, destReflectValue reflect.Value, replacedQuery string, cache time.Duration, mergedParams Params) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	destReflectValue = reflect.Indirect(destReflectValue)

	reflectType, reflectKind, multipleResults := getDestInfo(destReflectValue)
	reflectValue := reflect.New(reflectType)
	reflectValueElem := reflectValue.Elem()
	iface := reflectValue.Interface()

	var cacheBuf []byte

	destReflectKind := destReflectValue.Kind()
	rowsScanned := 0

	done := ctx.Done()

	var cases []reflect.SelectCase
	if destReflectKind == reflect.Chan {
		cases = []reflect.SelectCase{
			{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(done),
			},
			{
				Dir:  reflect.SelectSend,
				Chan: destReflectValue,
			},
		}
	}

	columnGetter, columnGetterOk := iface.(ColumnGetter)
	rowScanner, rowScannerOk := iface.(RowScanner)

	var fieldCols []fieldCol
	var singleColumn bool
	if !rowScannerOk {
		singleColumn = !isGenericStruct(iface, reflectType, reflectKind)
	}

	var singleColumnScanFunc func(dst reflect.Value, src []byte) error

	mapKey := "Key"
	var keyReflectValue reflect.Value

	scan := func(cols []Column, ptrs []interface{}) error {
		if rowScannerOk {
			return rowScanner.CoolMySQLScanRow(cols, ptrs)
		}

		if singleColumn {
			if singleColumnScanFunc == nil {
				singleColumnScanFunc = getScanFunc(cols[0], iface, reflectType, reflectKind)
			}

			src := []byte(*(ptrs[0].(*sql.RawBytes)))
			err := singleColumnScanFunc(reflectValueElem, src)
			return errors.Wrapf(err, "failed to scan %q into destination", src)
		}

		if fieldCols == nil {
			fieldCols = make([]fieldCol, 0, len(cols))

			colsMap := make(map[string]Column, len(cols))
			for _, c := range cols {
				colsMap[c.Name] = c
			}

			numField := reflectValueElem.NumField()
			for i := 0; i < numField; i++ {
				f := reflectValueElem.Field(i)
				if !f.CanInterface() {
					continue
				}

				ft := reflectType.Field(i)
				name, ok := ft.Tag.Lookup("mysql")
				if !ok {
					name = ft.Name
				}

				if c, ok := colsMap[name]; ok {
					fieldCols = append(fieldCols, fieldCol{
						fieldName: ft.Name,
						colName:   name,
						field:     f,
						col:       c.Index,
						scanFunc:  getScanFunc(c, f.Addr().Interface(), f.Type(), f.Kind()),
					})
				}
			}
		}

		for _, f := range fieldCols {
			src := []byte(*(ptrs[f.col].(*sql.RawBytes)))
			err := f.scanFunc(f.field, src)
			if err != nil {
				return errors.Wrapf(err, "failed to scan %q into field %q", src, f.fieldName)
			}

			if destReflectKind == reflect.Map && f.colName == mapKey {
				keyReflectValue = f.field
			}
		}

		if destReflectKind == reflect.Map {
			if !keyReflectValue.IsValid() {
				return errors.Errorf("no column named %q found in results for map key", mapKey)
			}
		}

		return nil
	}

	scanAndSend := func(cols []Column, ptrs []interface{}) error {
		err := scan(cols, ptrs)
		if err != nil {
			return err
		}

		if !multipleResults {
			destReflectValue.Set(reflectValueElem)
		} else {
			switch destReflectKind {
			case reflect.Chan:
				cases[1].Send = reflectValueElem
				switch index, _, _ := reflect.Select(cases); index {
				case 0:
					cancel()
					return nil
				}
			case reflect.Slice:
				destReflectValue.Set(reflect.Append(destReflectValue, reflectValueElem))
			case reflect.Map:
				destReflectValue.SetMapIndex(keyReflectValue, reflectValueElem)
			}
		}

		return nil
	}

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
		for i := 0; i < len(ptrs); i++ {
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
				if columnGetterOk {
					cols = columnGetter.CoolMySQLGetColumns(colTypes)
				} else {
					cols = getDestCols(iface, reflectValueElem, reflectType, reflectKind, colTypes)
				}

				if cache != 0 {
					serializeDestColumns(&cacheBuf, cols)
				}
			}

			if cache != 0 {
				serializeDestRow(&cacheBuf, cols, ptrs)
			}

			err = scanAndSend(cols, ptrs)
			if err != nil {
				return err
			}

			if !multipleResults {
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
		gob.NewEncoder(hasher).EncodeValue(reflectValueElem)
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

				err = scanAndSend(cols, ptrs)
				if err != nil {
					return nil
				}

				rowsScanned++

				if !multipleResults {
					break
				}
			}
		}
	}

	if !multipleResults && rowsScanned == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func getDestInfo(destReflectValue reflect.Value) (reflectType reflect.Type, reflectKind reflect.Kind, multipleResults bool) {
	reflectType = destReflectValue.Type()
	reflectKind = reflectType.Kind()

	if reflectType != reflectTypeBytes && (reflectKind == reflect.Chan || reflectKind == reflect.Slice || reflectKind == reflect.Map) {
		multipleResults = true
		reflectType = reflectType.Elem()
		reflectKind = reflectType.Kind()
	}

	return
}

func isGenericStruct(iface interface{}, reflectType reflect.Type, reflectKind reflect.Kind) bool {
	if reflectKind != reflect.Struct {
		return false
	}

	if reflectType == reflectTypeTime {
		return false
	}

	if _, ok := iface.(sql.Scanner); ok {
		return false
	}

	return true
}

// Column is the name and scan type of a query column
// used in (de)serialization
type Column struct {
	Name     string
	Index    int
	ScanType uint8
}

func getDestCols(iface interface{}, reflectValue reflect.Value, reflectType reflect.Type, reflectKind reflect.Kind, colTypes []*sql.ColumnType) (cols []Column) {
	if !isGenericStruct(iface, reflectType, reflectKind) {
		// not a "generic" (custom/created by user) struct, so we only have
		// one column to deal with
		return []Column{{
			Name:     colTypes[0].Name(),
			ScanType: ScanType(colTypes[0]),
		}}
	}

	// a custom struct created by the caller, so we need to figure
	// out how to map that struct's fields to the columns from the query

	numField := reflectType.NumField()

	colsCap := len(colTypes)
	if numField < colsCap {
		colsCap = numField
	}

	cols = make([]Column, 0, colsCap)

	fieldsMap := make(map[string]int, numField)
	for i := 0; i < numField; i++ {
		if !reflectValue.Field(i).CanInterface() {
			continue
		}

		f := reflectType.Field(i)
		name, ok := f.Tag.Lookup("mysql")
		if !ok {
			name = f.Name
		}

		fieldsMap[name] = i
	}

	for i, c := range colTypes {
		name := c.Name()
		if _, ok := fieldsMap[name]; ok {
			cols = append(cols, Column{
				Name:     name,
				Index:    i,
				ScanType: ScanType(c),
			})
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
	for _, c := range cols {
		rb := ptrs[c.Index].(*sql.RawBytes)
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
	for _, c := range cols {
		rb := ptrs[c.Index].(*sql.RawBytes)
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
			Index:    i,
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
