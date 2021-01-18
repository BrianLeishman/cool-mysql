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

// ErrDestInvalidType is an error about what types are allowed
var ErrDestInvalidType = errors.New("dest must be a channel of structs, ptr to a slice of structs, or a pointer to a single struct")

func checkDest(dest interface{}) (reflect.Value, reflect.Kind, reflect.Type, error) {
	ref := reflect.ValueOf(dest)
	kind := ref.Kind()

	if kind == reflect.Ptr {
		elem := ref.Elem()
		kind = elem.Kind()

		switch kind {
		case reflect.Struct:
			return ref, kind, elem.Type(), nil
		case reflect.Slice:
			// if dest is a pointer to a slice of structs
			strct := elem.Type().Elem()
			if strct.Kind() == reflect.Struct {
				return ref, kind, strct, nil
			}
		}

		goto Err
	}

	// if dest is a pointer to a slice of structs
	if kind == reflect.Chan {
		strct := ref.Type().Elem()
		if strct.Kind() == reflect.Struct {
			return ref, kind, strct, nil
		}
	}

Err:
	return reflect.Value{}, 0, nil, ErrDestInvalidType
}

type column struct {
	structIndex   uint16
	jsonableIndex uint16
	jsonable      bool
}

type field struct {
	name     string
	jsonable bool
	taken    bool
}

var rCtx = context.Background()

var selectSinglelight = new(singleflight.Group)

// FastDest is a type that implements all the helper funcs
// to tell us exactly how to store raw mysql data into it
// ideally, these funcs will be generated with `go generate`
type FastDest interface {
	CoolMySQLGetColumns(colTypes []*sql.ColumnType) (cols []Column)
	CoolMySQLRowScan(cols []Column, ptrs []interface{}) error
}

func (db *Database) Select(dest interface{}, query string, cache time.Duration, params ...Params) error {
	replacedQuery, mergedParams := ReplaceParams(query, params...)
	if db.die {
		fmt.Println(replacedQuery)
		os.Exit(0)
	}

	rd := reflect.ValueOf(dest)
	switch rd.Kind() {
	case reflect.Chan:
		go func() {
			err := _select(db, rd, replacedQuery, cache, mergedParams)
			if err != nil {
				panic(err)
			}
			rd.Close()
		}()
		return nil
	case reflect.Ptr:
		return _select(db, rd, replacedQuery, cache, mergedParams)
	default:
		return errors.New("cool-mysql: select destination must be a channel or a pointer to something")
	}
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

func _select(db *Database, rd reflect.Value, replacedQuery string, cache time.Duration, mergedParams Params) error {
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
		rows, err := db.Reads.Query(replacedQuery)
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
		cachedBytes, err, _ := selectSinglelight.Do(h, func() (interface{}, error) {
			b, err := db.redis.Get(rCtx, h).Bytes()
			if err == redis.Nil {
				err := readFromDB()
				if err != nil {
					return nil, err
				}
				scanned = true

				err = db.redis.Set(rCtx, h, cacheBuf, cache).Err()
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

// Select selects one or more rows into the
// chan of structs in the destination
// func (db *Database) _Select(dest interface{}, query string, cache time.Duration, params ...Params) error {
// 	replacedQuery, mergedParams := ReplaceParams(query, params...)
// 	if db.die {
// 		fmt.Println(replacedQuery)
// 		os.Exit(0)
// 	}

// 	refDest, kind, strct, err := checkDest(dest)
// 	if err != nil {
// 		return err
// 	}

// 	var msgpWriter *msgp.Writer
// 	var gobEncoder *gob.Encoder
// 	// var cacheBuffer *bytes.Buffer

// 	newStrct := reflect.New(strct).Interface()

// 	var msgpEncodable bool
// 	if cache != 0 {
// 		_, msgpEncodable = newStrct.(msgp.Encodable)
// 	}

// 	_, fastDest := newStrct.(FastDest)

// 	var start time.Time

// 	liveGet := func() error {
// 		start = time.Now()
// 		rows, err := db.Reads.Query(replacedQuery)
// 		execDuration := time.Since(start)
// 		start = time.Now()
// 		db.callLog(replacedQuery, mergedParams, execDuration)

// 		if err != nil {
// 			if kind == reflect.Chan {
// 				refDest.Close()
// 			}
// 			return Error{
// 				Err:           err,
// 				OriginalQuery: query,
// 				ReplacedQuery: replacedQuery,
// 				Params:        mergedParams,
// 			}
// 		}

// 		main := func() error {
// 			if db.Finished != nil {
// 				defer func() { db.Finished(false, replacedQuery, mergedParams, execDuration, time.Since(start)) }()
// 			}

// 			if kind == reflect.Chan {
// 				defer refDest.Close()
// 			}
// 			defer rows.Close()

// 			cols, _ := rows.ColumnTypes()
// 			ptrs := make([]interface{}, len(cols))
// 			for i := range ptrs {
// 				ptrs[i] = new(sql.RawBytes)
// 			}

// 			buf := new(bytes.Buffer)

// 			if fastDest {
// 			RowsLoop:
// 				for rows.Next() {
// 					v := reflect.New(strct)

// 					rows.Scan(ptrs...)
// 					// spew.Dump(ptrs)

// 					buf.Reset()

// 					v.Interface().(FastDest).CoolMySQLRowSerialize(buf, cols, ptrs)
// 					// spew.Dump(buf.Bytes())
// 					// os.Exit(0)

// 					// if cache != 0 {
// 					// 	cacheBuffer.Write(b)
// 					// }

// 					err := v.Interface().(FastDest).CoolMySQLRowScan(ptrs, cols)
// 					if err != nil {
// 						return err
// 					}

// 					v = v.Elem()

// 					switch kind {
// 					case reflect.Chan:
// 						refDest.Send(v)
// 					case reflect.Slice:
// 						refDest.Set(reflect.Append(refDest, v))
// 					default:
// 						refDest.Set(v)
// 						break RowsLoop
// 					}
// 				}

// 				return nil
// 			}

// 			pointers := make([]interface{}, len(cols))

// 			columns := make([]*column, len(cols))

// 			fieldsLen := strct.NumField()
// 			fields := make([]*field, fieldsLen)

// 			strctEx := reflect.New(strct).Elem()

// 			var jsonablesCount uint16
// 			for i, c := range cols {
// 				for j := 0; j < fieldsLen; j++ {
// 					if fields[j] == nil {
// 						f := strct.Field(j)
// 						name, ok := f.Tag.Lookup("mysql")
// 						if !ok {
// 							name = f.Name
// 						}
// 						kind := f.Type.Kind()

// 						var jsonable bool

// 						switch kind {
// 						case reflect.Map, reflect.Struct:
// 							jsonable = true
// 						case reflect.Array, reflect.Slice:
// 							// if it's a slice, but not a byte slice
// 							if f.Type.Elem().Kind() != reflect.Uint8 {
// 								jsonable = true
// 							}
// 						}

// 						if jsonable {
// 							prop := strctEx.Field(j)

// 							// don't let things that already handle themselves get json unmarshalled
// 							if _, ok := prop.Addr().Interface().(sql.Scanner); ok {
// 								jsonable = false
// 							}

// 							// we also have to ignore times specifically, because sql scanning
// 							// implements them literally, instead of the time.Time implementing sql.Scanner
// 							if _, ok := prop.Interface().(time.Time); ok {
// 								jsonable = false
// 							}
// 						}

// 						fields[j] = &field{
// 							name:     name,
// 							jsonable: jsonable,
// 						}
// 					}
// 					if fields[j].taken {
// 						continue
// 					}

// 					if fields[j].name == c.Name() {
// 						columns[i] = &column{
// 							structIndex:   uint16(j),
// 							jsonable:      fields[j].jsonable,
// 							jsonableIndex: jsonablesCount,
// 						}
// 						fields[j].taken = true

// 						if fields[j].jsonable {
// 							jsonablesCount++
// 						}
// 					}
// 				}
// 			}

// 			var x interface{}

// 			ran := false

// 			var jsonables [][]byte
// 			if jsonablesCount > 0 {
// 				jsonables = make([][]byte, jsonablesCount)
// 			}
// 		Rows:
// 			for rows.Next() {
// 				ran = true

// 				s := reflect.New(strct).Elem()

// 				for i, c := range columns {
// 					if c != nil {
// 						if !c.jsonable {
// 							pointers[i] = s.Field(int(c.structIndex)).Addr().Interface()
// 						} else {
// 							pointers[i] = &jsonables[c.jsonableIndex]
// 						}
// 					} else {
// 						pointers[i] = &x
// 					}
// 				}
// 				err = rows.Scan(pointers...)
// 				if err != nil {
// 					return errors.Wrapf(err, "failed to scan rows")
// 				}

// 				if jsonablesCount > 0 {
// 					for _, c := range columns {
// 						if c == nil || !c.jsonable || jsonables[c.jsonableIndex] == nil {
// 							continue
// 						}

// 						err := json.Unmarshal(jsonables[c.jsonableIndex], s.Field(int(c.structIndex)).Addr().Interface())
// 						if err != nil {
// 							return errors.Wrapf(err, "failed to unmarshal %q", jsonables[c.jsonableIndex])
// 						}
// 					}
// 				}

// 				if cache != 0 {
// 					switch {
// 					case msgpEncodable:
// 						err := s.Addr().Interface().(msgp.Encodable).EncodeMsg(msgpWriter)
// 						if err != nil {
// 							return errors.Wrapf(err, "failed to write struct to cache with msgp")
// 						}
// 					default:
// 						err := gobEncoder.EncodeValue(s)
// 						if err != nil {
// 							return errors.Wrapf(err, "failed to write struct to cache with gob")
// 						}
// 					}
// 				}

// 				switch kind {
// 				case reflect.Chan:
// 					refDest.Send(s)
// 				case reflect.Slice:
// 					refDest.Set(reflect.Append(refDest, s))
// 				case reflect.Struct:
// 					refDest.Set(s)
// 					break Rows
// 				}

// 				x = nil
// 			}

// 			if !ran && kind == reflect.Struct {
// 				return sql.ErrNoRows
// 			}

// 			return nil
// 		}

// 		switch kind {
// 		case reflect.Chan:
// 			if cache == 0 {
// 				go func() {
// 					if err := main(); err != nil {
// 						panic(err)
// 					}
// 				}()
// 			} else {
// 				return main()
// 			}
// 		case reflect.Slice, reflect.Struct:
// 			refDest = refDest.Elem()
// 			return main()
// 		}
// 		return nil
// 	}

// 	cacheGet := func(b []byte) error {
// 		execDuration := time.Since(start)
// 		start = time.Now()
// 		db.callLog("/* cached! */ "+replacedQuery, mergedParams, execDuration)

// 		main := func() error {
// 			if db.Finished != nil {
// 				defer func() { db.Finished(true, replacedQuery, mergedParams, execDuration, time.Since(start)) }()
// 			}

// 			if kind == reflect.Chan {
// 				defer refDest.Close()
// 			}

// 			var msgpReader *msgp.Reader
// 			var gobDecoder *gob.Decoder

// 			switch {
// 			case msgpEncodable:
// 				msgpReader = msgp.NewReader(bytes.NewReader(b))
// 			default:
// 				gobDecoder = gob.NewDecoder(bytes.NewReader(b))
// 			}

// 		Rows:
// 			for {
// 				var s reflect.Value

// 				switch {
// 				case msgpEncodable:
// 					s = reflect.New(strct)
// 					err := s.Interface().(msgp.Decodable).DecodeMsg(msgpReader)
// 					if err != nil && err.Error() == "EOF" {
// 						break Rows
// 					} else if err != nil {
// 						return errors.Wrapf(err, "failed to decode cached struct with msgp")
// 					}
// 					s = s.Elem()
// 				default:
// 					s = reflect.New(strct).Elem()
// 					err := gobDecoder.DecodeValue(s)
// 					if err == io.EOF {
// 						break Rows
// 					} else if err != nil {
// 						return errors.Wrapf(err, "failed to decode cached struct with gob")
// 					}
// 				}

// 				switch kind {
// 				case reflect.Chan:
// 					refDest.Send(s)
// 				case reflect.Slice:
// 					refDest.Set(reflect.Append(refDest, s))
// 				case reflect.Struct:
// 					refDest.Set(s)
// 					break Rows
// 				}
// 			}

// 			return nil
// 		}

// 		switch kind {
// 		case reflect.Chan:
// 			go func() {
// 				if err := main(); err != nil {
// 					panic(err)
// 				}
// 			}()
// 		case reflect.Slice, reflect.Struct:
// 			refDest = refDest.Elem()
// 			return main()
// 		}
// 		return nil
// 	}

// 	if cache != 0 {
// 		if db.redis == nil {
// 			return errors.New("cool-mysql select: cache time given without redis connection for query")
// 		}

// 		main := func() error {
// 			hasher := md5.New()
// 			gob.NewEncoder(hasher).EncodeValue(reflect.New(strct))
// 			hasher.Write([]byte(replacedQuery))
// 			h := base64.RawStdEncoding.EncodeToString(hasher.Sum(nil))

// 			destFilled := false
// 			cache, err, _ := selectSinglelight.Do(h, func() (interface{}, error) {
// 				start = time.Now()
// 				b, err := db.redis.Get(rCtx, h).Bytes()
// 				if err == redis.Nil {
// 					var buf bytes.Buffer
// 					switch {
// 					case msgpEncodable:
// 						msgpWriter = msgp.NewWriter(&buf)
// 					default:
// 						gobEncoder = gob.NewEncoder(&buf)
// 					}

// 					err = liveGet()
// 					if err != nil {
// 						return nil, err
// 					}
// 					destFilled = true

// 					if msgpEncodable {
// 						msgpWriter.Flush()
// 					}

// 					err = db.redis.Set(rCtx, h, buf.Bytes(), cache).Err()
// 					if err != nil {
// 						return nil, errors.Wrapf(err, "cool-mysql select: failed to set query cache to redis")
// 					}

// 					start = time.Now()

// 					return buf.Bytes(), nil
// 				}
// 				if err != nil {
// 					return nil, errors.Wrapf(err, "cool-mysql select: failed to get query cache from redis")
// 				}

// 				return b, nil
// 			})
// 			if err != nil {
// 				return err
// 			}

// 			if !destFilled {
// 				return cacheGet(cache.([]byte))
// 			}

// 			return nil
// 		}

// 		if kind == reflect.Chan {
// 			go func() {
// 				if err := main(); err != nil {
// 					panic(err)
// 				}
// 			}()
// 			return nil
// 		}

// 		return main()
// 	}

// 	// we got this far, so just fill the dest with a normal live get
// 	return liveGet()
// }
