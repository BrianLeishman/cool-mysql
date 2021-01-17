package mysql

import (
	"database/sql"
	"encoding/binary"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
)

type GenomeRow struct {
	UpID            string          `mysql:"upid"`
	AssemblyAcc     sql.NullString  `mysql:"assembly_acc"`
	AssemblyVersion sql.NullInt32   `mysql:"assembly_version"`
	TotalLength     decimal.Decimal `mysql:"total_length"`
	Created         time.Time       `mysql:"created"`
	One             int             `mysql:"1"`
}

func (z *GenomeRow) CoolMySQLExportedColCount() int {
	return 6
}

func (z *GenomeRow) CoolMySQLGetColumns(colTypes []*sql.ColumnType) (cols []Column) {
	cols = make([]Column, 0, z.CoolMySQLExportedColCount())
	for _, ct := range colTypes {
		switch n := ct.Name(); n {
		case "upid", "assembly_acc", "assembly_version", "total_length", "created", "1":
			cols = append(cols, Column{
				Name:     n,
				ScanType: ScanType(ct),
			})
		}
	}

	// spew.Dump(cols)
	// os.Exit(0)

	return cols
}

func (z *GenomeRow) CoolMySQLColumnsSerialize(buf *[]byte, cols []Column) {
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

func (z *GenomeRow) CoolMySQLRowSerialize(buf *[]byte, cols []Column, ptrs []interface{}) {
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

func (z *GenomeRow) CoolMySQLColumnsDeserialize(buf *[]byte) (cols []Column, err error) {
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

func (z *GenomeRow) CoolMySQLRowDeserialize(buf *[]byte) (ptrs []interface{}, err error) {
	ptrs = make([]interface{}, z.CoolMySQLExportedColCount())

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

func (z *GenomeRow) CoolMySQLRowScan(cols []Column, ptrs []interface{}) error {
	i := 0

	for _, c := range cols {
		src := []byte(*(ptrs[i].(*sql.RawBytes)))
		i++

		switch c.Name {
		case "upid":
			if len(src) == 0 {
				break
			}
			z.UpID = string(src)
		case "assembly_acc":
			err := ScanInto(c, &z.AssemblyAcc, src)
			if err != nil {
				return errors.Wrap(err, "failed to scan 'assembly_acc'")
			}
		case "assembly_version":
			err := ScanInto(c, &z.AssemblyVersion, src)
			if err != nil {
				return errors.Wrap(err, "failed to scan 'assembly_version'")
			}
		case "total_length":
			err := ScanInto(c, &z.TotalLength, src)
			if err != nil {
				return errors.Wrap(err, "failed to scan 'total_length'")
			}
		case "created":
			if len(src) == 0 {
				break
			}
			t, err := time.Parse(time.RFC3339Nano, string(src))
			if err != nil {
				return errors.Wrap(err, "failed to scan 'created'")
			}
			z.Created = t
		case "1":
			if len(src) == 0 {
				break
			}
			n, err := strconv.ParseInt(string(src), 10, 64)
			if err != nil {
				return errors.Wrap(err, "failed to scan '1'")
			}
			z.One = int(n)
		}
	}

	return nil
}

var coolDB *Database

func init() {
	var err error
	coolDB, err = New(user, pass, schema, host, port,
		user, pass, schema, host, port,
		nil)

	if err != nil {
		panic(err)
	}

	coolDB.EnableRedis("localhost:6379", "", 0)
}

func Benchmark_Genome_CoolFastDest_Select_Chan_NotCached(b *testing.B) {
	b.ReportAllocs()

	var genomeCh chan GenomeRow
	for n := 0; n < b.N; n++ {
		var i int
		genomeCh = make(chan GenomeRow, 100)
		err := coolDB.Select(genomeCh, "select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`,1 from`genome`limit 1000", 0)
		if err != nil {
			panic(err)
		}
		for r := range genomeCh {
			i += r.One
		}
		if i != 1000 {
			b.Fatal("didn't get 1k rows!")
		}
	}
}

func Benchmark_Genome_CoolFastDest_Select_Slice_NotCached(b *testing.B) {
	b.ReportAllocs()

	var genomes []GenomeRow
	for n := 0; n < b.N; n++ {
		var i int
		genomes = genomes[:0]
		err := coolDB.Select(&genomes, "select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`,1 from`genome`limit 1000", 0)
		if err != nil {
			panic(err)
		}
		for _, r := range genomes {
			i += r.One
		}
		if i != 1000 {
			b.Fatal("didn't get 1k rows!")
		}
	}
}
