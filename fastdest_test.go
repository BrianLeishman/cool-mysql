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

func (z *GenomeRow) CoolMySQLRowSerialize(b *[]byte, cols []*sql.ColumnType, ptrs []interface{}) {
	totalLen := 0
	for i, c := range cols {
		switch c.Name() {
		case "upid", "assembly_acc", "assembly_version", "total_length", "created", "1":
			rb := ptrs[i].(*sql.RawBytes)
			if *rb == nil {
				totalLen++
			} else {
				totalLen += 1 + 8 + len(*rb)
			}
		}
	}

	if cap(*b)-len(*b) < totalLen {
		tmp := make([]byte, len(*b), 2*cap(*b)+totalLen)
		copy(tmp, *b)
		*b = tmp
	}
	for i, c := range cols {
		switch c.Name() {
		case "upid", "assembly_acc", "assembly_version", "total_length", "created", "1":
			rb := ptrs[i].(*sql.RawBytes)
			if *rb == nil {
				*b = append(*b, 1)
			} else {
				*b = append(*b, 0)
				*b = (*b)[:len(*b)+8]
				binary.LittleEndian.PutUint64((*b)[len(*b)-8:], uint64(len(*rb)))
				*b = append(*b, *rb...)
			}
		}
	}
}

func (z *GenomeRow) CoolMySQLRowDeserialize(b *[]byte) (ptrs []interface{}, err error) {
	ptrs = make([]interface{}, 6) // number for exported fields

	if len(*b) == 0 {
		return nil, io.EOF
	}
	offset := 0

	for i := range ptrs {
		var src []byte
		null := (*b)[offset] == 1
		offset++
		if !null {
			size := int(binary.LittleEndian.Uint64((*b)[offset:]))
			offset += 8
			src = (*b)[offset : offset+size]
			offset += size
		}

		rb := sql.RawBytes(src)
		ptrs[i] = &rb
		i++
	}

	(*b) = (*b)[offset:]

	return ptrs, nil
}

func (z *GenomeRow) CoolMySQLRowScan(ptrs []interface{}) error {
	i := 0
Cols:
	for _, c := range cols {
		switch c.Name() {
		case "upid", "assembly_acc", "assembly_version", "total_length", "created", "1":
		default:
			continue Cols
		}

		src := []byte(*(ptrs[i].(*sql.RawBytes)))
		i++

		switch c.Name() {
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

var coolDB, _ = New(user, pass, schema, host, port,
	user, pass, schema, host, port,
	nil)

func init() {
	coolDB.EnableRedis("localhost:6379", "", 0)
}

func Benchmark_Genome_CoolFastDest_Select_Chan_NotCached(b *testing.B) {
	b.ReportAllocs()

	var genomeCh chan GenomeRow
	for n := 0; n < b.N; n++ {
		var i int
		genomeCh = make(chan GenomeRow)
		err := coolDB.Select(genomeCh, "select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`,1,sleep(1) from`genome`limit 1000", 0)
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
		err := coolDB.Select(&genomes, "select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`,1,sleep(1) from`genome`limit 1000", 0)
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
