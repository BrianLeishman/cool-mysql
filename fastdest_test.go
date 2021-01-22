package mysql

import (
	"database/sql"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

type GenomeRow struct {
	UpID            string          `mysql:"upid"`
	AssemblyAcc     string          `mysql:"assembly_acc"`
	AssemblyVersion int             `mysql:"assembly_version"`
	TotalLength     decimal.Decimal `mysql:"total_length"`
	Created         time.Time       `mysql:"created"`
	One             int             `mysql:"1"`
}

// func (z *GenomeRow) CoolMySQLGetColumns(colTypes []*sql.ColumnType) (cols []Column) {
// 	cols = make([]Column, 0, 6)
// 	for _, ct := range colTypes {
// 		switch n := ct.Name(); n {
// 		case "upid", "assembly_acc", "assembly_version", "total_length", "created", "1":
// 			cols = append(cols, Column{
// 				Name:     n,
// 				ScanType: ScanType(ct),
// 			})
// 		}
// 	}

// 	return cols
// }

// func (z *GenomeRow) CoolMySQLRowScan(cols []Column, ptrs []interface{}) error {
// 	i := 0

// 	for _, c := range cols {
// 		src := []byte(*(ptrs[i].(*sql.RawBytes)))
// 		i++

// 		switch c.Name {
// 		case "upid":
// 			if len(src) == 0 {
// 				break
// 			}
// 			z.UpID = string(src)
// 		case "assembly_acc":
// 			err := ScanInto(c, &z.AssemblyAcc, src)
// 			if err != nil {
// 				return errors.Wrap(err, "failed to scan 'assembly_acc'")
// 			}
// 		case "assembly_version":
// 			err := ScanInto(c, &z.AssemblyVersion, src)
// 			if err != nil {
// 				return errors.Wrap(err, "failed to scan 'assembly_version'")
// 			}
// 		case "total_length":
// 			err := ScanInto(c, &z.TotalLength, src)
// 			if err != nil {
// 				return errors.Wrap(err, "failed to scan 'total_length'")
// 			}
// 		case "created":
// 			if len(src) == 0 {
// 				break
// 			}
// 			t, err := time.Parse(time.RFC3339Nano, string(src))
// 			if err != nil {
// 				return errors.Wrap(err, "failed to scan 'created'")
// 			}
// 			z.Created = t
// 		case "1":
// 			if len(src) == 0 {
// 				break
// 			}
// 			n, err := strconv.ParseInt(string(src), 10, 64)
// 			if err != nil {
// 				return errors.Wrap(err, "failed to scan '1'")
// 			}
// 			z.One = int(n)
// 		}
// 	}

// 	return nil
// }

// func (z *GenomeRow) CoolMySQLSendChan(done <-chan struct{}, ch interface{}, e interface{}) error {
// 	select {
// 	case <-done:
// 		return context.Canceled
// 	case ch.(chan GenomeRow) <- e.(GenomeRow):
// 	}

// 	return nil
// }

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
		genomeCh = make(chan GenomeRow)
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

type genomeUpID string

func (z *genomeUpID) CoolMySQLGetColumns(colTypes []*sql.ColumnType) (cols []Column) {
	cols = make([]Column, 0, 1)
	for _, ct := range colTypes {
		cols = append(cols, Column{
			Name:     ct.Name(),
			ScanType: ScanType(ct),
		})
		break
	}

	return cols
}

func (z *genomeUpID) CoolMySQLRowScan(cols []Column, ptrs []interface{}) error {
	for i := range cols {
		src := []byte(*(ptrs[i].(*sql.RawBytes)))
		if len(src) == 0 {
			return nil
		}

		*z = genomeUpID(src)

		break
	}

	return nil
}

func Benchmark_Genome_CoolFastDest_Select_String_NotCached(b *testing.B) {
	b.ReportAllocs()

	var upID uint8
	for n := 0; n < b.N; n++ {
		err := coolDB.Select(&upID, "select 1,`upid`,`assembly_acc`,`assembly_version`,`created`,1 from`genome`limit 1000", 0)
		if err != nil {
			panic(err)
		}
		if upID == 0 {
			b.Fatal("didn't get an `upid`!")
		}
	}
}
