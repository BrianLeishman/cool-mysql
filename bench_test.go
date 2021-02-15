package mysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	mysql "github.com/StirlingMarketingGroup/cool-mysql"
	"github.com/davecgh/go-spew/spew"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
)

func Benchmark_Genome_Cool_Select_Chan_NotCached(b *testing.B) {
	b.ReportAllocs()

	type genomeRow struct {
		UpID            string          `mysql:"upid"`
		AssemblyAcc     sql.NullString  `mysql:"assembly_acc"`
		AssemblyVersion sql.NullInt32   `mysql:"assembly_version"`
		TotalLength     decimal.Decimal `mysql:"total_length"`
		Created         time.Time       `mysql:"created"`
		One             int             `mysql:"1"`
	}

	for n := 0; n < b.N; n++ {
		genomes := make(chan genomeRow)
		var i int
		err := coolDB.Select(genomes, "select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`,1 from`genome` limit 1000", 0)
		if err != nil {
			panic(err)
		}
		for r := range genomes {
			i += r.One
		}
		if i != 1000 {
			b.Fatal("didn't get 1k rows!")
		}
	}
}

func Benchmark_Genome_Cool_Select_Slice_NotCached(b *testing.B) {
	b.ReportAllocs()

	type genomeRow struct {
		UpID            string          `mysql:"upid"`
		AssemblyAcc     sql.NullString  `mysql:"assembly_acc"`
		AssemblyVersion sql.NullInt32   `mysql:"assembly_version"`
		TotalLength     decimal.Decimal `mysql:"total_length"`
		Created         time.Time       `mysql:"created"`
		One             int             `mysql:"1"`
	}

	var genomes []genomeRow
	for n := 0; n < b.N; n++ {
		var i int
		genomes = genomes[:0]
		err := coolDB.Select(&genomes, "select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`,1 from`genome` limit 1000", 0)
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

func Benchmark_Ints_Cool_Select_Slice_NotCached(b *testing.B) {
	b.ReportAllocs()

	var ints []int
	for n := 0; n < b.N; n++ {
		err := coolDB.Select(&ints, "select 69", 0)
		if err != nil {
			panic(err)
		}
		if ints[0] != 69 {
			b.Fatal("didn't get the 69!")
		}
	}
}

func Benchmark_Genome_Cool_Select_Struct_NotCached(b *testing.B) {
	db, err := mysql.New(user, pass, schema, host, port,
		user, pass, schema, host, port,
		nil)
	if err != nil {
		panic(err)
	}

	type genomeRow struct {
		UpID            string          `mysql:"upid"`
		AssemblyAcc     sql.NullString  `mysql:"assembly_acc"`
		AssemblyVersion sql.NullInt32   `mysql:"assembly_version"`
		TotalLength     decimal.Decimal `mysql:"total_length"`
		Created         time.Time       `mysql:"created"`
	}

	var genome genomeRow
	for n := 0; n < b.N; n++ {
		err := db.Select(&genome, "select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`from`genome`where`total_length`>@@TotalLength limit 1", 0, mysql.Params{
			"TotalLength": 28111,
		})
		if err != nil {
			panic(err)
		}
	}

	f, err := ioutil.TempFile("", "prefix")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(f.Name())
	spew.Fdump(f, len(genome.UpID))
}

func Benchmark_Genome_MySQL_Select_NotCached(b *testing.B) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=true",
		user,
		pass,
		host,
		port,
		schema,
	))
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	type genomeRow struct {
		UpID            string
		AssemblyAcc     sql.NullString
		AssemblyVersion sql.NullInt32
		TotalLength     decimal.Decimal
		Created         time.Time
	}

	for n := 0; n < b.N; n++ {
		var i int
		rows, err := db.Query("select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`from`genome`where`total_length`>? limit 1000", 28111)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var genome genomeRow
			err = rows.Scan(
				&genome.UpID,
				&genome.AssemblyAcc,
				&genome.AssemblyVersion,
				&genome.TotalLength,
				&genome.Created,
			)
			if genome.AssemblyAcc.Valid || genome.AssemblyAcc.String == "" {
				i++
			}
		}
		if i != 1000 {
			b.Fatal("didn't get 1k rows!")
		}
	}
}

var sqlxDB *sqlx.DB

func init() {
	var err error
	sqlxDB, err = sqlx.Connect("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=true",
		user,
		pass,
		host,
		port,
		schema,
	))
	if err != nil {
		panic(err)
	}
}

func Benchmark_Genome_SQLx_Select_Slice_NotCached(b *testing.B) {
	b.ReportAllocs()

	type genomeRow struct {
		UpID            string          `db:"upid"`
		AssemblyAcc     sql.NullString  `db:"assembly_acc"`
		AssemblyVersion sql.NullInt32   `db:"assembly_version"`
		TotalLength     decimal.Decimal `db:"total_length"`
		Created         time.Time       `db:"created"`
		One             int             `db:"1"`
	}

	var genomes []genomeRow
	for n := 0; n < b.N; n++ {
		var i int
		genomes = genomes[:0]
		err := sqlxDB.Select(&genomes, "select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`,1 from`genome` limit 1000")
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

func Benchmark_Ints_SQLx_Select_Slice_NotCached(b *testing.B) {
	b.ReportAllocs()

	var ints []int
	for n := 0; n < b.N; n++ {
		err := sqlxDB.Select(&ints, "select 69")
		if err != nil {
			panic(err)
		}
		if ints[0] != 69 {
			b.Fatal("didn't get the 69!")
		}
	}
}

func Benchmark_Genome_Cool_Select_Chan_IsCached(b *testing.B) {
	db, err := mysql.New(user, pass, schema, host, port,
		user, pass, schema, host, port,
		nil)
	if err != nil {
		panic(err)
	}

	db.EnableRedis("localhost:6379", "", 0)

	type genomeRow struct {
		UpID            string          `mysql:"upid"`
		AssemblyAcc     sql.NullString  `mysql:"assembly_acc"`
		AssemblyVersion sql.NullInt32   `mysql:"assembly_version"`
		TotalLength     decimal.Decimal `mysql:"total_length"`
		Created         time.Time       `mysql:"created"`
	}

	var genomeCh chan genomeRow
	for n := 0; n < b.N; n++ {
		genomeCh = make(chan genomeRow)
		err := db.Select(genomeCh, "select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`from`genome`where`total_length`>@@TotalLength limit 1000", 10*time.Second, mysql.Params{
			"TotalLength": 28111,
		})
		if err != nil {
			panic(err)
		}
	}
}

type GenomeRow struct {
	UpID            string          `mysql:"upid"`
	AssemblyAcc     string          `mysql:"assembly_acc"`
	AssemblyVersion sql.NullInt64   `mysql:"assembly_version"`
	TotalLength     decimal.Decimal `mysql:"total_length"`
	Created         time.Time       `mysql:"created"`
	One             int             `mysql:"1"`
}

func (z *GenomeRow) CoolMySQLGetColumns(colTypes []*sql.ColumnType) (cols []mysql.Column) {
	cols = make([]mysql.Column, 0, 6)
	for _, ct := range colTypes {
		switch n := ct.Name(); n {
		case "upid", "assembly_acc", "assembly_version", "total_length", "created", "1":
			cols = append(cols, mysql.Column{
				Name:     n,
				ScanType: mysql.ScanType(ct),
			})
		}
	}

	return cols
}

func (z *GenomeRow) CoolMySQLRowScan(cols []mysql.Column, ptrs []interface{}) error {
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
			if len(src) == 0 {
				break
			}
			z.AssemblyAcc = string(src)
		case "assembly_version":
			err := mysql.ScanInto(c, &z.AssemblyVersion, src)
			if err != nil {
				return errors.Wrap(err, "failed to scan 'assembly_version'")
			}
		case "total_length":
			err := mysql.ScanInto(c, &z.TotalLength, src)
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

func (z *GenomeRow) CoolMySQLSendChan(done <-chan struct{}, ch interface{}, e interface{}) error {
	select {
	case <-done:
		return context.Canceled
	case ch.(chan GenomeRow) <- e.(GenomeRow):
	}

	return nil
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

func (z *genomeUpID) CoolMySQLGetColumns(colTypes []*sql.ColumnType) (cols []mysql.Column) {
	cols = make([]mysql.Column, 0, 1)
	for _, ct := range colTypes {
		cols = append(cols, mysql.Column{
			Name:     ct.Name(),
			ScanType: mysql.ScanType(ct),
		})
		break
	}

	return cols
}

func (z *genomeUpID) CoolMySQLRowScan(cols []mysql.Column, ptrs []interface{}) error {
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

func Benchmark_Cool_Select_JSON_Chan_NotCached(b *testing.B) {
	db, err := mysql.New(user, pass, schema, host, port,
		user, pass, schema, host, port,
		nil)
	if err != nil {
		panic(err)
	}

	type testRow struct {
		Ints    []int
		Strings []string
		Map     map[string][]string
		Bytes   []byte
		Base64  [][]byte
	}

	var testCh chan testRow
	for n := 0; n < b.N; n++ {
		testCh = make(chan testRow)
		err := db.Select(testCh, "select'[1,2,3]'`Ints`,'[\"Swick\",\"Yeet\",\"swagswag\"]'`Strings`,"+
			"'{\"im a key\":[\"im a value\"]}'`Map`,random_bytes(8)`Bytes`,concat('[\"',to_base64(random_bytes(8)),'\"]')`Base64`,"+
			"'{\"FkXsNQIckkI\":[\"FkXsOgqbc_I\",\"FkXsPGWegGI\"]}'`Bid2s`", 0)
		if err != nil {
			panic(err)
		}
		for range testCh {
			// swag
		}
	}
}
