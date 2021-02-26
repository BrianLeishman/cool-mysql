package cool_test

import (
	"database/sql"
	"time"

	mysql "github.com/StirlingMarketingGroup/cool-mysql"
	"github.com/shopspring/decimal"
)

const user = "rfamro"
const pass = ""
const host = "mysql-rfam-public.ebi.ac.uk"
const port = 4497
const schema = "Rfam"

const cacheTime = 1

type genomeRow struct {
	UpID            string
	AssemblyAcc     sql.NullString
	AssemblyVersion sql.NullInt32
	TotalLength     decimal.Decimal
	Created         time.Time
}

var coolDB *mysql.Database

func init() {
	var err error
	coolDB, err = mysql.New(user, pass, schema, host, port,
		user, pass, schema, host, port,
		nil)

	if err != nil {
		panic(err)
	}

	coolDB.EnableRedis("localhost:6379", "", 0)
}
