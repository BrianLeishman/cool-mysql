package cool

import (
	"database/sql"
	"net"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-sql-driver/mysql"
)

// Database is a cool MySQL connection
type Database struct {
	Writes    *sql.DB
	writesDSN string

	Reads    *sql.DB
	readsDSN string

	Complete CompleteFunc

	die bool

	maxInsertSize int

	redis *redis.Client
}

// Clone returns a copy of the Database with the same connections
func (db *Database) Clone() *Database {
	clone := *db
	return &clone
}

// EnableRedis enables redis cache for select queries with cache times
// with the given connection information
func (db *Database) EnableRedis(address string, password string, redisDB int) {
	db.redis = redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password, // no password set
		DB:       redisDB,  // use default DB
	})
}

// CompleteFunc executes right before a Database method call return
type CompleteFunc func(cached bool, replacedQuery string, mergedParams Params, execDuration time.Duration, fetchDuration time.Duration)

// New creates a new Database
func New(wUser, wPass, wSchema, wHost string, wPort int,
	rUser, rPass, rSchema, rHost string, rPort int,
	timeZone *time.Location) (db *Database, err error) {
	writes := mysql.NewConfig()
	writes.User = wUser
	writes.Passwd = wPass
	writes.DBName = wSchema
	writes.Net = "tcp"
	writes.Addr = net.JoinHostPort(wHost, strconv.Itoa(wPort))
	writes.Loc = timeZone
	writes.ParseTime = true
	writes.InterpolateParams = true

	reads := mysql.NewConfig()
	reads.User = rUser
	reads.Passwd = rPass
	reads.DBName = rSchema
	reads.Net = "tcp"
	reads.Addr = net.JoinHostPort(rHost, strconv.Itoa(rPort))
	reads.Loc = timeZone
	reads.ParseTime = true
	reads.InterpolateParams = true

	return NewFromDSN(writes.FormatDSN(), reads.FormatDSN())
}

// Connect sets the writes and reads connections for the db
func (db *Database) Connect() (err error) {
	db.Writes, err = sql.Open("mysql", db.writesDSN)
	if err != nil {
		return err
	}

	err = db.Writes.Ping()
	if err != nil {
		return err
	}

	writesDSN, _ := mysql.ParseDSN(db.writesDSN)
	db.maxInsertSize = writesDSN.MaxAllowedPacket

	if db.readsDSN != db.writesDSN {
		db.Reads, err = sql.Open("mysql", db.readsDSN)
		if err != nil {
			return err
		}

		err = db.Reads.Ping()
		if err != nil {
			return err
		}
	} else {
		db.Reads = db.Writes
	}

	return nil
}

// Reconnect sets the writes and reads connections for the db
// alias of Connect
func (db *Database) Reconnect() (err error) {
	return db.Connect()
}

// NewFromDSN creates a new Database from config
// DSN strings for both connections
func NewFromDSN(writes, reads string) (db *Database, err error) {
	db = new(Database)

	db.writesDSN = writes
	db.readsDSN = reads

	err = db.Connect()
	if err != nil {
		return nil, err
	}

	return
}
