package cool

import (
	"context"
	"database/sql"
	"reflect"
	"time"
)

// Tx is a cool MySQL transaction
type Tx struct {
	Database *Database
	*sql.Tx
}

// Select stores the query results in the destination
func (tx *Tx) Select(dest interface{}, query string, cache time.Duration, params ...Params) error {
	return coolSelect(context.Background(), tx.Database, tx, reflect.ValueOf(dest), query, cache, params...)
}

// BeginWritesTx begins and returns a new transaction on the writes connection
func (db *Database) BeginWritesTx(ctx context.Context) (*Tx, error) {
	tx, err := db.Writes.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &Tx{
		Database: db,
		Tx:       tx,
	}, nil
}

// BeginReadsTx begins and returns a new transaction on the reads connection
func (db *Database) BeginReadsTx(ctx context.Context) (*Tx, error) {
	tx, err := db.Reads.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &Tx{
		Database: db,
		Tx:       tx,
	}, nil
}
