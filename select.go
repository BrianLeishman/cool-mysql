package cool

import (
	"context"
	"reflect"
	"time"

	"golang.org/x/sync/singleflight"
)

// Select stores the query results in the destination
func (db *Database) Select(dest interface{}, query string, cache time.Duration, params ...Params) error {
	return coolSelect(context.Background(), db, db.Reads, reflect.ValueOf(dest), query, cache, params...)
}

var selectSingleflight = new(singleflight.Group)

func coolSelect(ctx context.Context, db *Database, conn Queryer, dest reflect.Value, query string, cache time.Duration, params ...Params) error {

	return nil
}
