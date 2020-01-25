package mysql

import (
	"database/sql"
	"time"
)

type Meta struct {
	LastIntertID int64
	RowsAffected int64
	QueryTime    time.Duration
}

func newMeta(r sql.Result) *Meta {
	id, _ := r.LastInsertId()
	aff, _ := r.RowsAffected()

	return &Meta{id, aff, 0}
}
