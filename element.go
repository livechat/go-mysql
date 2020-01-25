package mysql

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
)

const (
	elementInt = iota
	elementString
	elementFloat
	elementTime
)

type Element struct {
	Type        int
	NullString  *sql.NullString
	NullInt64   *sql.NullInt64
	NullFloat64 *sql.NullFloat64
	NullTime    *mysql.NullTime
}

// Pointer returns a value if the element.
//
func (e *Element) Value() interface{} {
	switch e.Type {
	case elementInt:
		return e.NullInt64.Int64
	case elementString:
		return e.NullString.String
	case elementFloat:
		return e.NullFloat64.Float64
	case elementTime:
		return e.NullTime.Time
	}

	return nil
}

// IsNull returns true if element is null.
//
func (e *Element) IsNull() bool {
	switch e.Type {
	case elementInt:
		return e.NullInt64.Valid == false
	case elementString:
		return e.NullString.Valid == false
	case elementFloat:
		return e.NullFloat64.Valid == false
	case elementTime:
		return e.NullTime.Valid == false
	}

	return true
}

// Pointer returns a pointer to the element.
//
func (e *Element) Pointer() interface{} {
	switch e.Type {
	case elementInt:
		return e.NullInt64
	case elementString:
		return e.NullString
	case elementFloat:
		return e.NullFloat64
	case elementTime:
		return e.NullTime
	}

	return nil
}
