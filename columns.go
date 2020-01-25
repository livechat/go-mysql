package mysql

import (
	"database/sql"
	"errors"

	"github.com/go-sql-driver/mysql"
)

type Columns struct {
	columnTypes []*sql.ColumnType
	names       []string
	indexes     map[string]int
}

func newColumns(columnTypes []*sql.ColumnType) *Columns {
	return &Columns{columnTypes: columnTypes}
}

func (c *Columns) ColumnNames() []string {
	if c.names != nil {
		return c.names
	}

	c.names = make([]string, len(c.columnTypes))

	for i, column := range c.columnTypes {
		c.names[i] = column.Name()
	}

	return c.names
}

func (c *Columns) ColumnIndex(name string) (int, error) {
	if c.indexes == nil {
		c.indexes = make(map[string]int, len(c.columnTypes))

		for name, index := range c.ColumnNames() {
			c.indexes[index] = name
		}
	}

	if index, found := c.indexes[name]; found {
		return index, nil
	}

	return 0, errors.New("column name not found")
}

func (c *Columns) newRow() (*Row, error) {
	row := &Row{
		Elements: make([]*Element, len(c.columnTypes)),
		Columns:  c,
	}

	for i, column := range c.columnTypes {
		colType := column.ScanType()
		row.Elements[i] = &Element{}

		switch colType.String() {
		case "uint", "uint8", "uint16", "uint32", "uint64":
			fallthrough

		case "int", "int8", "int16", "int32", "int64", "sql.NullInt64":
			row.Elements[i].Type = elementInt
			row.Elements[i].NullInt64 = &sql.NullInt64{}

		case "string", "sql.RawBytes":
			row.Elements[i].Type = elementString
			row.Elements[i].NullString = &sql.NullString{}

		case "mysql.NullTime":
			row.Elements[i].Type = elementTime
			row.Elements[i].NullTime = &mysql.NullTime{}

		case "float32", "float64":

			row.Elements[i].Type = elementFloat
			row.Elements[i].NullFloat64 = &sql.NullFloat64{}

		default:
			return nil, errors.New("unhandled type" + colType.String())
		}
	}

	return row, nil
}
