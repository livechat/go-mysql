package mysql

import (
	"database/sql"
	"errors"
	"reflect"
	"time"
)

func newResultsFromSqlRows(rows *sql.Rows) (*Results, error) {
	results := &Results{}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	results.Columns = newColumns(columnTypes)

	for rows.Next() {
		row, err := results.Columns.newRow()
		if err != nil {
			return nil, err
		}

		pointers := row.pointers()
		rows.Scan(pointers...)

		results.Rows = append(results.Rows, row)
	}

	return results, nil
}

type Results struct {
	Columns   *Columns
	Rows      []*Row
	QueryTime time.Duration
}

// Count returns number of rows in results.
func (r *Results) Count() int {
	if r.Rows != nil {
		return len(r.Rows)
	}

	return 0
}

func conversionError(from string, to reflect.Value) error {
	return errors.New("sql conversion from " + from + ": wrong destination field type: " + to.Kind().String() + " " + to.Type().String())
}

/*
CastTo casts results to a given type. The type should be a pointer to the array of pointers.
Slice is allocated in the function and the length is the query results lenght.
Field tags must match column names.

   type Foo struct {
	   Size int `mysql:"size"`
	   Name string `mysql:"name"`
   }

   res, err := client.Query(ctx, "SELECT * FROM `foo`;")
   // handle err

   var f []*Foo

   res.ParseResults(&f)
*/
func (r *Results) CastTo(dst interface{}) error {
	arr := reflect.ValueOf(dst)

	if reflect.TypeOf(dst).Kind() == reflect.Ptr {
		arrType := reflect.TypeOf(dst).Elem()
		slice := reflect.MakeSlice(arrType, r.Count(), r.Count())
		arr = arr.Elem()
		arr.Set(slice)
	}

	for i := 0; i < arr.Len(); i++ {
		obj := reflect.New(arr.Index(i).Type().Elem())

		for j := 0; j < obj.Elem().Type().NumField(); j++ {
			tag := obj.Elem().Type().Field(j).Tag.Get("mysql")

			if element := r.Rows[i].GetElementByName(tag); element != nil {
				field := obj.Elem().Field(j)

				if element.IsNull() == false {
					if field.Kind() == reflect.Ptr && field.IsNil() {
						v := reflect.New(field.Type().Elem())
						field.Set(v)
						field = field.Elem()
					}

					switch element.Type {
					case elementString:
						switch field.Kind() {
						case reflect.String:
							field.SetString(element.NullString.String)
						default:
							return conversionError("string", field)
						}

					case elementTime:
						switch field.Kind() {
						case reflect.String:
							field.SetString(element.NullTime.Time.String())
						case reflect.Struct:
							if reflect.TypeOf(element.NullTime.Time).AssignableTo(field.Type()) {
								field.Set(reflect.ValueOf(element.NullTime.Time))
								continue
							}
							fallthrough

						default:
							return conversionError("time", field)
						}

					case elementFloat:
						return errors.New("not implemented")

					case elementInt:
						switch field.Kind() {
						case reflect.Bool:
							if element.NullInt64.Int64 > 0 {
								field.SetBool(true)
							} else {
								field.SetBool(false)
							}
						case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
							field.SetInt(element.NullInt64.Int64)
						case reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Uint:
							field.SetUint(uint64(element.NullInt64.Int64))
						case reflect.Struct:
							ts := time.Unix(element.NullInt64.Int64, 0)
							if reflect.TypeOf(ts).AssignableTo(field.Type()) {
								field.Set(reflect.ValueOf(ts))
								continue
							}

							fallthrough
						default:
							return conversionError("int", field)
						}
					}
				}
			}
		}

		arr.Index(i).Set(obj)
	}

	return nil
}
