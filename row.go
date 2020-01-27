package mysql

type Row struct {
	Elements []*Element
	Columns  *Columns
}

// Row represents one row of results.
//
func (r *Row) pointers() []interface{} {
	pointers := make([]interface{}, len(r.Elements))

	for i, element := range r.Elements {
		pointers[i] = element.Pointer()
	}

	return pointers
}

// GetElementByName returns element from the row with for a given column name.
//
func (r *Row) GetElementByName(name string) *Element {
	index, err := r.Columns.ColumnIndex(name)
	if err != nil {
		return nil
	}

	return r.Elements[index]
}
