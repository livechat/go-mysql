package mysql

import "time"

type MultiResults struct {
	Results   []*Results
	QueryTime time.Duration
}

// Count returns a number of multiple statements.
//
func (c *MultiResults) Count() int {
	if c.Results != nil {
		return len(c.Results)
	}

	return 0
}
