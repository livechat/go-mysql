package mysql

import (
	"database/sql"
	"time"
)

// internal stats struct
type stats struct {
	inProgressQueries   *int64
	totalSuccessQueries *int64
	totalFailedQueries  *int64
	sampleChan          chan *QueryStats
}

func (s *stats) sample(sample *QueryStats) {
	select {
	case s.sampleChan <- sample:
	default:
	}
}

func newStats() *stats {
	return &stats{
		inProgressQueries:   new(int64),
		totalSuccessQueries: new(int64),
		totalFailedQueries:  new(int64),
		sampleChan:          make(chan *QueryStats, 100),
	}
}

type Stats struct {
	sql.DBStats
	InProgressQueries   int64
	TotalSuccessQueries int64
	TotalFailedQueries  int64
}

type QueryStats struct {
	Query           string        // measured query
	ExecutionTime   time.Duration // time of query executoion including with db roudntrip
	QueueTime       time.Duration // time spent in queue waiting for connection
	TransactionTime time.Duration // total transaction time (returned for commit and rollback queries)
}
