package mysql

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"

	"time"

	"github.com/opentracing/opentracing-go"
)

type Transaction struct {
	tx *sql.Tx
	s  *stats

	config    *Config
	r         *relay
	done      []chan error
	mu        sync.RWMutex
	startedAt time.Time
	t         opentracing.Tracer
	span      opentracing.Span
}

func newTransaction(tx *sql.Tx, s *stats, config *Config, r *relay, t opentracing.Tracer, span opentracing.Span) *Transaction {
	return &Transaction{tx, s, config, r, make([]chan error, 0), sync.RWMutex{}, time.Now(), t, span}
}

func (t *Transaction) Call(ctx context.Context, procedure string, args ...interface{}) (*Results, error) {
	query := call(procedure, len(args))
	return t.Query(ctx, query, args...)
}

// Done returns a chanel. The chanel blocks until the transaction is over.
// When transaction is over, chanel returns nil for success or error when transaction failed.
//
func (t *Transaction) Done() chan error {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := make(chan error)
	t.done = append(t.done, c)
	return c
}

func (t *Transaction) Query(ctx context.Context, query string, args ...interface{}) (*Results, error) {
	var (
		err    error
		rows   *sql.Rows
		cancel func()
	)

	span := t.t.StartSpan("QUERY", opentracing.ChildOf(t.span.Context()))
	span.SetTag("db.type", "mysql")
	span.SetTag("db.statement", query)

	defer func(err *error) {
		if *err == nil {
			atomic.AddInt64(t.s.totalSuccessQueries, 1)
		} else {
			atomic.AddInt64(t.s.totalFailedQueries, 1)
			logger.FromCtx(ctx).Tag("mysql").Error(*err, query)
			span.SetTag("error", true)
		}
		span.Finish()
	}(&err)

	start := time.Now()
	if t.config.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, t.config.Timeout)
		defer cancel()
	}

	rows, err = t.tx.QueryContext(ctx, query, args...)
	queryTime := time.Now().Sub(start)
	t.s.sample(&QueryStats{query, queryTime, 0, 0})

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	results, err := newResultsFromSqlRows(rows)
	if err != nil {
		return nil, err
	}

	results.QueryTime = queryTime
	logger.FromCtx(ctx).Tag("mysql").Debug(query, queryTime, 0, 0)
	return results, nil
}

func (t *Transaction) MultiQuery(ctx context.Context, query string, args ...interface{}) (*MultiResults, error) {
	var (
		err    error
		rows   *sql.Rows
		cancel func()
	)

	defer func(err *error) {
		if *err == nil {
			atomic.AddInt64(t.s.totalSuccessQueries, 1)
		} else {
			atomic.AddInt64(t.s.totalFailedQueries, 1)
			logger.FromCtx(ctx).Tag("mysql").Error(*err, query)
		}

	}(&err)

	start := time.Now()
	if t.config.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, t.config.Timeout)
		defer cancel()
	}

	rows, err = t.tx.QueryContext(ctx, query, args...)
	queryTime := time.Now().Sub(start)
	t.s.sample(&QueryStats{query, queryTime, 0, 0})

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	multiResults := &MultiResults{
		Results:   []*Results{},
		QueryTime: queryTime,
	}

	for {
		results, err := newResultsFromSqlRows(rows)
		if err != nil {
			return nil, err
		}
		results.QueryTime = queryTime
		multiResults.Results = append(multiResults.Results, results)

		if rows.NextResultSet() == false {
			break
		}
	}

	logger.FromCtx(ctx).Tag("mysql").Debug(query, queryTime, 0, 0)
	return multiResults, nil
}

func (t *Transaction) Exec(ctx context.Context, query string, args ...interface{}) (*Meta, error) {
	var (
		err    error
		result sql.Result
		cancel func()
	)
	span := t.t.StartSpan("EXEC", opentracing.ChildOf(t.span.Context()))
	span.SetTag("db.type", "mysql")
	span.SetTag("db.statement", query)

	defer func(err *error) {
		if *err == nil {
			atomic.AddInt64(t.s.totalSuccessQueries, 1)
		} else {
			atomic.AddInt64(t.s.totalFailedQueries, 1)
			logger.FromCtx(ctx).Tag("mysql").Error(*err, query)
			span.SetTag("error", true)
		}
		span.Finish()
	}(&err)

	start := time.Now()
	if t.config.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, t.config.Timeout)
		defer cancel()
	}

	result, err = t.tx.ExecContext(ctx, query, args...)
	queryTime := time.Now().Sub(start)
	t.s.sample(&QueryStats{query, queryTime, 0, 0})

	if err != nil {
		return nil, err
	}

	meta := newMeta(result)
	meta.QueryTime = queryTime
	t.s.sample(&QueryStats{query, queryTime, 0, 0})

	logger.FromCtx(ctx).Tag("mysql").Debug(query, meta.QueryTime)
	return meta, nil
}

func (t *Transaction) Commit(ctx context.Context) error {
	if _, ok := ctx.Value("tx").(*Transaction); ok {
		return nil
	}
	span := t.t.StartSpan("COMMIT", opentracing.ChildOf(t.span.Context()))
	span.SetTag("db.type", "mysql")
	span.SetTag("db.statement", "COMMIT")

	defer atomic.AddInt64(t.s.inProgressQueries, -1)
	defer t.r.end()

	var err error

	defer func(err *error) {
		if *err == nil {
			atomic.AddInt64(t.s.totalSuccessQueries, 1)
		} else {
			atomic.AddInt64(t.s.totalFailedQueries, 1)
			logger.FromCtx(ctx).Tag("mysql").Error(*err, "COMMIT")
			span.SetTag("error", true)
		}
		span.Finish()
		t.span.Finish()
	}(&err)

	start := time.Now()
	err = t.tx.Commit()
	queryTime := time.Now().Sub(start)
	txTime := time.Now().Sub(t.startedAt)
	t.s.sample(&QueryStats{"COMMIT", queryTime, 0, txTime})
	logger.FromCtx(ctx).Tag("mysql").Debug("COMMIT", queryTime, 0, txTime)
	t.close(err)
	return err
}

func (t *Transaction) Rollback(ctx context.Context) error {
	if _, ok := ctx.Value("tx").(*Transaction); ok {
		return nil
	}
	span := t.t.StartSpan("ROLLBACK", opentracing.ChildOf(t.span.Context()))
	span.SetTag("db.type", "mysql")
	span.SetTag("db.statement", "ROLLBACK")

	defer atomic.AddInt64(t.s.inProgressQueries, -1)
	defer t.r.end()

	var err error

	defer func(err *error) {
		if *err == nil {
			atomic.AddInt64(t.s.totalSuccessQueries, 1)
		} else {
			atomic.AddInt64(t.s.totalFailedQueries, 1)
			logger.FromCtx(ctx).Tag("mysql").Error(*err, "ROLLBACK")
			span.SetTag("error", true)
		}
		span.Finish()
		t.span.Finish()
	}(&err)

	start := time.Now()
	err = t.tx.Rollback()
	queryTime := time.Now().Sub(start)
	txTime := time.Now().Sub(t.startedAt)

	t.s.sample(&QueryStats{"ROLLBACK", queryTime, 0, txTime})
	logger.FromCtx(ctx).Tag("mysql").Debug("ROLLBACK", queryTime, 0, txTime)
	if err != nil {
		t.close(err)
	} else {
		t.close(ErrRollback)
	}

	return err
}

// WithContext appends the transation to the context and returns it.
//
func (t *Transaction) WithContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, "tx", t)
}

func (t *Transaction) close(err error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, c := range t.done {
		select {
		case c <- err:
		default:
		}
	}
}
