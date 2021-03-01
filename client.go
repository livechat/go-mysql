// This package makes mysql operations easier. It's based on github.com/go-sql-driver/mysql.
// The package handles:
//
//  - connection pooling
//  - statistics
//  - slow logs
//  - transactions
//  - response parsing
//  - multiple statements
package mysql

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Client struct {
	db      *sql.DB
	replica *Client
	config  *Config
	s       *stats

	mu sync.Mutex
	r  *relay
}

// NewClient creates a new client for a given DSN.
//  [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
//
// Useful query params:
//  interpolateParams bool - reduces the number of roundtrips, the driver has to prepare a statement instead of a server
//  time_zone - set connection timezone
//  multiStatements - allow multiple statements
func NewClient(dsn string) (*Client, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	cfg := NewDefaultConfig()
	s := newStats()

	c := &Client{db, nil, cfg, s, sync.Mutex{}, &relay{}}
	c.SetConfig(cfg)

	return c, nil
}

func (c *Client) SetReplica(replica *Client) {
	c.replica = replica
}

// SetConfig applies config passed in cfg.
//
func (c *Client) SetConfig(cfg *Config) {
	c.config = cfg
	c.db.SetMaxIdleConns(cfg.MaxIdleConns)
	c.db.SetMaxOpenConns(cfg.MaxOpenConns)
	c.db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	c.r.setRelaySize(cfg.MaxOpenConns * 10)
}

// Begin opens or returns trasaction found in the context.
// Options can be null.
//
func (c *Client) Begin(ctx context.Context, opts *sql.TxOptions) (*Transaction, error) {
	if opts != nil && opts.ReadOnly && c.replica != nil {
		return c.replica.Begin(ctx, opts)
	}

	var err error

	if tx, ok := ctx.Value("tx").(*Transaction); ok {
		return tx, nil
	}

	defer func(e *error) {
		if *e != nil {
			atomic.AddInt64(c.s.inProgressQueries, -1)
		}
	}(&err)

	if err := c.limitReached(); err != nil {
		return nil, err
	}

	queueTime := c.r.start()
	defer c.r.conditionalEnd(&err)

	start := time.Now()
	tx, err := c.db.BeginTx(ctx, opts)
	queryTime := time.Now().Sub(start)

	c.s.sample(&QueryStats{"BEGIN", queryTime, queueTime, 0})
	logger.FromCtx(ctx).Tag("mysql").Debug("BEGIN", queryTime, 0)
	if err != nil {
		return nil, err
	}

	return newTransaction(tx, c.s, c.config, c.r), nil
}

/*
Call prepares a stored procedure end executes it. Params are passed as variadic arguments.

 res, err := client.Call(ctx, "SP_ListAll", page)
 if err != nil {
   return err
 }
*/
func (c *Client) Call(ctx context.Context, procedure string, args ...interface{}) (*Results, error) {
	query := call(procedure, len(args))
	return c.Query(ctx, query, args...)
}

/*
MultiCall prepares a stored procedure end executes it. Params are passed as variadic arguments.
Can't be used for multiple stored procedure calls, but handles multiple restults from a stored procedure.

 res, err := client.Call(ctx, "SP_ListAll", page)
 if err != nil {
   return err
 }
*/
func (c *Client) MultiCall(ctx context.Context, procedure string, args ...interface{}) (*MultiResults, error) {
	query := call(procedure, len(args))
	return c.MultiQuery(ctx, query, args...)
}

/*
Exec prepares a query that does not return any data except metadata and executes it, for example inserts, updates, etc..
*/
func (c *Client) Exec(ctx context.Context, query string, args ...interface{}) (*Meta, error) {

	var (
		i      int = 1
		err    error
		result sql.Result
		cancel func()
	)

	defer func(err *error) {
		atomic.AddInt64(c.s.inProgressQueries, -1)

		if *err == nil {
			atomic.AddInt64(c.s.totalSuccessQueries, 1)
		} else {
			atomic.AddInt64(c.s.totalFailedQueries, 1)
			logger.FromCtx(ctx).Tag("mysql").Error(*err, query)
		}
	}(&err)

	if err = c.limitReached(); err != nil {
		return nil, err
	}

	if c.config.RetryOnDeadlock {
		i += c.config.RetryOnDeadlockCount
	}

	if c.config.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.config.Timeout)
		defer cancel()
	}

	queueTime := c.r.start()
	defer c.r.end()

	start := time.Now()

	for ; i > 0; i-- {
		result, err = c.db.ExecContext(ctx, query, args...)

		if IsErrorCode(err, ErrMySQLDeadlock) {
			logger.FromCtx(ctx).Tag("mysql").Warning("deadlock", query)
			time.Sleep(c.config.RetryOnDeadlockDelay)
			continue
		}

		break
	}

	queryTime := time.Now().Sub(start)
	c.s.sample(&QueryStats{query, queryTime, queueTime, 0})
	if err != nil {
		return nil, err
	}

	meta := newMeta(result)
	meta.QueryTime = queryTime

	logger.FromCtx(ctx).Tag("mysql").Debug(query, meta.QueryTime)
	return meta, nil
}

func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (*Results, error) {
	if c.replica != nil {
		return c.replica.Query(ctx, query, args...)
	}

	var (
		i      int = 1
		err    error
		rows   *sql.Rows
		cancel func()
	)

	defer func(err *error) {
		atomic.AddInt64(c.s.inProgressQueries, -1)
		if *err == nil {
			atomic.AddInt64(c.s.totalSuccessQueries, 1)
		} else {
			atomic.AddInt64(c.s.totalFailedQueries, 1)
			logger.FromCtx(ctx).Tag("mysql").Error(*err, query)
		}
	}(&err)

	if err = c.limitReached(); err != nil {
		return nil, err
	}

	if c.config.RetryOnDeadlock {
		i += c.config.RetryOnDeadlockCount
	}

	if c.config.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.config.Timeout)
		defer cancel()
	}

	queueTime := c.r.start()
	defer c.r.end()

	start := time.Now()

	if checkIfSyncNeeded(ctx) {
		c.db.ExecContext(ctx, "SET SESSION wsrep_sync_wait=1")
		defer c.db.ExecContext(ctx, "SET SESSION wsrep_sync_wait=0")
	}

	for ; i > 0; i-- {
		rows, err = c.db.QueryContext(ctx, query, args...)

		if IsErrorCode(err, ErrMySQLDeadlock) {
			time.Sleep(c.config.RetryOnDeadlockDelay)
			continue
		}

		break
	}

	queryTime := time.Now().Sub(start)
	c.s.sample(&QueryStats{query, queryTime, queueTime, 0})

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	results, err := newResultsFromSqlRows(rows)
	if err != nil {
		return nil, err
	}

	results.QueryTime = queryTime
	logger.FromCtx(ctx).Tag("mysql").Debug(query, queryTime, queueTime)
	return results, nil
}

/*
MultiQuery executes multiple queries at once and returns as MultiResults.

 mRes, err := client.MultiQuery(ctx, "SELECT NOW() as date; SELECT @@version as version;")
 if err != nil {
   return err
 }
 fmt.Println(mRes.Results[0].Rows[0].GetElementByName("date").Element.NullTime.Time)
 fmt.Println(mRes.Results[1].Rows[0].GetElementByName("version").Element.NullString.String)
*/
func (c *Client) MultiQuery(ctx context.Context, query string, args ...interface{}) (*MultiResults, error) {

	var (
		i      int = 1
		err    error
		rows   *sql.Rows
		cancel func()
	)

	defer func(err *error) {
		atomic.AddInt64(c.s.inProgressQueries, -1)
		if *err == nil {
			atomic.AddInt64(c.s.totalSuccessQueries, 1)
		} else {
			atomic.AddInt64(c.s.totalFailedQueries, 1)
			logger.FromCtx(ctx).Tag("mysql").Error(*err, query)
		}
	}(&err)

	if err = c.limitReached(); err != nil {
		return nil, err
	}

	if c.config.RetryOnDeadlock {
		i += c.config.RetryOnDeadlockCount
	}

	if c.config.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.config.Timeout)
		defer cancel()
	}

	queueTime := c.r.start()
	defer c.r.end()

	start := time.Now()

	for ; i > 0; i-- {
		rows, err = c.db.QueryContext(ctx, query, args...)

		if IsErrorCode(err, ErrMySQLDeadlock) {
			time.Sleep(c.config.RetryOnDeadlockDelay)
			continue
		}

		break
	}

	queryTime := time.Now().Sub(start)
	c.s.sample(&QueryStats{query, queryTime, queueTime, 0})

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

	logger.FromCtx(ctx).Tag("mysql").Debug(query, queryTime, queueTime)
	return multiResults, nil
}

// QueryTx executes a query in a transantion context if transaction exists.
//
func (c *Client) QueryTx(ctx context.Context, query string, args ...interface{}) (*Results, error) {
	if tx, ok := ctx.Value("tx").(*Transaction); ok {
		return tx.Query(ctx, query, args...)
	} else {
		return c.Query(ctx, query, args...)
	}
}

func (c *Client) Stats() *Stats {
	c.mu.Lock()
	defer c.mu.Unlock()

	s := &Stats{
		DBStats:             c.db.Stats(),
		InProgressQueries:   atomic.LoadInt64(c.s.inProgressQueries),
		TotalSuccessQueries: atomic.LoadInt64(c.s.totalSuccessQueries),
		TotalFailedQueries:  atomic.LoadInt64(c.s.totalFailedQueries),
	}

	return s
}

func (c *Client) SamplesChan() chan *QueryStats {
	return c.s.sampleChan
}

func (c *Client) limitReached() error {
	atomic.AddInt64(c.s.inProgressQueries, 1)

	c.mu.Lock()
	defer c.mu.Unlock()

	internalStats := c.db.Stats()

	if atomic.LoadInt64(c.s.inProgressQueries)-int64(internalStats.MaxOpenConnections) > c.config.MaxQueuedQueries {
		return ErrQueueOverloaded
	}

	return nil
}
