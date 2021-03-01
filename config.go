package mysql

import "time"

type Config struct {

	// Drivery retry a query if deadlock occured
	RetryOnDeadlock bool

	// Retry deadlock limit
	RetryOnDeadlockCount int

	// Time between retries
	RetryOnDeadlockDelay time.Duration

	// Query timeout set in context, if d <= 0 then timeout isn't set
	Timeout time.Duration

	// Limit of waiting calls for connection
	MaxQueuedQueries int64

	// If MaxIdleConns is greater than 0 and the new MaxOpenConns is less than MaxIdleConns, then MaxIdleConns will be reduced to match the new MaxOpenConns limit.
	// If n <= 0, then there is no limit on the number of open connections. The default is 0 (unlimited).
	MaxOpenConns int

	// If n <= 0, no idle connections are retained.
	MaxIdleConns int

	// If d <= 0, connections are reused forever.
	ConnMaxLifetime time.Duration

	// Check if support for synchronous reads after transaction should be used
	SyncAfterTransaction bool
}

func NewDefaultConfig() *Config {
	return &Config{
		RetryOnDeadlock:      true,
		RetryOnDeadlockCount: 5,
		RetryOnDeadlockDelay: time.Millisecond * 10,
		Timeout:              time.Second * 10,
		MaxQueuedQueries:     10000,

		MaxOpenConns:    20,
		MaxIdleConns:    20,
		ConnMaxLifetime: time.Second * 60,

		SyncAfterTransaction: false,
	}
}
