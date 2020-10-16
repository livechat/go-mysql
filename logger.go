package mysql

import (
	"context"

	m "github.com/go-sql-driver/mysql"
)

var (
	logger Logger
)

func init() {
	logger = &defaultLogger{}
}

// Logger interface for enabling logs.
//
type Logger interface {
	FromCtx(context.Context) Logger // get logger from context if found or return new logger, useful for context logs
	Tag(string) Logger              // this package tags all logs with mysql tag

	Error(...interface{})   // used for mysql and connection errors
	Warning(...interface{}) // used for deadlocks
	Debug(...interface{})   // used queries
}

type defaultLogger struct{}

func (d *defaultLogger) FromCtx(ctx context.Context) Logger {
	return d
}

func (d *defaultLogger) Tag(s string) Logger {
	return d
}
func (d *defaultLogger) Error(p ...interface{})   {}
func (d *defaultLogger) Warning(p ...interface{}) {}
func (d *defaultLogger) Debug(p ...interface{})   {}

type loggerWrapper struct {
	l Logger
}

func (lw *loggerWrapper) Print(v ...interface{}) {
	lw.l.Error(v...)
}

// The default logger does not log anything, this function overrides default logger.
// The logger must compatible with a Logger interface.
func SetLogger(l Logger) {
	logger = l
	m.SetLogger(&loggerWrapper{l})
}
