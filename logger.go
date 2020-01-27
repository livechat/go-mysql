package mysql

import "context"

var (
	logger Logger
)

func init() {
	logger = &defaultLogger{}
}

type Logger interface {
	FromCtx(context.Context) Logger
	Tag(string) Logger

	Error(...interface{})
	Warning(...interface{})
	Debug(...interface{})
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

func SetLogger(l Logger) {
	logger = l
}
