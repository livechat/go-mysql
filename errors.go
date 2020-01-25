package mysql

import (
	"errors"

	mysql "github.com/go-sql-driver/mysql"
)

type SQLErrorNumber uint16

const (
	ErrMySQLDeadlock   SQLErrorNumber = 1213
	ErrMySQLDupEntry                  = 1062
	ErrMySQLCoinstaint                = 1452
)

var (
	ErrQueueOverloaded = errors.New("queue overloaded")
	ErrWrongReference  = errors.New("err wrong reference")
	ErrRollback        = errors.New("tx rollback")
)

// IsErrorCode checks if the error is one of standard mysql error codes.
//
//  if IsErrorCode(err, ErrMySQLDupEntry) {
//    // handle duplicate entry
//  }
func IsErrorCode(err error, no SQLErrorNumber) bool {
	if mysqlErr, ok := err.(*mysql.MySQLError); ok && mysqlErr.Number == uint16(no) {
		return true
	}

	return false
}
