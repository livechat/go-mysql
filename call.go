package mysql

import (
	"fmt"
	"strings"
)

func call(procedure string, argsCount int) string {
	params := make([]string, argsCount)
	for i := 0; i < argsCount; i++ {
		params[i] = "?"
	}

	query := fmt.Sprintf("CALL %s(%s);", procedure, strings.Join(params, ", "))

	return query
}
