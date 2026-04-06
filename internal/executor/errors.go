package executor

import (
	"fmt"

	"github.com/Khorlane/RovaDB/internal/dberr"
)

func newExecError(msg string) error {
	return dberr.NewExec(msg)
}

func newTableNotFoundError(tableName string) error {
	if tableName == "" {
		return newExecError("table not found")
	}
	return newExecError(fmt.Sprintf("table not found: %s", tableName))
}

func newColumnNotFoundError(columnName string) error {
	if columnName == "" {
		return errColumnDoesNotExist
	}
	return newExecError(fmt.Sprintf("column not found: %s", columnName))
}
