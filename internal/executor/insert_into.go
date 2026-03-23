package executor

import (
	"errors"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func executeInsert(stmt *parser.InsertStmt, tables map[string]*Table) error {
	table, ok := tables[stmt.TableName]
	if !ok {
		return errors.New("executor: table does not exist")
	}
	if len(stmt.Values) != len(table.Columns) {
		return errors.New("executor: wrong value count")
	}

	row := append([]parser.Value(nil), stmt.Values...)
	table.Rows = append(table.Rows, row)
	return nil
}
