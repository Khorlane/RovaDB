package executor

import (
	"github.com/Khorlane/RovaDB/internal/parser"
)

func executeInsert(stmt *parser.InsertStmt, tables map[string]*Table) error {
	table, ok := tables[stmt.TableName]
	if !ok {
		return errTableDoesNotExist
	}
	if len(stmt.Values) != len(table.Columns) {
		return errWrongValueCount
	}

	row := append([]parser.Value(nil), stmt.Values...)
	table.Rows = append(table.Rows, row)
	return nil
}
