package executor

import (
	"github.com/Khorlane/RovaDB/internal/parser"
)

func executeInsert(stmt *parser.InsertStmt, tables map[string]*Table) error {
	table, ok := tables[stmt.TableName]
	if !ok {
		return errTableDoesNotExist
	}

	if len(stmt.Columns) == 0 {
		if len(stmt.Values) != len(table.Columns) {
			return errWrongValueCount
		}

		row := append([]parser.Value(nil), stmt.Values...)
		table.Rows = append(table.Rows, row)
		return nil
	}

	if len(stmt.Columns) != len(table.Columns) || len(stmt.Values) != len(table.Columns) {
		return errWrongValueCount
	}

	row := make([]parser.Value, len(table.Columns))
	seen := make(map[int]struct{}, len(stmt.Columns))
	for i, name := range stmt.Columns {
		idx := -1
		for j, column := range table.Columns {
			if column == name {
				idx = j
				break
			}
		}
		if idx < 0 {
			return errColumnDoesNotExist
		}
		if _, ok := seen[idx]; ok {
			return errWrongValueCount
		}
		seen[idx] = struct{}{}
		row[idx] = stmt.Values[i]
	}
	if len(seen) != len(table.Columns) {
		return errWrongValueCount
	}

	table.Rows = append(table.Rows, row)
	return nil
}
