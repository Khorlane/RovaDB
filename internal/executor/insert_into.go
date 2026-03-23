package executor

import (
	"github.com/Khorlane/RovaDB/internal/parser"
)

func executeInsert(stmt *parser.InsertStmt, tables map[string]*Table) (int64, error) {
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, errTableDoesNotExist
	}

	if len(stmt.Columns) == 0 {
		if len(stmt.Values) != len(table.Columns) {
			return 0, errWrongValueCount
		}

		row := append([]parser.Value(nil), stmt.Values...)
		table.Rows = append(table.Rows, row)
		return 1, nil
	}

	if len(stmt.Columns) != len(table.Columns) || len(stmt.Values) != len(table.Columns) {
		return 0, errWrongValueCount
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
			return 0, errColumnDoesNotExist
		}
		if _, ok := seen[idx]; ok {
			return 0, errWrongValueCount
		}
		seen[idx] = struct{}{}
		row[idx] = stmt.Values[i]
	}
	if len(seen) != len(table.Columns) {
		return 0, errWrongValueCount
	}

	table.Rows = append(table.Rows, row)
	return 1, nil
}
