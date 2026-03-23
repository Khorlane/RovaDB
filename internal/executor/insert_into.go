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
		for i, value := range stmt.Values {
			if !valueMatchesColumnType(value, table.Columns[i].Type) {
				return 0, errTypeMismatch
			}
		}

		row := append([]parser.Value(nil), stmt.Values...)
		table.Rows = append(table.Rows, row)
		if err := rebuildIndexesForTable(table); err != nil {
			return 0, err
		}
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
			if column.Name == name {
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
		if !valueMatchesColumnType(stmt.Values[i], table.Columns[idx].Type) {
			return 0, errTypeMismatch
		}
		seen[idx] = struct{}{}
		row[idx] = stmt.Values[i]
	}
	if len(seen) != len(table.Columns) {
		return 0, errWrongValueCount
	}

	table.Rows = append(table.Rows, row)
	if err := rebuildIndexesForTable(table); err != nil {
		return 0, err
	}
	return 1, nil
}

func valueMatchesColumnType(value parser.Value, typeName string) bool {
	if value.Kind == parser.ValueKindNull {
		return true
	}
	switch typeName {
	case parser.ColumnTypeInt:
		return value.Kind == parser.ValueKindInt64
	case parser.ColumnTypeText:
		return value.Kind == parser.ValueKindString
	default:
		return false
	}
}
