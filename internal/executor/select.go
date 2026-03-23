package executor

import (
	"github.com/Khorlane/RovaDB/internal/parser"
)

func Select(sel *parser.SelectExpr, tables map[string]*Table) ([][]parser.Value, error) {
	if sel == nil || sel.TableName == "" {
		return nil, errUnsupportedStatement
	}

	table, ok := tables[sel.TableName]
	if !ok {
		return nil, errTableDoesNotExist
	}

	indexes, err := resolveSelectColumns(sel, table)
	if err != nil {
		return nil, err
	}
	whereIndex := -1
	if sel.HasWhere {
		whereIndex, err = resolveColumnIndex(sel.WhereColumn, table)
		if err != nil {
			return nil, err
		}
	}

	rows := make([][]parser.Value, 0, len(table.Rows))
	for _, row := range table.Rows {
		if whereIndex >= 0 && !valuesEqual(row[whereIndex], sel.WhereValue) {
			continue
		}
		out := make([]parser.Value, 0, len(indexes))
		for _, idx := range indexes {
			out = append(out, row[idx])
		}
		rows = append(rows, out)
	}

	return rows, nil
}

func resolveSelectColumns(sel *parser.SelectExpr, table *Table) ([]int, error) {
	if sel.SelectAll {
		indexes := make([]int, 0, len(table.Columns))
		for i := range table.Columns {
			indexes = append(indexes, i)
		}
		return indexes, nil
	}

	indexes := make([]int, 0, len(sel.Columns))
	for _, name := range sel.Columns {
		idx, err := resolveColumnIndex(name, table)
		if err != nil {
			return nil, errColumnDoesNotExist
		}
		indexes = append(indexes, idx)
	}

	return indexes, nil
}

func resolveColumnIndex(name string, table *Table) (int, error) {
	for i, column := range table.Columns {
		if column.Name == name {
			return i, nil
		}
	}

	return -1, errColumnDoesNotExist
}

func valuesEqual(left, right parser.Value) bool {
	if left.Kind != right.Kind {
		return false
	}

	switch left.Kind {
	case parser.ValueKindInt64:
		return left.I64 == right.I64
	case parser.ValueKindString:
		return left.Str == right.Str
	default:
		return false
	}
}
