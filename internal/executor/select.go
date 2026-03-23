package executor

import (
	"errors"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func Select(sel *parser.SelectExpr, tables map[string]*Table) ([][]parser.Value, error) {
	if sel == nil || sel.TableName == "" {
		return nil, errors.New("executor: invalid select")
	}

	table, ok := tables[sel.TableName]
	if !ok {
		return nil, errors.New("executor: table does not exist")
	}

	indexes, err := resolveSelectColumns(sel, table)
	if err != nil {
		return nil, err
	}

	rows := make([][]parser.Value, 0, len(table.Rows))
	for _, row := range table.Rows {
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
		idx := -1
		for i, column := range table.Columns {
			if column == name {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, errors.New("executor: column does not exist")
		}
		indexes = append(indexes, idx)
	}

	return indexes, nil
}
