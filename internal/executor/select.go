package executor

import (
	"sort"

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
	if err := validateWhereColumns(sel.Where, table); err != nil {
		return nil, err
	}
	baseRows := make([][]parser.Value, 0, len(table.Rows))
	for _, row := range table.Rows {
		match, err := evalWhere(row, table, sel.Where)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		baseRows = append(baseRows, row)
	}
	if err := sortRows(baseRows, table, sel.OrderBy); err != nil {
		return nil, err
	}

	rows := make([][]parser.Value, 0, len(baseRows))
	for _, row := range baseRows {
		out := make([]parser.Value, 0, len(indexes))
		for _, idx := range indexes {
			out = append(out, row[idx])
		}
		rows = append(rows, out)
	}

	return rows, nil
}

func ProjectedColumnNames(sel *parser.SelectExpr, table *Table) ([]string, error) {
	if sel == nil || table == nil {
		return nil, errUnsupportedStatement
	}
	if len(sel.Columns) == 0 {
		names := make([]string, 0, len(table.Columns))
		for _, column := range table.Columns {
			names = append(names, column.Name)
		}
		return names, nil
	}

	names := make([]string, 0, len(sel.Columns))
	for _, name := range sel.Columns {
		if _, err := resolveColumnIndex(name, table); err != nil {
			return nil, errColumnDoesNotExist
		}
		names = append(names, name)
	}
	return names, nil
}

func resolveSelectColumns(sel *parser.SelectExpr, table *Table) ([]int, error) {
	if len(sel.Columns) == 0 {
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

func evalWhere(row []parser.Value, table *Table, where *parser.WhereClause) (bool, error) {
	if where == nil {
		return true, nil
	}

	for _, cond := range where.Conditions {
		idx, err := resolveColumnIndex(cond.Left, table)
		if err != nil {
			return false, err
		}

		match, err := compareValues(cond.Operator, row[idx], cond.Right)
		if err != nil {
			return false, err
		}
		if !match {
			return false, nil
		}
	}

	return true, nil
}

func validateWhereColumns(where *parser.WhereClause, table *Table) error {
	if where == nil {
		return nil
	}

	for _, cond := range where.Conditions {
		if _, err := resolveColumnIndex(cond.Left, table); err != nil {
			return err
		}
	}

	return nil
}

func sortRows(rows [][]parser.Value, table *Table, orderBy *parser.OrderByClause) error {
	if orderBy == nil {
		return nil
	}

	idx, err := resolveColumnIndex(orderBy.Column, table)
	if err != nil {
		return err
	}

	var sortErr error
	sort.SliceStable(rows, func(i, j int) bool {
		if sortErr != nil {
			return false
		}

		cmp, err := compareSortableValues(rows[i][idx], rows[j][idx])
		if err != nil {
			sortErr = err
			return false
		}
		if orderBy.Desc {
			return cmp > 0
		}
		return cmp < 0
	})

	return sortErr
}
