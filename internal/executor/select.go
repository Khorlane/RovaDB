package executor

import (
	"sort"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

func Select(plan *planner.SelectPlan, tables map[string]*Table) ([][]parser.Value, error) {
	if err := validateSelectPlan(plan); err != nil {
		return nil, err
	}

	sel := plan.Stmt
	if sel.TableName == "" {
		return nil, errUnsupportedStatement
	}

	table, ok := tables[sel.TableName]
	if !ok {
		return nil, errTableDoesNotExist
	}
	switch plan.ScanType {
	case planner.ScanTypeTable:
		return executeSelectRows(sel, table, table.Rows)
	case planner.ScanTypeIndex:
		return executeIndexSelect(plan, table)
	default:
		return nil, errInvalidSelectPlan
	}
}

func executeIndexSelect(plan *planner.SelectPlan, table *Table) ([][]parser.Value, error) {
	if plan == nil || plan.IndexScan == nil || table == nil {
		return nil, errInvalidSelectPlan
	}

	index := table.Indexes[plan.IndexScan.ColumnName]
	if index == nil {
		return nil, errInvalidSelectPlan
	}

	rowPositions := index.LookupEqual(plan.IndexScan.Value)
	candidateRows := make([][]parser.Value, 0, len(rowPositions))
	for _, rowIndex := range rowPositions {
		if rowIndex < 0 || rowIndex >= len(table.Rows) {
			return nil, errInvalidSelectPlan
		}
		candidateRows = append(candidateRows, table.Rows[rowIndex])
	}

	return executeSelectRows(plan.Stmt, table, candidateRows)
}

func executeSelectRows(sel *parser.SelectExpr, table *Table, candidateRows [][]parser.Value) ([][]parser.Value, error) {
	indexes, err := resolveSelectColumns(sel, table)
	if err != nil {
		return nil, err
	}
	if err := validateWhereColumns(sel.Where, table); err != nil {
		return nil, err
	}
	if sel.IsCountStar {
		if sel.OrderBy != nil {
			return nil, errCountOrderByUnsupported
		}
		count := int64(0)
		for _, row := range candidateRows {
			match, err := evalWhere(row, table, sel.Where)
			if err != nil {
				return nil, err
			}
			if match {
				count++
			}
		}
		return [][]parser.Value{{parser.Int64Value(count)}}, nil
	}
	baseRows := make([][]parser.Value, 0, len(candidateRows))
	for _, row := range candidateRows {
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

func validateSelectPlan(plan *planner.SelectPlan) error {
	if plan == nil || plan.Stmt == nil {
		return errUnsupportedStatement
	}
	if plan.Stmt.TableName == "" {
		return nil
	}
	switch plan.ScanType {
	case planner.ScanTypeTable:
		if plan.TableScan == nil || plan.TableScan.TableName != plan.Stmt.TableName {
			return errInvalidSelectPlan
		}
	case planner.ScanTypeIndex:
		if plan.IndexScan == nil || plan.IndexScan.TableName != plan.Stmt.TableName || plan.IndexScan.ColumnName == "" {
			return errInvalidSelectPlan
		}
	default:
		return errInvalidSelectPlan
	}
	return nil
}

func ProjectedColumnNames(plan *planner.SelectPlan, table *Table) ([]string, error) {
	if plan == nil || plan.Stmt == nil || table == nil {
		return nil, errUnsupportedStatement
	}

	sel := plan.Stmt
	if sel.IsCountStar {
		return []string{"count"}, nil
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
	if len(where.Items) == 0 {
		return true, nil
	}

	current, err := evalWhereCondition(row, table, where.Items[0].Condition)
	if err != nil {
		return false, err
	}

	for _, item := range where.Items[1:] {
		next, err := evalWhereCondition(row, table, item.Condition)
		if err != nil {
			return false, err
		}
		switch item.Op {
		case parser.BooleanOpAnd:
			current = current && next
		case parser.BooleanOpOr:
			current = current || next
		default:
			return false, errUnsupportedStatement
		}
	}

	return current, nil
}

func evalWhereCondition(row []parser.Value, table *Table, cond parser.Condition) (bool, error) {
	idx, err := resolveColumnIndex(cond.Left, table)
	if err != nil {
		return false, err
	}

	return compareValues(cond.Operator, row[idx], cond.Right)
}

func validateWhereColumns(where *parser.WhereClause, table *Table) error {
	if where == nil {
		return nil
	}

	for _, item := range where.Items {
		if _, err := resolveColumnIndex(item.Condition.Left, table); err != nil {
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
