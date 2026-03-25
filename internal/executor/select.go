package executor

import (
	"sort"
	"strings"

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
	if err := validatePredicateOrWhereColumns(sel.Predicate, sel.Where, table); err != nil {
		return nil, err
	}
	if err := validateProjectionExprs(sel, table); err != nil {
		return nil, err
	}
	if sel.IsCountStar {
		if sel.OrderBy != nil {
			return nil, errCountOrderByUnsupported
		}
		count := int64(0)
		for _, row := range candidateRows {
			match, err := evalPredicateOrWhere(row, table, sel.Predicate, sel.Where)
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
		match, err := evalPredicateOrWhere(row, table, sel.Predicate, sel.Where)
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
		out, err := projectRow(sel, table, row)
		if err != nil {
			return nil, err
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
	if len(sel.ProjectionExprs) > 0 {
		if err := validateProjectionExprs(sel, table); err != nil {
			return nil, err
		}
		if len(sel.ProjectionLabels) == len(sel.ProjectionExprs) {
			return append([]string(nil), sel.ProjectionLabels...), nil
		}
		names := make([]string, 0, len(sel.ProjectionExprs))
		for _, expr := range sel.ProjectionExprs {
			if expr != nil && expr.Kind == parser.ValueExprKindColumnRef {
				names = append(names, expr.Column)
			} else {
				names = append(names, "expr")
			}
		}
		return names, nil
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

func projectRow(sel *parser.SelectExpr, table *Table, row []parser.Value) ([]parser.Value, error) {
	if len(sel.ProjectionExprs) > 0 {
		out := make([]parser.Value, 0, len(sel.ProjectionExprs))
		for _, expr := range sel.ProjectionExprs {
			value, err := evalValueExpr(row, table, expr)
			if err != nil {
				return nil, err
			}
			out = append(out, value)
		}
		return out, nil
	}

	indexes, err := resolveSelectColumns(sel, table)
	if err != nil {
		return nil, err
	}
	out := make([]parser.Value, 0, len(indexes))
	for _, idx := range indexes {
		out = append(out, row[idx])
	}
	return out, nil
}

func validateProjectionExprs(sel *parser.SelectExpr, table *Table) error {
	if sel == nil {
		return nil
	}
	if len(sel.ProjectionExprs) == 0 {
		if len(sel.Columns) == 0 {
			return nil
		}
		_, err := resolveSelectColumns(sel, table)
		return err
	}
	for _, expr := range sel.ProjectionExprs {
		if err := validateValueExprColumns(expr, table); err != nil {
			return err
		}
	}
	return nil
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
	baseName, err := normalizeQualifiedColumnName(name, table)
	if err != nil {
		return -1, err
	}
	for i, column := range table.Columns {
		if column.Name == baseName {
			return i, nil
		}
	}

	return -1, errColumnDoesNotExist
}

func normalizeQualifiedColumnName(name string, table *Table) (string, error) {
	if !strings.Contains(name, ".") {
		return name, nil
	}
	parts := strings.Split(name, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", errColumnDoesNotExist
	}
	if table == nil || parts[0] != table.Name {
		return "", errColumnDoesNotExist
	}
	return parts[1], nil
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

func evalPredicateOrWhere(row []parser.Value, table *Table, predicate *parser.PredicateExpr, where *parser.WhereClause) (bool, error) {
	if predicate != nil {
		return evalPredicate(row, table, predicate)
	}
	return evalWhere(row, table, where)
}

func evalPredicate(row []parser.Value, table *Table, predicate *parser.PredicateExpr) (bool, error) {
	if predicate == nil {
		return true, nil
	}

	switch predicate.Kind {
	case parser.PredicateKindComparison:
		if predicate.Comparison == nil {
			return false, errUnsupportedStatement
		}
		return evalWhereCondition(row, table, *predicate.Comparison)
	case parser.PredicateKindAnd:
		left, err := evalPredicate(row, table, predicate.Left)
		if err != nil {
			return false, err
		}
		if !left {
			return false, nil
		}
		return evalPredicate(row, table, predicate.Right)
	case parser.PredicateKindOr:
		left, err := evalPredicate(row, table, predicate.Left)
		if err != nil {
			return false, err
		}
		if left {
			return true, nil
		}
		return evalPredicate(row, table, predicate.Right)
	case parser.PredicateKindNot:
		inner, err := evalPredicate(row, table, predicate.Inner)
		if err != nil {
			return false, err
		}
		return !inner, nil
	default:
		return false, errUnsupportedStatement
	}
}

func evalWhereCondition(row []parser.Value, table *Table, cond parser.Condition) (bool, error) {
	if cond.LeftExpr != nil && cond.RightExpr != nil {
		left, err := evalValueExpr(row, table, cond.LeftExpr)
		if err != nil {
			return false, err
		}
		right, err := evalValueExpr(row, table, cond.RightExpr)
		if err != nil {
			return false, err
		}
		return compareValues(cond.Operator, left, right)
	}

	idx, err := resolveColumnIndex(cond.Left, table)
	if err != nil {
		return false, err
	}

	if cond.RightRef != "" {
		rightIdx, err := resolveColumnIndex(cond.RightRef, table)
		if err != nil {
			return false, err
		}
		return compareValues(cond.Operator, row[idx], row[rightIdx])
	}

	return compareValues(cond.Operator, row[idx], cond.Right)
}

func validateWhereColumns(where *parser.WhereClause, table *Table) error {
	if where == nil {
		return nil
	}

	for _, item := range where.Items {
		if item.Condition.LeftExpr != nil && item.Condition.RightExpr != nil {
			if err := validateValueExprColumns(item.Condition.LeftExpr, table); err != nil {
				return err
			}
			if err := validateValueExprColumns(item.Condition.RightExpr, table); err != nil {
				return err
			}
			continue
		}
		if _, err := resolveColumnIndex(item.Condition.Left, table); err != nil {
			return err
		}
		if item.Condition.RightRef != "" {
			if _, err := resolveColumnIndex(item.Condition.RightRef, table); err != nil {
				return err
			}
		}
	}

	return nil
}

func validatePredicateOrWhereColumns(predicate *parser.PredicateExpr, where *parser.WhereClause, table *Table) error {
	if predicate != nil {
		return validatePredicateColumns(predicate, table)
	}
	return validateWhereColumns(where, table)
}

func validatePredicateColumns(predicate *parser.PredicateExpr, table *Table) error {
	if predicate == nil {
		return nil
	}

	switch predicate.Kind {
	case parser.PredicateKindComparison:
		if predicate.Comparison == nil {
			return errUnsupportedStatement
		}
		if predicate.Comparison.LeftExpr != nil && predicate.Comparison.RightExpr != nil {
			if err := validateValueExprColumns(predicate.Comparison.LeftExpr, table); err != nil {
				return err
			}
			return validateValueExprColumns(predicate.Comparison.RightExpr, table)
		}
		_, err := resolveColumnIndex(predicate.Comparison.Left, table)
		if err != nil {
			return err
		}
		if predicate.Comparison.RightRef != "" {
			_, err = resolveColumnIndex(predicate.Comparison.RightRef, table)
		}
		return err
	case parser.PredicateKindAnd, parser.PredicateKindOr:
		if err := validatePredicateColumns(predicate.Left, table); err != nil {
			return err
		}
		return validatePredicateColumns(predicate.Right, table)
	case parser.PredicateKindNot:
		return validatePredicateColumns(predicate.Inner, table)
	default:
		return errUnsupportedStatement
	}
}

func evalValueExpr(row []parser.Value, table *Table, expr *parser.ValueExpr) (parser.Value, error) {
	if expr == nil {
		return parser.Value{}, errUnsupportedStatement
	}

	switch expr.Kind {
	case parser.ValueExprKindLiteral:
		return expr.Value, nil
	case parser.ValueExprKindColumnRef:
		name := expr.Column
		if expr.Qualifier != "" {
			name = expr.Qualifier + "." + expr.Column
		}
		idx, err := resolveColumnIndex(name, table)
		if err != nil {
			return parser.Value{}, err
		}
		return row[idx], nil
	case parser.ValueExprKindParen:
		return evalValueExpr(row, table, expr.Inner)
	case parser.ValueExprKindFunctionCall:
		arg, err := evalValueExpr(row, table, expr.Arg)
		if err != nil {
			return parser.Value{}, err
		}
		return evalScalarFunction(expr.FuncName, arg)
	default:
		return parser.Value{}, errUnsupportedStatement
	}
}

func validateValueExprColumns(expr *parser.ValueExpr, table *Table) error {
	if expr == nil {
		return nil
	}

	switch expr.Kind {
	case parser.ValueExprKindLiteral:
		return nil
	case parser.ValueExprKindColumnRef:
		name := expr.Column
		if expr.Qualifier != "" {
			name = expr.Qualifier + "." + expr.Column
		}
		_, err := resolveColumnIndex(name, table)
		return err
	case parser.ValueExprKindParen:
		return validateValueExprColumns(expr.Inner, table)
	case parser.ValueExprKindFunctionCall:
		return validateValueExprColumns(expr.Arg, table)
	default:
		return errUnsupportedStatement
	}
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
