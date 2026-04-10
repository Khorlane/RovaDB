package executor

import (
	"sort"
	"strings"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

// This file owns SELECT runtime behavior. The executor-facing seam starts at
// SelectExecutionHandoff; direct planner shell input wrappers remain temporary
// compatibility helpers while outer-seam tightening continues.
func Select(plan *planner.SelectPlan, tables map[string]*Table) ([][]parser.Value, error) {
	handoff, err := NewSelectExecutionHandoff(plan)
	if err != nil {
		return nil, err
	}
	return SelectWithHandoff(handoff, tables)
}

func SelectWithHandoff(handoff *SelectExecutionHandoff, tables map[string]*Table) ([][]parser.Value, error) {
	bridge, err := selectBridgeFromHandoff(handoff)
	if err != nil {
		return nil, err
	}
	return selectWithBridge(bridge, tables)
}

func selectBridgeFromHandoff(handoff *SelectExecutionHandoff) (*selectPlanBridge, error) {
	if handoff == nil || handoff.bridge == nil {
		return nil, errInvalidSelectPlan
	}
	return handoff.bridge, nil
}

func selectWithBridge(bridge *selectPlanBridge, tables map[string]*Table) ([][]parser.Value, error) {
	sel := bridge.query
	if sel.tableName == "" {
		return nil, errUnsupportedStatement
	}
	if len(sel.from) > 1 && bridge.scanKind != selectScanKindJoin {
		return nil, errUnsupportedStatement
	}

	switch bridge.scanKind {
	case selectScanKindJoin:
		return executeJoinSelect(bridge, tables)
	case selectScanKindTable:
		table, err := bridge.singleTable(tables)
		if err != nil {
			return nil, err
		}
		return executeSelectRows(sel, table, table.Rows)
	case selectScanKindIndex:
		table, err := bridge.singleTable(tables)
		if err != nil {
			return nil, err
		}
		return executeIndexSelect(bridge, table)
	default:
		return nil, errInvalidSelectPlan
	}
}

func executeIndexSelect(plan *selectPlanBridge, table *Table) ([][]parser.Value, error) {
	if plan == nil || plan.scanKind != selectScanKindIndex || plan.query == nil || table == nil {
		return nil, errInvalidSelectPlan
	}
	if plan.accessPath.IndexLookup.ColumnName == "" {
		return nil, errInvalidSelectPlan
	}
	// Public indexed query execution now lives in the DB-owned page-backed path.
	// This executor helper remains metadata-only for isolated unit coverage.
	return executeSelectRows(plan.query, table, table.Rows)
}

// SelectCandidateRows executes a planned single-table select against caller-supplied candidate rows.
func SelectCandidateRows(plan *planner.SelectPlan, table *Table, candidateRows [][]parser.Value) ([][]parser.Value, error) {
	handoff, err := NewSelectExecutionHandoff(plan)
	if err != nil {
		return nil, errInvalidSelectPlan
	}
	return SelectCandidateRowsWithHandoff(handoff, table, candidateRows)
}

// SelectCandidateRowsWithHandoff executes a planned single-table select against
// caller-supplied candidate rows through the executor-owned handoff.
func SelectCandidateRowsWithHandoff(handoff *SelectExecutionHandoff, table *Table, candidateRows [][]parser.Value) ([][]parser.Value, error) {
	bridge, err := selectBridgeFromHandoff(handoff)
	if err != nil || table == nil {
		return nil, errInvalidSelectPlan
	}
	if bridge.scanKind == selectScanKindJoin {
		return nil, errInvalidSelectPlan
	}
	return executeSelectRows(bridge.query, table, candidateRows)
}

func executeSelectRows(sel *runtimeSelectQuery, table *Table, candidateRows [][]parser.Value) ([][]parser.Value, error) {
	if err := validateSelectFilterColumns(sel, table); err != nil {
		return nil, err
	}
	if err := validateProjectionExprs(sel, table); err != nil {
		return nil, err
	}
	if sel.isCountStar {
		if len(sel.orderBys) > 0 || sel.orderBy != nil {
			return nil, errCountOrderByUnsupported
		}
		// COUNT(*) is reduced directly from matching base rows rather than flowing
		// through normal projection logic.
		count := int64(0)
		for _, row := range candidateRows {
			match, err := evalSelectFilter(row, sel, table)
			if err != nil {
				return nil, err
			}
			if match {
				count++
			}
		}
		value, err := publicIntResult(count)
		if err != nil {
			return nil, err
		}
		return [][]parser.Value{{value}}, nil
	}
	baseRows := make([][]parser.Value, 0, len(candidateRows))
	for _, row := range candidateRows {
		match, err := evalSelectFilter(row, sel, table)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		baseRows = append(baseRows, row)
	}
	if hasAggregateProjection(sel) {
		// Aggregate SELECT consumes the filtered base rows before any row-by-row
		// projection happens.
		return executeAggregateSelectRows(sel, table, baseRows)
	}
	if err := sortSelectRows(baseRows, sel, table, selectOrderByList(sel)); err != nil {
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

func hasAggregateProjection(sel *runtimeSelectQuery) bool {
	if sel == nil {
		return false
	}
	if sel.isCountStar {
		return true
	}
	for _, expr := range sel.projectionExprs {
		if isAggregateExpr(expr) {
			return true
		}
	}
	return false
}

func validateAggregateProjectionShape(sel *runtimeSelectQuery) error {
	if sel == nil {
		return nil
	}
	// Current aggregate support is intentionally narrow: aggregate projections
	// stand alone and do not combine with ORDER BY or mixed projection shapes.
	if len(sel.orderBys) > 0 || sel.orderBy != nil {
		return errUnsupportedStatement
	}
	if sel.isCountStar {
		return nil
	}
	if len(sel.projectionExprs) == 0 {
		return errUnsupportedStatement
	}
	for _, expr := range sel.projectionExprs {
		if !isAggregateExpr(expr) {
			return errUnsupportedStatement
		}
	}
	return nil
}

func executeAggregateSelectRows(sel *runtimeSelectQuery, table *Table, rows [][]parser.Value) ([][]parser.Value, error) {
	if err := validateAggregateProjectionShape(sel); err != nil {
		return nil, err
	}
	if sel.isCountStar {
		value, err := publicIntResult(int64(len(rows)))
		if err != nil {
			return nil, err
		}
		return [][]parser.Value{{value}}, nil
	}
	out := make([]parser.Value, 0, len(sel.projectionExprs))
	for _, expr := range sel.projectionExprs {
		value, err := evalAggregateExprRows(expr, rows, func(row []parser.Value, expr *runtimeValueExpr) (parser.Value, error) {
			return evalSelectValueExpr(row, sel, table, expr)
		})
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return [][]parser.Value{out}, nil
}

func validateSelectPlan(plan *planner.SelectPlan) error {
	_, err := NewSelectExecutionHandoff(plan)
	return err
}

func ProjectedColumnNames(plan *planner.SelectPlan, table *Table) ([]string, error) {
	handoff, err := NewSelectExecutionHandoff(plan)
	if err == nil {
		return ProjectedColumnNamesWithHandoff(handoff, table)
	}
	if plan == nil || plan.Query == nil || table == nil {
		return nil, errUnsupportedStatement
	}
	// Keep non-runtime planner helpers such as index-only column projection on
	// their existing compatibility path while normal SELECT execution moves to
	// the executor-owned handoff.
	return projectedColumnNames(runtimeSelectQueryFromPlan(plan.Query), table, validateProjectionExprs, resolveSelectColumnIndex)
}

func ProjectedColumnNamesWithHandoff(handoff *SelectExecutionHandoff, table *Table) ([]string, error) {
	bridge, err := selectBridgeFromHandoff(handoff)
	if err != nil || table == nil || bridge.query == nil {
		return nil, errUnsupportedStatement
	}
	return projectedColumnNames(bridge.query, table, validateProjectionExprs, resolveSelectColumnIndex)
}

type selectProjectionExprValidator func(sel *runtimeSelectQuery, table *Table) error
type selectProjectionColumnResolver func(sel *runtimeSelectQuery, name string, table *Table) (int, error)

func projectedColumnNames(sel *runtimeSelectQuery, table *Table, validateExprs selectProjectionExprValidator, resolveColumn selectProjectionColumnResolver) ([]string, error) {
	if sel.isCountStar {
		return []string{"count"}, nil
	}
	if len(sel.projectionExprs) > 0 {
		if err := validateExprs(sel, table); err != nil {
			return nil, err
		}
		names := make([]string, 0, len(sel.projectionExprs))
		for i, expr := range sel.projectionExprs {
			if alias := projectionAliasAt(sel, i); alias != "" {
				names = append(names, alias)
				continue
			}
			if i < len(sel.projectionLabels) && sel.projectionLabels[i] != "" {
				names = append(names, sel.projectionLabels[i])
				continue
			}
			if expr != nil && expr.kind == runtimeValueExprKindColumnRef {
				names = append(names, expr.column)
			} else {
				names = append(names, "expr")
			}
		}
		return names, nil
	}
	if len(sel.columns) == 0 {
		names := make([]string, 0, len(table.Columns))
		for _, column := range table.Columns {
			names = append(names, column.Name)
		}
		return names, nil
	}

	names := make([]string, 0, len(sel.columns))
	for _, name := range sel.columns {
		if _, err := resolveColumn(sel, name, table); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

func projectRow(sel *runtimeSelectQuery, table *Table, row []parser.Value) ([]parser.Value, error) {
	if len(sel.projectionExprs) > 0 {
		out := make([]parser.Value, 0, len(sel.projectionExprs))
		for _, expr := range sel.projectionExprs {
			value, err := evalSelectValueExpr(row, sel, table, expr)
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

func validateProjectionExprs(sel *runtimeSelectQuery, table *Table) error {
	if sel == nil {
		return nil
	}
	if len(sel.projectionExprs) == 0 {
		if len(sel.columns) == 0 {
			return nil
		}
		_, err := resolveSelectColumns(sel, table)
		return err
	}
	for _, expr := range sel.projectionExprs {
		if err := validateSelectValueExprColumns(sel, expr, table); err != nil {
			return err
		}
	}
	return nil
}

func resolveSelectColumns(sel *runtimeSelectQuery, table *Table) ([]int, error) {
	if len(sel.columns) == 0 {
		indexes := make([]int, 0, len(table.Columns))
		for i := range table.Columns {
			indexes = append(indexes, i)
		}
		return indexes, nil
	}

	indexes := make([]int, 0, len(sel.columns))
	for _, name := range sel.columns {
		idx, err := resolveSelectColumnIndex(sel, name, table)
		if err != nil {
			return nil, err
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

func resolveSelectColumnIndex(sel *runtimeSelectQuery, name string, table *Table) (int, error) {
	baseName, err := normalizeSelectQualifiedColumnName(sel, name, table)
	if err != nil {
		return -1, err
	}
	for i, column := range table.Columns {
		if column.Name == baseName {
			return i, nil
		}
	}
	return -1, newColumnNotFoundError(name)
}

func normalizeSelectQualifiedColumnName(sel *runtimeSelectQuery, name string, table *Table) (string, error) {
	if !strings.Contains(name, ".") {
		return name, nil
	}
	parts := strings.Split(name, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", newColumnNotFoundError(name)
	}
	if table == nil {
		return "", newColumnNotFoundError(name)
	}
	tableRef := sel.primaryTableRef()
	if tableRef == nil {
		if parts[0] != table.Name {
			return "", newColumnNotFoundError(name)
		}
		return parts[1], nil
	}
	if parts[0] != tableRef.name && (tableRef.alias == "" || parts[0] != tableRef.alias) {
		return "", newColumnNotFoundError(name)
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

func evalFilter(row []parser.Value, table *Table, predicate *parser.PredicateExpr, where *parser.WhereClause) (bool, error) {
	if predicate != nil {
		return evalPredicate(row, table, predicate)
	}
	return evalWhere(row, table, where)
}

func evalSelectFilter(row []parser.Value, sel *runtimeSelectQuery, table *Table) (bool, error) {
	if sel != nil && sel.predicate != nil {
		return evalSelectPredicate(row, sel, table, sel.predicate)
	}
	if sel != nil {
		return evalSelectWhere(row, sel, table, sel.where)
	}
	return true, nil
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

func evalSelectPredicate(row []parser.Value, sel *runtimeSelectQuery, table *Table, predicate *runtimePredicateExpr) (bool, error) {
	if predicate == nil {
		return true, nil
	}

	switch predicate.kind {
	case runtimePredicateKindComparison:
		if predicate.comparison == nil {
			return false, errUnsupportedStatement
		}
		return evalSelectWhereCondition(row, sel, table, *predicate.comparison)
	case runtimePredicateKindAnd:
		left, err := evalSelectPredicate(row, sel, table, predicate.left)
		if err != nil {
			return false, err
		}
		if !left {
			return false, nil
		}
		return evalSelectPredicate(row, sel, table, predicate.right)
	case runtimePredicateKindOr:
		left, err := evalSelectPredicate(row, sel, table, predicate.left)
		if err != nil {
			return false, err
		}
		if left {
			return true, nil
		}
		return evalSelectPredicate(row, sel, table, predicate.right)
	case runtimePredicateKindNot:
		inner, err := evalSelectPredicate(row, sel, table, predicate.inner)
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

func evalSelectWhere(row []parser.Value, sel *runtimeSelectQuery, table *Table, where *runtimeWhereClause) (bool, error) {
	if where == nil {
		return true, nil
	}
	if len(where.items) == 0 {
		return true, nil
	}

	current, err := evalSelectWhereCondition(row, sel, table, where.items[0].condition)
	if err != nil {
		return false, err
	}

	for _, item := range where.items[1:] {
		next, err := evalSelectWhereCondition(row, sel, table, item.condition)
		if err != nil {
			return false, err
		}
		switch item.op {
		case runtimeBooleanOpAnd:
			current = current && next
		case runtimeBooleanOpOr:
			current = current || next
		default:
			return false, errUnsupportedStatement
		}
	}

	return current, nil
}

func evalSelectWhereCondition(row []parser.Value, sel *runtimeSelectQuery, table *Table, cond runtimeCondition) (bool, error) {
	if cond.leftExpr != nil && cond.rightExpr != nil {
		left, err := evalSelectValueExpr(row, sel, table, cond.leftExpr)
		if err != nil {
			return false, err
		}
		right, err := evalSelectValueExpr(row, sel, table, cond.rightExpr)
		if err != nil {
			return false, err
		}
		return compareValues(cond.operator, left, right)
	}

	idx, err := resolveSelectColumnIndex(sel, cond.left, table)
	if err != nil {
		return false, err
	}

	if cond.rightRef != "" {
		rightIdx, err := resolveSelectColumnIndex(sel, cond.rightRef, table)
		if err != nil {
			return false, err
		}
		return compareValues(cond.operator, row[idx], row[rightIdx])
	}

	return compareValues(cond.operator, row[idx], cond.right)
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

func validateFilterColumns(predicate *parser.PredicateExpr, where *parser.WhereClause, table *Table) error {
	if predicate != nil {
		return validatePredicateColumns(predicate, table)
	}
	return validateWhereColumns(where, table)
}

func validateSelectFilterColumns(sel *runtimeSelectQuery, table *Table) error {
	if sel != nil && sel.predicate != nil {
		return validateSelectPredicateColumns(sel, sel.predicate, table)
	}
	if sel != nil {
		return validateSelectWhereColumns(sel, sel.where, table)
	}
	return nil
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

func validateSelectPredicateColumns(sel *runtimeSelectQuery, predicate *runtimePredicateExpr, table *Table) error {
	if predicate == nil {
		return nil
	}

	switch predicate.kind {
	case runtimePredicateKindComparison:
		if predicate.comparison == nil {
			return errUnsupportedStatement
		}
		if predicate.comparison.leftExpr != nil && predicate.comparison.rightExpr != nil {
			if err := validateSelectValueExprColumns(sel, predicate.comparison.leftExpr, table); err != nil {
				return err
			}
			return validateSelectValueExprColumns(sel, predicate.comparison.rightExpr, table)
		}
		_, err := resolveSelectColumnIndex(sel, predicate.comparison.left, table)
		if err != nil {
			return err
		}
		if predicate.comparison.rightRef != "" {
			_, err = resolveSelectColumnIndex(sel, predicate.comparison.rightRef, table)
		}
		return err
	case runtimePredicateKindAnd, runtimePredicateKindOr:
		if err := validateSelectPredicateColumns(sel, predicate.left, table); err != nil {
			return err
		}
		return validateSelectPredicateColumns(sel, predicate.right, table)
	case runtimePredicateKindNot:
		return validateSelectPredicateColumns(sel, predicate.inner, table)
	default:
		return errUnsupportedStatement
	}
}

func validateSelectWhereColumns(sel *runtimeSelectQuery, where *runtimeWhereClause, table *Table) error {
	if where == nil {
		return nil
	}
	for _, item := range where.items {
		if item.condition.leftExpr != nil && item.condition.rightExpr != nil {
			if err := validateSelectValueExprColumns(sel, item.condition.leftExpr, table); err != nil {
				return err
			}
			if err := validateSelectValueExprColumns(sel, item.condition.rightExpr, table); err != nil {
				return err
			}
			continue
		}
		if _, err := resolveSelectColumnIndex(sel, item.condition.left, table); err != nil {
			return err
		}
		if item.condition.rightRef != "" {
			if _, err := resolveSelectColumnIndex(sel, item.condition.rightRef, table); err != nil {
				return err
			}
		}
	}
	return nil
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
	case parser.ValueExprKindBinary:
		left, err := evalValueExpr(row, table, expr.Left)
		if err != nil {
			return parser.Value{}, err
		}
		right, err := evalValueExpr(row, table, expr.Right)
		if err != nil {
			return parser.Value{}, err
		}
		return evalBinaryValueExpr(int(expr.Op), left, right)
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

func evalSelectValueExpr(row []parser.Value, sel *runtimeSelectQuery, table *Table, expr *runtimeValueExpr) (parser.Value, error) {
	if expr == nil {
		return parser.Value{}, errUnsupportedStatement
	}

	switch expr.kind {
	case runtimeValueExprKindLiteral:
		return expr.value, nil
	case runtimeValueExprKindColumnRef:
		name := expr.column
		if expr.qualifier != "" {
			name = expr.qualifier + "." + expr.column
		}
		idx, err := resolveSelectColumnIndex(sel, name, table)
		if err != nil {
			return parser.Value{}, err
		}
		return row[idx], nil
	case runtimeValueExprKindParen:
		return evalSelectValueExpr(row, sel, table, expr.inner)
	case runtimeValueExprKindBinary:
		left, err := evalSelectValueExpr(row, sel, table, expr.left)
		if err != nil {
			return parser.Value{}, err
		}
		right, err := evalSelectValueExpr(row, sel, table, expr.right)
		if err != nil {
			return parser.Value{}, err
		}
		return evalBinaryValueExpr(int(expr.op), left, right)
	case runtimeValueExprKindFunctionCall:
		arg, err := evalSelectValueExpr(row, sel, table, expr.arg)
		if err != nil {
			return parser.Value{}, err
		}
		return evalScalarFunction(expr.funcName, arg)
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
	case parser.ValueExprKindBinary:
		if err := validateValueExprColumns(expr.Left, table); err != nil {
			return err
		}
		return validateValueExprColumns(expr.Right, table)
	case parser.ValueExprKindFunctionCall:
		return validateValueExprColumns(expr.Arg, table)
	default:
		return errUnsupportedStatement
	}
}

func validateSelectValueExprColumns(sel *runtimeSelectQuery, expr *runtimeValueExpr, table *Table) error {
	if expr == nil {
		return nil
	}
	switch expr.kind {
	case runtimeValueExprKindLiteral:
		return nil
	case runtimeValueExprKindColumnRef:
		name := expr.column
		if expr.qualifier != "" {
			name = expr.qualifier + "." + expr.column
		}
		_, err := resolveSelectColumnIndex(sel, name, table)
		return err
	case runtimeValueExprKindParen:
		return validateSelectValueExprColumns(sel, expr.inner, table)
	case runtimeValueExprKindBinary:
		if err := validateSelectValueExprColumns(sel, expr.left, table); err != nil {
			return err
		}
		return validateSelectValueExprColumns(sel, expr.right, table)
	case runtimeValueExprKindFunctionCall:
		return validateSelectValueExprColumns(sel, expr.arg, table)
	case runtimeValueExprKindAggregateCall:
		if expr.starArg {
			if strings.EqualFold(expr.funcName, "COUNT") {
				return nil
			}
			return errUnsupportedStatement
		}
		return validateSelectValueExprColumns(sel, expr.arg, table)
	default:
		return errUnsupportedStatement
	}
}

func selectOrderByList(sel *runtimeSelectQuery) []runtimeOrderByClause {
	if sel == nil {
		return nil
	}
	if len(sel.orderBys) > 0 {
		return sel.orderBys
	}
	if sel.orderBy != nil {
		return []runtimeOrderByClause{*sel.orderBy}
	}
	return nil
}

func projectionAliasAt(sel *runtimeSelectQuery, idx int) string {
	if sel == nil || idx < 0 || idx >= len(sel.projectionAliases) {
		return ""
	}
	return sel.projectionAliases[idx]
}

func projectionExprForOrderByAlias(sel *runtimeSelectQuery, alias string) *runtimeValueExpr {
	if sel == nil || alias == "" {
		return nil
	}
	for i, expr := range sel.projectionExprs {
		if projectionAliasAt(sel, i) == alias {
			return expr
		}
	}
	return nil
}

func sortSelectRows(rows [][]parser.Value, sel *runtimeSelectQuery, table *Table, orderBys []runtimeOrderByClause) error {
	if len(orderBys) == 0 {
		return nil
	}
	type orderByResolver struct {
		index int
		expr  *runtimeValueExpr
	}
	resolvers := make([]orderByResolver, 0, len(orderBys))
	for _, orderBy := range orderBys {
		if expr := projectionExprForOrderByAlias(sel, orderBy.column); expr != nil {
			resolvers = append(resolvers, orderByResolver{index: -1, expr: expr})
			continue
		}
		idx, err := resolveSelectColumnIndex(sel, orderBy.column, table)
		if err != nil {
			return err
		}
		resolvers = append(resolvers, orderByResolver{index: idx})
	}

	var sortErr error
	sort.SliceStable(rows, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		for idxPos, resolver := range resolvers {
			left := parser.Value{}
			right := parser.Value{}
			var err error
			if resolver.expr != nil {
				left, err = evalSelectValueExpr(rows[i], sel, table, resolver.expr)
				if err != nil {
					sortErr = err
					return false
				}
				right, err = evalSelectValueExpr(rows[j], sel, table, resolver.expr)
				if err != nil {
					sortErr = err
					return false
				}
			} else {
				left = rows[i][resolver.index]
				right = rows[j][resolver.index]
			}
			cmp, err := compareSortableValues(left, right)
			if err != nil {
				sortErr = err
				return false
			}
			if cmp == 0 {
				continue
			}
			if orderBys[idxPos].desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	return sortErr
}
