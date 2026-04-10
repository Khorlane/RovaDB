package executor

import (
	"sort"
	"strings"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

// Join SELECT execution stays runtime-owned here: planner selects the join scan
// shape, and executor owns joined-row resolution, filtering, ordering, and
// materialization from that plan data.

type joinSelectSource struct {
	ref    runtimeTableRef
	table  *Table
	offset int
}

type joinSelectResolver struct {
	sources []joinSelectSource
}

func executeJoinSelect(plan *selectPlanBridge, tables map[string]*Table) ([][]parser.Value, error) {
	if plan == nil || plan.query == nil || plan.scanType != planner.ScanTypeJoin {
		return nil, errInvalidSelectPlan
	}
	if !isSupportedJoinSelectShape(plan.query) {
		return nil, errUnsupportedStatement
	}

	leftTable, rightTable, err := plan.joinTables(tables)
	if err != nil {
		return nil, err
	}

	resolver := newJoinSelectResolver(plan.query, leftTable, rightTable)
	leftIdx, err := resolver.resolveQualifiedColumnIndex(plan.join.leftTableName, plan.join.leftTableAlias, plan.join.leftColumnName)
	if err != nil {
		return nil, err
	}
	rightIdx, err := resolver.resolveQualifiedColumnIndex(plan.join.rightTableName, plan.join.rightTableAlias, plan.join.rightColumnName)
	if err != nil {
		return nil, err
	}

	if err := validateJoinFilterColumns(plan.query, resolver); err != nil {
		return nil, err
	}
	if err := validateJoinProjectionExprs(plan.query, resolver); err != nil {
		return nil, err
	}

	joinedRows := make([][]parser.Value, 0)
	for _, leftRow := range leftTable.Rows {
		for _, rightRow := range rightTable.Rows {
			row := append(append([]parser.Value{}, leftRow...), rightRow...)
			match, err := compareValues("=", row[leftIdx], row[rightIdx])
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
			whereMatch, err := evalJoinPredicateOrWhere(row, plan.query, resolver)
			if err != nil {
				return nil, err
			}
			if !whereMatch {
				continue
			}
			joinedRows = append(joinedRows, row)
		}
	}

	if plan.query.isCountStar {
		if len(plan.query.orderBys) > 0 || plan.query.orderBy != nil {
			return nil, errCountOrderByUnsupported
		}
		value, err := publicIntResult(int64(len(joinedRows)))
		if err != nil {
			return nil, err
		}
		return [][]parser.Value{{value}}, nil
	}
	if hasAggregateProjection(plan.query) {
		return executeJoinAggregateSelectRows(plan.query, joinedRows, resolver)
	}

	if err := sortJoinRows(joinedRows, plan.query, resolver); err != nil {
		return nil, err
	}

	rows := make([][]parser.Value, 0, len(joinedRows))
	for _, row := range joinedRows {
		out, err := projectJoinRow(plan.query, row, resolver)
		if err != nil {
			return nil, err
		}
		rows = append(rows, out)
	}
	return rows, nil
}

func executeJoinAggregateSelectRows(sel *runtimeSelectQuery, rows [][]parser.Value, resolver *joinSelectResolver) ([][]parser.Value, error) {
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
			return evalJoinValueExpr(row, expr, resolver)
		})
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return [][]parser.Value{out}, nil
}

func ProjectedColumnNamesForPlan(plan *planner.SelectPlan, tables map[string]*Table) ([]string, error) {
	bridge, err := bridgeSelectPlan(plan)
	if err != nil {
		return nil, err
	}
	if bridge.scanType != planner.ScanTypeJoin {
		table, err := bridge.singleTable(tables)
		if err != nil {
			return nil, err
		}
		return projectedColumnNames(bridge.query, table, validateProjectionExprs, resolveSelectColumnIndex)
	}
	leftTable, rightTable, err := bridge.joinTables(tables)
	if err != nil {
		return nil, err
	}
	resolver := newJoinSelectResolver(bridge.query, leftTable, rightTable)
	return projectedJoinColumnNames(bridge.query, resolver)
}

func projectedJoinColumnNames(sel *runtimeSelectQuery, resolver *joinSelectResolver) ([]string, error) {
	if sel.isCountStar {
		return []string{"count"}, nil
	}
	if len(sel.projectionExprs) > 0 {
		if err := validateJoinProjectionExprs(sel, resolver); err != nil {
			return nil, err
		}
		if len(sel.projectionLabels) == len(sel.projectionExprs) {
			return append([]string(nil), sel.projectionLabels...), nil
		}
		names := make([]string, 0, len(sel.projectionExprs))
		for _, expr := range sel.projectionExprs {
			if expr != nil && expr.kind == runtimeValueExprKindColumnRef {
				names = append(names, expr.column)
			} else {
				names = append(names, "expr")
			}
		}
		return names, nil
	}
	if len(sel.columns) == 0 {
		return resolver.starColumnNames(), nil
	}
	return append([]string(nil), sel.columns...), nil
}

func newJoinSelectResolver(sel *runtimeSelectQuery, leftTable, rightTable *Table) *joinSelectResolver {
	leftRef := runtimeTableRef{name: leftTable.Name}
	rightRef := runtimeTableRef{name: rightTable.Name}
	if sel != nil {
		if len(sel.from) > 0 {
			leftRef = sel.from[0]
		}
		if len(sel.joins) > 0 {
			rightRef = sel.joins[0].right
		} else if len(sel.from) > 1 {
			rightRef = sel.from[1]
		}
	}

	return &joinSelectResolver{
		sources: []joinSelectSource{
			{ref: leftRef, table: leftTable, offset: 0},
			{ref: rightRef, table: rightTable, offset: len(leftTable.Columns)},
		},
	}
}

func isSupportedJoinSelectShape(sel *runtimeSelectQuery) bool {
	if sel == nil {
		return false
	}
	if len(sel.from) == 1 && len(sel.joins) == 1 {
		return true
	}
	if len(sel.from) == 2 && len(sel.joins) == 0 {
		return true
	}
	return false
}

func (r *joinSelectResolver) resolveColumnIndex(name string) (int, error) {
	if strings.Contains(name, ".") {
		parts := strings.Split(name, ".")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return -1, errColumnDoesNotExist
		}
		for _, source := range r.sources {
			if parts[0] == source.ref.name || (source.ref.alias != "" && parts[0] == source.ref.alias) {
				return resolveColumnOffset(source.table, source.offset, parts[1])
			}
		}
		return -1, errColumnDoesNotExist
	}

	match := -1
	for _, source := range r.sources {
		idx, err := resolveColumnOffset(source.table, source.offset, name)
		if err != nil {
			continue
		}
		if match >= 0 {
			return -1, errColumnDoesNotExist
		}
		match = idx
	}
	if match < 0 {
		return -1, errColumnDoesNotExist
	}
	return match, nil
}

func (r *joinSelectResolver) resolveQualifiedColumnIndex(tableName, alias, columnName string) (int, error) {
	for _, source := range r.sources {
		if tableName == source.ref.name || (alias != "" && alias == source.ref.alias) {
			return resolveColumnOffset(source.table, source.offset, columnName)
		}
	}
	return -1, errColumnDoesNotExist
}

func (r *joinSelectResolver) starColumnNames() []string {
	names := make([]string, 0)
	for _, source := range r.sources {
		for _, column := range source.table.Columns {
			names = append(names, column.Name)
		}
	}
	return names
}

func resolveColumnOffset(table *Table, offset int, name string) (int, error) {
	for i, column := range table.Columns {
		if column.Name == name {
			return offset + i, nil
		}
	}
	return -1, errColumnDoesNotExist
}

func validateJoinFilterColumns(sel *runtimeSelectQuery, resolver *joinSelectResolver) error {
	if sel != nil && sel.predicate != nil {
		return validateJoinPredicateColumns(sel.predicate, resolver)
	}
	if sel != nil {
		return validateJoinWhereColumns(sel.where, resolver)
	}
	return nil
}

func validateJoinPredicateColumns(predicate *runtimePredicateExpr, resolver *joinSelectResolver) error {
	if predicate == nil {
		return nil
	}
	switch predicate.kind {
	case runtimePredicateKindComparison:
		if predicate.comparison == nil {
			return errUnsupportedStatement
		}
		if predicate.comparison.leftExpr != nil && predicate.comparison.rightExpr != nil {
			if err := validateJoinValueExprColumns(predicate.comparison.leftExpr, resolver); err != nil {
				return err
			}
			return validateJoinValueExprColumns(predicate.comparison.rightExpr, resolver)
		}
		if _, err := resolver.resolveColumnIndex(predicate.comparison.left); err != nil {
			return err
		}
		if predicate.comparison.rightRef != "" {
			_, err := resolver.resolveColumnIndex(predicate.comparison.rightRef)
			return err
		}
		return nil
	case runtimePredicateKindAnd, runtimePredicateKindOr:
		if err := validateJoinPredicateColumns(predicate.left, resolver); err != nil {
			return err
		}
		return validateJoinPredicateColumns(predicate.right, resolver)
	case runtimePredicateKindNot:
		return validateJoinPredicateColumns(predicate.inner, resolver)
	default:
		return errUnsupportedStatement
	}
}

func validateJoinWhereColumns(where *runtimeWhereClause, resolver *joinSelectResolver) error {
	if where == nil {
		return nil
	}
	for _, item := range where.items {
		if item.condition.leftExpr != nil && item.condition.rightExpr != nil {
			if err := validateJoinValueExprColumns(item.condition.leftExpr, resolver); err != nil {
				return err
			}
			if err := validateJoinValueExprColumns(item.condition.rightExpr, resolver); err != nil {
				return err
			}
			continue
		}
		if _, err := resolver.resolveColumnIndex(item.condition.left); err != nil {
			return err
		}
		if item.condition.rightRef != "" {
			if _, err := resolver.resolveColumnIndex(item.condition.rightRef); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateJoinProjectionExprs(sel *runtimeSelectQuery, resolver *joinSelectResolver) error {
	if sel == nil {
		return nil
	}
	if len(sel.projectionExprs) == 0 {
		if len(sel.columns) == 0 {
			return nil
		}
		for _, name := range sel.columns {
			if _, err := resolver.resolveColumnIndex(name); err != nil {
				return err
			}
		}
		return nil
	}
	for _, expr := range sel.projectionExprs {
		if err := validateJoinValueExprColumns(expr, resolver); err != nil {
			return err
		}
	}
	return nil
}

func validateJoinValueExprColumns(expr *runtimeValueExpr, resolver *joinSelectResolver) error {
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
		_, err := resolver.resolveColumnIndex(name)
		return err
	case runtimeValueExprKindParen:
		return validateJoinValueExprColumns(expr.inner, resolver)
	case runtimeValueExprKindBinary:
		if err := validateJoinValueExprColumns(expr.left, resolver); err != nil {
			return err
		}
		return validateJoinValueExprColumns(expr.right, resolver)
	case runtimeValueExprKindFunctionCall:
		return validateJoinValueExprColumns(expr.arg, resolver)
	case runtimeValueExprKindAggregateCall:
		if expr.starArg {
			if strings.EqualFold(expr.funcName, "COUNT") {
				return nil
			}
			return errUnsupportedStatement
		}
		return validateJoinValueExprColumns(expr.arg, resolver)
	default:
		return errUnsupportedStatement
	}
}

func evalJoinPredicateOrWhere(row []parser.Value, sel *runtimeSelectQuery, resolver *joinSelectResolver) (bool, error) {
	if sel != nil && sel.predicate != nil {
		return evalJoinPredicate(row, sel.predicate, resolver)
	}
	if sel != nil {
		return evalJoinWhere(row, sel.where, resolver)
	}
	return true, nil
}

func evalJoinPredicate(row []parser.Value, predicate *runtimePredicateExpr, resolver *joinSelectResolver) (bool, error) {
	if predicate == nil {
		return true, nil
	}
	switch predicate.kind {
	case runtimePredicateKindComparison:
		if predicate.comparison == nil {
			return false, errUnsupportedStatement
		}
		return evalJoinWhereCondition(row, *predicate.comparison, resolver)
	case runtimePredicateKindAnd:
		left, err := evalJoinPredicate(row, predicate.left, resolver)
		if err != nil {
			return false, err
		}
		if !left {
			return false, nil
		}
		return evalJoinPredicate(row, predicate.right, resolver)
	case runtimePredicateKindOr:
		left, err := evalJoinPredicate(row, predicate.left, resolver)
		if err != nil {
			return false, err
		}
		if left {
			return true, nil
		}
		return evalJoinPredicate(row, predicate.right, resolver)
	case runtimePredicateKindNot:
		inner, err := evalJoinPredicate(row, predicate.inner, resolver)
		if err != nil {
			return false, err
		}
		return !inner, nil
	default:
		return false, errUnsupportedStatement
	}
}

func evalJoinWhere(row []parser.Value, where *runtimeWhereClause, resolver *joinSelectResolver) (bool, error) {
	if where == nil {
		return true, nil
	}
	if len(where.items) == 0 {
		return true, nil
	}
	current, err := evalJoinWhereCondition(row, where.items[0].condition, resolver)
	if err != nil {
		return false, err
	}
	for _, item := range where.items[1:] {
		next, err := evalJoinWhereCondition(row, item.condition, resolver)
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

func evalJoinWhereCondition(row []parser.Value, cond runtimeCondition, resolver *joinSelectResolver) (bool, error) {
	if cond.leftExpr != nil && cond.rightExpr != nil {
		left, err := evalJoinValueExpr(row, cond.leftExpr, resolver)
		if err != nil {
			return false, err
		}
		right, err := evalJoinValueExpr(row, cond.rightExpr, resolver)
		if err != nil {
			return false, err
		}
		return compareValues(cond.operator, left, right)
	}

	idx, err := resolver.resolveColumnIndex(cond.left)
	if err != nil {
		return false, err
	}
	if cond.rightRef != "" {
		rightIdx, err := resolver.resolveColumnIndex(cond.rightRef)
		if err != nil {
			return false, err
		}
		return compareValues(cond.operator, row[idx], row[rightIdx])
	}
	return compareValues(cond.operator, row[idx], cond.right)
}

func evalJoinValueExpr(row []parser.Value, expr *runtimeValueExpr, resolver *joinSelectResolver) (parser.Value, error) {
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
		idx, err := resolver.resolveColumnIndex(name)
		if err != nil {
			return parser.Value{}, err
		}
		return row[idx], nil
	case runtimeValueExprKindParen:
		return evalJoinValueExpr(row, expr.inner, resolver)
	case runtimeValueExprKindBinary:
		left, err := evalJoinValueExpr(row, expr.left, resolver)
		if err != nil {
			return parser.Value{}, err
		}
		right, err := evalJoinValueExpr(row, expr.right, resolver)
		if err != nil {
			return parser.Value{}, err
		}
		return evalBinaryValueExpr(int(expr.op), left, right)
	case runtimeValueExprKindFunctionCall:
		arg, err := evalJoinValueExpr(row, expr.arg, resolver)
		if err != nil {
			return parser.Value{}, err
		}
		return evalScalarFunction(expr.funcName, arg)
	default:
		return parser.Value{}, errUnsupportedStatement
	}
}

func sortJoinRows(rows [][]parser.Value, sel *runtimeSelectQuery, resolver *joinSelectResolver) error {
	if sel == nil {
		return nil
	}
	orderBys := selectOrderByList(sel)
	if len(orderBys) == 0 {
		return nil
	}
	indexes := make([]int, 0, len(orderBys))
	for _, orderBy := range orderBys {
		idx, err := resolver.resolveColumnIndex(orderBy.column)
		if err != nil {
			return err
		}
		indexes = append(indexes, idx)
	}
	var sortErr error
	sort.SliceStable(rows, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		for idxPos, idx := range indexes {
			cmp, err := compareSortableValues(rows[i][idx], rows[j][idx])
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

func projectJoinRow(sel *runtimeSelectQuery, row []parser.Value, resolver *joinSelectResolver) ([]parser.Value, error) {
	if len(sel.projectionExprs) > 0 {
		out := make([]parser.Value, 0, len(sel.projectionExprs))
		for _, expr := range sel.projectionExprs {
			value, err := evalJoinValueExpr(row, expr, resolver)
			if err != nil {
				return nil, err
			}
			out = append(out, value)
		}
		return out, nil
	}
	if len(sel.columns) == 0 {
		return append([]parser.Value(nil), row...), nil
	}
	out := make([]parser.Value, 0, len(sel.columns))
	for _, name := range sel.columns {
		idx, err := resolver.resolveColumnIndex(name)
		if err != nil {
			return nil, err
		}
		out = append(out, row[idx])
	}
	return out, nil
}
