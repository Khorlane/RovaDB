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
	ref    planner.TableRef
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

	if plan.query.IsCountStar {
		if len(plan.query.OrderBys) > 0 || plan.query.OrderBy != nil {
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

func executeJoinAggregateSelectRows(sel *planner.SelectQuery, rows [][]parser.Value, resolver *joinSelectResolver) ([][]parser.Value, error) {
	if err := validateAggregateProjectionShape(sel); err != nil {
		return nil, err
	}
	if sel.IsCountStar {
		value, err := publicIntResult(int64(len(rows)))
		if err != nil {
			return nil, err
		}
		return [][]parser.Value{{value}}, nil
	}
	out := make([]parser.Value, 0, len(sel.ProjectionExprs))
	for _, expr := range sel.ProjectionExprs {
		value, err := evalAggregateExprRows(expr, rows, func(row []parser.Value, expr *planner.ValueExpr) (parser.Value, error) {
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

func projectedJoinColumnNames(sel *planner.SelectQuery, resolver *joinSelectResolver) ([]string, error) {
	if sel.IsCountStar {
		return []string{"count"}, nil
	}
	if len(sel.ProjectionExprs) > 0 {
		if err := validateJoinProjectionExprs(sel, resolver); err != nil {
			return nil, err
		}
		if len(sel.ProjectionLabels) == len(sel.ProjectionExprs) {
			return append([]string(nil), sel.ProjectionLabels...), nil
		}
		names := make([]string, 0, len(sel.ProjectionExprs))
		for _, expr := range sel.ProjectionExprs {
			if expr != nil && expr.Kind == planner.ValueExprKindColumnRef {
				names = append(names, expr.Column)
			} else {
				names = append(names, "expr")
			}
		}
		return names, nil
	}
	if len(sel.Columns) == 0 {
		return resolver.starColumnNames(), nil
	}
	return append([]string(nil), sel.Columns...), nil
}

func newJoinSelectResolver(sel *planner.SelectQuery, leftTable, rightTable *Table) *joinSelectResolver {
	leftRef := planner.TableRef{Name: leftTable.Name}
	rightRef := planner.TableRef{Name: rightTable.Name}
	if sel != nil {
		if len(sel.From) > 0 {
			leftRef = sel.From[0]
		}
		if len(sel.Joins) > 0 {
			rightRef = sel.Joins[0].Right
		} else if len(sel.From) > 1 {
			rightRef = sel.From[1]
		}
	}

	return &joinSelectResolver{
		sources: []joinSelectSource{
			{ref: leftRef, table: leftTable, offset: 0},
			{ref: rightRef, table: rightTable, offset: len(leftTable.Columns)},
		},
	}
}

func isSupportedJoinSelectShape(sel *planner.SelectQuery) bool {
	if sel == nil {
		return false
	}
	if len(sel.From) == 1 && len(sel.Joins) == 1 {
		return true
	}
	if len(sel.From) == 2 && len(sel.Joins) == 0 {
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
			if parts[0] == source.ref.Name || (source.ref.Alias != "" && parts[0] == source.ref.Alias) {
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
		if tableName == source.ref.Name || (alias != "" && alias == source.ref.Alias) {
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

func validateJoinFilterColumns(sel *planner.SelectQuery, resolver *joinSelectResolver) error {
	if sel != nil && sel.Predicate != nil {
		return validateJoinPredicateColumns(sel.Predicate, resolver)
	}
	if sel != nil {
		return validateJoinWhereColumns(sel.Where, resolver)
	}
	return nil
}

func validateJoinPredicateColumns(predicate *planner.PredicateExpr, resolver *joinSelectResolver) error {
	if predicate == nil {
		return nil
	}
	switch predicate.Kind {
	case planner.PredicateKindComparison:
		if predicate.Comparison == nil {
			return errUnsupportedStatement
		}
		if predicate.Comparison.LeftExpr != nil && predicate.Comparison.RightExpr != nil {
			if err := validateJoinValueExprColumns(predicate.Comparison.LeftExpr, resolver); err != nil {
				return err
			}
			return validateJoinValueExprColumns(predicate.Comparison.RightExpr, resolver)
		}
		if _, err := resolver.resolveColumnIndex(predicate.Comparison.Left); err != nil {
			return err
		}
		if predicate.Comparison.RightRef != "" {
			_, err := resolver.resolveColumnIndex(predicate.Comparison.RightRef)
			return err
		}
		return nil
	case planner.PredicateKindAnd, planner.PredicateKindOr:
		if err := validateJoinPredicateColumns(predicate.Left, resolver); err != nil {
			return err
		}
		return validateJoinPredicateColumns(predicate.Right, resolver)
	case planner.PredicateKindNot:
		return validateJoinPredicateColumns(predicate.Inner, resolver)
	default:
		return errUnsupportedStatement
	}
}

func validateJoinWhereColumns(where *planner.WhereClause, resolver *joinSelectResolver) error {
	if where == nil {
		return nil
	}
	for _, item := range where.Items {
		if item.Condition.LeftExpr != nil && item.Condition.RightExpr != nil {
			if err := validateJoinValueExprColumns(item.Condition.LeftExpr, resolver); err != nil {
				return err
			}
			if err := validateJoinValueExprColumns(item.Condition.RightExpr, resolver); err != nil {
				return err
			}
			continue
		}
		if _, err := resolver.resolveColumnIndex(item.Condition.Left); err != nil {
			return err
		}
		if item.Condition.RightRef != "" {
			if _, err := resolver.resolveColumnIndex(item.Condition.RightRef); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateJoinProjectionExprs(sel *planner.SelectQuery, resolver *joinSelectResolver) error {
	if sel == nil {
		return nil
	}
	if len(sel.ProjectionExprs) == 0 {
		if len(sel.Columns) == 0 {
			return nil
		}
		for _, name := range sel.Columns {
			if _, err := resolver.resolveColumnIndex(name); err != nil {
				return err
			}
		}
		return nil
	}
	for _, expr := range sel.ProjectionExprs {
		if err := validateJoinValueExprColumns(expr, resolver); err != nil {
			return err
		}
	}
	return nil
}

func validateJoinValueExprColumns(expr *planner.ValueExpr, resolver *joinSelectResolver) error {
	if expr == nil {
		return nil
	}
	switch expr.Kind {
	case planner.ValueExprKindLiteral:
		return nil
	case planner.ValueExprKindColumnRef:
		name := expr.Column
		if expr.Qualifier != "" {
			name = expr.Qualifier + "." + expr.Column
		}
		_, err := resolver.resolveColumnIndex(name)
		return err
	case planner.ValueExprKindParen:
		return validateJoinValueExprColumns(expr.Inner, resolver)
	case planner.ValueExprKindBinary:
		if err := validateJoinValueExprColumns(expr.Left, resolver); err != nil {
			return err
		}
		return validateJoinValueExprColumns(expr.Right, resolver)
	case planner.ValueExprKindFunctionCall:
		return validateJoinValueExprColumns(expr.Arg, resolver)
	case planner.ValueExprKindAggregateCall:
		if expr.StarArg {
			if strings.EqualFold(expr.FuncName, "COUNT") {
				return nil
			}
			return errUnsupportedStatement
		}
		return validateJoinValueExprColumns(expr.Arg, resolver)
	default:
		return errUnsupportedStatement
	}
}

func evalJoinPredicateOrWhere(row []parser.Value, sel *planner.SelectQuery, resolver *joinSelectResolver) (bool, error) {
	if sel != nil && sel.Predicate != nil {
		return evalJoinPredicate(row, sel.Predicate, resolver)
	}
	if sel != nil {
		return evalJoinWhere(row, sel.Where, resolver)
	}
	return true, nil
}

func evalJoinPredicate(row []parser.Value, predicate *planner.PredicateExpr, resolver *joinSelectResolver) (bool, error) {
	if predicate == nil {
		return true, nil
	}
	switch predicate.Kind {
	case planner.PredicateKindComparison:
		if predicate.Comparison == nil {
			return false, errUnsupportedStatement
		}
		return evalJoinWhereCondition(row, *predicate.Comparison, resolver)
	case planner.PredicateKindAnd:
		left, err := evalJoinPredicate(row, predicate.Left, resolver)
		if err != nil {
			return false, err
		}
		if !left {
			return false, nil
		}
		return evalJoinPredicate(row, predicate.Right, resolver)
	case planner.PredicateKindOr:
		left, err := evalJoinPredicate(row, predicate.Left, resolver)
		if err != nil {
			return false, err
		}
		if left {
			return true, nil
		}
		return evalJoinPredicate(row, predicate.Right, resolver)
	case planner.PredicateKindNot:
		inner, err := evalJoinPredicate(row, predicate.Inner, resolver)
		if err != nil {
			return false, err
		}
		return !inner, nil
	default:
		return false, errUnsupportedStatement
	}
}

func evalJoinWhere(row []parser.Value, where *planner.WhereClause, resolver *joinSelectResolver) (bool, error) {
	if where == nil {
		return true, nil
	}
	if len(where.Items) == 0 {
		return true, nil
	}
	current, err := evalJoinWhereCondition(row, where.Items[0].Condition, resolver)
	if err != nil {
		return false, err
	}
	for _, item := range where.Items[1:] {
		next, err := evalJoinWhereCondition(row, item.Condition, resolver)
		if err != nil {
			return false, err
		}
		switch item.Op {
		case planner.BooleanOpAnd:
			current = current && next
		case planner.BooleanOpOr:
			current = current || next
		default:
			return false, errUnsupportedStatement
		}
	}
	return current, nil
}

func evalJoinWhereCondition(row []parser.Value, cond planner.Condition, resolver *joinSelectResolver) (bool, error) {
	if cond.LeftExpr != nil && cond.RightExpr != nil {
		left, err := evalJoinValueExpr(row, cond.LeftExpr, resolver)
		if err != nil {
			return false, err
		}
		right, err := evalJoinValueExpr(row, cond.RightExpr, resolver)
		if err != nil {
			return false, err
		}
		return compareValues(cond.Operator, left, right)
	}

	idx, err := resolver.resolveColumnIndex(cond.Left)
	if err != nil {
		return false, err
	}
	if cond.RightRef != "" {
		rightIdx, err := resolver.resolveColumnIndex(cond.RightRef)
		if err != nil {
			return false, err
		}
		return compareValues(cond.Operator, row[idx], row[rightIdx])
	}
	return compareValues(cond.Operator, row[idx], parserValueFromPlan(cond.Right))
}

func evalJoinValueExpr(row []parser.Value, expr *planner.ValueExpr, resolver *joinSelectResolver) (parser.Value, error) {
	if expr == nil {
		return parser.Value{}, errUnsupportedStatement
	}
	switch expr.Kind {
	case planner.ValueExprKindLiteral:
		return parserValueFromPlan(expr.Value), nil
	case planner.ValueExprKindColumnRef:
		name := expr.Column
		if expr.Qualifier != "" {
			name = expr.Qualifier + "." + expr.Column
		}
		idx, err := resolver.resolveColumnIndex(name)
		if err != nil {
			return parser.Value{}, err
		}
		return row[idx], nil
	case planner.ValueExprKindParen:
		return evalJoinValueExpr(row, expr.Inner, resolver)
	case planner.ValueExprKindBinary:
		left, err := evalJoinValueExpr(row, expr.Left, resolver)
		if err != nil {
			return parser.Value{}, err
		}
		right, err := evalJoinValueExpr(row, expr.Right, resolver)
		if err != nil {
			return parser.Value{}, err
		}
		return evalBinaryValueExpr(int(expr.Op), left, right)
	case planner.ValueExprKindFunctionCall:
		arg, err := evalJoinValueExpr(row, expr.Arg, resolver)
		if err != nil {
			return parser.Value{}, err
		}
		return evalScalarFunction(expr.FuncName, arg)
	default:
		return parser.Value{}, errUnsupportedStatement
	}
}

func sortJoinRows(rows [][]parser.Value, sel *planner.SelectQuery, resolver *joinSelectResolver) error {
	if sel == nil {
		return nil
	}
	orderBys := selectOrderByList(sel)
	if len(orderBys) == 0 {
		return nil
	}
	indexes := make([]int, 0, len(orderBys))
	for _, orderBy := range orderBys {
		idx, err := resolver.resolveColumnIndex(orderBy.Column)
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
			if orderBys[idxPos].Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	return sortErr
}

func projectJoinRow(sel *planner.SelectQuery, row []parser.Value, resolver *joinSelectResolver) ([]parser.Value, error) {
	if len(sel.ProjectionExprs) > 0 {
		out := make([]parser.Value, 0, len(sel.ProjectionExprs))
		for _, expr := range sel.ProjectionExprs {
			value, err := evalJoinValueExpr(row, expr, resolver)
			if err != nil {
				return nil, err
			}
			out = append(out, value)
		}
		return out, nil
	}
	if len(sel.Columns) == 0 {
		return append([]parser.Value(nil), row...), nil
	}
	out := make([]parser.Value, 0, len(sel.Columns))
	for _, name := range sel.Columns {
		idx, err := resolver.resolveColumnIndex(name)
		if err != nil {
			return nil, err
		}
		out = append(out, row[idx])
	}
	return out, nil
}
