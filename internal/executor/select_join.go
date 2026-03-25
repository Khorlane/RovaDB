package executor

import (
	"sort"
	"strings"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

type joinSelectSource struct {
	ref    parser.TableRef
	table  *Table
	offset int
}

type joinSelectResolver struct {
	sources []joinSelectSource
}

func executeJoinSelect(plan *planner.SelectPlan, tables map[string]*Table) ([][]parser.Value, error) {
	if plan == nil || plan.Stmt == nil || plan.JoinScan == nil {
		return nil, errInvalidSelectPlan
	}
	if len(plan.Stmt.From) != 1 || len(plan.Stmt.Joins) != 1 {
		return nil, errUnsupportedStatement
	}

	leftTable := tables[plan.JoinScan.LeftTableName]
	rightTable := tables[plan.JoinScan.RightTableName]
	if leftTable == nil || rightTable == nil {
		return nil, errTableDoesNotExist
	}

	resolver := newJoinSelectResolver(plan.Stmt, leftTable, rightTable)
	leftIdx, err := resolver.resolveQualifiedColumnIndex(plan.JoinScan.LeftTableName, plan.JoinScan.LeftTableAlias, plan.JoinScan.LeftColumnName)
	if err != nil {
		return nil, err
	}
	rightIdx, err := resolver.resolveQualifiedColumnIndex(plan.JoinScan.RightTableName, plan.JoinScan.RightTableAlias, plan.JoinScan.RightColumnName)
	if err != nil {
		return nil, err
	}

	if err := validateJoinPredicateOrWhereColumns(plan.Stmt, resolver); err != nil {
		return nil, err
	}
	if err := validateJoinProjectionExprs(plan.Stmt, resolver); err != nil {
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
			whereMatch, err := evalJoinPredicateOrWhere(row, plan.Stmt, resolver)
			if err != nil {
				return nil, err
			}
			if !whereMatch {
				continue
			}
			joinedRows = append(joinedRows, row)
		}
	}

	if plan.Stmt.IsCountStar {
		if plan.Stmt.OrderBy != nil {
			return nil, errCountOrderByUnsupported
		}
		return [][]parser.Value{{parser.Int64Value(int64(len(joinedRows)))}}, nil
	}

	if err := sortJoinRows(joinedRows, plan.Stmt, resolver); err != nil {
		return nil, err
	}

	rows := make([][]parser.Value, 0, len(joinedRows))
	for _, row := range joinedRows {
		out, err := projectJoinRow(plan.Stmt, row, resolver)
		if err != nil {
			return nil, err
		}
		rows = append(rows, out)
	}
	return rows, nil
}

func ProjectedColumnNamesForPlan(plan *planner.SelectPlan, tables map[string]*Table) ([]string, error) {
	if plan == nil || plan.Stmt == nil {
		return nil, errUnsupportedStatement
	}
	if plan.ScanType != planner.ScanTypeJoin {
		return ProjectedColumnNames(plan, tables[plan.Stmt.TableName])
	}
	if plan.JoinScan == nil {
		return nil, errInvalidSelectPlan
	}
	leftTable := tables[plan.JoinScan.LeftTableName]
	rightTable := tables[plan.JoinScan.RightTableName]
	if leftTable == nil || rightTable == nil {
		return nil, errTableDoesNotExist
	}
	resolver := newJoinSelectResolver(plan.Stmt, leftTable, rightTable)

	sel := plan.Stmt
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
			if expr != nil && expr.Kind == parser.ValueExprKindColumnRef {
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

func newJoinSelectResolver(sel *parser.SelectExpr, leftTable, rightTable *Table) *joinSelectResolver {
	leftRef := parser.TableRef{Name: leftTable.Name}
	rightRef := parser.TableRef{Name: rightTable.Name}
	if sel != nil {
		if len(sel.From) > 0 {
			leftRef = sel.From[0]
		}
		if len(sel.Joins) > 0 {
			rightRef = sel.Joins[0].Right
		}
	}

	return &joinSelectResolver{
		sources: []joinSelectSource{
			{ref: leftRef, table: leftTable, offset: 0},
			{ref: rightRef, table: rightTable, offset: len(leftTable.Columns)},
		},
	}
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

func validateJoinPredicateOrWhereColumns(sel *parser.SelectExpr, resolver *joinSelectResolver) error {
	if sel != nil && sel.Predicate != nil {
		return validateJoinPredicateColumns(sel.Predicate, resolver)
	}
	if sel != nil {
		return validateJoinWhereColumns(sel.Where, resolver)
	}
	return nil
}

func validateJoinPredicateColumns(predicate *parser.PredicateExpr, resolver *joinSelectResolver) error {
	if predicate == nil {
		return nil
	}
	switch predicate.Kind {
	case parser.PredicateKindComparison:
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
	case parser.PredicateKindAnd, parser.PredicateKindOr:
		if err := validateJoinPredicateColumns(predicate.Left, resolver); err != nil {
			return err
		}
		return validateJoinPredicateColumns(predicate.Right, resolver)
	case parser.PredicateKindNot:
		return validateJoinPredicateColumns(predicate.Inner, resolver)
	default:
		return errUnsupportedStatement
	}
}

func validateJoinWhereColumns(where *parser.WhereClause, resolver *joinSelectResolver) error {
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

func validateJoinProjectionExprs(sel *parser.SelectExpr, resolver *joinSelectResolver) error {
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

func validateJoinValueExprColumns(expr *parser.ValueExpr, resolver *joinSelectResolver) error {
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
		_, err := resolver.resolveColumnIndex(name)
		return err
	case parser.ValueExprKindParen:
		return validateJoinValueExprColumns(expr.Inner, resolver)
	case parser.ValueExprKindFunctionCall:
		return validateJoinValueExprColumns(expr.Arg, resolver)
	default:
		return errUnsupportedStatement
	}
}

func evalJoinPredicateOrWhere(row []parser.Value, sel *parser.SelectExpr, resolver *joinSelectResolver) (bool, error) {
	if sel != nil && sel.Predicate != nil {
		return evalJoinPredicate(row, sel.Predicate, resolver)
	}
	if sel != nil {
		return evalJoinWhere(row, sel.Where, resolver)
	}
	return true, nil
}

func evalJoinPredicate(row []parser.Value, predicate *parser.PredicateExpr, resolver *joinSelectResolver) (bool, error) {
	if predicate == nil {
		return true, nil
	}
	switch predicate.Kind {
	case parser.PredicateKindComparison:
		if predicate.Comparison == nil {
			return false, errUnsupportedStatement
		}
		return evalJoinWhereCondition(row, *predicate.Comparison, resolver)
	case parser.PredicateKindAnd:
		left, err := evalJoinPredicate(row, predicate.Left, resolver)
		if err != nil {
			return false, err
		}
		if !left {
			return false, nil
		}
		return evalJoinPredicate(row, predicate.Right, resolver)
	case parser.PredicateKindOr:
		left, err := evalJoinPredicate(row, predicate.Left, resolver)
		if err != nil {
			return false, err
		}
		if left {
			return true, nil
		}
		return evalJoinPredicate(row, predicate.Right, resolver)
	case parser.PredicateKindNot:
		inner, err := evalJoinPredicate(row, predicate.Inner, resolver)
		if err != nil {
			return false, err
		}
		return !inner, nil
	default:
		return false, errUnsupportedStatement
	}
}

func evalJoinWhere(row []parser.Value, where *parser.WhereClause, resolver *joinSelectResolver) (bool, error) {
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

func evalJoinWhereCondition(row []parser.Value, cond parser.Condition, resolver *joinSelectResolver) (bool, error) {
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
	return compareValues(cond.Operator, row[idx], cond.Right)
}

func evalJoinValueExpr(row []parser.Value, expr *parser.ValueExpr, resolver *joinSelectResolver) (parser.Value, error) {
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
		idx, err := resolver.resolveColumnIndex(name)
		if err != nil {
			return parser.Value{}, err
		}
		return row[idx], nil
	case parser.ValueExprKindParen:
		return evalJoinValueExpr(row, expr.Inner, resolver)
	case parser.ValueExprKindFunctionCall:
		arg, err := evalJoinValueExpr(row, expr.Arg, resolver)
		if err != nil {
			return parser.Value{}, err
		}
		return evalScalarFunction(expr.FuncName, arg)
	default:
		return parser.Value{}, errUnsupportedStatement
	}
}

func sortJoinRows(rows [][]parser.Value, sel *parser.SelectExpr, resolver *joinSelectResolver) error {
	if sel == nil || sel.OrderBy == nil {
		return nil
	}
	idx, err := resolver.resolveColumnIndex(sel.OrderBy.Column)
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
		if sel.OrderBy.Desc {
			return cmp > 0
		}
		return cmp < 0
	})
	return sortErr
}

func projectJoinRow(sel *parser.SelectExpr, row []parser.Value, resolver *joinSelectResolver) ([]parser.Value, error) {
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
