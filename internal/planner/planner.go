package planner

import (
	"strings"

	"github.com/Khorlane/RovaDB/internal/parser"
)

// This file owns parsed-statement-to-plan translation only. Planner still
// consumes parser AST directly here; v0.41 may tighten that handoff earlier,
// but parser-owned structures should stop at the earliest honest planning boundary.

// TableMetadata is the minimal planner-side table info used for scan choice.
type TableMetadata struct {
	SimpleIndexes map[string]SimpleIndex
}

// SimpleIndex is the minimal logical metadata needed for simple equality planning.
type SimpleIndex struct {
	TableName  string
	ColumnName string
	IndexID    uint32
	RootPageID uint32
}

// PlanSelect creates a basic execution plan for SELECT.
// Current behavior defaults to table scan unless optional metadata supports
// a simple equality index scan decision.
func PlanSelect(stmt *parser.SelectExpr, tables ...map[string]*TableMetadata) (*SelectPlan, error) {
	if stmt == nil {
		return nil, newPlanError("unsupported query form")
	}

	query := queryFromParser(stmt)
	plan := &SelectPlan{
		Query: query,
	}
	if joinScan, ok := chooseJoinScan(query); ok {
		plan.ScanType = ScanTypeJoin
		plan.JoinScan = joinScan
		return plan, nil
	}
	if len(query.From) > 1 || len(query.Joins) > 0 {
		return nil, newPlanError("unsupported query form")
	}
	if query != nil && query.TableName != "" {
		if indexOnlyScan := chooseIndexOnlyScan(query, firstTableMetadata(tables)); indexOnlyScan != nil {
			plan.ScanType = ScanTypeIndexOnly
			plan.IndexOnlyScan = indexOnlyScan
			return plan, nil
		}
		if indexScan := chooseIndexScan(query, firstTableMetadata(tables)); indexScan != nil {
			plan.ScanType = ScanTypeIndex
			plan.IndexScan = indexScan
			return plan, nil
		}
		plan.ScanType = ScanTypeTable
		plan.TableScan = &TableScan{TableName: query.TableName}
	}
	return plan, nil
}

func chooseIndexOnlyScan(query *SelectQuery, tables map[string]*TableMetadata) *IndexOnlyScan {
	// Index-only remains a special seam path in this milestone line and is
	// intentionally isolated until the outer seam is regularized.
	if query == nil || query.TableName == "" || tables == nil {
		return nil
	}
	if len(query.From) > 1 || len(query.Joins) > 0 {
		return nil
	}

	table := tables[query.TableName]
	if table == nil {
		return nil
	}

	if query.IsCountStar {
		if query.Where != nil || query.Predicate != nil || len(query.OrderBys) > 0 || query.OrderBy != nil {
			return nil
		}
		columnName, ok := firstEligibleSimpleIndexName(table)
		if !ok {
			return nil
		}
		return &IndexOnlyScan{
			TableName:   query.TableName,
			ColumnNames: []string{columnName},
			CountStar:   true,
		}
	}

	columnName, ok := simpleIndexOnlyProjectionColumn(query)
	if !ok {
		return nil
	}
	if _, ok := eligibleSimpleIndexForColumn(table, query.TableName, columnName); !ok {
		return nil
	}
	return &IndexOnlyScan{
		TableName:   query.TableName,
		ColumnNames: []string{columnName},
	}
}

func firstEligibleSimpleIndexName(table *TableMetadata) (string, bool) {
	if table == nil || len(table.SimpleIndexes) == 0 {
		return "", false
	}
	best := ""
	for columnName, index := range table.SimpleIndexes {
		if _, ok := eligibleSimpleIndexForColumn(table, index.TableName, columnName); !ok {
			continue
		}
		if best == "" || columnName < best {
			best = columnName
		}
	}
	if best == "" {
		return "", false
	}
	return best, true
}

func simpleIndexOnlyProjectionColumn(query *SelectQuery) (string, bool) {
	if query == nil || query.IsCountStar {
		return "", false
	}
	if query.Where != nil || query.Predicate != nil {
		return "", false
	}
	if len(query.OrderBys) > 0 || query.OrderBy != nil {
		return "", false
	}
	if len(query.ProjectionExprs) != 1 {
		return "", false
	}
	if len(query.ProjectionAliases) > 0 && query.ProjectionAliases[0] != "" {
		return "", false
	}
	expr := query.ProjectionExprs[0]
	if expr == nil || expr.Kind != ValueExprKindColumnRef || expr.Column == "" {
		return "", false
	}
	normalized, ok := normalizePlannerColumnName(columnRefName(expr), query.PrimaryTableRef())
	if !ok || normalized == "" {
		return "", false
	}
	return normalized, true
}

func columnRefName(expr *ValueExpr) string {
	if expr == nil {
		return ""
	}
	if expr.Qualifier != "" {
		return expr.Qualifier + "." + expr.Column
	}
	return expr.Column
}

func chooseJoinScan(query *SelectQuery) (*JoinScan, bool) {
	if query == nil {
		return nil, false
	}

	if len(query.From) == 1 && len(query.Joins) == 1 {
		join := query.Joins[0]
		if join.Predicate == nil || join.Predicate.Kind != PredicateKindComparison || join.Predicate.Comparison == nil {
			return nil, false
		}
		cond := join.Predicate.Comparison
		if cond.Operator != "=" || cond.LeftExpr == nil || cond.RightExpr == nil {
			return nil, false
		}
		_, leftName, ok := valueExprOperandShape(cond.LeftExpr)
		if !ok || leftName == "" {
			return nil, false
		}
		_, rightName, ok := valueExprOperandShape(cond.RightExpr)
		if !ok || rightName == "" {
			return nil, false
		}

		leftRef := query.From[0]
		rightRef := join.Right
		return joinScanFromColumnPair(leftRef, rightRef, leftName, rightName)
	}

	if len(query.From) == 2 && len(query.Joins) == 0 {
		leftRef := query.From[0]
		rightRef := query.From[1]
		leftName, rightName, ok := commaJoinEqualityColumns(query)
		if !ok {
			return nil, false
		}
		return joinScanFromColumnPair(leftRef, rightRef, leftName, rightName)
	}

	return nil, false
}

func commaJoinEqualityColumns(query *SelectQuery) (string, string, bool) {
	if query == nil {
		return "", "", false
	}
	if query.Predicate != nil {
		if left, right, ok := findJoinEqualityInPredicate(query.Predicate); ok {
			return left, right, true
		}
	}
	return findJoinEqualityInWhere(query.Where)
}

func findJoinEqualityInPredicate(predicate *PredicateExpr) (string, string, bool) {
	if predicate == nil {
		return "", "", false
	}
	switch predicate.Kind {
	case PredicateKindComparison:
		if predicate.Comparison == nil || predicate.Comparison.Operator != "=" {
			return "", "", false
		}
		return joinEqualityColumnsFromCondition(*predicate.Comparison)
	case PredicateKindAnd, PredicateKindOr:
		if left, right, ok := findJoinEqualityInPredicate(predicate.Left); ok {
			return left, right, true
		}
		return findJoinEqualityInPredicate(predicate.Right)
	case PredicateKindNot:
		return findJoinEqualityInPredicate(predicate.Inner)
	default:
		return "", "", false
	}
}

func findJoinEqualityInWhere(where *WhereClause) (string, string, bool) {
	if where == nil {
		return "", "", false
	}
	for _, item := range where.Items {
		left, right, ok := joinEqualityColumnsFromCondition(item.Condition)
		if ok {
			return left, right, true
		}
	}
	return "", "", false
}

func joinEqualityColumnsFromCondition(cond Condition) (string, string, bool) {
	if cond.Operator != "=" {
		return "", "", false
	}
	if cond.LeftExpr != nil && cond.RightExpr != nil {
		_, leftName, ok := valueExprOperandShape(cond.LeftExpr)
		if !ok || leftName == "" {
			return "", "", false
		}
		_, rightName, ok := valueExprOperandShape(cond.RightExpr)
		if !ok || rightName == "" {
			return "", "", false
		}
		return leftName, rightName, true
	}
	if cond.Left == "" || cond.RightRef == "" {
		return "", "", false
	}
	return cond.Left, cond.RightRef, true
}

func joinScanFromColumnPair(leftRef, rightRef TableRef, leftName, rightName string) (*JoinScan, bool) {
	leftColumn, okLeft := normalizeJoinColumnName(leftName, leftRef)
	rightColumn, okRight := normalizeJoinColumnName(rightName, rightRef)
	if okLeft && okRight {
		return &JoinScan{
			LeftTableName:   leftRef.Name,
			LeftTableAlias:  leftRef.Alias,
			LeftColumnName:  leftColumn,
			RightTableName:  rightRef.Name,
			RightTableAlias: rightRef.Alias,
			RightColumnName: rightColumn,
		}, true
	}

	leftColumn, okLeft = normalizeJoinColumnName(leftName, rightRef)
	rightColumn, okRight = normalizeJoinColumnName(rightName, leftRef)
	if okLeft && okRight {
		return &JoinScan{
			LeftTableName:   leftRef.Name,
			LeftTableAlias:  leftRef.Alias,
			LeftColumnName:  rightColumn,
			RightTableName:  rightRef.Name,
			RightTableAlias: rightRef.Alias,
			RightColumnName: leftColumn,
		}, true
	}

	return nil, false
}

func normalizeJoinColumnName(name string, tableRef TableRef) (string, bool) {
	parts := strings.Split(name, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", false
	}
	if parts[0] != tableRef.Name && (tableRef.Alias == "" || parts[0] != tableRef.Alias) {
		return "", false
	}
	return parts[1], true
}

func firstTableMetadata(tables []map[string]*TableMetadata) map[string]*TableMetadata {
	if len(tables) == 0 {
		return nil
	}
	return tables[0]
}

func chooseIndexScan(query *SelectQuery, tables map[string]*TableMetadata) *IndexScan {
	if query == nil || query.TableName == "" || tables == nil {
		return nil
	}

	columnName, value, ok := indexedEquality(query)
	if !ok {
		return nil
	}

	table := tables[query.TableName]
	if table == nil {
		return nil
	}
	index, ok := eligibleSimpleIndexForColumn(table, query.TableName, columnName)
	if !ok {
		return nil
	}

	return &IndexScan{
		TableName:   index.TableName,
		ColumnName:  index.ColumnName,
		LookupValue: value,
	}
}

func eligibleSimpleIndexForColumn(table *TableMetadata, tableName, columnName string) (SimpleIndex, bool) {
	if table == nil || table.SimpleIndexes == nil || tableName == "" || columnName == "" {
		return SimpleIndex{}, false
	}
	index, ok := table.SimpleIndexes[columnName]
	if !ok {
		return SimpleIndex{}, false
	}
	if index.TableName != tableName || index.ColumnName != columnName || index.IndexID == 0 || index.RootPageID == 0 {
		return SimpleIndex{}, false
	}
	return index, true
}

func indexedEquality(query *SelectQuery) (string, Value, bool) {
	if query == nil {
		return "", Value{}, false
	}
	tableRef := query.PrimaryTableRef()
	if query.Predicate != nil {
		return indexedEqualityFromPredicate(query.Predicate, tableRef)
	}
	return indexedEqualityFromWhere(query.Where, tableRef)
}

func indexedEqualityFromPredicate(predicate *PredicateExpr, tableRef *TableRef) (string, Value, bool) {
	if predicate == nil || predicate.Kind != PredicateKindComparison || predicate.Comparison == nil {
		return "", Value{}, false
	}
	if predicate.Comparison.Operator != "=" {
		return "", Value{}, false
	}
	if predicate.Comparison.LeftExpr != nil && predicate.Comparison.RightExpr != nil {
		leftValue, leftColumn, ok := valueExprOperandShape(predicate.Comparison.LeftExpr)
		if !ok || leftColumn == "" || leftValue.Kind != ValueKindInvalid {
			return "", Value{}, false
		}
		rightValue, rightColumn, ok := valueExprOperandShape(predicate.Comparison.RightExpr)
		if !ok || rightColumn != "" {
			return "", Value{}, false
		}
		normalized, ok := normalizePlannerColumnName(leftColumn, tableRef)
		if !ok {
			return "", Value{}, false
		}
		return normalized, rightValue, true
	}
	if predicate.Comparison.RightRef != "" {
		return "", Value{}, false
	}
	normalized, ok := normalizePlannerColumnName(predicate.Comparison.Left, tableRef)
	if !ok {
		return "", Value{}, false
	}
	return normalized, predicate.Comparison.Right, true
}

func valueExprOperandShape(expr *ValueExpr) (Value, string, bool) {
	if expr == nil {
		return Value{}, "", false
	}

	switch expr.Kind {
	case ValueExprKindLiteral:
		return expr.Value, "", true
	case ValueExprKindColumnRef:
		if expr.Qualifier != "" {
			return Value{}, expr.Qualifier + "." + expr.Column, true
		}
		return Value{}, expr.Column, true
	case ValueExprKindParen:
		return valueExprOperandShape(expr.Inner)
	default:
		return Value{}, "", false
	}
}

func normalizePlannerColumnName(name string, tableRef *TableRef) (string, bool) {
	if !strings.Contains(name, ".") {
		return name, true
	}
	parts := strings.Split(name, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", false
	}
	if tableRef == nil {
		return "", false
	}
	if parts[0] != tableRef.Name && (tableRef.Alias == "" || parts[0] != tableRef.Alias) {
		return "", false
	}
	return parts[1], true
}

func indexedEqualityFromWhere(where *WhereClause, tableRef *TableRef) (string, Value, bool) {
	if where == nil || len(where.Items) != 1 {
		return "", Value{}, false
	}

	item := where.Items[0]
	if item.Op != "" || item.Condition.Operator != "=" {
		return "", Value{}, false
	}

	normalized, ok := normalizePlannerColumnName(item.Condition.Left, tableRef)
	if !ok {
		return "", Value{}, false
	}
	return normalized, item.Condition.Right, true
}
