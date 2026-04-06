package planner

import (
	"strings"

	"github.com/Khorlane/RovaDB/internal/parser"
)

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

	plan := &SelectPlan{
		Stmt: stmt,
	}
	if joinScan, ok := chooseJoinScan(stmt); ok {
		plan.ScanType = ScanTypeJoin
		plan.JoinScan = joinScan
		return plan, nil
	}
	if len(stmt.From) > 1 || len(stmt.Joins) > 0 {
		return nil, newPlanError("unsupported query form")
	}
	if stmt != nil && stmt.TableName != "" {
		if indexOnlyScan := chooseIndexOnlyScan(stmt, firstTableMetadata(tables)); indexOnlyScan != nil {
			plan.ScanType = ScanTypeIndexOnly
			plan.IndexOnlyScan = indexOnlyScan
			return plan, nil
		}
		if indexScan := chooseIndexScan(stmt, firstTableMetadata(tables)); indexScan != nil {
			plan.ScanType = ScanTypeIndex
			plan.IndexScan = indexScan
			return plan, nil
		}
		plan.ScanType = ScanTypeTable
		plan.TableScan = &TableScan{TableName: stmt.TableName}
	}
	return plan, nil
}

func chooseIndexOnlyScan(stmt *parser.SelectExpr, tables map[string]*TableMetadata) *IndexOnlyScan {
	if stmt == nil || stmt.TableName == "" || tables == nil {
		return nil
	}
	if len(stmt.From) > 1 || len(stmt.Joins) > 0 {
		return nil
	}

	table := tables[stmt.TableName]
	if table == nil {
		return nil
	}

	if stmt.IsCountStar {
		if _, ok := firstEligibleSimpleIndexName(table); !ok {
			return nil
		}
		return &IndexOnlyScan{
			TableName: stmt.TableName,
			CountStar: true,
		}
	}

	columnName, ok := simpleIndexOnlyProjectionColumn(stmt)
	if !ok {
		return nil
	}
	if _, ok := eligibleSimpleIndexForColumn(table, stmt.TableName, columnName); !ok {
		return nil
	}
	return &IndexOnlyScan{
		TableName:   stmt.TableName,
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

func simpleIndexOnlyProjectionColumn(stmt *parser.SelectExpr) (string, bool) {
	if stmt == nil || stmt.IsCountStar {
		return "", false
	}
	if stmt.Where != nil || stmt.Predicate != nil {
		return "", false
	}
	if len(stmt.OrderBys) > 0 || stmt.OrderBy != nil {
		return "", false
	}
	if len(stmt.ProjectionExprs) != 1 {
		return "", false
	}
	expr := stmt.ProjectionExprs[0]
	if expr == nil || expr.Kind != parser.ValueExprKindColumnRef || expr.Column == "" {
		return "", false
	}
	normalized, ok := normalizePlannerColumnName(columnRefName(expr), stmt.PrimaryTableRef())
	if !ok || normalized == "" {
		return "", false
	}
	return normalized, true
}

func columnRefName(expr *parser.ValueExpr) string {
	if expr == nil {
		return ""
	}
	if expr.Qualifier != "" {
		return expr.Qualifier + "." + expr.Column
	}
	return expr.Column
}

func chooseJoinScan(stmt *parser.SelectExpr) (*JoinScan, bool) {
	if stmt == nil {
		return nil, false
	}

	if len(stmt.From) == 1 && len(stmt.Joins) == 1 {
		join := stmt.Joins[0]
		if join.Predicate == nil || join.Predicate.Kind != parser.PredicateKindComparison || join.Predicate.Comparison == nil {
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

		leftRef := stmt.From[0]
		rightRef := join.Right
		return joinScanFromColumnPair(leftRef, rightRef, leftName, rightName)
	}

	if len(stmt.From) == 2 && len(stmt.Joins) == 0 {
		leftRef := stmt.From[0]
		rightRef := stmt.From[1]
		leftName, rightName, ok := commaJoinEqualityColumns(stmt)
		if !ok {
			return nil, false
		}
		return joinScanFromColumnPair(leftRef, rightRef, leftName, rightName)
	}

	return nil, false
}

func commaJoinEqualityColumns(stmt *parser.SelectExpr) (string, string, bool) {
	if stmt == nil {
		return "", "", false
	}
	if stmt.Predicate != nil {
		if left, right, ok := findJoinEqualityInPredicate(stmt.Predicate); ok {
			return left, right, true
		}
	}
	return findJoinEqualityInWhere(stmt.Where)
}

func findJoinEqualityInPredicate(predicate *parser.PredicateExpr) (string, string, bool) {
	if predicate == nil {
		return "", "", false
	}
	switch predicate.Kind {
	case parser.PredicateKindComparison:
		if predicate.Comparison == nil || predicate.Comparison.Operator != "=" {
			return "", "", false
		}
		return joinEqualityColumnsFromCondition(*predicate.Comparison)
	case parser.PredicateKindAnd, parser.PredicateKindOr:
		if left, right, ok := findJoinEqualityInPredicate(predicate.Left); ok {
			return left, right, true
		}
		return findJoinEqualityInPredicate(predicate.Right)
	case parser.PredicateKindNot:
		return findJoinEqualityInPredicate(predicate.Inner)
	default:
		return "", "", false
	}
}

func findJoinEqualityInWhere(where *parser.WhereClause) (string, string, bool) {
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

func joinEqualityColumnsFromCondition(cond parser.Condition) (string, string, bool) {
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

func joinScanFromColumnPair(leftRef, rightRef parser.TableRef, leftName, rightName string) (*JoinScan, bool) {
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

func normalizeJoinColumnName(name string, tableRef parser.TableRef) (string, bool) {
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

func chooseIndexScan(stmt *parser.SelectExpr, tables map[string]*TableMetadata) *IndexScan {
	if stmt == nil || stmt.TableName == "" || tables == nil {
		return nil
	}

	columnName, value, ok := indexedEquality(stmt)
	if !ok {
		return nil
	}

	table := tables[stmt.TableName]
	if table == nil {
		return nil
	}
	index, ok := eligibleSimpleIndexForColumn(table, stmt.TableName, columnName)
	if !ok {
		return nil
	}

	return &IndexScan{
		TableName:  index.TableName,
		ColumnName: index.ColumnName,
		Value:      value,
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

func indexedEquality(stmt *parser.SelectExpr) (string, parser.Value, bool) {
	if stmt == nil {
		return "", parser.Value{}, false
	}
	tableRef := stmt.PrimaryTableRef()
	if stmt.Predicate != nil {
		return indexedEqualityFromPredicate(stmt.Predicate, tableRef)
	}
	return indexedEqualityFromWhere(stmt.Where, tableRef)
}

func indexedEqualityFromPredicate(predicate *parser.PredicateExpr, tableRef *parser.TableRef) (string, parser.Value, bool) {
	if predicate == nil || predicate.Kind != parser.PredicateKindComparison || predicate.Comparison == nil {
		return "", parser.Value{}, false
	}
	if predicate.Comparison.Operator != "=" {
		return "", parser.Value{}, false
	}
	if predicate.Comparison.LeftExpr != nil && predicate.Comparison.RightExpr != nil {
		leftValue, leftColumn, ok := valueExprOperandShape(predicate.Comparison.LeftExpr)
		if !ok || leftColumn == "" || leftValue.Kind != parser.ValueKindInvalid {
			return "", parser.Value{}, false
		}
		rightValue, rightColumn, ok := valueExprOperandShape(predicate.Comparison.RightExpr)
		if !ok || rightColumn != "" {
			return "", parser.Value{}, false
		}
		normalized, ok := normalizePlannerColumnName(leftColumn, tableRef)
		if !ok {
			return "", parser.Value{}, false
		}
		return normalized, rightValue, true
	}
	if predicate.Comparison.RightRef != "" {
		return "", parser.Value{}, false
	}
	normalized, ok := normalizePlannerColumnName(predicate.Comparison.Left, tableRef)
	if !ok {
		return "", parser.Value{}, false
	}
	return normalized, predicate.Comparison.Right, true
}

func valueExprOperandShape(expr *parser.ValueExpr) (parser.Value, string, bool) {
	if expr == nil {
		return parser.Value{}, "", false
	}

	switch expr.Kind {
	case parser.ValueExprKindLiteral:
		return expr.Value, "", true
	case parser.ValueExprKindColumnRef:
		if expr.Qualifier != "" {
			return parser.Value{}, expr.Qualifier + "." + expr.Column, true
		}
		return parser.Value{}, expr.Column, true
	case parser.ValueExprKindParen:
		return valueExprOperandShape(expr.Inner)
	default:
		return parser.Value{}, "", false
	}
}

func normalizePlannerColumnName(name string, tableRef *parser.TableRef) (string, bool) {
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

func indexedEqualityFromWhere(where *parser.WhereClause, tableRef *parser.TableRef) (string, parser.Value, bool) {
	if where == nil || len(where.Items) != 1 {
		return "", parser.Value{}, false
	}

	item := where.Items[0]
	if item.Op != "" || item.Condition.Operator != "=" {
		return "", parser.Value{}, false
	}

	normalized, ok := normalizePlannerColumnName(item.Condition.Left, tableRef)
	if !ok {
		return "", parser.Value{}, false
	}
	return normalized, item.Condition.Right, true
}
