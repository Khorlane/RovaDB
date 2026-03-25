package planner

import (
	"strings"

	"github.com/Khorlane/RovaDB/internal/parser"
)

// TableMetadata is the minimal planner-side table info used for scan choice.
type TableMetadata struct {
	Indexes map[string]*BasicIndex
}

// PlanSelect creates a basic execution plan for SELECT.
// Current behavior defaults to table scan unless optional metadata supports
// a simple equality index scan decision.
func PlanSelect(stmt *parser.SelectExpr, tables ...map[string]*TableMetadata) (*SelectPlan, error) {
	if stmt == nil {
		return nil, newPlanError("unsupported query form")
	}
	if len(stmt.From) > 1 {
		return nil, newPlanError("unsupported query form")
	}

	plan := &SelectPlan{
		Stmt: stmt,
	}
	if stmt != nil && stmt.TableName != "" {
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

	columnName, value, ok := simpleEqualityPredicate(stmt)
	if !ok {
		return nil
	}

	table := tables[stmt.TableName]
	if table == nil || table.Indexes == nil || table.Indexes[columnName] == nil {
		return nil
	}

	return &IndexScan{
		TableName:  stmt.TableName,
		ColumnName: columnName,
		Value:      value,
	}
}

func simpleEqualityPredicate(stmt *parser.SelectExpr) (string, parser.Value, bool) {
	if stmt == nil {
		return "", parser.Value{}, false
	}
	tableRef := stmt.PrimaryTableRef()
	if stmt.Predicate != nil {
		return simpleEqualityPredicateFromPredicate(stmt.Predicate, tableRef)
	}
	return simpleEqualityPredicateFromWhere(stmt.Where, tableRef)
}

func simpleEqualityPredicateFromPredicate(predicate *parser.PredicateExpr, tableRef *parser.TableRef) (string, parser.Value, bool) {
	if predicate == nil || predicate.Kind != parser.PredicateKindComparison || predicate.Comparison == nil {
		return "", parser.Value{}, false
	}
	if predicate.Comparison.Operator != "=" {
		return "", parser.Value{}, false
	}
	if predicate.Comparison.LeftExpr != nil && predicate.Comparison.RightExpr != nil {
		leftValue, leftColumn, ok := parserOperandShape(predicate.Comparison.LeftExpr)
		if !ok || leftColumn == "" || leftValue.Kind != parser.ValueKindInvalid {
			return "", parser.Value{}, false
		}
		rightValue, rightColumn, ok := parserOperandShape(predicate.Comparison.RightExpr)
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

func parserOperandShape(expr *parser.ValueExpr) (parser.Value, string, bool) {
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
		return parserOperandShape(expr.Inner)
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

func simpleEqualityPredicateFromWhere(where *parser.WhereClause, tableRef *parser.TableRef) (string, parser.Value, bool) {
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
