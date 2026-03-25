package planner

import "github.com/Khorlane/RovaDB/internal/parser"

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
	if stmt.Predicate != nil {
		return simpleEqualityPredicateFromPredicate(stmt.Predicate)
	}
	return simpleEqualityPredicateFromWhere(stmt.Where)
}

func simpleEqualityPredicateFromPredicate(predicate *parser.PredicateExpr) (string, parser.Value, bool) {
	if predicate == nil || predicate.Kind != parser.PredicateKindComparison || predicate.Comparison == nil {
		return "", parser.Value{}, false
	}
	if predicate.Comparison.Operator != "=" {
		return "", parser.Value{}, false
	}
	return predicate.Comparison.Left, predicate.Comparison.Right, true
}

func simpleEqualityPredicateFromWhere(where *parser.WhereClause) (string, parser.Value, bool) {
	if where == nil || len(where.Items) != 1 {
		return "", parser.Value{}, false
	}

	item := where.Items[0]
	if item.Op != "" || item.Condition.Operator != "=" {
		return "", parser.Value{}, false
	}

	return item.Condition.Left, item.Condition.Right, true
}
