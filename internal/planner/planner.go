package planner

import "github.com/Khorlane/RovaDB/internal/parser"

// PlanSelect creates a basic execution plan for SELECT.
// Current behavior is pass-through.
func PlanSelect(stmt *parser.SelectExpr) (*SelectPlan, error) {
	plan := &SelectPlan{
		Stmt: stmt,
	}
	if stmt != nil && stmt.TableName != "" {
		plan.ScanType = ScanTypeTable
		plan.TableScan = &TableScan{TableName: stmt.TableName}
	}
	return plan, nil
}
