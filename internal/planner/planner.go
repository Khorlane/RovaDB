package planner

import "github.com/Khorlane/RovaDB/internal/parser"

// PlanSelect creates a basic execution plan for SELECT.
// Current behavior is pass-through.
func PlanSelect(stmt *parser.SelectExpr) (*SelectPlan, error) {
	return &SelectPlan{
		Stmt: stmt,
	}, nil
}
