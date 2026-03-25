package planner

import "github.com/Khorlane/RovaDB/internal/parser"

// SelectPlan represents a planned SELECT execution.
type SelectPlan struct {
	Stmt      *parser.SelectExpr
	ScanType  ScanType
	TableScan *TableScan
	IndexScan *IndexScan
	JoinScan  *JoinScan
}
