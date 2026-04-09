package planner

import "github.com/Khorlane/RovaDB/internal/parser"

// SelectPlan is planner-owned plan data for SELECT. It defines logical query
// shape only and must not accumulate execution-time helper state.
//
// NOTE: Stmt currently carries a parser-owned structure through the planner to
// executor boundary. That is a known pressure point to narrow in later slices.
type SelectPlan struct {
	Stmt          *parser.SelectExpr
	ScanType      ScanType
	TableScan     *TableScan
	IndexScan     *IndexScan
	IndexOnlyScan *IndexOnlyScan
	JoinScan      *JoinScan
}
