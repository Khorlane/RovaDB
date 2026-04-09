package planner

// SelectPlan is planner-owned plan data for SELECT. It defines logical query
// shape only and must not accumulate execution-time helper state.
type SelectPlan struct {
	Query         *SelectQuery
	ScanType      ScanType
	TableScan     *TableScan
	IndexScan     *IndexScan
	IndexOnlyScan *IndexOnlyScan
	JoinScan      *JoinScan
}
