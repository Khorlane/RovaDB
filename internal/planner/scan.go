package planner

import "github.com/Khorlane/RovaDB/internal/parser"

// ScanType identifies how rows will be accessed.
type ScanType string

const (
	ScanTypeTable     ScanType = "table"
	ScanTypeIndex     ScanType = "index"
	ScanTypeJoin      ScanType = "join"
	ScanTypeIndexOnly ScanType = "index_only"
)

// TableScan represents a full table scan strategy.
type TableScan struct {
	TableName string
}

// IndexScan represents a single-column equality lookup strategy.
type IndexScan struct {
	TableName  string
	ColumnName string
	Value      parser.Value
}

// IndexOnlyScan is the reserved narrow planner contract for future index-only
// access. In this milestone line, "index-only" means the query result can be
// produced entirely from index contents plus index-structure metadata without
// fetching base table rows. Eligibility is intentionally narrow, starting with
// simple COUNT(*) and simple indexed-column projection shapes. Any uncertain or
// unsupported case must fall back to existing table/index scan paths.
//
// This type is a design anchor only in ixonly.1; planner/runtime wiring lands
// in later slices.
type IndexOnlyScan struct {
	TableName   string
	IndexName   string
	ColumnNames []string
	CountStar   bool
}

// JoinScan represents a two-table inner equality join.
type JoinScan struct {
	LeftTableName   string
	LeftTableAlias  string
	LeftColumnName  string
	RightTableName  string
	RightTableAlias string
	RightColumnName string
}
