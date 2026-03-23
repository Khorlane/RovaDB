package planner

import "github.com/Khorlane/RovaDB/internal/parser"

// ScanType identifies how rows will be accessed.
type ScanType string

const (
	ScanTypeTable ScanType = "table"
	ScanTypeIndex ScanType = "index"
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
