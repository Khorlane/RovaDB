package planner

// ScanType identifies how rows will be accessed.
type ScanType string

const (
	ScanTypeTable ScanType = "table"
)

// TableScan represents a full table scan strategy.
type TableScan struct {
	TableName string
}
