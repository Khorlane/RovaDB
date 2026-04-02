package planner

// BasicIndex is a metadata-only single-column index shell.
type BasicIndex struct {
	TableName  string
	ColumnName string
	IndexID    uint32
	RootPageID uint32
}

// NewBasicIndex builds a metadata-only single-column index shell.
func NewBasicIndex(tableName, columnName string) *BasicIndex {
	return &BasicIndex{
		TableName:  tableName,
		ColumnName: columnName,
	}
}
