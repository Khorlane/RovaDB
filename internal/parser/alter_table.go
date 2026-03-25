package parser

// AlterTableAddColumnStmt is the minimal parsed form for ALTER TABLE ... ADD COLUMN.
type AlterTableAddColumnStmt struct {
	TableName string
	Column    ColumnDef
}

func parseAlterTable(input string) (*AlterTableAddColumnStmt, error) {
	return parseAlterTableTokens(input)
}
