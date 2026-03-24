package parser

import "strings"

// AlterTableAddColumnStmt is the minimal parsed form for ALTER TABLE ... ADD COLUMN.
type AlterTableAddColumnStmt struct {
	TableName string
	Column    ColumnDef
}

func parseAlterTable(input string) (*AlterTableAddColumnStmt, error) {
	const prefix = "ALTER TABLE"

	trimmed := strings.TrimSpace(input)
	if !strings.HasPrefix(strings.ToUpper(trimmed), prefix+" ") {
		return nil, errUnsupportedStatement
	}

	parts := strings.Fields(trimmed)
	if len(parts) != 7 ||
		!strings.EqualFold(parts[0], "ALTER") ||
		!strings.EqualFold(parts[1], "TABLE") ||
		!strings.EqualFold(parts[3], "ADD") ||
		!strings.EqualFold(parts[4], "COLUMN") {
		return nil, newParseError("unsupported alter table form")
	}

	tableName := strings.TrimSpace(parts[2])
	columnName := strings.TrimSpace(parts[5])
	typeName := strings.ToUpper(strings.TrimSpace(parts[6]))
	if tableName == "" || columnName == "" {
		return nil, newParseError("unsupported alter table form")
	}
	if typeName != ColumnTypeInt && typeName != ColumnTypeText {
		return nil, newParseError("unsupported alter table form")
	}

	return &AlterTableAddColumnStmt{
		TableName: tableName,
		Column: ColumnDef{
			Name: columnName,
			Type: typeName,
		},
	}, nil
}
