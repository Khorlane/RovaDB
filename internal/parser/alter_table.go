package parser

// AlterTableAddColumnStmt is the minimal parsed form for ALTER TABLE ... ADD COLUMN.
type AlterTableAddColumnStmt struct {
	TableName string
	Column    ColumnDef
}

// AlterTableAddPrimaryKeyStmt is the parsed ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY shape.
type AlterTableAddPrimaryKeyStmt struct {
	TableName  string
	PrimaryKey PrimaryKeyDef
}

// AlterTableAddForeignKeyStmt is the parsed ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY shape.
type AlterTableAddForeignKeyStmt struct {
	TableName  string
	ForeignKey ForeignKeyDef
}

// AlterTableDropPrimaryKeyStmt is the parsed ALTER TABLE ... DROP PRIMARY KEY shape.
type AlterTableDropPrimaryKeyStmt struct {
	TableName string
}

// AlterTableDropForeignKeyStmt is the parsed ALTER TABLE ... DROP FOREIGN KEY name shape.
type AlterTableDropForeignKeyStmt struct {
	TableName      string
	ConstraintName string
}

func parseAlterTable(input string) (any, error) {
	return parseAlterTableTokens(input)
}
