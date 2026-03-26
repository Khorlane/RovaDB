package parser

type IndexColumn struct {
	Name string
	Desc bool
}

type CreateIndexStmt struct {
	Name      string
	TableName string
	Unique    bool
	Columns   []IndexColumn
}

type DropTableStmt struct {
	Name string
}

type DropIndexStmt struct {
	Name string
}

type CommitStmt struct{}

type RollbackStmt struct{}
