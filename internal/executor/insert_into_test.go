package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestExecuteInsert(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []string{"id", "name"}},
	}

	err := Execute(&parser.InsertStmt{
		TableName: "users",
		Values:    []parser.Value{parser.Int64Value(1), parser.StringValue("steve")},
	}, tables)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if len(tables["users"].Rows) != 1 {
		t.Fatalf("Execute() rows len = %d, want 1", len(tables["users"].Rows))
	}
	row := tables["users"].Rows[0]
	if len(row) != 2 || row[0] != parser.Int64Value(1) || row[1] != parser.StringValue("steve") {
		t.Fatalf("Execute() row = %#v, want [1 'steve']", row)
	}
}

func TestExecuteInsertMissingTable(t *testing.T) {
	err := Execute(&parser.InsertStmt{
		TableName: "users",
		Values:    []parser.Value{parser.Int64Value(1)},
	}, map[string]*Table{})
	if err == nil {
		t.Fatal("Execute() error = nil, want missing table error")
	}
}

func TestExecuteInsertWrongValueCount(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []string{"id", "name"}},
	}

	err := Execute(&parser.InsertStmt{
		TableName: "users",
		Values:    []parser.Value{parser.Int64Value(1)},
	}, tables)
	if err == nil {
		t.Fatal("Execute() error = nil, want wrong value count error")
	}
}
