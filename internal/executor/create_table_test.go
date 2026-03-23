package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestExecuteCreateTable(t *testing.T) {
	tables := make(map[string]*Table)

	err := Execute(&parser.CreateTableStmt{
		Name:    "users",
		Columns: []string{"id", "name"},
	}, tables)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	got := tables["users"]
	if got == nil {
		t.Fatal("Execute() did not create table")
	}
	if got.Name != "users" {
		t.Fatalf("Execute() table name = %q, want %q", got.Name, "users")
	}
	if len(got.Columns) != 2 || got.Columns[0] != "id" || got.Columns[1] != "name" {
		t.Fatalf("Execute() columns = %#v, want []string{\"id\", \"name\"}", got.Columns)
	}
}

func TestExecuteCreateTableDuplicate(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []string{"id"}},
	}

	err := Execute(&parser.CreateTableStmt{
		Name:    "users",
		Columns: []string{"id", "name"},
	}, tables)
	if err == nil {
		t.Fatal("Execute() error = nil, want duplicate table error")
	}
}
