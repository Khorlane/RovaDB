package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestExecuteCreateTable(t *testing.T) {
	tables := make(map[string]*Table)

	affected, err := Execute(&parser.CreateTableStmt{
		Name:    "users",
		Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
	}, tables)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if affected != 0 {
		t.Fatalf("Execute() affected = %d, want 0", affected)
	}

	got := tables["users"]
	if got == nil {
		t.Fatal("Execute() did not create table")
	}
	if got.Name != "users" {
		t.Fatalf("Execute() table name = %q, want %q", got.Name, "users")
	}
	if len(got.Columns) != 2 || got.Columns[0] != (parser.ColumnDef{Name: "id", Type: parser.ColumnTypeInt}) || got.Columns[1] != (parser.ColumnDef{Name: "name", Type: parser.ColumnTypeText}) {
		t.Fatalf("Execute() columns = %#v, want typed id/name columns", got.Columns)
	}
}

func TestExecuteCreateTableDuplicate(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}}},
	}

	_, err := Execute(&parser.CreateTableStmt{
		Name:    "users",
		Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
	}, tables)
	if err == nil {
		t.Fatal("Execute() error = nil, want duplicate table error")
	}
}

func TestExecuteCreateTableNamedPrimaryKeyBuildsConstraintMetadata(t *testing.T) {
	tables := make(map[string]*Table)

	_, err := Execute(&parser.CreateTableStmt{
		Name:    "users",
		Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "org_id", Type: parser.ColumnTypeInt}},
		PrimaryKey: &parser.PrimaryKeyDef{
			Name:      "pk_users",
			Columns:   []string{"id", "org_id"},
			IndexName: "idx_users_pk",
		},
	}, tables)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	got := tables["users"]
	if got == nil {
		t.Fatal("users table = nil, want created table")
	}
	if got.TableID == 0 {
		t.Fatal("TableID = 0, want assigned table id for constraint metadata")
	}
	if got.PrimaryKeyDef == nil || got.PrimaryKeyDef.Name != "pk_users" || got.PrimaryKeyDef.IndexID == 0 {
		t.Fatalf("PrimaryKeyDef = %#v, want pk_users with assigned index id", got.PrimaryKeyDef)
	}
	if len(got.IndexDefs) != 1 {
		t.Fatalf("len(IndexDefs) = %d, want 1", len(got.IndexDefs))
	}
	if got.IndexDefs[0].Name != "idx_users_pk" || !got.IndexDefs[0].Unique {
		t.Fatalf("IndexDefs[0] = %#v, want unique idx_users_pk", got.IndexDefs[0])
	}
	if len(got.IndexDefs[0].Columns) != 2 || got.IndexDefs[0].Columns[0].Name != "id" || got.IndexDefs[0].Columns[1].Name != "org_id" {
		t.Fatalf("IndexDefs[0].Columns = %#v, want [id org_id]", got.IndexDefs[0].Columns)
	}
}

func TestExecuteCreateTableNamedForeignKeyNotImplementedYet(t *testing.T) {
	tables := make(map[string]*Table)

	_, err := Execute(&parser.CreateTableStmt{
		Name:    "users",
		Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "team_id", Type: parser.ColumnTypeInt}},
		ForeignKeys: []parser.ForeignKeyDef{{
			Name:          "fk_users_team",
			Columns:       []string{"team_id"},
			ParentTable:   "teams",
			ParentColumns: []string{"id"},
			IndexName:     "idx_users_team",
			OnDelete:      parser.ForeignKeyDeleteActionRestrict,
		}},
	}, tables)
	if err != errNotImplemented {
		t.Fatalf("Execute() error = %v, want %v", err, errNotImplemented)
	}
}
