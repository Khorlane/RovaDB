package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestExecuteDeleteAllRows(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("steve")},
				{parser.Int64Value(2), parser.StringValue("bob")},
			},
		},
	}

	affected, err := executeDelete(&parser.DeleteStmt{TableName: "users"}, tables)
	if err != nil {
		t.Fatalf("executeDelete() error = %v", err)
	}
	if affected != 2 {
		t.Fatalf("executeDelete() affected = %d, want 2", affected)
	}
	if len(tables["users"].Rows) != 0 {
		t.Fatalf("executeDelete() rows = %#v, want empty", tables["users"].Rows)
	}
}

func TestExecuteDeleteIntWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("steve")},
				{parser.Int64Value(2), parser.StringValue("bob")},
				{parser.Int64Value(1), parser.StringValue("sam")},
			},
		},
	}

	affected, err := executeDelete(&parser.DeleteStmt{
		TableName:   "users",
		HasWhere:    true,
		WhereColumn: "id",
		WhereValue:  parser.Int64Value(1),
	}, tables)
	if err != nil {
		t.Fatalf("executeDelete() error = %v", err)
	}
	if affected != 2 {
		t.Fatalf("executeDelete() affected = %d, want 2", affected)
	}
	if len(tables["users"].Rows) != 1 || tables["users"].Rows[0][1] != parser.StringValue("bob") {
		t.Fatalf("executeDelete() rows = %#v, want only bob row", tables["users"].Rows)
	}
}

func TestExecuteDeleteStringWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("steve")},
				{parser.Int64Value(2), parser.StringValue("bob")},
			},
		},
	}

	affected, err := executeDelete(&parser.DeleteStmt{
		TableName:   "users",
		HasWhere:    true,
		WhereColumn: "name",
		WhereValue:  parser.StringValue("bob"),
	}, tables)
	if err != nil {
		t.Fatalf("executeDelete() error = %v", err)
	}
	if affected != 1 {
		t.Fatalf("executeDelete() affected = %d, want 1", affected)
	}
	if len(tables["users"].Rows) != 1 || tables["users"].Rows[0][1] != parser.StringValue("steve") {
		t.Fatalf("executeDelete() rows = %#v, want only steve row", tables["users"].Rows)
	}
}

func TestExecuteDeleteUnknownWhereColumn(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
		},
	}

	_, err := executeDelete(&parser.DeleteStmt{
		TableName:   "users",
		HasWhere:    true,
		WhereColumn: "email",
		WhereValue:  parser.StringValue("bob"),
	}, tables)
	if err != errColumnDoesNotExist {
		t.Fatalf("executeDelete() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestExecuteDeleteNoMatchesLeavesRows(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("steve")},
				{parser.Int64Value(2), parser.StringValue("bob")},
			},
		},
	}

	affected, err := executeDelete(&parser.DeleteStmt{
		TableName:   "users",
		HasWhere:    true,
		WhereColumn: "name",
		WhereValue:  parser.StringValue("sam"),
	}, tables)
	if err != nil {
		t.Fatalf("executeDelete() error = %v", err)
	}
	if affected != 0 {
		t.Fatalf("executeDelete() affected = %d, want 0", affected)
	}
	if len(tables["users"].Rows) != 2 || tables["users"].Rows[0][1] != parser.StringValue("steve") || tables["users"].Rows[1][1] != parser.StringValue("bob") {
		t.Fatalf("executeDelete() rows = %#v, want rows unchanged", tables["users"].Rows)
	}
}
