package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestExecuteUpdateAllRows(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
				{parser.Int64Value(2), parser.StringValue("sam")},
			},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("bob")},
		},
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 2 {
		t.Fatalf("executeUpdate() affected = %d, want 2", affected)
	}
	if tables["users"].Rows[0][1] != parser.StringValue("bob") || tables["users"].Rows[1][1] != parser.StringValue("bob") {
		t.Fatalf("executeUpdate() rows = %#v, want all names=bob", tables["users"].Rows)
	}
}

func TestExecuteUpdateIntWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
				{parser.Int64Value(2), parser.StringValue("sam")},
			},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("bob")},
		},
		HasWhere:    true,
		WhereColumn: "id",
		WhereValue:  parser.Int64Value(1),
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 1 {
		t.Fatalf("executeUpdate() affected = %d, want 1", affected)
	}
	if tables["users"].Rows[0][1] != parser.StringValue("bob") || tables["users"].Rows[1][1] != parser.StringValue("sam") {
		t.Fatalf("executeUpdate() rows = %#v, want only first row changed", tables["users"].Rows)
	}
}

func TestExecuteUpdateStringWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
				{parser.Int64Value(2), parser.StringValue("sam")},
			},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "id", Value: parser.Int64Value(3)},
		},
		HasWhere:    true,
		WhereColumn: "name",
		WhereValue:  parser.StringValue("alice"),
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 1 {
		t.Fatalf("executeUpdate() affected = %d, want 1", affected)
	}
	if tables["users"].Rows[0][0] != parser.Int64Value(3) || tables["users"].Rows[1][0] != parser.Int64Value(2) {
		t.Fatalf("executeUpdate() rows = %#v, want only alice row id changed", tables["users"].Rows)
	}
}

func TestExecuteUpdateUnknownAssignmentColumn(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []string{"id", "name"}},
	}

	_, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "email", Value: parser.StringValue("bob")},
		},
	}, tables)
	if err != errColumnDoesNotExist {
		t.Fatalf("executeUpdate() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestExecuteUpdateUnknownWhereColumn(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []string{"id", "name"}},
	}

	_, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("bob")},
		},
		HasWhere:    true,
		WhereColumn: "email",
		WhereValue:  parser.StringValue("alice"),
	}, tables)
	if err != errColumnDoesNotExist {
		t.Fatalf("executeUpdate() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestExecuteUpdateNoMatchesLeavesRows(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
			},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("bob")},
		},
		HasWhere:    true,
		WhereColumn: "id",
		WhereValue:  parser.Int64Value(2),
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 0 {
		t.Fatalf("executeUpdate() affected = %d, want 0", affected)
	}
	if tables["users"].Rows[0][1] != parser.StringValue("alice") {
		t.Fatalf("executeUpdate() rows = %#v, want unchanged", tables["users"].Rows)
	}
}

func TestExecuteUpdateMultipleAssignments(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
			},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("bob")},
			{Column: "id", Value: parser.Int64Value(2)},
		},
		HasWhere:    true,
		WhereColumn: "name",
		WhereValue:  parser.StringValue("alice"),
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 1 {
		t.Fatalf("executeUpdate() affected = %d, want 1", affected)
	}
	if tables["users"].Rows[0][0] != parser.Int64Value(2) || tables["users"].Rows[0][1] != parser.StringValue("bob") {
		t.Fatalf("executeUpdate() rows = %#v, want updated id and name", tables["users"].Rows)
	}
}
