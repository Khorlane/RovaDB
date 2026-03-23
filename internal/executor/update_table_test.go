package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestExecuteUpdateAllRows(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
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
}

func TestExecuteUpdateIntWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
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
}

func TestExecuteUpdateStringWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
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
}

func TestExecuteUpdateUnknownAssignmentColumn(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}}},
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
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}}},
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
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
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
}

func TestExecuteUpdateMultipleAssignments(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
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
}

func TestExecuteUpdateWrongType(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
			},
		},
	}

	_, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "id", Value: parser.StringValue("oops")},
		},
	}, tables)
	if err != errTypeMismatch {
		t.Fatalf("executeUpdate() error = %v, want %v", err, errTypeMismatch)
	}
}
