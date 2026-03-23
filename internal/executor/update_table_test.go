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
		Where: where(parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(1)}),
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
		Where: where(parser.Condition{Left: "name", Operator: "=", Right: parser.StringValue("alice")}),
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
		Where: where(parser.Condition{Left: "email", Operator: "=", Right: parser.StringValue("alice")}),
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
		Where: where(parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(2)}),
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
		Where: where(parser.Condition{Left: "name", Operator: "=", Right: parser.StringValue("alice")}),
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

func TestExecuteUpdateWithAndWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
				{parser.Int64Value(2), parser.StringValue("bob")},
				{parser.Int64Value(3), parser.StringValue("bob")},
			},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("cara")},
		},
		Where: where(
			parser.Condition{Left: "id", Operator: ">", Right: parser.Int64Value(1)},
			parser.ConditionChainItem{Op: parser.BooleanOpAnd, Condition: parser.Condition{Left: "name", Operator: "=", Right: parser.StringValue("bob")}},
		),
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 2 {
		t.Fatalf("executeUpdate() affected = %d, want 2", affected)
	}
}

func TestExecuteUpdateWithOrWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
				{parser.Int64Value(2), parser.StringValue("bob")},
				{parser.Int64Value(3), parser.StringValue("cara")},
			},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("updated")},
		},
		Where: where(
			parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(1)},
			parser.ConditionChainItem{Op: parser.BooleanOpOr, Condition: parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(3)}},
		),
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 2 {
		t.Fatalf("executeUpdate() affected = %d, want 2", affected)
	}
	if tables["users"].Rows[0][1] != parser.StringValue("updated") || tables["users"].Rows[1][1] != parser.StringValue("bob") || tables["users"].Rows[2][1] != parser.StringValue("updated") {
		t.Fatalf("rows = %#v, want ids 1 and 3 updated", tables["users"].Rows)
	}
}
