package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestExecuteDeleteAllRows(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("steve")}, {parser.Int64Value(2), parser.StringValue("bob")}}},
	}

	affected, err := executeDelete(&parser.DeleteStmt{TableName: "users"}, tables)
	if err != nil {
		t.Fatalf("executeDelete() error = %v", err)
	}
	if affected != 2 || len(tables["users"].Rows) != 0 {
		t.Fatalf("executeDelete() affected=%d rows=%#v, want 2 and empty", affected, tables["users"].Rows)
	}
}

func TestExecuteDeleteIntWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("steve")}, {parser.Int64Value(2), parser.StringValue("bob")}, {parser.Int64Value(1), parser.StringValue("sam")}}},
	}

	affected, err := executeDelete(&parser.DeleteStmt{TableName: "users", Where: &parser.WhereClause{Conditions: []parser.Condition{{Left: "id", Operator: "=", Right: parser.Int64Value(1)}}}}, tables)
	if err != nil {
		t.Fatalf("executeDelete() error = %v", err)
	}
	if affected != 2 || len(tables["users"].Rows) != 1 || tables["users"].Rows[0][1] != parser.StringValue("bob") {
		t.Fatalf("executeDelete() affected=%d rows=%#v, want only bob row left", affected, tables["users"].Rows)
	}
}

func TestExecuteDeleteStringWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("steve")}, {parser.Int64Value(2), parser.StringValue("bob")}}},
	}

	affected, err := executeDelete(&parser.DeleteStmt{TableName: "users", Where: &parser.WhereClause{Conditions: []parser.Condition{{Left: "name", Operator: "=", Right: parser.StringValue("bob")}}}}, tables)
	if err != nil {
		t.Fatalf("executeDelete() error = %v", err)
	}
	if affected != 1 || len(tables["users"].Rows) != 1 || tables["users"].Rows[0][1] != parser.StringValue("steve") {
		t.Fatalf("executeDelete() affected=%d rows=%#v, want only steve row left", affected, tables["users"].Rows)
	}
}

func TestExecuteDeleteUnknownWhereColumn(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols()},
	}

	_, err := executeDelete(&parser.DeleteStmt{TableName: "users", Where: &parser.WhereClause{Conditions: []parser.Condition{{Left: "email", Operator: "=", Right: parser.StringValue("bob")}}}}, tables)
	if err != errColumnDoesNotExist {
		t.Fatalf("executeDelete() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestExecuteDeleteNoMatchesLeavesRows(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("steve")}, {parser.Int64Value(2), parser.StringValue("bob")}}},
	}

	affected, err := executeDelete(&parser.DeleteStmt{TableName: "users", Where: &parser.WhereClause{Conditions: []parser.Condition{{Left: "name", Operator: "=", Right: parser.StringValue("sam")}}}}, tables)
	if err != nil {
		t.Fatalf("executeDelete() error = %v", err)
	}
	if affected != 0 || len(tables["users"].Rows) != 2 {
		t.Fatalf("executeDelete() affected=%d rows=%#v, want unchanged rows", affected, tables["users"].Rows)
	}
}

func TestExecuteDeleteWithAndWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(3), parser.StringValue("bob")},
		}},
	}

	affected, err := executeDelete(&parser.DeleteStmt{
		TableName: "users",
		Where: &parser.WhereClause{Conditions: []parser.Condition{
			{Left: "id", Operator: ">", Right: parser.Int64Value(1)},
			{Left: "name", Operator: "=", Right: parser.StringValue("bob")},
		}},
	}, tables)
	if err != nil {
		t.Fatalf("executeDelete() error = %v", err)
	}
	if affected != 2 || len(tables["users"].Rows) != 1 || tables["users"].Rows[0][1] != parser.StringValue("alice") {
		t.Fatalf("executeDelete() affected=%d rows=%#v, want only alice row left", affected, tables["users"].Rows)
	}
}
