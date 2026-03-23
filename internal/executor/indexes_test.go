package executor

import (
	"reflect"
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

func indexedUsersTable() *Table {
	table := &Table{
		Name:    "users",
		Columns: typedCols(),
		Rows:    [][]parser.Value{},
		Indexes: map[string]*planner.BasicIndex{
			"name": planner.NewBasicIndex("users", "name"),
		},
	}
	_ = rebuildIndexesForTable(table)
	return table
}

func TestInsertRebuildsIndexes(t *testing.T) {
	table := indexedUsersTable()
	tables := map[string]*Table{"users": table}

	if _, err := executeInsert(&parser.InsertStmt{
		TableName: "users",
		Values:    []parser.Value{parser.Int64Value(1), parser.StringValue("alice")},
	}, tables); err != nil {
		t.Fatalf("executeInsert() first error = %v", err)
	}
	if _, err := executeInsert(&parser.InsertStmt{
		TableName: "users",
		Values:    []parser.Value{parser.Int64Value(2), parser.StringValue("alice")},
	}, tables); err != nil {
		t.Fatalf("executeInsert() second error = %v", err)
	}

	got := table.Indexes["name"].LookupEqual(parser.StringValue("alice"))
	if !reflect.DeepEqual(got, []int{0, 1}) {
		t.Fatalf("LookupEqual(alice) = %#v, want []int{0, 1}", got)
	}
}

func TestUpdateRebuildsIndexes(t *testing.T) {
	table := indexedUsersTable()
	table.Rows = [][]parser.Value{
		{parser.Int64Value(1), parser.StringValue("alice")},
		{parser.Int64Value(2), parser.StringValue("bob")},
	}
	if err := rebuildIndexesForTable(table); err != nil {
		t.Fatalf("rebuildIndexesForTable() setup error = %v", err)
	}
	tables := map[string]*Table{"users": table}

	if _, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("cara")},
		},
		Where: where(parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(1)}),
	}, tables); err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}

	if got := table.Indexes["name"].LookupEqual(parser.StringValue("alice")); got != nil {
		t.Fatalf("LookupEqual(alice) = %#v, want nil", got)
	}
	if got := table.Indexes["name"].LookupEqual(parser.StringValue("cara")); !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("LookupEqual(cara) = %#v, want []int{0}", got)
	}
}

func TestDeleteRebuildsIndexes(t *testing.T) {
	table := indexedUsersTable()
	table.Rows = [][]parser.Value{
		{parser.Int64Value(1), parser.StringValue("alice")},
		{parser.Int64Value(2), parser.StringValue("bob")},
		{parser.Int64Value(3), parser.StringValue("alice")},
	}
	if err := rebuildIndexesForTable(table); err != nil {
		t.Fatalf("rebuildIndexesForTable() setup error = %v", err)
	}
	tables := map[string]*Table{"users": table}

	if _, err := executeDelete(&parser.DeleteStmt{
		TableName: "users",
		Where:     where(parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(1)}),
	}, tables); err != nil {
		t.Fatalf("executeDelete() error = %v", err)
	}

	got := table.Indexes["name"].LookupEqual(parser.StringValue("alice"))
	if !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("LookupEqual(alice) = %#v, want []int{1}", got)
	}
}

func TestMixedMutationsKeepIndexesConsistent(t *testing.T) {
	table := indexedUsersTable()
	tables := map[string]*Table{"users": table}

	for _, stmt := range []*parser.InsertStmt{
		{TableName: "users", Values: []parser.Value{parser.Int64Value(1), parser.StringValue("alice")}},
		{TableName: "users", Values: []parser.Value{parser.Int64Value(2), parser.StringValue("bob")}},
		{TableName: "users", Values: []parser.Value{parser.Int64Value(3), parser.StringValue("cara")}},
	} {
		if _, err := executeInsert(stmt, tables); err != nil {
			t.Fatalf("executeInsert() error = %v", err)
		}
	}

	if _, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("alice")},
		},
		Where: where(parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(2)}),
	}, tables); err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}

	if _, err := executeDelete(&parser.DeleteStmt{
		TableName: "users",
		Where:     where(parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(1)}),
	}, tables); err != nil {
		t.Fatalf("executeDelete() error = %v", err)
	}

	if got := table.Indexes["name"].LookupEqual(parser.StringValue("alice")); !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("LookupEqual(alice) = %#v, want []int{0}", got)
	}
	if got := table.Indexes["name"].LookupEqual(parser.StringValue("bob")); got != nil {
		t.Fatalf("LookupEqual(bob) = %#v, want nil", got)
	}
	if got := table.Indexes["name"].LookupEqual(parser.StringValue("cara")); !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("LookupEqual(cara) = %#v, want []int{1}", got)
	}
}
