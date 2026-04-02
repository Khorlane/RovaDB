package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

func TestRebuildIndexesForTableKeepsMetadataShells(t *testing.T) {
	table := &Table{
		Name:    "users",
		Columns: typedCols(),
		Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
		},
		Indexes: map[string]*planner.BasicIndex{
			"name": nil,
		},
	}

	if err := rebuildIndexesForTable(table); err != nil {
		t.Fatalf("rebuildIndexesForTable() error = %v", err)
	}

	index := table.Indexes["name"]
	if index == nil {
		t.Fatal("table.Indexes[name] = nil, want metadata shell")
	}
	if index.TableName != "users" || index.ColumnName != "name" {
		t.Fatalf("index metadata = (%q, %q), want (%q, %q)", index.TableName, index.ColumnName, "users", "name")
	}
}

func TestInsertKeepsIndexMetadataShell(t *testing.T) {
	table := &Table{
		Name:    "users",
		Columns: typedCols(),
		Indexes: map[string]*planner.BasicIndex{
			"name": planner.NewBasicIndex("users", "name"),
		},
	}
	tables := map[string]*Table{"users": table}

	if _, err := executeInsert(&parser.InsertStmt{
		TableName: "users",
		Values:    []parser.Value{parser.Int64Value(1), parser.StringValue("alice")},
	}, tables); err != nil {
		t.Fatalf("executeInsert() error = %v", err)
	}

	index := table.Indexes["name"]
	if index == nil {
		t.Fatal("table.Indexes[name] = nil, want metadata shell")
	}
	if index.TableName != "users" || index.ColumnName != "name" {
		t.Fatalf("index metadata = (%q, %q), want (%q, %q)", index.TableName, index.ColumnName, "users", "name")
	}
}

func TestUpdateKeepsIndexMetadataShell(t *testing.T) {
	table := &Table{
		Name:    "users",
		Columns: typedCols(),
		Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
		},
		Indexes: map[string]*planner.BasicIndex{
			"name": planner.NewBasicIndex("users", "name"),
		},
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

	index := table.Indexes["name"]
	if index == nil {
		t.Fatal("table.Indexes[name] = nil, want metadata shell")
	}
	if index.TableName != "users" || index.ColumnName != "name" {
		t.Fatalf("index metadata = (%q, %q), want (%q, %q)", index.TableName, index.ColumnName, "users", "name")
	}
}

func TestDeleteKeepsIndexMetadataShell(t *testing.T) {
	table := &Table{
		Name:    "users",
		Columns: typedCols(),
		Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
		},
		Indexes: map[string]*planner.BasicIndex{
			"name": planner.NewBasicIndex("users", "name"),
		},
	}
	tables := map[string]*Table{"users": table}

	if _, err := executeDelete(&parser.DeleteStmt{
		TableName: "users",
		Where:     where(parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(1)}),
	}, tables); err != nil {
		t.Fatalf("executeDelete() error = %v", err)
	}

	index := table.Indexes["name"]
	if index == nil {
		t.Fatal("table.Indexes[name] = nil, want metadata shell")
	}
	if index.TableName != "users" || index.ColumnName != "name" {
		t.Fatalf("index metadata = (%q, %q), want (%q, %q)", index.TableName, index.ColumnName, "users", "name")
	}
}
