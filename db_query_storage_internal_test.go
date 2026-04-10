package rovadb

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

func TestQueryReloadsRowsFromStorageInsteadOfStaleTableCache(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	db.tables["users"].Rows = [][]parser.Value{
		{parser.Int64Value(99), parser.StringValue("stale-a")},
		{parser.Int64Value(100), parser.StringValue("stale-b")},
	}

	rows, err := db.Query("SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	want := []struct {
		id   int
		name string
	}{
		{1, "alice"},
		{2, "bob"},
	}
	for i, tc := range want {
		if !rows.Next() {
			t.Fatalf("Next() row %d = false, want true", i)
		}
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan() row %d error = %v", i, err)
		}
		if id != tc.id || name != tc.name {
			t.Fatalf("row %d = (%d, %q), want (%d, %q)", i, id, name, tc.id, tc.name)
		}
	}
	if rows.Next() {
		t.Fatal("Next() after rows = true, want false")
	}
}

func TestTableNamesForSelectUsesExecutorAccessPathForIndexScan(t *testing.T) {
	plan := &planner.SelectPlan{
		Query:    &planner.SelectQuery{TableName: "users"},
		ScanType: planner.ScanTypeIndex,
		IndexScan: &planner.IndexScan{
			TableName:   "users",
			ColumnName:  "name",
			LookupValue: planner.StringValue("alice"),
		},
	}

	handoff, err := executor.NewSelectExecutionHandoff(plan)
	if err != nil {
		t.Fatalf("NewSelectExecutionHandoff() error = %v", err)
	}
	names := tableNamesForSelectHandoff(handoff)
	if len(names) != 1 || names[0] != "users" {
		t.Fatalf("tableNamesForSelectHandoff() = %#v, want [users]", names)
	}

	accessPath, err := executor.DescribeSelectAccessPath(plan)
	if err != nil {
		t.Fatalf("DescribeSelectAccessPath() error = %v", err)
	}
	if accessPath.Kind != executor.SelectAccessPathKindIndex {
		t.Fatalf("DescribeSelectAccessPath().Kind = %v, want %v", accessPath.Kind, executor.SelectAccessPathKindIndex)
	}
	if accessPath.IndexLookup.TableName != "users" || accessPath.IndexLookup.ColumnName != "name" {
		t.Fatalf("DescribeSelectAccessPath() = %#v, want users/name", accessPath)
	}
	if accessPath.IndexLookup.LookupValue != parser.StringValue("alice") {
		t.Fatalf("DescribeSelectAccessPath().IndexLookup.LookupValue = %#v, want %#v", accessPath.IndexLookup.LookupValue, parser.StringValue("alice"))
	}
}
