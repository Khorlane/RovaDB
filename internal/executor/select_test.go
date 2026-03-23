package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestSelectAllColumns(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("steve")},
				{parser.Int64Value(2), parser.StringValue("sam")},
			},
		},
	}

	rows, err := Select(&parser.SelectExpr{TableName: "users", SelectAll: true}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 2 || len(rows[0]) != 2 {
		t.Fatalf("Select() rows = %#v, want 2x2 rows", rows)
	}
}

func TestSelectSubsetColumns(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("steve")},
			},
		},
	}

	rows, err := Select(&parser.SelectExpr{TableName: "users", Columns: []string{"name"}}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] != parser.StringValue("steve") {
		t.Fatalf("Select() rows = %#v, want [[steve]]", rows)
	}
}

func TestSelectInvalidColumn(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
		},
	}

	_, err := Select(&parser.SelectExpr{TableName: "users", Columns: []string{"email"}}, tables)
	if err == nil {
		t.Fatal("Select() error = nil, want invalid column error")
	}
}
