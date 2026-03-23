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
	if rows[1][0] != parser.Int64Value(2) || rows[1][1] != parser.StringValue("sam") {
		t.Fatalf("Select() second row = %#v, want [2 sam]", rows[1])
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

func TestSelectEmptyTable(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
		},
	}

	rows, err := Select(&parser.SelectExpr{TableName: "users", SelectAll: true}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("Select() rows = %#v, want empty", rows)
	}
}

func TestSelectRequestedOrder(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("steve")},
			},
		},
	}

	rows, err := Select(&parser.SelectExpr{TableName: "users", Columns: []string{"name", "id"}}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || len(rows[0]) != 2 {
		t.Fatalf("Select() rows = %#v, want one 2-column row", rows)
	}
	if rows[0][0] != parser.StringValue("steve") || rows[0][1] != parser.Int64Value(1) {
		t.Fatalf("Select() row = %#v, want [steve 1]", rows[0])
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
	if err != errColumnDoesNotExist {
		t.Fatalf("Select() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestSelectWithIntWhere(t *testing.T) {
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

	rows, err := Select(&parser.SelectExpr{
		TableName:   "users",
		SelectAll:   true,
		HasWhere:    true,
		WhereColumn: "id",
		WhereValue:  parser.Int64Value(1),
	}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || rows[0][0] != parser.Int64Value(1) {
		t.Fatalf("Select() rows = %#v, want one row with id 1", rows)
	}
}

func TestSelectWithStringWhere(t *testing.T) {
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

	rows, err := Select(&parser.SelectExpr{
		TableName:   "users",
		Columns:     []string{"name"},
		HasWhere:    true,
		WhereColumn: "name",
		WhereValue:  parser.StringValue("bob"),
	}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] != parser.StringValue("bob") {
		t.Fatalf("Select() rows = %#v, want [[bob]]", rows)
	}
}

func TestSelectWithUnknownWhereColumn(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
		},
	}

	_, err := Select(&parser.SelectExpr{
		TableName:   "users",
		SelectAll:   true,
		HasWhere:    true,
		WhereColumn: "email",
		WhereValue:  parser.StringValue("bob"),
	}, tables)
	if err != errColumnDoesNotExist {
		t.Fatalf("Select() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestSelectWithNoMatches(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []string{"id", "name"},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("steve")},
			},
		},
	}

	rows, err := Select(&parser.SelectExpr{
		TableName:   "users",
		SelectAll:   true,
		HasWhere:    true,
		WhereColumn: "name",
		WhereValue:  parser.StringValue("bob"),
	}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("Select() rows = %#v, want empty", rows)
	}
}

func TestSelectMissingTable(t *testing.T) {
	_, err := Select(&parser.SelectExpr{TableName: "users", SelectAll: true}, map[string]*Table{})
	if err != errTableDoesNotExist {
		t.Fatalf("Select() error = %v, want %v", err, errTableDoesNotExist)
	}
}
