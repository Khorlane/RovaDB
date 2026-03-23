package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func typedCols() []parser.ColumnDef {
	return []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}}
}

func TestSelectAllColumns(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: typedCols(),
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("steve")},
				{parser.Int64Value(2), parser.StringValue("sam")},
			},
		},
	}

	rows, err := Select(&parser.SelectExpr{TableName: "users"}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 2 || len(rows[0]) != 2 {
		t.Fatalf("Select() rows = %#v, want 2x2 rows", rows)
	}
}

func TestSelectSubsetColumns(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("steve")}}},
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
		"users": {Name: "users", Columns: typedCols()},
	}

	rows, err := Select(&parser.SelectExpr{TableName: "users"}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("Select() rows = %#v, want empty", rows)
	}
}

func TestSelectRequestedOrder(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("steve")}}},
	}

	rows, err := Select(&parser.SelectExpr{TableName: "users", Columns: []string{"name", "id"}}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || len(rows[0]) != 2 || rows[0][0] != parser.StringValue("steve") || rows[0][1] != parser.Int64Value(1) {
		t.Fatalf("Select() rows = %#v, want [steve 1]", rows)
	}
}

func TestSelectInvalidColumn(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols()},
	}

	_, err := Select(&parser.SelectExpr{TableName: "users", Columns: []string{"email"}}, tables)
	if err != errColumnDoesNotExist {
		t.Fatalf("Select() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestSelectWithIntWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("steve")}, {parser.Int64Value(2), parser.StringValue("bob")}}},
	}

	rows, err := Select(&parser.SelectExpr{TableName: "users", HasWhere: true, WhereColumn: "id", WhereValue: parser.Int64Value(1)}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("Select() rows = %#v, want one row", rows)
	}
}

func TestSelectWithStringWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("steve")}, {parser.Int64Value(2), parser.StringValue("bob")}}},
	}

	rows, err := Select(&parser.SelectExpr{TableName: "users", Columns: []string{"name"}, HasWhere: true, WhereColumn: "name", WhereValue: parser.StringValue("bob")}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || rows[0][0] != parser.StringValue("bob") {
		t.Fatalf("Select() rows = %#v, want [[bob]]", rows)
	}
}

func TestSelectWithUnknownWhereColumn(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols()},
	}

	_, err := Select(&parser.SelectExpr{TableName: "users", HasWhere: true, WhereColumn: "email", WhereValue: parser.StringValue("bob")}, tables)
	if err != errColumnDoesNotExist {
		t.Fatalf("Select() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestSelectWithNoMatches(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("steve")}}},
	}

	rows, err := Select(&parser.SelectExpr{TableName: "users", HasWhere: true, WhereColumn: "name", WhereValue: parser.StringValue("bob")}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("Select() rows = %#v, want empty", rows)
	}
}

func TestSelectMissingTable(t *testing.T) {
	_, err := Select(&parser.SelectExpr{TableName: "users"}, map[string]*Table{})
	if err != errTableDoesNotExist {
		t.Fatalf("Select() error = %v, want %v", err, errTableDoesNotExist)
	}
}

func TestProjectedColumnNamesAllColumns(t *testing.T) {
	table := &Table{Name: "users", Columns: typedCols()}

	got, err := ProjectedColumnNames(&parser.SelectExpr{TableName: "users"}, table)
	if err != nil {
		t.Fatalf("ProjectedColumnNames() error = %v", err)
	}
	if len(got) != 2 || got[0] != "id" || got[1] != "name" {
		t.Fatalf("ProjectedColumnNames() = %#v, want [id name]", got)
	}
}

func TestProjectedColumnNamesInvalidColumn(t *testing.T) {
	table := &Table{Name: "users", Columns: typedCols()}

	_, err := ProjectedColumnNames(&parser.SelectExpr{TableName: "users", Columns: []string{"email"}}, table)
	if err != errColumnDoesNotExist {
		t.Fatalf("ProjectedColumnNames() error = %v, want %v", err, errColumnDoesNotExist)
	}
}
