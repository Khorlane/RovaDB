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

	rows, err := Select(&parser.SelectExpr{TableName: "users", Where: &parser.WhereClause{Conditions: []parser.Condition{{Left: "id", Operator: "=", Right: parser.Int64Value(1)}}}}, tables)
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

	rows, err := Select(&parser.SelectExpr{TableName: "users", Columns: []string{"name"}, Where: &parser.WhereClause{Conditions: []parser.Condition{{Left: "name", Operator: "=", Right: parser.StringValue("bob")}}}}, tables)
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

	_, err := Select(&parser.SelectExpr{TableName: "users", Where: &parser.WhereClause{Conditions: []parser.Condition{{Left: "email", Operator: "=", Right: parser.StringValue("bob")}}}}, tables)
	if err != errColumnDoesNotExist {
		t.Fatalf("Select() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestSelectWithNoMatches(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("steve")}}},
	}

	rows, err := Select(&parser.SelectExpr{TableName: "users", Where: &parser.WhereClause{Conditions: []parser.Condition{{Left: "name", Operator: "=", Right: parser.StringValue("bob")}}}}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("Select() rows = %#v, want empty", rows)
	}
}

func TestSelectWithNumericComparisons(t *testing.T) {
	tests := []struct {
		name     string
		where    *parser.WhereClause
		wantRows []int64
	}{
		{name: "greater than", where: &parser.WhereClause{Conditions: []parser.Condition{{Left: "id", Operator: ">", Right: parser.Int64Value(2)}}}, wantRows: []int64{3, 4}},
		{name: "greater equal", where: &parser.WhereClause{Conditions: []parser.Condition{{Left: "id", Operator: ">=", Right: parser.Int64Value(2)}}}, wantRows: []int64{2, 3, 4}},
		{name: "less than", where: &parser.WhereClause{Conditions: []parser.Condition{{Left: "id", Operator: "<", Right: parser.Int64Value(3)}}}, wantRows: []int64{1, 2}},
		{name: "less equal", where: &parser.WhereClause{Conditions: []parser.Condition{{Left: "id", Operator: "<=", Right: parser.Int64Value(3)}}}, wantRows: []int64{1, 2, 3}},
		{name: "not equal", where: &parser.WhereClause{Conditions: []parser.Condition{{Left: "id", Operator: "!=", Right: parser.Int64Value(3)}}}, wantRows: []int64{1, 2, 4}},
	}

	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(3), parser.StringValue("cara")},
			{parser.Int64Value(4), parser.StringValue("dina")},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := Select(&parser.SelectExpr{TableName: "users", Columns: []string{"id"}, Where: tc.where}, tables)
			if err != nil {
				t.Fatalf("Select() error = %v", err)
			}
			if len(rows) != len(tc.wantRows) {
				t.Fatalf("len(rows) = %d, want %d", len(rows), len(tc.wantRows))
			}
			for i, want := range tc.wantRows {
				if rows[i][0] != parser.Int64Value(want) {
					t.Fatalf("rows[%d][0] = %#v, want %#v", i, rows[i][0], parser.Int64Value(want))
				}
			}
		})
	}
}

func TestSelectWithStringNotEqual(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
		}},
	}

	rows, err := Select(&parser.SelectExpr{TableName: "users", Columns: []string{"name"}, Where: &parser.WhereClause{Conditions: []parser.Condition{{Left: "name", Operator: "!=", Right: parser.StringValue("bob")}}}}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || rows[0][0] != parser.StringValue("alice") {
		t.Fatalf("Select() rows = %#v, want [[alice]]", rows)
	}
}

func TestSelectWhereTypeMismatch(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("alice")}}},
	}

	_, err := Select(&parser.SelectExpr{TableName: "users", Where: &parser.WhereClause{Conditions: []parser.Condition{{Left: "id", Operator: "=", Right: parser.StringValue("abc")}}}}, tables)
	if err != errTypeMismatch {
		t.Fatalf("Select() error = %v, want %v", err, errTypeMismatch)
	}
}

func TestSelectWithAndConditions(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(3), parser.StringValue("cara")},
			{parser.Int64Value(4), parser.StringValue("dina")},
		}},
	}

	rows, err := Select(&parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"id"},
		Where: &parser.WhereClause{Conditions: []parser.Condition{
			{Left: "id", Operator: ">", Right: parser.Int64Value(1)},
			{Left: "id", Operator: "<", Right: parser.Int64Value(4)},
		}},
	}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 2 || rows[0][0] != parser.Int64Value(2) || rows[1][0] != parser.Int64Value(3) {
		t.Fatalf("Select() rows = %#v, want [[2] [3]]", rows)
	}
}

func TestSelectWithAndMixedTypes(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(3), parser.StringValue("cara")},
		}},
	}

	rows, err := Select(&parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"name"},
		Where: &parser.WhereClause{Conditions: []parser.Condition{
			{Left: "id", Operator: ">", Right: parser.Int64Value(1)},
			{Left: "name", Operator: "!=", Right: parser.StringValue("bob")},
		}},
	}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || rows[0][0] != parser.StringValue("cara") {
		t.Fatalf("Select() rows = %#v, want [[cara]]", rows)
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
