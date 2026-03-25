package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

func where(condition parser.Condition, rest ...parser.ConditionChainItem) *parser.WhereClause {
	items := []parser.ConditionChainItem{{Condition: condition}}
	items = append(items, rest...)
	return &parser.WhereClause{Items: items}
}

func planSelect(t *testing.T, stmt *parser.SelectExpr) *planner.SelectPlan {
	t.Helper()

	plan, err := planner.PlanSelect(stmt)
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	return plan
}

func typedCols() []parser.ColumnDef {
	return []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}}
}

func boolCols() []parser.ColumnDef {
	return []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "active", Type: parser.ColumnTypeBool}, {Name: "name", Type: parser.ColumnTypeText}}
}

func realCols() []parser.ColumnDef {
	return []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "x", Type: parser.ColumnTypeReal}, {Name: "name", Type: parser.ColumnTypeText}}
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

	rows, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users"}), tables)
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

	rows, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users", Columns: []string{"name"}}), tables)
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

	rows, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users"}), tables)
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

	rows, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users", Columns: []string{"name", "id"}}), tables)
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

	_, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users", Columns: []string{"email"}}), tables)
	if err != errColumnDoesNotExist {
		t.Fatalf("Select() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestSelectWithIntWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("steve")}, {parser.Int64Value(2), parser.StringValue("bob")}}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users", Where: where(parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(1)})}), tables)
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

	rows, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users", Columns: []string{"name"}, Where: where(parser.Condition{Left: "name", Operator: "=", Right: parser.StringValue("bob")})}), tables)
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

	_, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users", Where: where(parser.Condition{Left: "email", Operator: "=", Right: parser.StringValue("bob")})}), tables)
	if err != errColumnDoesNotExist {
		t.Fatalf("Select() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestSelectWithNoMatches(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("steve")}}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users", Where: where(parser.Condition{Left: "name", Operator: "=", Right: parser.StringValue("bob")})}), tables)
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
		{name: "greater than", where: where(parser.Condition{Left: "id", Operator: ">", Right: parser.Int64Value(2)}), wantRows: []int64{3, 4}},
		{name: "greater equal", where: where(parser.Condition{Left: "id", Operator: ">=", Right: parser.Int64Value(2)}), wantRows: []int64{2, 3, 4}},
		{name: "less than", where: where(parser.Condition{Left: "id", Operator: "<", Right: parser.Int64Value(3)}), wantRows: []int64{1, 2}},
		{name: "less equal", where: where(parser.Condition{Left: "id", Operator: "<=", Right: parser.Int64Value(3)}), wantRows: []int64{1, 2, 3}},
		{name: "not equal", where: where(parser.Condition{Left: "id", Operator: "!=", Right: parser.Int64Value(3)}), wantRows: []int64{1, 2, 4}},
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
			rows, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users", Columns: []string{"id"}, Where: tc.where}), tables)
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

	rows, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users", Columns: []string{"name"}, Where: where(parser.Condition{Left: "name", Operator: "!=", Right: parser.StringValue("bob")})}), tables)
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

	_, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users", Where: where(parser.Condition{Left: "id", Operator: "=", Right: parser.StringValue("abc")})}), tables)
	if err != errTypeMismatch {
		t.Fatalf("Select() error = %v, want %v", err, errTypeMismatch)
	}
}

func TestSelectWithBoolWhere(t *testing.T) {
	tables := map[string]*Table{
		"flags": {Name: "flags", Columns: boolCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.BoolValue(true), parser.StringValue("alpha")},
			{parser.Int64Value(2), parser.BoolValue(false), parser.StringValue("beta")},
			{parser.Int64Value(3), parser.NullValue(), parser.StringValue("gamma")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "flags",
		Columns:   []string{"id"},
		Where:     where(parser.Condition{Left: "active", Operator: "=", Right: parser.BoolValue(true)}),
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || rows[0][0] != parser.Int64Value(1) {
		t.Fatalf("Select() rows = %#v, want [[1]]", rows)
	}
}

func TestSelectWithBoolNotEqualWhere(t *testing.T) {
	tables := map[string]*Table{
		"flags": {Name: "flags", Columns: boolCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.BoolValue(true), parser.StringValue("alpha")},
			{parser.Int64Value(2), parser.BoolValue(false), parser.StringValue("beta")},
			{parser.Int64Value(3), parser.NullValue(), parser.StringValue("gamma")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "flags",
		Columns:   []string{"id"},
		Where:     where(parser.Condition{Left: "active", Operator: "!=", Right: parser.BoolValue(true)}),
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 2 || rows[0][0] != parser.Int64Value(2) || rows[1][0] != parser.Int64Value(3) {
		t.Fatalf("Select() rows = %#v, want [[2] [3]]", rows)
	}
}

func TestSelectWhereBoolTypeMismatch(t *testing.T) {
	tables := map[string]*Table{
		"flags": {Name: "flags", Columns: boolCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.BoolValue(true), parser.StringValue("alpha")}}},
	}

	_, err := Select(planSelect(t, &parser.SelectExpr{TableName: "flags", Where: where(parser.Condition{Left: "active", Operator: "=", Right: parser.Int64Value(1)})}), tables)
	if err != errTypeMismatch {
		t.Fatalf("Select() error = %v, want %v", err, errTypeMismatch)
	}
}

func TestSelectWithRealWhereComparisons(t *testing.T) {
	tests := []struct {
		name     string
		where    *parser.WhereClause
		wantRows []int64
	}{
		{name: "equals", where: where(parser.Condition{Left: "x", Operator: "=", Right: parser.RealValue(3.14)}), wantRows: []int64{2}},
		{name: "not equals", where: where(parser.Condition{Left: "x", Operator: "!=", Right: parser.RealValue(3.14)}), wantRows: []int64{1, 3}},
		{name: "less than", where: where(parser.Condition{Left: "x", Operator: "<", Right: parser.RealValue(3.0)}), wantRows: []int64{1}},
		{name: "less equal", where: where(parser.Condition{Left: "x", Operator: "<=", Right: parser.RealValue(3.14)}), wantRows: []int64{1, 2}},
		{name: "greater than", where: where(parser.Condition{Left: "x", Operator: ">", Right: parser.RealValue(-1.0)}), wantRows: []int64{2, 3}},
		{name: "greater equal", where: where(parser.Condition{Left: "x", Operator: ">=", Right: parser.RealValue(10.25)}), wantRows: []int64{3}},
	}

	tables := map[string]*Table{
		"measurements": {Name: "measurements", Columns: realCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.RealValue(-2.5), parser.StringValue("neg")},
			{parser.Int64Value(2), parser.RealValue(3.14), parser.StringValue("pi")},
			{parser.Int64Value(3), parser.RealValue(10.25), parser.StringValue("hi")},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := Select(planSelect(t, &parser.SelectExpr{TableName: "measurements", Columns: []string{"id"}, Where: tc.where}), tables)
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

func TestSelectWhereRealTypeMismatch(t *testing.T) {
	tables := map[string]*Table{
		"measurements": {Name: "measurements", Columns: realCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.RealValue(3.14), parser.StringValue("pi")}}},
	}

	tests := []*parser.WhereClause{
		where(parser.Condition{Left: "x", Operator: "=", Right: parser.Int64Value(3)}),
		where(parser.Condition{Left: "x", Operator: "=", Right: parser.StringValue("3.14")}),
		where(parser.Condition{Left: "x", Operator: "=", Right: parser.BoolValue(true)}),
	}

	for _, clause := range tests {
		_, err := Select(planSelect(t, &parser.SelectExpr{TableName: "measurements", Where: clause}), tables)
		if err != errTypeMismatch {
			t.Fatalf("Select() error = %v, want %v", err, errTypeMismatch)
		}
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

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"id"},
		Where: where(
			parser.Condition{Left: "id", Operator: ">", Right: parser.Int64Value(1)},
			parser.ConditionChainItem{Op: parser.BooleanOpAnd, Condition: parser.Condition{Left: "id", Operator: "<", Right: parser.Int64Value(4)}},
		),
	}), tables)
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

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"name"},
		Where: where(
			parser.Condition{Left: "id", Operator: ">", Right: parser.Int64Value(1)},
			parser.ConditionChainItem{Op: parser.BooleanOpAnd, Condition: parser.Condition{Left: "name", Operator: "!=", Right: parser.StringValue("bob")}},
		),
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || rows[0][0] != parser.StringValue("cara") {
		t.Fatalf("Select() rows = %#v, want [[cara]]", rows)
	}
}

func TestSelectWithOrConditions(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(3), parser.StringValue("cara")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"id"},
		Where: where(
			parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(1)},
			parser.ConditionChainItem{Op: parser.BooleanOpOr, Condition: parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(3)}},
		),
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 2 || rows[0][0] != parser.Int64Value(1) || rows[1][0] != parser.Int64Value(3) {
		t.Fatalf("Select() rows = %#v, want [[1] [3]]", rows)
	}
}

func TestSelectWithOrNoMatches(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"id"},
		Where: where(
			parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(3)},
			parser.ConditionChainItem{Op: parser.BooleanOpOr, Condition: parser.Condition{Left: "name", Operator: "=", Right: parser.StringValue("cara")}},
		),
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("Select() rows = %#v, want empty", rows)
	}
}

func TestSelectWhereUsesBooleanPrecedence(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(1), parser.StringValue("bob")},
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(3), parser.StringValue("cara")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"name"},
		Predicate: &parser.PredicateExpr{
			Kind: parser.PredicateKindOr,
			Left: &parser.PredicateExpr{
				Kind:       parser.PredicateKindComparison,
				Comparison: &parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(1)},
			},
			Right: &parser.PredicateExpr{
				Kind: parser.PredicateKindAnd,
				Left: &parser.PredicateExpr{
					Kind:       parser.PredicateKindComparison,
					Comparison: &parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(2)},
				},
				Right: &parser.PredicateExpr{
					Kind:       parser.PredicateKindComparison,
					Comparison: &parser.Condition{Left: "name", Operator: "=", Right: parser.StringValue("bob")},
				},
			},
		},
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 3 || rows[0][0] != parser.StringValue("alice") || rows[1][0] != parser.StringValue("bob") || rows[2][0] != parser.StringValue("bob") {
		t.Fatalf("Select() rows = %#v, want precedence-aware [[alice] [bob] [bob]]", rows)
	}
}

func TestSelectWhereSupportsNotAndGrouping(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(3), parser.StringValue("cara")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"id"},
		Predicate: &parser.PredicateExpr{
			Kind: parser.PredicateKindNot,
			Inner: &parser.PredicateExpr{
				Kind: parser.PredicateKindOr,
				Left: &parser.PredicateExpr{
					Kind:       parser.PredicateKindComparison,
					Comparison: &parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(1)},
				},
				Right: &parser.PredicateExpr{
					Kind:       parser.PredicateKindComparison,
					Comparison: &parser.Condition{Left: "name", Operator: "=", Right: parser.StringValue("cara")},
				},
			},
		},
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || rows[0][0] != parser.Int64Value(2) {
		t.Fatalf("Select() rows = %#v, want [[2]]", rows)
	}
}

func TestSelectWhereSupportsColumnComparison(t *testing.T) {
	tables := map[string]*Table{
		"pairs": {Name: "pairs", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "mirror", Type: parser.ColumnTypeInt},
		}, Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.Int64Value(1)},
			{parser.Int64Value(2), parser.Int64Value(3)},
			{parser.Int64Value(4), parser.Int64Value(4)},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "pairs",
		Columns:   []string{"id"},
		Predicate: &parser.PredicateExpr{
			Kind:       parser.PredicateKindComparison,
			Comparison: &parser.Condition{Left: "id", Operator: "=", RightRef: "mirror"},
		},
		OrderBy: &parser.OrderByClause{Column: "id"},
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 2 || rows[0][0] != parser.Int64Value(1) || rows[1][0] != parser.Int64Value(4) {
		t.Fatalf("Select() rows = %#v, want [[1] [4]]", rows)
	}
}

func TestSelectOrderByIntAsc(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(3), parser.StringValue("cara")},
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"id"},
		OrderBy:   &parser.OrderByClause{Column: "id"},
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if rows[0][0] != parser.Int64Value(1) || rows[1][0] != parser.Int64Value(2) || rows[2][0] != parser.Int64Value(3) {
		t.Fatalf("Select() rows = %#v, want [[1] [2] [3]]", rows)
	}
}

func TestSelectOrderByIntDesc(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(3), parser.StringValue("cara")},
			{parser.Int64Value(2), parser.StringValue("bob")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"id"},
		OrderBy:   &parser.OrderByClause{Column: "id", Desc: true},
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if rows[0][0] != parser.Int64Value(3) || rows[1][0] != parser.Int64Value(2) || rows[2][0] != parser.Int64Value(1) {
		t.Fatalf("Select() rows = %#v, want [[3] [2] [1]]", rows)
	}
}

func TestSelectOrderByStringAsc(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(3), parser.StringValue("cara")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"name"},
		OrderBy:   &parser.OrderByClause{Column: "name"},
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if rows[0][0] != parser.StringValue("alice") || rows[1][0] != parser.StringValue("bob") || rows[2][0] != parser.StringValue("cara") {
		t.Fatalf("Select() rows = %#v, want [[alice] [bob] [cara]]", rows)
	}
}

func TestSelectOrderByStringDesc(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(3), parser.StringValue("cara")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"name"},
		OrderBy:   &parser.OrderByClause{Column: "name", Desc: true},
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if rows[0][0] != parser.StringValue("cara") || rows[1][0] != parser.StringValue("bob") || rows[2][0] != parser.StringValue("alice") {
		t.Fatalf("Select() rows = %#v, want [[cara] [bob] [alice]]", rows)
	}
}

func TestSelectOrderByWithWhereAndProjection(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(3), parser.StringValue("cara")},
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"name"},
		Where:     where(parser.Condition{Left: "id", Operator: ">", Right: parser.Int64Value(1)}),
		OrderBy:   &parser.OrderByClause{Column: "id", Desc: true},
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if rows[0][0] != parser.StringValue("cara") || rows[1][0] != parser.StringValue("bob") {
		t.Fatalf("Select() rows = %#v, want [[cara] [bob]]", rows)
	}
}

func TestSelectOrderByUnknownColumn(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("alice")}}},
	}

	_, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		OrderBy:   &parser.OrderByClause{Column: "age"},
	}), tables)
	if err != errColumnDoesNotExist {
		t.Fatalf("Select() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestSelectCountStarEmptyTable(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols()},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users", IsCountStar: true}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] != parser.Int64Value(0) {
		t.Fatalf("Select() rows = %#v, want [[0]]", rows)
	}
}

func TestSelectCountStarPopulatedTable(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(3), parser.StringValue("cara")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users", IsCountStar: true}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || rows[0][0] != parser.Int64Value(3) {
		t.Fatalf("Select() rows = %#v, want [[3]]", rows)
	}
}

func TestSelectCountStarWithWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(3), parser.StringValue("cara")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName:   "users",
		IsCountStar: true,
		Where:       where(parser.Condition{Left: "id", Operator: ">", Right: parser.Int64Value(1)}),
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || rows[0][0] != parser.Int64Value(2) {
		t.Fatalf("Select() rows = %#v, want [[2]]", rows)
	}
}

func TestSelectCountStarOrderByUnsupported(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("alice")}}},
	}

	_, err := Select(planSelect(t, &parser.SelectExpr{
		TableName:   "users",
		IsCountStar: true,
		OrderBy:     &parser.OrderByClause{Column: "id"},
	}), tables)
	if err != errCountOrderByUnsupported {
		t.Fatalf("Select() error = %v, want %v", err, errCountOrderByUnsupported)
	}
}

func TestSelectMissingTable(t *testing.T) {
	_, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users"}), map[string]*Table{})
	if err != errTableDoesNotExist {
		t.Fatalf("Select() error = %v, want %v", err, errTableDoesNotExist)
	}
}

func TestProjectedColumnNamesAllColumns(t *testing.T) {
	table := &Table{Name: "users", Columns: typedCols()}

	got, err := ProjectedColumnNames(planSelect(t, &parser.SelectExpr{TableName: "users"}), table)
	if err != nil {
		t.Fatalf("ProjectedColumnNames() error = %v", err)
	}
	if len(got) != 2 || got[0] != "id" || got[1] != "name" {
		t.Fatalf("ProjectedColumnNames() = %#v, want [id name]", got)
	}
}

func TestProjectedColumnNamesInvalidColumn(t *testing.T) {
	table := &Table{Name: "users", Columns: typedCols()}

	_, err := ProjectedColumnNames(planSelect(t, &parser.SelectExpr{TableName: "users", Columns: []string{"email"}}), table)
	if err != errColumnDoesNotExist {
		t.Fatalf("ProjectedColumnNames() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestProjectedColumnNamesCountStar(t *testing.T) {
	table := &Table{Name: "users", Columns: typedCols()}

	got, err := ProjectedColumnNames(planSelect(t, &parser.SelectExpr{TableName: "users", IsCountStar: true}), table)
	if err != nil {
		t.Fatalf("ProjectedColumnNames() error = %v", err)
	}
	if len(got) != 1 || got[0] != "count" {
		t.Fatalf("ProjectedColumnNames() = %#v, want [count]", got)
	}
}

func TestValidateSelectPlanLiteralAllowsNilTableScan(t *testing.T) {
	plan, err := planner.PlanSelect(&parser.SelectExpr{
		Expr: &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: 1},
	})
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}

	if err := validateSelectPlan(plan); err != nil {
		t.Fatalf("validateSelectPlan() error = %v, want nil", err)
	}
}

func TestSelectInvalidPlanMissingTableScan(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{{parser.Int64Value(1), parser.StringValue("alice")}}},
	}

	plan := &planner.SelectPlan{
		Stmt: &parser.SelectExpr{TableName: "users"},
	}

	_, err := Select(plan, tables)
	if err != errInvalidSelectPlan {
		t.Fatalf("Select() error = %v, want %v", err, errInvalidSelectPlan)
	}
}

func TestSelectWithIndexScan(t *testing.T) {
	table := &Table{
		Name:    "users",
		Columns: typedCols(),
		Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(3), parser.StringValue("alice")},
		},
		Indexes: map[string]*planner.BasicIndex{
			"name": planner.NewBasicIndex("users", "name"),
		},
	}
	if err := rebuildIndexesForTable(table); err != nil {
		t.Fatalf("rebuildIndexesForTable() error = %v", err)
	}

	rows, err := Select(&planner.SelectPlan{
		Stmt: &parser.SelectExpr{
			TableName: "users",
			Columns:   []string{"id"},
			Where:     where(parser.Condition{Left: "name", Operator: "=", Right: parser.StringValue("alice")}),
		},
		ScanType: planner.ScanTypeIndex,
		IndexScan: &planner.IndexScan{
			TableName:  "users",
			ColumnName: "name",
			Value:      parser.StringValue("alice"),
		},
	}, map[string]*Table{"users": table})
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 2 || rows[0][0] != parser.Int64Value(1) || rows[1][0] != parser.Int64Value(3) {
		t.Fatalf("Select() rows = %#v, want [[1] [3]]", rows)
	}
}

func TestSelectInvalidPlanMissingIndexScanPayload(t *testing.T) {
	plan := &planner.SelectPlan{
		Stmt:     &parser.SelectExpr{TableName: "users"},
		ScanType: planner.ScanTypeIndex,
	}

	_, err := Select(plan, map[string]*Table{"users": {Name: "users", Columns: typedCols()}})
	if err != errInvalidSelectPlan {
		t.Fatalf("Select() error = %v, want %v", err, errInvalidSelectPlan)
	}
}

func TestSelectInvalidPlanMissingRuntimeIndex(t *testing.T) {
	table := &Table{
		Name:    "users",
		Columns: typedCols(),
		Rows:    [][]parser.Value{{parser.Int64Value(1), parser.StringValue("alice")}},
	}

	_, err := Select(&planner.SelectPlan{
		Stmt: &parser.SelectExpr{
			TableName: "users",
			Where:     where(parser.Condition{Left: "name", Operator: "=", Right: parser.StringValue("alice")}),
		},
		ScanType: planner.ScanTypeIndex,
		IndexScan: &planner.IndexScan{
			TableName:  "users",
			ColumnName: "name",
			Value:      parser.StringValue("alice"),
		},
	}, map[string]*Table{"users": table})
	if err != errInvalidSelectPlan {
		t.Fatalf("Select() error = %v, want %v", err, errInvalidSelectPlan)
	}
}
