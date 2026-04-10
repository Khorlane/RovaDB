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
	if err == nil || err.Error() != "execution: column not found: email" {
		t.Fatalf("Select() error = %v, want %q", err, "execution: column not found: email")
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
	if err == nil || err.Error() != "execution: column not found: email" {
		t.Fatalf("Select() error = %v, want %q", err, "execution: column not found: email")
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

func TestSelectStringEqualityIsCaseInsensitive(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("Bob")},
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(3), parser.StringValue("BOB")},
			{parser.Int64Value(4), parser.StringValue("alice")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"name"},
		Where:     where(parser.Condition{Left: "name", Operator: "=", Right: parser.StringValue("bob")}),
		OrderBy:   &parser.OrderByClause{Column: "id"},
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 3 ||
		rows[0][0] != parser.StringValue("Bob") ||
		rows[1][0] != parser.StringValue("bob") ||
		rows[2][0] != parser.StringValue("BOB") {
		t.Fatalf("Select() rows = %#v, want Bob/bob/BOB matches", rows)
	}
}

func TestSelectStringOrderingComparisonsAreCaseInsensitive(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("Alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(3), parser.StringValue("Charles")},
		}},
	}

	lessRows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"name"},
		Where:     where(parser.Condition{Left: "name", Operator: "<", Right: parser.StringValue("bob")}),
		OrderBy:   &parser.OrderByClause{Column: "id"},
	}), tables)
	if err != nil {
		t.Fatalf("Select(<) error = %v", err)
	}
	if len(lessRows) != 1 || lessRows[0][0] != parser.StringValue("Alice") {
		t.Fatalf("Select(<) rows = %#v, want [[Alice]]", lessRows)
	}

	greaterRows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"name"},
		Where:     where(parser.Condition{Left: "name", Operator: ">", Right: parser.StringValue("bob")}),
		OrderBy:   &parser.OrderByClause{Column: "id"},
	}), tables)
	if err != nil {
		t.Fatalf("Select(>) error = %v", err)
	}
	if len(greaterRows) != 1 || greaterRows[0][0] != parser.StringValue("Charles") {
		t.Fatalf("Select(>) rows = %#v, want [[Charles]]", greaterRows)
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

func TestSelectWhereSupportsFunctionOperands(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("ALICE")},
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(3), parser.StringValue("Cara")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"id"},
		Predicate: &parser.PredicateExpr{
			Kind: parser.PredicateKindComparison,
			Comparison: &parser.Condition{
				LeftExpr: &parser.ValueExpr{
					Kind:     parser.ValueExprKindFunctionCall,
					FuncName: "LOWER",
					Arg:      &parser.ValueExpr{Kind: parser.ValueExprKindColumnRef, Column: "name"},
				},
				Operator: "=",
				RightExpr: &parser.ValueExpr{
					Kind:  parser.ValueExprKindLiteral,
					Value: parser.StringValue("bob"),
				},
			},
		},
		OrderBy: &parser.OrderByClause{Column: "id"},
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || rows[0][0] != parser.Int64Value(2) {
		t.Fatalf("Select() rows = %#v, want [[2]]", rows)
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

func TestSelectOrderByStringAscIsCaseInsensitive(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(1), parser.StringValue("Alice")},
			{parser.Int64Value(3), parser.StringValue("Charles")},
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
	if rows[0][0] != parser.StringValue("Alice") || rows[1][0] != parser.StringValue("bob") || rows[2][0] != parser.StringValue("Charles") {
		t.Fatalf("Select() rows = %#v, want [[Alice] [bob] [Charles]]", rows)
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

func TestSelectOrderByProjectionAlias(t *testing.T) {
	tables := map[string]*Table{
		"customers": {Name: "customers", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(1), parser.StringValue("alice")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName:         "customers",
		ProjectionExprs:   []*parser.ValueExpr{{Kind: parser.ValueExprKindColumnRef, Column: "id"}},
		ProjectionLabels:  []string{"id AS cust_nbr"},
		ProjectionAliases: []string{"cust_nbr"},
		OrderBy:           &parser.OrderByClause{Column: "cust_nbr"},
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 2 || rows[0][0] != parser.Int64Value(1) || rows[1][0] != parser.Int64Value(2) {
		t.Fatalf("Select() rows = %#v, want [[1] [2]]", rows)
	}
}

func TestSelectOrderByMultipleColumns(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(2), parser.StringValue("alice")},
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(3), parser.StringValue("bob")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		Columns:   []string{"name", "id"},
		OrderBys: []parser.OrderByClause{
			{Column: "name"},
			{Column: "id", Desc: true},
		},
		OrderBy: &parser.OrderByClause{Column: "name"},
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if rows[0][0] != parser.StringValue("alice") || rows[0][1] != parser.Int64Value(2) ||
		rows[1][0] != parser.StringValue("alice") || rows[1][1] != parser.Int64Value(1) ||
		rows[2][0] != parser.StringValue("bob") || rows[2][1] != parser.Int64Value(3) {
		t.Fatalf("Select() rows = %#v, want alice/2 alice/1 bob/3", rows)
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
	if err == nil || err.Error() != "execution: column not found: age" {
		t.Fatalf("Select() error = %v, want %q", err, "execution: column not found: age")
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

func TestSelectAggregateFunctionsSingleTable(t *testing.T) {
	tables := map[string]*Table{
		"metrics": {Name: "metrics", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "name", Type: parser.ColumnTypeText},
			{Name: "score", Type: parser.ColumnTypeReal},
		}, Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("beta"), parser.RealValue(1.5)},
			{parser.Int64Value(2), parser.StringValue("alpha"), parser.RealValue(2.5)},
			{parser.Int64Value(3), parser.StringValue("gamma"), parser.RealValue(3.0)},
		}},
	}

	stmt, ok := parser.ParseSelectExpr("SELECT COUNT(name), AVG(score), SUM(score), MIN(name), MAX(score) FROM metrics")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	plan, err := planner.PlanSelect(stmt)
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	rows, err := Select(plan, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || len(rows[0]) != 5 {
		t.Fatalf("rows = %#v, want one aggregate row", rows)
	}
	if rows[0][0] != parser.Int64Value(3) || rows[0][1] != parser.RealValue((1.5+2.5+3.0)/3.0) || rows[0][2] != parser.RealValue(7.0) || rows[0][3] != parser.StringValue("alpha") || rows[0][4] != parser.RealValue(3.0) {
		t.Fatalf("rows[0] = %#v, want [3 2.333... 7 alpha 3.0]", rows[0])
	}
}

func TestSelectAggregateFunctionsJoin(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "dept_id", Type: parser.ColumnTypeInt},
		}, Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.Int64Value(10)},
			{parser.Int64Value(2), parser.Int64Value(20)},
			{parser.Int64Value(3), parser.Int64Value(10)},
		}},
		"departments": {Name: "departments", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "name", Type: parser.ColumnTypeText},
		}, Rows: [][]parser.Value{
			{parser.Int64Value(10), parser.StringValue("eng")},
			{parser.Int64Value(20), parser.StringValue("ops")},
		}},
	}

	stmt, ok := parser.ParseSelectExpr("SELECT COUNT(d.name), MIN(d.name), MAX(d.name) FROM users u JOIN departments d ON u.dept_id = d.id")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	plan, err := planner.PlanSelect(stmt)
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	rows, err := Select(plan, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || len(rows[0]) != 3 {
		t.Fatalf("rows = %#v, want one aggregate row", rows)
	}
	if rows[0][0] != parser.Int64Value(3) || rows[0][1] != parser.StringValue("eng") || rows[0][2] != parser.StringValue("ops") {
		t.Fatalf("rows[0] = %#v, want [3 eng ops]", rows[0])
	}
}

func TestSelectAggregateProjectionRejectsMixedAggregateAndNonAggregate(t *testing.T) {
	tables := map[string]*Table{
		"metrics": {Name: "metrics", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "score", Type: parser.ColumnTypeReal},
		}, Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.RealValue(1.5)},
		}},
	}

	stmt, ok := parser.ParseSelectExpr("SELECT AVG(score), id FROM metrics")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	plan, err := planner.PlanSelect(stmt)
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	_, err = Select(plan, tables)
	if err != errUnsupportedStatement {
		t.Fatalf("Select() error = %v, want %v", err, errUnsupportedStatement)
	}
}

func TestSelectMissingTable(t *testing.T) {
	_, err := Select(planSelect(t, &parser.SelectExpr{TableName: "users"}), map[string]*Table{})
	want := newTableNotFoundError("users")
	if err == nil || err.Error() != want.Error() {
		t.Fatalf("Select() error = %v, want %v", err, want)
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
	if err == nil || err.Error() != "execution: column not found: email" {
		t.Fatalf("ProjectedColumnNames() error = %v, want %q", err, "execution: column not found: email")
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

func TestProjectedColumnNamesExpressionProjection(t *testing.T) {
	table := &Table{Name: "users", Columns: typedCols()}

	got, err := ProjectedColumnNames(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		ProjectionExprs: []*parser.ValueExpr{
			{Kind: parser.ValueExprKindFunctionCall, FuncName: "LOWER", Arg: &parser.ValueExpr{Kind: parser.ValueExprKindColumnRef, Column: "name"}},
			{Kind: parser.ValueExprKindColumnRef, Column: "id"},
		},
		ProjectionLabels: []string{"LOWER(name)", "id"},
	}), table)
	if err != nil {
		t.Fatalf("ProjectedColumnNames() error = %v", err)
	}
	if len(got) != 2 || got[0] != "LOWER(name)" || got[1] != "id" {
		t.Fatalf("ProjectedColumnNames() = %#v, want [LOWER(name) id]", got)
	}
}

func TestProjectedColumnNamesUsesAliasWhenPresent(t *testing.T) {
	table := &Table{Name: "customers", Columns: typedCols()}

	got, err := ProjectedColumnNames(planSelect(t, &parser.SelectExpr{
		TableName:         "customers",
		ProjectionExprs:   []*parser.ValueExpr{{Kind: parser.ValueExprKindColumnRef, Column: "id"}},
		ProjectionLabels:  []string{"id AS cust_nbr"},
		ProjectionAliases: []string{"cust_nbr"},
	}), table)
	if err != nil {
		t.Fatalf("ProjectedColumnNames() error = %v", err)
	}
	if len(got) != 1 || got[0] != "cust_nbr" {
		t.Fatalf("ProjectedColumnNames() = %#v, want [cust_nbr]", got)
	}
}

func TestSelectQualifiedProjectionAndPredicate(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		ProjectionExprs: []*parser.ValueExpr{
			{Kind: parser.ValueExprKindColumnRef, Qualifier: "users", Column: "id"},
		},
		ProjectionLabels: []string{"users.id"},
		Predicate: &parser.PredicateExpr{
			Kind: parser.PredicateKindComparison,
			Comparison: &parser.Condition{
				LeftExpr:  &parser.ValueExpr{Kind: parser.ValueExprKindColumnRef, Qualifier: "users", Column: "name"},
				Operator:  "=",
				RightExpr: &parser.ValueExpr{Kind: parser.ValueExprKindLiteral, Value: parser.StringValue("bob")},
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

func TestSelectProjectionAndPredicateArithmetic(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		ProjectionExprs: []*parser.ValueExpr{
			{
				Kind:  parser.ValueExprKindBinary,
				Op:    parser.ValueExprBinaryOpAdd,
				Left:  &parser.ValueExpr{Kind: parser.ValueExprKindColumnRef, Column: "id"},
				Right: &parser.ValueExpr{Kind: parser.ValueExprKindLiteral, Value: parser.Int64Value(1)},
			},
		},
		ProjectionLabels: []string{"id + 1"},
		Predicate: &parser.PredicateExpr{
			Kind: parser.PredicateKindComparison,
			Comparison: &parser.Condition{
				LeftExpr: &parser.ValueExpr{
					Kind:  parser.ValueExprKindBinary,
					Op:    parser.ValueExprBinaryOpAdd,
					Left:  &parser.ValueExpr{Kind: parser.ValueExprKindColumnRef, Column: "id"},
					Right: &parser.ValueExpr{Kind: parser.ValueExprKindLiteral, Value: parser.Int64Value(1)},
				},
				Operator:  "=",
				RightExpr: &parser.ValueExpr{Kind: parser.ValueExprKindLiteral, Value: parser.Int64Value(3)},
			},
		},
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || rows[0][0] != parser.Int64Value(3) {
		t.Fatalf("rows = %#v, want [[3]]", rows)
	}
}

func TestSelectAliasQualifiedProjectionPredicateAndOrderBy(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: typedCols(), Rows: [][]parser.Value{
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(1), parser.StringValue("alice")},
		}},
	}

	rows, err := Select(planSelect(t, &parser.SelectExpr{
		TableName: "users",
		From:      []parser.TableRef{{Name: "users", Alias: "u"}},
		ProjectionExprs: []*parser.ValueExpr{
			{Kind: parser.ValueExprKindColumnRef, Qualifier: "u", Column: "id"},
		},
		ProjectionLabels: []string{"u.id"},
		Predicate: &parser.PredicateExpr{
			Kind: parser.PredicateKindComparison,
			Comparison: &parser.Condition{
				LeftExpr:  &parser.ValueExpr{Kind: parser.ValueExprKindColumnRef, Qualifier: "u", Column: "name"},
				Operator:  "!=",
				RightExpr: &parser.ValueExpr{Kind: parser.ValueExprKindLiteral, Value: parser.StringValue("bob")},
			},
		},
		OrderBy: &parser.OrderByClause{Column: "u.id"},
	}), tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || rows[0][0] != parser.Int64Value(1) {
		t.Fatalf("Select() rows = %#v, want [[1]]", rows)
	}
}

func TestSelectExplicitJoinWithAliases(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "name", Type: parser.ColumnTypeText},
			{Name: "dept_id", Type: parser.ColumnTypeInt},
		}, Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice"), parser.Int64Value(10)},
			{parser.Int64Value(2), parser.StringValue("bob"), parser.Int64Value(20)},
			{parser.Int64Value(3), parser.StringValue("cara"), parser.Int64Value(10)},
		}},
		"departments": {Name: "departments", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "name", Type: parser.ColumnTypeText},
		}, Rows: [][]parser.Value{
			{parser.Int64Value(10), parser.StringValue("eng")},
			{parser.Int64Value(20), parser.StringValue("ops")},
		}},
	}

	stmt, ok := parser.ParseSelectExpr("SELECT u.name, d.name FROM users u JOIN departments d ON u.dept_id = d.id WHERE d.name != 'ops' ORDER BY u.id")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := planner.PlanSelect(stmt)
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}

	rows, err := Select(plan, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(rows))
	}
	if rows[0][0] != parser.StringValue("alice") || rows[0][1] != parser.StringValue("eng") {
		t.Fatalf("rows[0] = %#v, want [alice eng]", rows[0])
	}
	if rows[1][0] != parser.StringValue("cara") || rows[1][1] != parser.StringValue("eng") {
		t.Fatalf("rows[1] = %#v, want [cara eng]", rows[1])
	}
}

func TestProjectedColumnNamesForPlanJoin(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "dept_id", Type: parser.ColumnTypeInt},
		}},
		"departments": {Name: "departments", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "name", Type: parser.ColumnTypeText},
		}},
	}

	stmt, ok := parser.ParseSelectExpr("SELECT u.id, d.name FROM users u JOIN departments d ON u.dept_id = d.id")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	plan, err := planner.PlanSelect(stmt)
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}

	got, err := ProjectedColumnNamesForPlan(plan, tables)
	if err != nil {
		t.Fatalf("ProjectedColumnNamesForPlan() error = %v", err)
	}
	if len(got) != 2 || got[0] != "u.id" || got[1] != "d.name" {
		t.Fatalf("ProjectedColumnNamesForPlan() = %#v, want [u.id d.name]", got)
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
		Query: &planner.SelectQuery{TableName: "users"},
	}

	_, err := Select(plan, tables)
	if err != errInvalidSelectPlan {
		t.Fatalf("Select() error = %v, want %v", err, errInvalidSelectPlan)
	}
}

func TestSelectWithHandoffTableScan(t *testing.T) {
	table := &Table{
		Name:    "users",
		Columns: typedCols(),
		Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
		},
	}

	handoff, err := NewSelectExecutionHandoff(&planner.SelectPlan{
		Query: &planner.SelectQuery{
			TableName: "users",
			Columns:   []string{"name"},
			OrderBy:   &planner.OrderByClause{Column: "id"},
		},
		ScanType:  planner.ScanTypeTable,
		TableScan: &planner.TableScan{TableName: "users"},
	})
	if err != nil {
		t.Fatalf("NewSelectExecutionHandoff() error = %v", err)
	}

	rows, err := SelectWithHandoff(handoff, map[string]*Table{"users": table})
	if err != nil {
		t.Fatalf("SelectWithHandoff() error = %v", err)
	}
	if len(rows) != 2 || rows[0][0] != parser.StringValue("alice") || rows[1][0] != parser.StringValue("bob") {
		t.Fatalf("SelectWithHandoff() rows = %#v, want [[alice] [bob]]", rows)
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
	}

	rows, err := Select(&planner.SelectPlan{
		Query: &planner.SelectQuery{
			TableName: "users",
			Columns:   []string{"id"},
			Where: &planner.WhereClause{Items: []planner.ConditionChainItem{{
				Condition: planner.Condition{Left: "name", Operator: "=", Right: planner.StringValue("alice")},
			}}},
		},
		ScanType: planner.ScanTypeIndex,
		IndexScan: &planner.IndexScan{
			TableName:   "users",
			ColumnName:  "name",
			LookupValue: planner.StringValue("alice"),
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
		Query:    &planner.SelectQuery{TableName: "users"},
		ScanType: planner.ScanTypeIndex,
	}

	_, err := Select(plan, map[string]*Table{"users": {Name: "users", Columns: typedCols()}})
	if err != errInvalidSelectPlan {
		t.Fatalf("Select() error = %v, want %v", err, errInvalidSelectPlan)
	}
}

func TestSelectIndexScanDoesNotRequireRuntimeIndexShell(t *testing.T) {
	table := &Table{
		Name:    "users",
		Columns: typedCols(),
		Rows:    [][]parser.Value{{parser.Int64Value(1), parser.StringValue("alice")}},
	}

	rows, err := Select(&planner.SelectPlan{
		Query: &planner.SelectQuery{
			TableName: "users",
			Where: &planner.WhereClause{Items: []planner.ConditionChainItem{{
				Condition: planner.Condition{Left: "name", Operator: "=", Right: planner.StringValue("alice")},
			}}},
		},
		ScanType: planner.ScanTypeIndex,
		IndexScan: &planner.IndexScan{
			TableName:   "users",
			ColumnName:  "name",
			LookupValue: planner.StringValue("alice"),
		},
	}, map[string]*Table{"users": table})
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || rows[0][0] != parser.Int64Value(1) || rows[0][1] != parser.StringValue("alice") {
		t.Fatalf("Select() rows = %#v, want [[1 alice]]", rows)
	}
}

func TestSelectIndexScanBridgeAdaptsPlannerProjectionExpressions(t *testing.T) {
	table := &Table{
		Name:    "users",
		Columns: typedCols(),
		Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(3), parser.StringValue("alice")},
		},
	}

	rows, err := Select(&planner.SelectPlan{
		Query: &planner.SelectQuery{
			TableName: "users",
			ProjectionExprs: []*planner.ValueExpr{{
				Kind: planner.ValueExprKindBinary,
				Op:   planner.ValueExprBinaryOpAdd,
				Left: &planner.ValueExpr{Kind: planner.ValueExprKindColumnRef, Column: "id"},
				Right: &planner.ValueExpr{
					Kind:  planner.ValueExprKindLiteral,
					Value: planner.Int64Value(10),
				},
			}},
			ProjectionLabels: []string{"id + 10"},
			Predicate: &planner.PredicateExpr{
				Kind: planner.PredicateKindComparison,
				Comparison: &planner.Condition{
					LeftExpr:  &planner.ValueExpr{Kind: planner.ValueExprKindColumnRef, Column: "name"},
					Operator:  "=",
					RightExpr: &planner.ValueExpr{Kind: planner.ValueExprKindLiteral, Value: planner.StringValue("alice")},
				},
			},
			OrderBy: &planner.OrderByClause{Column: "id"},
		},
		ScanType: planner.ScanTypeIndex,
		IndexScan: &planner.IndexScan{
			TableName:   "users",
			ColumnName:  "name",
			LookupValue: planner.StringValue("alice"),
		},
	}, map[string]*Table{"users": table})
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 2 || rows[0][0] != parser.Int64Value(11) || rows[1][0] != parser.Int64Value(13) {
		t.Fatalf("Select() rows = %#v, want [[11] [13]]", rows)
	}
}

func TestSelectCandidateRowsWithHandoffIndexScan(t *testing.T) {
	table := &Table{
		Name:    "users",
		Columns: typedCols(),
	}

	handoff, err := NewSelectExecutionHandoff(&planner.SelectPlan{
		Query: &planner.SelectQuery{
			TableName: "users",
			Columns:   []string{"id"},
			Where: &planner.WhereClause{Items: []planner.ConditionChainItem{{
				Condition: planner.Condition{Left: "name", Operator: "=", Right: planner.StringValue("alice")},
			}}},
		},
		ScanType: planner.ScanTypeIndex,
		IndexScan: &planner.IndexScan{
			TableName:   "users",
			ColumnName:  "name",
			LookupValue: planner.StringValue("alice"),
		},
	})
	if err != nil {
		t.Fatalf("NewSelectExecutionHandoff() error = %v", err)
	}

	rows, err := SelectCandidateRowsWithHandoff(handoff, table, [][]parser.Value{
		{parser.Int64Value(1), parser.StringValue("alice")},
		{parser.Int64Value(3), parser.StringValue("alice")},
	})
	if err != nil {
		t.Fatalf("SelectCandidateRowsWithHandoff() error = %v", err)
	}
	if len(rows) != 2 || rows[0][0] != parser.Int64Value(1) || rows[1][0] != parser.Int64Value(3) {
		t.Fatalf("SelectCandidateRowsWithHandoff() rows = %#v, want [[1] [3]]", rows)
	}
}

func TestSelectJoinBridgeAdaptsPlannerPredicateAndProjection(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "name", Type: parser.ColumnTypeText},
			{Name: "dept_id", Type: parser.ColumnTypeInt},
		}, Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice"), parser.Int64Value(10)},
			{parser.Int64Value(2), parser.StringValue("bob"), parser.Int64Value(20)},
			{parser.Int64Value(3), parser.StringValue("cara"), parser.Int64Value(10)},
		}},
		"departments": {Name: "departments", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "name", Type: parser.ColumnTypeText},
		}, Rows: [][]parser.Value{
			{parser.Int64Value(10), parser.StringValue("eng")},
			{parser.Int64Value(20), parser.StringValue("ops")},
		}},
	}

	rows, err := Select(&planner.SelectPlan{
		Query: &planner.SelectQuery{
			TableName: "users",
			From:      []planner.TableRef{{Name: "users", Alias: "u"}},
			Joins: []planner.JoinClause{{
				Right: planner.TableRef{Name: "departments", Alias: "d"},
			}},
			ProjectionExprs: []*planner.ValueExpr{
				{Kind: planner.ValueExprKindColumnRef, Qualifier: "u", Column: "name"},
				{Kind: planner.ValueExprKindFunctionCall, FuncName: "UPPER", Arg: &planner.ValueExpr{Kind: planner.ValueExprKindColumnRef, Qualifier: "d", Column: "name"}},
			},
			ProjectionLabels: []string{"u.name", "UPPER(d.name)"},
			Predicate: &planner.PredicateExpr{
				Kind: planner.PredicateKindComparison,
				Comparison: &planner.Condition{
					LeftExpr:  &planner.ValueExpr{Kind: planner.ValueExprKindColumnRef, Qualifier: "d", Column: "name"},
					Operator:  "=",
					RightExpr: &planner.ValueExpr{Kind: planner.ValueExprKindLiteral, Value: planner.StringValue("eng")},
				},
			},
			OrderBy: &planner.OrderByClause{Column: "u.id"},
		},
		ScanType: planner.ScanTypeJoin,
		JoinScan: &planner.JoinScan{
			LeftTableName:   "users",
			LeftTableAlias:  "u",
			LeftColumnName:  "dept_id",
			RightTableName:  "departments",
			RightTableAlias: "d",
			RightColumnName: "id",
		},
	}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 2 || rows[0][0] != parser.StringValue("alice") || rows[0][1] != parser.StringValue("ENG") || rows[1][0] != parser.StringValue("cara") || rows[1][1] != parser.StringValue("ENG") {
		t.Fatalf("Select() rows = %#v, want [[alice ENG] [cara ENG]]", rows)
	}
}

func TestSelectWithHandoffJoinScan(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "name", Type: parser.ColumnTypeText},
			{Name: "dept_id", Type: parser.ColumnTypeInt},
		}, Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice"), parser.Int64Value(10)},
			{parser.Int64Value(2), parser.StringValue("bob"), parser.Int64Value(20)},
			{parser.Int64Value(3), parser.StringValue("cara"), parser.Int64Value(10)},
		}},
		"departments": {Name: "departments", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "name", Type: parser.ColumnTypeText},
		}, Rows: [][]parser.Value{
			{parser.Int64Value(10), parser.StringValue("eng")},
			{parser.Int64Value(20), parser.StringValue("ops")},
		}},
	}

	handoff, err := NewSelectExecutionHandoff(&planner.SelectPlan{
		Query: &planner.SelectQuery{
			TableName: "users",
			From:      []planner.TableRef{{Name: "users", Alias: "u"}},
			Joins:     []planner.JoinClause{{Right: planner.TableRef{Name: "departments", Alias: "d"}}},
			Columns:   []string{"u.name", "d.name"},
			Predicate: &planner.PredicateExpr{
				Kind: planner.PredicateKindComparison,
				Comparison: &planner.Condition{
					LeftExpr:  &planner.ValueExpr{Kind: planner.ValueExprKindColumnRef, Qualifier: "d", Column: "name"},
					Operator:  "=",
					RightExpr: &planner.ValueExpr{Kind: planner.ValueExprKindLiteral, Value: planner.StringValue("eng")},
				},
			},
			OrderBy: &planner.OrderByClause{Column: "u.id"},
		},
		ScanType: planner.ScanTypeJoin,
		JoinScan: &planner.JoinScan{
			LeftTableName:   "users",
			LeftTableAlias:  "u",
			LeftColumnName:  "dept_id",
			RightTableName:  "departments",
			RightTableAlias: "d",
			RightColumnName: "id",
		},
	})
	if err != nil {
		t.Fatalf("NewSelectExecutionHandoff() error = %v", err)
	}

	rows, err := SelectWithHandoff(handoff, tables)
	if err != nil {
		t.Fatalf("SelectWithHandoff() error = %v", err)
	}
	if len(rows) != 2 || rows[0][0] != parser.StringValue("alice") || rows[0][1] != parser.StringValue("eng") || rows[1][0] != parser.StringValue("cara") || rows[1][1] != parser.StringValue("eng") {
		t.Fatalf("SelectWithHandoff() rows = %#v, want [[alice eng] [cara eng]]", rows)
	}
}

func TestSelectAggregateBridgeAdaptsPlannerAggregateExpr(t *testing.T) {
	tables := map[string]*Table{
		"metrics": {Name: "metrics", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "score", Type: parser.ColumnTypeReal},
		}, Rows: [][]parser.Value{
			{parser.Int64Value(1), parser.RealValue(1.5)},
			{parser.Int64Value(2), parser.RealValue(2.5)},
			{parser.Int64Value(3), parser.RealValue(3.0)},
		}},
	}

	rows, err := Select(&planner.SelectPlan{
		Query: &planner.SelectQuery{
			TableName: "metrics",
			ProjectionExprs: []*planner.ValueExpr{
				{Kind: planner.ValueExprKindAggregateCall, FuncName: "AVG", Arg: &planner.ValueExpr{Kind: planner.ValueExprKindColumnRef, Column: "score"}},
				{Kind: planner.ValueExprKindAggregateCall, FuncName: "MAX", Arg: &planner.ValueExpr{Kind: planner.ValueExprKindColumnRef, Column: "score"}},
			},
			ProjectionLabels: []string{"AVG(score)", "MAX(score)"},
			Predicate: &planner.PredicateExpr{
				Kind: planner.PredicateKindComparison,
				Comparison: &planner.Condition{
					LeftExpr:  &planner.ValueExpr{Kind: planner.ValueExprKindColumnRef, Column: "id"},
					Operator:  ">",
					RightExpr: &planner.ValueExpr{Kind: planner.ValueExprKindLiteral, Value: planner.Int64Value(1)},
				},
			},
		},
		ScanType:  planner.ScanTypeTable,
		TableScan: &planner.TableScan{TableName: "metrics"},
	}, tables)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if len(rows) != 1 || len(rows[0]) != 2 || rows[0][0] != parser.RealValue((2.5+3.0)/2.0) || rows[0][1] != parser.RealValue(3.0) {
		t.Fatalf("Select() rows = %#v, want [[2.75 3.0]]", rows)
	}
}
