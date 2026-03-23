package parser

import "testing"

func TestParseSelectExprProjectionColumns(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT id, name FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil {
		t.Fatal("ParseSelectExpr() = nil, want value")
	}
	if got.TableName != "users" {
		t.Fatalf("TableName = %q, want %q", got.TableName, "users")
	}
	if len(got.Columns) != 2 || got.Columns[0] != "id" || got.Columns[1] != "name" {
		t.Fatalf("Columns = %#v, want [id name]", got.Columns)
	}
}

func TestParseSelectExprSingleProjectionColumn(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT id FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil {
		t.Fatal("ParseSelectExpr() = nil, want value")
	}
	if len(got.Columns) != 1 || got.Columns[0] != "id" {
		t.Fatalf("Columns = %#v, want [id]", got.Columns)
	}
}

func TestParseSelectExprCountStar(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "count star", sql: "SELECT COUNT(*) FROM users"},
		{name: "count star where", sql: "SELECT COUNT(*) FROM users WHERE id > 1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseSelectExpr(tc.sql)
			if !ok {
				t.Fatal("ParseSelectExpr() ok = false, want true")
			}
			if got == nil {
				t.Fatal("ParseSelectExpr() = nil, want value")
			}
			if !got.IsCountStar || got.TableName != "users" {
				t.Fatalf("ParseSelectExpr() = %#v, want COUNT(*) select from users", got)
			}
		})
	}
}

func TestParseSelectExprSelectStarUsesNilColumns(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT * FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil {
		t.Fatal("ParseSelectExpr() = nil, want value")
	}
	if got.Columns != nil {
		t.Fatalf("Columns = %#v, want nil for SELECT *", got.Columns)
	}
}

func TestParseSelectExprInvalidCountStar(t *testing.T) {
	for _, sql := range []string{
		"SELECT COUNT(id) FROM users",
		"SELECT COUNT(*), name FROM users",
		"SELECT COUNT(*) name FROM users",
		"SELECT COUNT( * ) FROM users",
	} {
		if got, ok := ParseSelectExpr(sql); ok {
			t.Fatalf("ParseSelectExpr(%q) = %#v, want parse failure", sql, got)
		}
	}
}

func TestParseSelectExprWhereOperators(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		left     string
		operator string
		right    Value
	}{
		{name: "equals", sql: "SELECT id FROM users WHERE id = 1", left: "id", operator: "=", right: Int64Value(1)},
		{name: "not equals", sql: "SELECT id FROM users WHERE id != 1", left: "id", operator: "!=", right: Int64Value(1)},
		{name: "less than", sql: "SELECT id FROM users WHERE id < 10", left: "id", operator: "<", right: Int64Value(10)},
		{name: "less equal", sql: "SELECT id FROM users WHERE id <= 10", left: "id", operator: "<=", right: Int64Value(10)},
		{name: "greater than", sql: "SELECT id FROM users WHERE id > 10", left: "id", operator: ">", right: Int64Value(10)},
		{name: "greater equal", sql: "SELECT id FROM users WHERE id >= 10", left: "id", operator: ">=", right: Int64Value(10)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseSelectExpr(tc.sql)
			if !ok {
				t.Fatal("ParseSelectExpr() ok = false, want true")
			}
			if got == nil || got.Where == nil {
				t.Fatalf("ParseSelectExpr() = %#v, want WHERE clause", got)
			}
			if len(got.Where.Items) != 1 {
				t.Fatalf("len(Where.Items) = %d, want 1", len(got.Where.Items))
			}
			cond := got.Where.Items[0].Condition
			if cond.Left != tc.left || cond.Operator != tc.operator || cond.Right != tc.right {
				t.Fatalf("Condition = %#v, want left=%q op=%q right=%#v", cond, tc.left, tc.operator, tc.right)
			}
		})
	}
}

func TestParseSelectExprWhereConditionChain(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []ConditionChainItem
	}{
		{
			name: "single condition",
			sql:  "SELECT id FROM users WHERE id > 1",
			want: []ConditionChainItem{{Condition: Condition{Left: "id", Operator: ">", Right: Int64Value(1)}}},
		},
		{
			name: "and",
			sql:  "SELECT id FROM users WHERE id > 1 AND name != 'bob'",
			want: []ConditionChainItem{
				{Condition: Condition{Left: "id", Operator: ">", Right: Int64Value(1)}},
				{Op: BooleanOpAnd, Condition: Condition{Left: "name", Operator: "!=", Right: StringValue("bob")}},
			},
		},
		{
			name: "or",
			sql:  "SELECT id FROM users WHERE id = 1 OR id = 2",
			want: []ConditionChainItem{
				{Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(1)}},
				{Op: BooleanOpOr, Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(2)}},
			},
		},
		{
			name: "and or",
			sql:  "SELECT id FROM users WHERE id = 1 AND id = 2 OR name = 'bob'",
			want: []ConditionChainItem{
				{Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(1)}},
				{Op: BooleanOpAnd, Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(2)}},
				{Op: BooleanOpOr, Condition: Condition{Left: "name", Operator: "=", Right: StringValue("bob")}},
			},
		},
		{
			name: "or and",
			sql:  "SELECT id FROM users WHERE id = 1 OR id = 2 AND name = 'bob'",
			want: []ConditionChainItem{
				{Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(1)}},
				{Op: BooleanOpOr, Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(2)}},
				{Op: BooleanOpAnd, Condition: Condition{Left: "name", Operator: "=", Right: StringValue("bob")}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseSelectExpr(tc.sql)
			if !ok {
				t.Fatal("ParseSelectExpr() ok = false, want true")
			}
			if got == nil || got.Where == nil {
				t.Fatalf("ParseSelectExpr() = %#v, want WHERE clause", got)
			}
			if len(got.Where.Items) != len(tc.want) {
				t.Fatalf("len(Where.Items) = %d, want %d", len(got.Where.Items), len(tc.want))
			}
			for i := range tc.want {
				if got.Where.Items[i] != tc.want[i] {
					t.Fatalf("Where.Items[%d] = %#v, want %#v", i, got.Where.Items[i], tc.want[i])
				}
			}
		})
	}
}

func TestParseSelectExprInvalidWhereBooleanChains(t *testing.T) {
	for _, sql := range []string{
		"SELECT id FROM users WHERE id = 1 OR",
		"SELECT id FROM users WHERE AND id = 1",
		"SELECT id FROM users WHERE id = 1 XOR id = 2",
	} {
		if got, ok := ParseSelectExpr(sql); ok {
			t.Fatalf("ParseSelectExpr(%q) = %#v, want parse failure", sql, got)
		}
	}
}

func TestParseSelectExprOrderBy(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		column string
		desc   bool
	}{
		{name: "default asc", sql: "SELECT * FROM users ORDER BY id", column: "id", desc: false},
		{name: "explicit asc", sql: "SELECT * FROM users ORDER BY id ASC", column: "id", desc: false},
		{name: "desc", sql: "SELECT * FROM users ORDER BY id DESC", column: "id", desc: true},
		{name: "with where", sql: "SELECT name FROM users WHERE id > 1 ORDER BY name DESC", column: "name", desc: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseSelectExpr(tc.sql)
			if !ok {
				t.Fatal("ParseSelectExpr() ok = false, want true")
			}
			if got == nil || got.OrderBy == nil {
				t.Fatalf("ParseSelectExpr() = %#v, want ORDER BY clause", got)
			}
			if got.OrderBy.Column != tc.column || got.OrderBy.Desc != tc.desc {
				t.Fatalf("OrderBy = %#v, want column=%q desc=%v", got.OrderBy, tc.column, tc.desc)
			}
		})
	}
}
