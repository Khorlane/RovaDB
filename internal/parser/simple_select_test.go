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
			if len(got.Where.Conditions) != 1 {
				t.Fatalf("len(Where.Conditions) = %d, want 1", len(got.Where.Conditions))
			}
			cond := got.Where.Conditions[0]
			if cond.Left != tc.left || cond.Operator != tc.operator || cond.Right != tc.right {
				t.Fatalf("Condition = %#v, want left=%q op=%q right=%#v", cond, tc.left, tc.operator, tc.right)
			}
		})
	}
}

func TestParseSelectExprWhereAndConditions(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT id FROM users WHERE id > 1 AND name != 'bob' AND id < 10")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil || got.Where == nil {
		t.Fatalf("ParseSelectExpr() = %#v, want WHERE clause", got)
	}
	if len(got.Where.Conditions) != 3 {
		t.Fatalf("len(Where.Conditions) = %d, want 3", len(got.Where.Conditions))
	}
	want := []Condition{
		{Left: "id", Operator: ">", Right: Int64Value(1)},
		{Left: "name", Operator: "!=", Right: StringValue("bob")},
		{Left: "id", Operator: "<", Right: Int64Value(10)},
	}
	for i := range want {
		if got.Where.Conditions[i] != want[i] {
			t.Fatalf("Where.Conditions[%d] = %#v, want %#v", i, got.Where.Conditions[i], want[i])
		}
	}
}
