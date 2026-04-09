package parser

import "testing"

func TestParseSelectExprDirect(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		ok   bool
		want *Expr
	}{
		{
			name: "select integer",
			sql:  "SELECT 1",
			ok:   true,
			want: &Expr{Kind: ExprKindInt64Literal, I64: 1},
		},
		{
			name: "select string",
			sql:  "SELECT 'hello'",
			ok:   true,
			want: &Expr{Kind: ExprKindStringLiteral, Str: "hello"},
		},
		{
			name: "select true",
			sql:  "SELECT TRUE",
			ok:   true,
			want: &Expr{Kind: ExprKindBoolLiteral, Bool: true},
		},
		{
			name: "select real",
			sql:  "SELECT 3.14",
			ok:   true,
			want: &Expr{Kind: ExprKindRealLiteral, F64: 3.14},
		},
		{
			name: "select false mixed case",
			sql:  "SELECT False",
			ok:   true,
			want: &Expr{Kind: ExprKindBoolLiteral, Bool: false},
		},
		{
			name: "select one plus two",
			sql:  "SELECT 1+2",
			ok:   true,
			want: &Expr{
				Kind:  ExprKindInt64Binary,
				Op:    BinaryOpAdd,
				Left:  &Expr{Kind: ExprKindInt64Literal, I64: 1},
				Right: &Expr{Kind: ExprKindInt64Literal, I64: 2},
			},
		},
		{
			name: "select minus one plus two",
			sql:  "SELECT -1+2",
			ok:   true,
			want: &Expr{
				Kind:  ExprKindInt64Binary,
				Op:    BinaryOpAdd,
				Left:  &Expr{Kind: ExprKindInt64Literal, I64: -1},
				Right: &Expr{Kind: ExprKindInt64Literal, I64: 2},
			},
		},
		{
			name: "select one plus two spaced",
			sql:  "SELECT 1 + 2",
			ok:   true,
			want: &Expr{
				Kind:  ExprKindInt64Binary,
				Op:    BinaryOpAdd,
				Left:  &Expr{Kind: ExprKindInt64Literal, I64: 1},
				Right: &Expr{Kind: ExprKindInt64Literal, I64: 2},
			},
		},
		{
			name: "select minus one plus two spaced",
			sql:  "SELECT -1 + 2",
			ok:   true,
			want: &Expr{
				Kind:  ExprKindInt64Binary,
				Op:    BinaryOpAdd,
				Left:  &Expr{Kind: ExprKindInt64Literal, I64: -1},
				Right: &Expr{Kind: ExprKindInt64Literal, I64: 2},
			},
		},
		{
			name: "select parenthesized one plus two",
			sql:  "SELECT (1+2)",
			ok:   true,
			want: &Expr{
				Kind: ExprKindParen,
				Inner: &Expr{
					Kind:  ExprKindInt64Binary,
					Op:    BinaryOpAdd,
					Left:  &Expr{Kind: ExprKindInt64Literal, I64: 1},
					Right: &Expr{Kind: ExprKindInt64Literal, I64: 2},
				},
			},
		},
		{
			name: "select table columns",
			sql:  "SELECT id, name FROM users",
			ok:   true,
			want: &Expr{},
		},
		{
			name: "select table star",
			sql:  "SELECT * FROM users",
			ok:   true,
			want: &Expr{},
		},
		{
			name: "select table columns no space after comma",
			sql:  "SELECT id,name FROM users",
			ok:   true,
			want: &Expr{},
		},
		{
			name: "select table star mixed spacing",
			sql:  "SELECT  *  FROM  users",
			ok:   true,
			want: &Expr{},
		},
		{
			name: "select table columns mixed spacing",
			sql:  "SELECT  id ,name  FROM  users",
			ok:   true,
			want: &Expr{},
		},
		{
			name: "select table star where int",
			sql:  "SELECT * FROM users WHERE id = 1",
			ok:   true,
			want: &Expr{},
		},
		{
			name: "select table column where string",
			sql:  "SELECT name FROM users WHERE name = 'bob'",
			ok:   true,
			want: &Expr{},
		},
		{
			name: "select table order by desc",
			sql:  "SELECT * FROM users ORDER BY id DESC",
			ok:   true,
			want: &Expr{},
		},
		{
			name: "select count star",
			sql:  "SELECT COUNT(*) FROM users",
			ok:   true,
			want: &Expr{},
		},
		{
			name: "select count column",
			sql:  "SELECT COUNT(id) FROM users",
			ok:   true,
			want: &Expr{},
		},
		{
			name: "select count mixed projection",
			sql:  "SELECT COUNT(*), name FROM users",
			ok:   true,
			want: &Expr{},
		},
		{name: "select identifier", sql: "SELECT abc", ok: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseSelectExpr(tc.sql)
			if ok != tc.ok {
				t.Fatalf("ParseSelectExpr() ok = %v, want %v", ok, tc.ok)
			}
			if !tc.ok {
				if got != nil {
					t.Fatalf("ParseSelectExpr() = %#v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("ParseSelectExpr() = nil, want value")
			}
			if tc.name == "select table columns" || tc.name == "select table columns no space after comma" || tc.name == "select table columns mixed spacing" {
				if got.TableName != "users" || len(got.Columns) != 2 || got.Columns[0] != "id" || got.Columns[1] != "name" {
					t.Fatalf("ParseSelectExpr() = %#v, want table users columns [id name]", got)
				}
				return
			}
			if tc.name == "select table column where string" {
				if got.TableName != "users" || len(got.Columns) != 1 || got.Columns[0] != "name" || got.Where == nil || len(got.Where.Items) != 1 || got.Where.Items[0].Condition.Left != "name" || got.Where.Items[0].Condition.Operator != "=" || got.Where.Items[0].Condition.Right != StringValue("bob") {
					t.Fatalf("ParseSelectExpr() = %#v, want table users columns [name] where name='bob'", got)
				}
				return
			}
			if tc.name == "select table star" || tc.name == "select table star mixed spacing" || tc.name == "select table star where int" {
				if tc.name == "select table star where int" {
					if got.TableName != "users" || got.Columns != nil || got.Where == nil || len(got.Where.Items) != 1 || got.Where.Items[0].Condition.Left != "id" || got.Where.Items[0].Condition.Operator != "=" || got.Where.Items[0].Condition.Right != Int64Value(1) {
						t.Fatalf("ParseSelectExpr() = %#v, want table users select all where id=1", got)
					}
					return
				}
				if got.TableName != "users" || got.Columns != nil {
					t.Fatalf("ParseSelectExpr() = %#v, want table users select all", got)
				}
				return
			}
			if tc.name == "select table order by desc" {
				if got.TableName != "users" || got.Columns != nil || got.OrderBy == nil || got.OrderBy.Column != "id" || !got.OrderBy.Desc {
					t.Fatalf("ParseSelectExpr() = %#v, want table users order by id desc", got)
				}
				return
			}
			if tc.name == "select count star" {
				if got.TableName != "users" || !got.IsCountStar || got.OrderBy != nil {
					t.Fatalf("ParseSelectExpr() = %#v, want table users count(*)", got)
				}
				return
			}
			if tc.name == "select count column" {
				if got.TableName != "users" || len(got.ProjectionExprs) != 1 || got.ProjectionExprs[0].Kind != ValueExprKindAggregateCall || got.ProjectionExprs[0].FuncName != "COUNT" {
					t.Fatalf("ParseSelectExpr() = %#v, want table users count(id)", got)
				}
				return
			}
			if tc.name == "select count mixed projection" {
				if got.TableName != "users" || len(got.ProjectionExprs) != 2 || got.ProjectionExprs[0].Kind != ValueExprKindAggregateCall || got.ProjectionExprs[1].Kind != ValueExprKindColumnRef {
					t.Fatalf("ParseSelectExpr() = %#v, want table users count(*) and name projection", got)
				}
				return
			}
			if !equalExpr(got.Expr, tc.want) {
				t.Fatalf("ParseSelectExpr().Expr = %#v, want %#v", got.Expr, tc.want)
			}
		})
	}
}

func equalExpr(got, want *Expr) bool {
	if got == nil || want == nil {
		return got == want
	}
	if got.Kind != want.Kind || got.I64 != want.I64 || got.F64 != want.F64 || got.Str != want.Str || got.Bool != want.Bool || got.Op != want.Op {
		return false
	}

	return equalExpr(got.Left, want.Left) && equalExpr(got.Right, want.Right) && equalExpr(got.Inner, want.Inner)
}
