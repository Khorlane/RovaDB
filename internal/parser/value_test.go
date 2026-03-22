package parser

import "testing"

func TestInt64Value(t *testing.T) {
	got := Int64Value(42)
	if got.Kind != ValueKindInt64 {
		t.Fatalf("Int64Value().Kind = %v, want %v", got.Kind, ValueKindInt64)
	}
	if got.I64 != 42 {
		t.Fatalf("Int64Value().I64 = %d, want 42", got.I64)
	}
}

func TestStringValue(t *testing.T) {
	got := StringValue("hello")
	if got.Kind != ValueKindString {
		t.Fatalf("StringValue().Kind = %v, want %v", got.Kind, ValueKindString)
	}
	if got.Str != "hello" {
		t.Fatalf("StringValue().Str = %q, want %q", got.Str, "hello")
	}
}

func TestParseSelectExprValueKinds(t *testing.T) {
	intSel, ok := ParseSelectExpr("SELECT 1")
	if !ok {
		t.Fatal("ParseSelectExpr(SELECT 1) ok = false, want true")
	}
	if intSel.Expr == nil {
		t.Fatal("ParseSelectExpr(SELECT 1).Expr = nil, want value")
	}
	if intSel.Expr.Kind != ExprKindInt64Literal {
		t.Fatalf("ParseSelectExpr(SELECT 1).Expr.Kind = %v, want %v", intSel.Expr.Kind, ExprKindInt64Literal)
	}
	if intSel.Expr.I64 != 1 {
		t.Fatalf("ParseSelectExpr(SELECT 1).Expr.I64 = %d, want 1", intSel.Expr.I64)
	}

	strSel, ok := ParseSelectExpr("SELECT 'hi'")
	if !ok {
		t.Fatal("ParseSelectExpr(SELECT 'hi') ok = false, want true")
	}
	if strSel.Expr == nil {
		t.Fatal("ParseSelectExpr(SELECT 'hi').Expr = nil, want value")
	}
	if strSel.Expr.Kind != ExprKindStringLiteral {
		t.Fatalf("ParseSelectExpr(SELECT 'hi').Expr.Kind = %v, want %v", strSel.Expr.Kind, ExprKindStringLiteral)
	}
	if strSel.Expr.Str != "hi" {
		t.Fatalf("ParseSelectExpr(SELECT 'hi').Expr.Str = %q, want %q", strSel.Expr.Str, "hi")
	}
}
