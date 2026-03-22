package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestEvalIntLiteral(t *testing.T) {
	got, err := Eval(&parser.Expr{Kind: parser.ExprKindInt64Literal, I64: 42})
	if err != nil {
		t.Fatalf("Eval() error = %v", err)
	}
	if got != parser.Int64Value(42) {
		t.Fatalf("Eval() = %#v, want %#v", got, parser.Int64Value(42))
	}
}

func TestEvalStringLiteral(t *testing.T) {
	got, err := Eval(&parser.Expr{Kind: parser.ExprKindStringLiteral, Str: "hello"})
	if err != nil {
		t.Fatalf("Eval() error = %v", err)
	}
	if got != parser.StringValue("hello") {
		t.Fatalf("Eval() = %#v, want %#v", got, parser.StringValue("hello"))
	}
}

func TestEvalBinaryAdd(t *testing.T) {
	got, err := Eval(&parser.Expr{
		Kind:  parser.ExprKindInt64Binary,
		Op:    parser.BinaryOpAdd,
		Left:  &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: 1},
		Right: &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: 2},
	})
	if err != nil {
		t.Fatalf("Eval() error = %v", err)
	}
	if got != parser.Int64Value(3) {
		t.Fatalf("Eval() = %#v, want %#v", got, parser.Int64Value(3))
	}
}

func TestEvalBinarySub(t *testing.T) {
	got, err := Eval(&parser.Expr{
		Kind:  parser.ExprKindInt64Binary,
		Op:    parser.BinaryOpSub,
		Left:  &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: 5},
		Right: &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: 3},
	})
	if err != nil {
		t.Fatalf("Eval() error = %v", err)
	}
	if got != parser.Int64Value(2) {
		t.Fatalf("Eval() = %#v, want %#v", got, parser.Int64Value(2))
	}
}
