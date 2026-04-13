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

func TestEvalRealLiteral(t *testing.T) {
	got, err := Eval(&parser.Expr{Kind: parser.ExprKindRealLiteral, F64: 3.14})
	if err != nil {
		t.Fatalf("Eval() error = %v", err)
	}
	if got != parser.RealValue(3.14) {
		t.Fatalf("Eval() = %#v, want %#v", got, parser.RealValue(3.14))
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

func TestEvalBinaryValueExprTypedIntegerArithmeticPreservesWidth(t *testing.T) {
	tests := []struct {
		name  string
		op    parser.ValueExprBinaryOp
		left  parser.Value
		right parser.Value
		want  parser.Value
	}{
		{
			name:  "smallint plus smallint stays smallint",
			op:    parser.ValueExprBinaryOpAdd,
			left:  parser.SmallIntValue(7),
			right: parser.SmallIntValue(2),
			want:  parser.SmallIntValue(9),
		},
		{
			name:  "int plus int stays int",
			op:    parser.ValueExprBinaryOpAdd,
			left:  parser.IntValue(10),
			right: parser.IntValue(5),
			want:  parser.IntValue(15),
		},
		{
			name:  "bigint minus bigint stays bigint",
			op:    parser.ValueExprBinaryOpSub,
			left:  parser.BigIntValue(1 << 40),
			right: parser.BigIntValue(9),
			want:  parser.BigIntValue((1 << 40) - 9),
		},
		{
			name:  "smallint plus fitting literal resolves to smallint",
			op:    parser.ValueExprBinaryOpAdd,
			left:  parser.SmallIntValue(8),
			right: parser.Int64Value(3),
			want:  parser.SmallIntValue(11),
		},
		{
			name:  "int plus fitting literal resolves to int",
			op:    parser.ValueExprBinaryOpAdd,
			left:  parser.IntValue(20),
			right: parser.Int64Value(4),
			want:  parser.IntValue(24),
		},
		{
			name:  "bigint plus fitting literal resolves to bigint",
			op:    parser.ValueExprBinaryOpAdd,
			left:  parser.BigIntValue(1 << 40),
			right: parser.Int64Value(6),
			want:  parser.BigIntValue((1 << 40) + 6),
		},
		{
			name:  "fitting literal plus typed smallint resolves to smallint",
			op:    parser.ValueExprBinaryOpAdd,
			left:  parser.Int64Value(4),
			right: parser.SmallIntValue(9),
			want:  parser.SmallIntValue(13),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := evalBinaryValueExpr(int(tc.op), tc.left, tc.right)
			if err != nil {
				t.Fatalf("evalBinaryValueExpr() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("evalBinaryValueExpr() = %#v, want %#v", got, tc.want)
			}
			if got.Kind == parser.ValueKindIntegerLiteral {
				t.Fatalf("evalBinaryValueExpr() kind = %v, want typed integer kind", got.Kind)
			}
		})
	}
}

func TestEvalBinaryValueExprTypedIntegerArithmeticRejectsMixedWidths(t *testing.T) {
	tests := []struct {
		name  string
		left  parser.Value
		right parser.Value
	}{
		{
			name:  "smallint plus int",
			left:  parser.SmallIntValue(1),
			right: parser.IntValue(2),
		},
		{
			name:  "smallint plus bigint",
			left:  parser.SmallIntValue(1),
			right: parser.BigIntValue(2),
		},
		{
			name:  "int plus bigint",
			left:  parser.IntValue(1),
			right: parser.BigIntValue(2),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := evalBinaryValueExpr(int(parser.ValueExprBinaryOpAdd), tc.left, tc.right)
			if err != errTypeMismatch {
				t.Fatalf("evalBinaryValueExpr() error = %v, want %v", err, errTypeMismatch)
			}
		})
	}
}

func TestEvalBinaryValueExprTypedIntegerArithmeticDetectsOverflow(t *testing.T) {
	tests := []struct {
		name  string
		left  parser.Value
		right parser.Value
	}{
		{
			name:  "smallint overflow",
			left:  parser.SmallIntValue(32767),
			right: parser.SmallIntValue(1),
		},
		{
			name:  "int overflow",
			left:  parser.IntValue(2147483647),
			right: parser.IntValue(1),
		},
		{
			name:  "bigint overflow",
			left:  parser.BigIntValue(9223372036854775807),
			right: parser.BigIntValue(1),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := evalBinaryValueExpr(int(parser.ValueExprBinaryOpAdd), tc.left, tc.right)
			if err != errIntOutOfRange {
				t.Fatalf("evalBinaryValueExpr() error = %v, want %v", err, errIntOutOfRange)
			}
		})
	}
}

func TestEvalBinaryValueExprTypedIntegerArithmeticRejectsOutOfRangeResolvedLiteral(t *testing.T) {
	tests := []struct {
		name  string
		left  parser.Value
		right parser.Value
	}{
		{
			name:  "smallint plus out of range literal",
			left:  parser.SmallIntValue(1),
			right: parser.Int64Value(40000),
		},
		{
			name:  "int plus out of range literal",
			left:  parser.IntValue(1),
			right: parser.Int64Value(1 << 40),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := evalBinaryValueExpr(int(parser.ValueExprBinaryOpAdd), tc.left, tc.right)
			if err != errTypeMismatch {
				t.Fatalf("evalBinaryValueExpr() error = %v, want %v", err, errTypeMismatch)
			}
		})
	}
}

func TestEvalBinaryValueExprLeavesNonIntegerArithmeticStable(t *testing.T) {
	got, err := evalBinaryValueExpr(int(parser.ValueExprBinaryOpAdd), parser.RealValue(1.25), parser.RealValue(2.5))
	if err != nil {
		t.Fatalf("evalBinaryValueExpr() error = %v", err)
	}
	if got != parser.RealValue(3.75) {
		t.Fatalf("evalBinaryValueExpr() = %#v, want %#v", got, parser.RealValue(3.75))
	}
}

func TestEvalScalarFunctionAbsPreservesTypedIntegerWidthAndOverflow(t *testing.T) {
	got, err := evalScalarFunction("ABS", parser.SmallIntValue(-7))
	if err != nil {
		t.Fatalf("evalScalarFunction() error = %v", err)
	}
	if got != parser.SmallIntValue(7) {
		t.Fatalf("evalScalarFunction() = %#v, want %#v", got, parser.SmallIntValue(7))
	}

	_, err = evalScalarFunction("ABS", parser.IntValue(-2147483648))
	if err != errIntOutOfRange {
		t.Fatalf("evalScalarFunction() overflow error = %v, want %v", err, errIntOutOfRange)
	}
}
