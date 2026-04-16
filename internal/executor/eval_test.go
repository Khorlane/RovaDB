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
			name:  "int minus int stays int",
			op:    parser.ValueExprBinaryOpSub,
			left:  parser.IntValue(10),
			right: parser.IntValue(5),
			want:  parser.IntValue(5),
		},
		{
			name:  "bigint minus bigint stays bigint",
			op:    parser.ValueExprBinaryOpSub,
			left:  parser.BigIntValue(1 << 40),
			right: parser.BigIntValue(9),
			want:  parser.BigIntValue((1 << 40) - 9),
		},
		{
			name:  "bigint plus bigint stays bigint",
			op:    parser.ValueExprBinaryOpAdd,
			left:  parser.BigIntValue(1 << 40),
			right: parser.BigIntValue(9),
			want:  parser.BigIntValue((1 << 40) + 9),
		},
		{
			name:  "smallint minus smallint stays smallint",
			op:    parser.ValueExprBinaryOpSub,
			left:  parser.SmallIntValue(7),
			right: parser.SmallIntValue(2),
			want:  parser.SmallIntValue(5),
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
		{
			name:  "bigint minus fitting literal resolves to bigint",
			op:    parser.ValueExprBinaryOpSub,
			left:  parser.BigIntValue(1 << 40),
			right: parser.Int64Value(6),
			want:  parser.BigIntValue((1 << 40) - 6),
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
		op    parser.ValueExprBinaryOp
		left  parser.Value
		right parser.Value
	}{
		{
			name:  "smallint plus int",
			op:    parser.ValueExprBinaryOpAdd,
			left:  parser.SmallIntValue(1),
			right: parser.IntValue(2),
		},
		{
			name:  "smallint plus bigint",
			op:    parser.ValueExprBinaryOpAdd,
			left:  parser.SmallIntValue(1),
			right: parser.BigIntValue(2),
		},
		{
			name:  "int plus bigint",
			op:    parser.ValueExprBinaryOpAdd,
			left:  parser.IntValue(1),
			right: parser.BigIntValue(2),
		},
		{
			name:  "bigint minus int",
			op:    parser.ValueExprBinaryOpSub,
			left:  parser.BigIntValue(3),
			right: parser.IntValue(2),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := evalBinaryValueExpr(int(tc.op), tc.left, tc.right)
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

func TestCompareValuesTemporalFamiliesUseTypedPayloads(t *testing.T) {
	tests := []struct {
		name  string
		op    string
		left  parser.Value
		right parser.Value
		want  bool
	}{
		{
			name:  "date equality",
			op:    "=",
			left:  parser.DateValue(20553),
			right: parser.DateValue(20553),
			want:  true,
		},
		{
			name:  "date range comparison",
			op:    "<",
			left:  parser.DateValue(20553),
			right: parser.DateValue(20554),
			want:  true,
		},
		{
			name:  "time equality",
			op:    "=",
			left:  parser.TimeValue(49521),
			right: parser.TimeValue(49521),
			want:  true,
		},
		{
			name:  "time range comparison",
			op:    ">=",
			left:  parser.TimeValue(49522),
			right: parser.TimeValue(49521),
			want:  true,
		},
		{
			name:  "timestamp equality ignores zone id",
			op:    "=",
			left:  parser.TimestampValue(1775828721000, 0),
			right: parser.TimestampValue(1775828721000, 7),
			want:  true,
		},
		{
			name:  "timestamp range comparison uses millis",
			op:    ">",
			left:  parser.TimestampValue(1775828781000, 9),
			right: parser.TimestampValue(1775828721000, 1),
			want:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := compareValues(tc.op, tc.left, tc.right)
			if err != nil {
				t.Fatalf("compareValues() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("compareValues() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCompareValuesTemporalFamiliesRejectCrossFamilyAndNonTemporalComparisons(t *testing.T) {
	tests := []struct {
		name  string
		left  parser.Value
		right parser.Value
	}{
		{
			name:  "date vs time",
			left:  parser.DateValue(20553),
			right: parser.TimeValue(49521),
		},
		{
			name:  "date vs timestamp",
			left:  parser.DateValue(20553),
			right: parser.TimestampValue(1775828721000, 0),
		},
		{
			name:  "time vs timestamp",
			left:  parser.TimeValue(49521),
			right: parser.TimestampValue(1775828721000, 0),
		},
		{
			name:  "date vs text",
			left:  parser.DateValue(20553),
			right: parser.StringValue("2026-04-10"),
		},
		{
			name:  "time vs int",
			left:  parser.TimeValue(49521),
			right: parser.Int64Value(49521),
		},
		{
			name:  "timestamp vs real",
			left:  parser.TimestampValue(1775828721000, 0),
			right: parser.RealValue(1775828721000),
		},
		{
			name:  "timestamp vs bool",
			left:  parser.TimestampValue(1775828721000, 0),
			right: parser.BoolValue(true),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := compareValues("=", tc.left, tc.right)
			if err != errTypeMismatch {
				t.Fatalf("compareValues() error = %v, want %v", err, errTypeMismatch)
			}
		})
	}
}

func TestCompareValuesRejectsUnresolvedTimestampOperands(t *testing.T) {
	tests := []struct {
		name  string
		left  parser.Value
		right parser.Value
	}{
		{
			name:  "unresolved vs resolved",
			left:  parser.TimestampUnresolvedValue(2026, 4, 10, 13, 45, 21),
			right: parser.TimestampValue(1775828721000, 0),
		},
		{
			name:  "resolved vs unresolved",
			left:  parser.TimestampValue(1775828721000, 0),
			right: parser.TimestampUnresolvedValue(2026, 4, 10, 13, 45, 21),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := compareValues("=", tc.left, tc.right)
			if err != errUnresolvedTimestamp {
				t.Fatalf("compareValues() error = %v, want %v", err, errUnresolvedTimestamp)
			}
		})
	}
}

func TestCompareSortableValuesTemporalFamiliesUseTypedPayloads(t *testing.T) {
	tests := []struct {
		name  string
		left  parser.Value
		right parser.Value
		want  int
	}{
		{
			name:  "date ordering",
			left:  parser.DateValue(20553),
			right: parser.DateValue(20554),
			want:  -1,
		},
		{
			name:  "time ordering",
			left:  parser.TimeValue(49522),
			right: parser.TimeValue(49521),
			want:  1,
		},
		{
			name:  "timestamp ordering ignores zone id",
			left:  parser.TimestampValue(1775828721000, 3),
			right: parser.TimestampValue(1775828721000, 8),
			want:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := compareSortableValues(tc.left, tc.right)
			if err != nil {
				t.Fatalf("compareSortableValues() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("compareSortableValues() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCompareSortableValuesRejectsUnresolvedTimestampOperands(t *testing.T) {
	_, err := compareSortableValues(
		parser.TimestampUnresolvedValue(2026, 4, 10, 13, 45, 21),
		parser.TimestampValue(1775828721000, 0),
	)
	if err != errUnresolvedTimestamp {
		t.Fatalf("compareSortableValues() error = %v, want %v", err, errUnresolvedTimestamp)
	}
}
