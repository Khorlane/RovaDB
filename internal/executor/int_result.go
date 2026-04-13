package executor

import (
	"math"

	"github.com/Khorlane/RovaDB/internal/parser"
)

var errIntOutOfRange = newExecError("integer out of range")

// publicIntResult keeps executor-owned untyped integer expression results as
// SQL integer literals until a later typed context forces resolution.
func publicIntResult(v int64) (parser.Value, error) {
	if !parser.PublicIntInRange(v) {
		return parser.Value{}, errIntOutOfRange
	}
	return parser.IntegerLiteralValue(v), nil
}

func integerValueBounds(kind parser.ValueKind) (int64, int64, bool) {
	switch kind {
	case parser.ValueKindSmallInt:
		return math.MinInt16, math.MaxInt16, true
	case parser.ValueKindInt:
		return math.MinInt32, math.MaxInt32, true
	case parser.ValueKindBigInt:
		return math.MinInt64, math.MaxInt64, true
	default:
		return 0, 0, false
	}
}

func buildTypedIntegerValue(kind parser.ValueKind, value int64) (parser.Value, error) {
	minValue, maxValue, ok := integerValueBounds(kind)
	if !ok {
		return parser.Value{}, errTypeMismatch
	}
	if value < minValue || value > maxValue {
		return parser.Value{}, errIntOutOfRange
	}
	switch kind {
	case parser.ValueKindSmallInt:
		return parser.SmallIntValue(int16(value)), nil
	case parser.ValueKindInt:
		return parser.IntValue(int32(value)), nil
	case parser.ValueKindBigInt:
		return parser.BigIntValue(value), nil
	default:
		return parser.Value{}, errTypeMismatch
	}
}

func checkedInt64Add(left, right int64) (int64, bool) {
	if right > 0 && left > math.MaxInt64-right {
		return 0, true
	}
	if right < 0 && left < math.MinInt64-right {
		return 0, true
	}
	return left + right, false
}

func checkedInt64Sub(left, right int64) (int64, bool) {
	if right > 0 && left < math.MinInt64+right {
		return 0, true
	}
	if right < 0 && left > math.MaxInt64+right {
		return 0, true
	}
	return left - right, false
}

func checkedInt64Abs(value int64) (int64, bool) {
	if value == math.MinInt64 {
		return 0, true
	}
	if value < 0 {
		return -value, false
	}
	return value, false
}

func resolveIntegerArithmeticOperands(left, right parser.Value) (parser.Value, parser.Value, parser.ValueKind, error) {
	switch {
	case left.IsTypedInteger() && right.IsTypedInteger():
		if left.Kind != right.Kind {
			return parser.Value{}, parser.Value{}, parser.ValueKindInvalid, errTypeMismatch
		}
		return left, right, left.Kind, nil
	case left.IsTypedInteger() && right.IsIntegerLiteral():
		resolved, err := resolveUntypedIntegerLiteralToValueKind(right, left.Kind)
		if err != nil {
			return parser.Value{}, parser.Value{}, parser.ValueKindInvalid, err
		}
		return left, resolved, left.Kind, nil
	case left.IsIntegerLiteral() && right.IsTypedInteger():
		resolved, err := resolveUntypedIntegerLiteralToValueKind(left, right.Kind)
		if err != nil {
			return parser.Value{}, parser.Value{}, parser.ValueKindInvalid, err
		}
		return resolved, right, right.Kind, nil
	case left.IsIntegerLiteral() && right.IsIntegerLiteral():
		return left, right, parser.ValueKindIntegerLiteral, nil
	default:
		return parser.Value{}, parser.Value{}, parser.ValueKindInvalid, errTypeMismatch
	}
}

func evalIntegerArithmeticResult(op int, left, right parser.Value) (parser.Value, error) {
	resolvedLeft, resolvedRight, resultKind, err := resolveIntegerArithmeticOperands(left, right)
	if err != nil {
		return parser.Value{}, err
	}

	leftValue := resolvedLeft.IntegerValue()
	rightValue := resolvedRight.IntegerValue()

	var result int64
	var overflow bool
	switch op {
	case int(runtimeValueExprBinaryOpAdd):
		result, overflow = checkedInt64Add(leftValue, rightValue)
	case int(runtimeValueExprBinaryOpSub):
		result, overflow = checkedInt64Sub(leftValue, rightValue)
	default:
		return parser.Value{}, errInvalidExpression
	}
	if overflow {
		return parser.Value{}, errIntOutOfRange
	}

	if resultKind == parser.ValueKindIntegerLiteral {
		return publicIntResult(result)
	}
	return buildTypedIntegerValue(resultKind, result)
}

func evalIntegerAbsResult(value parser.Value) (parser.Value, error) {
	absValue, overflow := checkedInt64Abs(value.IntegerValue())
	if overflow {
		return parser.Value{}, errIntOutOfRange
	}
	if value.IsIntegerLiteral() {
		return publicIntResult(absValue)
	}
	return buildTypedIntegerValue(value.Kind, absValue)
}
