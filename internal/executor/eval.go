package executor

import (
	"strings"

	"github.com/Khorlane/RovaDB/internal/parser"
)

var errInvalidExpression = newExecError("unsupported query form")

var errUnsupportedComparisonOp = newExecError("unsupported query form")

// Eval evaluates the tiny Stage 1 expression model into a Value.
func Eval(expr *parser.Expr) (parser.Value, error) {
	if expr == nil {
		return parser.Value{}, errInvalidExpression
	}

	switch expr.Kind {
	case parser.ExprKindInt64Literal:
		return buildUntypedIntegerLiteralResult(expr.I64)
	case parser.ExprKindRealLiteral:
		return parser.RealValue(expr.F64), nil
	case parser.ExprKindStringLiteral:
		return parser.StringValue(expr.Str), nil
	case parser.ExprKindBoolLiteral:
		return parser.BoolValue(expr.Bool), nil
	case parser.ExprKindParen:
		return Eval(expr.Inner)
	case parser.ExprKindInt64Binary:
		left, err := Eval(expr.Left)
		if err != nil {
			return parser.Value{}, err
		}
		right, err := Eval(expr.Right)
		if err != nil {
			return parser.Value{}, err
		}
		if !left.IsInteger() || !right.IsInteger() {
			return parser.Value{}, errInvalidExpression
		}
		return evalIntegerArithmeticResult(int(expr.Op), left, right)
	default:
		return parser.Value{}, errInvalidExpression
	}
}

func compareValues(op string, left, right parser.Value) (bool, error) {
	if left.Kind == parser.ValueKindNull || right.Kind == parser.ValueKindNull {
		switch op {
		case "=":
			return left.Kind == parser.ValueKindNull && right.Kind == parser.ValueKindNull, nil
		case "!=":
			return !(left.Kind == parser.ValueKindNull && right.Kind == parser.ValueKindNull), nil
		default:
			return false, errTypeMismatch
		}
	}
	if left.Kind == parser.ValueKindTimestampUnresolved || right.Kind == parser.ValueKindTimestampUnresolved {
		return false, errUnresolvedTimestamp
	}
	if left.IsInteger() && right.IsInteger() {
		resolvedLeft, resolvedRight, err := resolveIntegerComparisonOperands(left, right)
		if err != nil {
			return false, err
		}
		return compareIntegerValues(op, resolvedLeft.IntegerValue(), resolvedRight.IntegerValue())
	}
	if left.Kind != right.Kind {
		return false, errTypeMismatch
	}

	switch left.Kind {
	case parser.ValueKindDate:
		return compareIntegerValues(op, int64(left.DateDays), int64(right.DateDays))
	case parser.ValueKindTime:
		return compareIntegerValues(op, int64(left.TimeSeconds), int64(right.TimeSeconds))
	case parser.ValueKindTimestamp:
		return compareIntegerValues(op, left.TimestampMillis, right.TimestampMillis)
	case parser.ValueKindString:
		cmp := compareTextValues(left.Str, right.Str)
		switch op {
		case "=":
			return strings.EqualFold(left.Str, right.Str), nil
		case "!=":
			return !strings.EqualFold(left.Str, right.Str), nil
		case "<":
			return cmp < 0, nil
		case "<=":
			return cmp <= 0, nil
		case ">":
			return cmp > 0, nil
		case ">=":
			return cmp >= 0, nil
		default:
			return false, errUnsupportedComparisonOp
		}
	case parser.ValueKindBool:
		switch op {
		case "=":
			return left.Bool == right.Bool, nil
		case "!=":
			return left.Bool != right.Bool, nil
		default:
			return false, errUnsupportedComparisonOp
		}
	case parser.ValueKindReal:
		switch op {
		case "=":
			return left.F64 == right.F64, nil
		case "!=":
			return left.F64 != right.F64, nil
		case "<":
			return left.F64 < right.F64, nil
		case "<=":
			return left.F64 <= right.F64, nil
		case ">":
			return left.F64 > right.F64, nil
		case ">=":
			return left.F64 >= right.F64, nil
		default:
			return false, errUnsupportedComparisonOp
		}
	default:
		return false, errTypeMismatch
	}
}

func compareSortableValues(left, right parser.Value) (int, error) {
	if left.Kind == parser.ValueKindNull || right.Kind == parser.ValueKindNull {
		return 0, errTypeMismatch
	}
	if left.Kind == parser.ValueKindTimestampUnresolved || right.Kind == parser.ValueKindTimestampUnresolved {
		return 0, errUnresolvedTimestamp
	}
	if left.IsInteger() && right.IsInteger() {
		resolvedLeft, resolvedRight, err := resolveIntegerComparisonOperands(left, right)
		if err != nil {
			return 0, err
		}
		return compareSortableIntegerValues(resolvedLeft.IntegerValue(), resolvedRight.IntegerValue()), nil
	}
	if left.Kind != right.Kind {
		return 0, errTypeMismatch
	}

	switch left.Kind {
	case parser.ValueKindDate:
		return compareSortableIntegerValues(int64(left.DateDays), int64(right.DateDays)), nil
	case parser.ValueKindTime:
		return compareSortableIntegerValues(int64(left.TimeSeconds), int64(right.TimeSeconds)), nil
	case parser.ValueKindTimestamp:
		return compareSortableIntegerValues(left.TimestampMillis, right.TimestampMillis), nil
	case parser.ValueKindString:
		switch cmp := compareTextValues(left.Str, right.Str); {
		case cmp < 0:
			return -1, nil
		case cmp > 0:
			return 1, nil
		default:
			return 0, nil
		}
	case parser.ValueKindReal:
		switch {
		case left.F64 < right.F64:
			return -1, nil
		case left.F64 > right.F64:
			return 1, nil
		default:
			return 0, nil
		}
	default:
		return 0, errTypeMismatch
	}
}

func compareTextValues(left, right string) int {
	return strings.Compare(strings.ToLower(left), strings.ToLower(right))
}

func evalScalarFunction(name string, arg parser.Value) (parser.Value, error) {
	switch strings.ToUpper(name) {
	case "LOWER":
		if arg.Kind != parser.ValueKindString {
			return parser.Value{}, errTypeMismatch
		}
		return parser.StringValue(strings.ToLower(arg.Str)), nil
	case "UPPER":
		if arg.Kind != parser.ValueKindString {
			return parser.Value{}, errTypeMismatch
		}
		return parser.StringValue(strings.ToUpper(arg.Str)), nil
	case "LENGTH":
		if arg.Kind != parser.ValueKindString {
			return parser.Value{}, errTypeMismatch
		}
		return buildUntypedIntegerLiteralResult(int64(len(arg.Str)))
	case "ABS":
		switch arg.Kind {
		case parser.ValueKindIntegerLiteral, parser.ValueKindSmallInt, parser.ValueKindInt, parser.ValueKindBigInt:
			return evalIntegerAbsResult(arg)
		case parser.ValueKindReal:
			if arg.F64 < 0 {
				return parser.RealValue(-arg.F64), nil
			}
			return arg, nil
		default:
			return parser.Value{}, errTypeMismatch
		}
	default:
		return parser.Value{}, errInvalidExpression
	}
}

func evalBinaryValueExpr(op int, left, right parser.Value) (parser.Value, error) {
	if left.IsInteger() && right.IsInteger() {
		return evalIntegerArithmeticResult(op, left, right)
	}
	if left.Kind != right.Kind {
		return parser.Value{}, errTypeMismatch
	}
	switch left.Kind {
	case parser.ValueKindReal:
		switch op {
		case int(runtimeValueExprBinaryOpAdd):
			return parser.RealValue(left.F64 + right.F64), nil
		case int(runtimeValueExprBinaryOpSub):
			return parser.RealValue(left.F64 - right.F64), nil
		default:
			return parser.Value{}, errInvalidExpression
		}
	default:
		return parser.Value{}, errTypeMismatch
	}
}

func isAggregateExpr(expr *runtimeValueExpr) bool {
	return expr != nil && expr.kind == runtimeValueExprKindAggregateCall
}

func compareIntegerValues(op string, left, right int64) (bool, error) {
	switch op {
	case "=":
		return left == right, nil
	case "!=":
		return left != right, nil
	case "<":
		return left < right, nil
	case "<=":
		return left <= right, nil
	case ">":
		return left > right, nil
	case ">=":
		return left >= right, nil
	default:
		return false, errUnsupportedComparisonOp
	}
}

func compareSortableIntegerValues(left, right int64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func resolveIntegerComparisonOperands(left, right parser.Value) (parser.Value, parser.Value, error) {
	switch {
	case left.IsIntegerLiteral() && right.IsTypedInteger():
		resolved, err := resolveUntypedIntegerLiteralToValueKind(left, right.Kind)
		if err != nil {
			return parser.Value{}, parser.Value{}, err
		}
		return resolved, right, nil
	case left.IsTypedInteger() && right.IsIntegerLiteral():
		resolved, err := resolveUntypedIntegerLiteralToValueKind(right, left.Kind)
		if err != nil {
			return parser.Value{}, parser.Value{}, err
		}
		return left, resolved, nil
	default:
		return left, right, nil
	}
}

func resolveUntypedIntegerLiteralToValueKind(value parser.Value, targetKind parser.ValueKind) (parser.Value, error) {
	if !value.IsIntegerLiteral() {
		return parser.Value{}, errTypeMismatch
	}
	switch targetKind {
	case parser.ValueKindSmallInt:
		return resolveUntypedIntegerLiteralToExactWidth(value, targetKind)
	case parser.ValueKindInt:
		return resolveUntypedIntegerLiteralToExactWidth(value, targetKind)
	case parser.ValueKindBigInt:
		return resolveUntypedIntegerLiteralToExactWidth(value, targetKind)
	default:
		return value, nil
	}
}

func resolveUntypedIntegerLiteralToExactWidth(value parser.Value, targetKind parser.ValueKind) (parser.Value, error) {
	if !value.IsIntegerLiteral() {
		return parser.Value{}, errTypeMismatch
	}
	resolved, err := buildExactWidthIntegerValue(targetKind, value.I64)
	if err == errIntOutOfRange {
		return parser.Value{}, errTypeMismatch
	}
	return resolved, err
}
