package executor

import (
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
		return parser.Int64Value(expr.I64), nil
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
		if left.Kind != parser.ValueKindInt64 || right.Kind != parser.ValueKindInt64 {
			return parser.Value{}, errInvalidExpression
		}

		switch expr.Op {
		case parser.BinaryOpAdd:
			return parser.Int64Value(left.I64 + right.I64), nil
		case parser.BinaryOpSub:
			return parser.Int64Value(left.I64 - right.I64), nil
		default:
			return parser.Value{}, errInvalidExpression
		}
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
	if left.Kind != right.Kind {
		return false, errTypeMismatch
	}

	switch left.Kind {
	case parser.ValueKindInt64:
		switch op {
		case "=":
			return left.I64 == right.I64, nil
		case "!=":
			return left.I64 != right.I64, nil
		case "<":
			return left.I64 < right.I64, nil
		case "<=":
			return left.I64 <= right.I64, nil
		case ">":
			return left.I64 > right.I64, nil
		case ">=":
			return left.I64 >= right.I64, nil
		default:
			return false, errUnsupportedComparisonOp
		}
	case parser.ValueKindString:
		switch op {
		case "=":
			return left.Str == right.Str, nil
		case "!=":
			return left.Str != right.Str, nil
		case "<":
			return left.Str < right.Str, nil
		case "<=":
			return left.Str <= right.Str, nil
		case ">":
			return left.Str > right.Str, nil
		case ">=":
			return left.Str >= right.Str, nil
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
	default:
		return false, errTypeMismatch
	}
}

func compareSortableValues(left, right parser.Value) (int, error) {
	if left.Kind == parser.ValueKindNull || right.Kind == parser.ValueKindNull {
		return 0, errTypeMismatch
	}
	if left.Kind != right.Kind {
		return 0, errTypeMismatch
	}

	switch left.Kind {
	case parser.ValueKindInt64:
		switch {
		case left.I64 < right.I64:
			return -1, nil
		case left.I64 > right.I64:
			return 1, nil
		default:
			return 0, nil
		}
	case parser.ValueKindString:
		switch {
		case left.Str < right.Str:
			return -1, nil
		case left.Str > right.Str:
			return 1, nil
		default:
			return 0, nil
		}
	default:
		return 0, errTypeMismatch
	}
}
