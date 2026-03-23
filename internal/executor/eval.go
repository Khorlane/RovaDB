package executor

import (
	"errors"

	"github.com/Khorlane/RovaDB/internal/parser"
)

var errInvalidExpression = errors.New("executor: invalid expression")

var errUnsupportedComparisonOp = errors.New("executor: unsupported comparison operator")

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
	default:
		return false, errTypeMismatch
	}
}
