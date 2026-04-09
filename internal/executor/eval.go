package executor

import (
	"strings"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
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
		return publicIntResult(expr.I64)
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
		if left.Kind != parser.ValueKindInt64 || right.Kind != parser.ValueKindInt64 {
			return parser.Value{}, errInvalidExpression
		}

		switch expr.Op {
		case parser.BinaryOpAdd:
			return publicIntResult(left.I64 + right.I64)
		case parser.BinaryOpSub:
			return publicIntResult(left.I64 - right.I64)
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
		return publicIntResult(int64(len(arg.Str)))
	case "ABS":
		switch arg.Kind {
		case parser.ValueKindInt64:
			if arg.I64 < 0 {
				return publicIntResult(-arg.I64)
			}
			return publicIntResult(arg.I64)
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
	if left.Kind != right.Kind {
		return parser.Value{}, errTypeMismatch
	}
	switch left.Kind {
	case parser.ValueKindInt64:
		switch op {
		case int(planner.ValueExprBinaryOpAdd):
			return publicIntResult(left.I64 + right.I64)
		case int(planner.ValueExprBinaryOpSub):
			return publicIntResult(left.I64 - right.I64)
		default:
			return parser.Value{}, errInvalidExpression
		}
	case parser.ValueKindReal:
		switch op {
		case int(planner.ValueExprBinaryOpAdd):
			return parser.RealValue(left.F64 + right.F64), nil
		case int(planner.ValueExprBinaryOpSub):
			return parser.RealValue(left.F64 - right.F64), nil
		default:
			return parser.Value{}, errInvalidExpression
		}
	default:
		return parser.Value{}, errTypeMismatch
	}
}

func isAggregateExpr(expr *planner.ValueExpr) bool {
	return expr != nil && expr.Kind == planner.ValueExprKindAggregateCall
}
