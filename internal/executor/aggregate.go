package executor

import (
	"strings"

	"github.com/Khorlane/RovaDB/internal/parser"
)

type aggregateValueEvaluator func(row []parser.Value, expr *runtimeValueExpr) (parser.Value, error)

func evalAggregateExprRows(expr *runtimeValueExpr, rows [][]parser.Value, eval aggregateValueEvaluator) (parser.Value, error) {
	if expr == nil || expr.kind != runtimeValueExprKindAggregateCall {
		return parser.Value{}, errUnsupportedStatement
	}

	name := strings.ToUpper(expr.funcName)
	switch name {
	case "COUNT":
		if expr.starArg {
			return publicIntResult(int64(len(rows)))
		}
		count := int64(0)
		for _, row := range rows {
			value, err := eval(row, expr.arg)
			if err != nil {
				return parser.Value{}, err
			}
			if value.Kind != parser.ValueKindNull {
				count++
			}
		}
		return publicIntResult(count)
	case "MIN":
		return aggregateMinMax(rows, expr.arg, eval, true)
	case "MAX":
		return aggregateMinMax(rows, expr.arg, eval, false)
	case "AVG":
		return aggregateAvg(rows, expr.arg, eval)
	case "SUM":
		return aggregateSum(rows, expr.arg, eval)
	default:
		return parser.Value{}, errUnsupportedStatement
	}
}

func aggregateMinMax(rows [][]parser.Value, arg *runtimeValueExpr, eval aggregateValueEvaluator, wantMin bool) (parser.Value, error) {
	var result parser.Value
	found := false
	for _, row := range rows {
		value, err := eval(row, arg)
		if err != nil {
			return parser.Value{}, err
		}
		if value.Kind == parser.ValueKindNull {
			continue
		}
		switch value.Kind {
		case parser.ValueKindInt64, parser.ValueKindString, parser.ValueKindReal:
		default:
			return parser.Value{}, errTypeMismatch
		}
		if !found {
			result = value
			found = true
			continue
		}
		cmp, err := compareSortableValues(result, value)
		if err != nil {
			return parser.Value{}, err
		}
		if (wantMin && cmp > 0) || (!wantMin && cmp < 0) {
			result = value
		}
	}
	if !found {
		return parser.NullValue(), nil
	}
	return result, nil
}

func aggregateAvg(rows [][]parser.Value, arg *runtimeValueExpr, eval aggregateValueEvaluator) (parser.Value, error) {
	sum := 0.0
	count := 0
	for _, row := range rows {
		value, err := eval(row, arg)
		if err != nil {
			return parser.Value{}, err
		}
		switch value.Kind {
		case parser.ValueKindNull:
			continue
		case parser.ValueKindInt64:
			sum += float64(value.I64)
		case parser.ValueKindReal:
			sum += value.F64
		default:
			return parser.Value{}, errTypeMismatch
		}
		count++
	}
	if count == 0 {
		return parser.NullValue(), nil
	}
	return parser.RealValue(sum / float64(count)), nil
}

func aggregateSum(rows [][]parser.Value, arg *runtimeValueExpr, eval aggregateValueEvaluator) (parser.Value, error) {
	sum := 0.0
	found := false
	for _, row := range rows {
		value, err := eval(row, arg)
		if err != nil {
			return parser.Value{}, err
		}
		switch value.Kind {
		case parser.ValueKindNull:
			continue
		case parser.ValueKindInt64:
			sum += float64(value.I64)
		case parser.ValueKindReal:
			sum += value.F64
		default:
			return parser.Value{}, errTypeMismatch
		}
		found = true
	}
	if !found {
		return parser.NullValue(), nil
	}
	return parser.RealValue(sum), nil
}
