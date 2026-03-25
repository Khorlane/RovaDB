package parser

import (
	"fmt"
)

// BindPlaceholders replaces positional placeholder values with concrete values
// in left-to-right encounter order.
func BindPlaceholders(stmt any, args []any) error {
	values := collectBindableValues(stmt)
	if len(values) != len(args) {
		return newBindError("placeholder count mismatch")
	}

	for i, target := range values {
		if target == nil {
			return newBindError("unexpected placeholder left unbound")
		}

		bound, err := bindArgumentValue(args[i])
		if err != nil {
			return err
		}
		*target = bound
	}

	if containsPlaceholder(stmt) {
		return newBindError("unexpected placeholder left unbound")
	}

	syncLegacyWhereFromPredicate(stmt)

	return nil
}

func collectBindableValues(stmt any) []*Value {
	switch stmt := stmt.(type) {
	case *SelectExpr:
		values := make([]*Value, 0)
		for _, join := range stmt.Joins {
			values = append(values, collectPredicateValues(join.Predicate)...)
		}
		if stmt.Predicate != nil {
			return append(values, collectPredicateValues(stmt.Predicate)...)
		}
		return append(values, collectWhereValues(stmt.Where)...)
	case *InsertStmt:
		values := make([]*Value, 0, len(stmt.Values))
		for i := range stmt.Values {
			if stmt.Values[i].Kind == ValueKindPlaceholder {
				values = append(values, &stmt.Values[i])
			}
		}
		return values
	case *UpdateStmt:
		values := make([]*Value, 0, len(stmt.Assignments))
		for i := range stmt.Assignments {
			if stmt.Assignments[i].Value.Kind == ValueKindPlaceholder {
				values = append(values, &stmt.Assignments[i].Value)
			}
		}
		if stmt.Predicate != nil {
			return append(values, collectPredicateValues(stmt.Predicate)...)
		}
		return append(values, collectWhereValues(stmt.Where)...)
	case *DeleteStmt:
		if stmt.Predicate != nil {
			return collectPredicateValues(stmt.Predicate)
		}
		return collectWhereValues(stmt.Where)
	default:
		return nil
	}
}

func collectWhereValues(where *WhereClause) []*Value {
	if where == nil {
		return nil
	}

	values := make([]*Value, 0, len(where.Items))
	for i := range where.Items {
		if where.Items[i].Condition.Right.Kind == ValueKindPlaceholder {
			values = append(values, &where.Items[i].Condition.Right)
		}
	}
	return values
}

func containsPlaceholder(stmt any) bool {
	for _, value := range collectAllValues(stmt) {
		if value != nil && value.Kind == ValueKindPlaceholder {
			return true
		}
	}
	return false
}

func collectAllValues(stmt any) []*Value {
	switch stmt := stmt.(type) {
	case *SelectExpr:
		values := make([]*Value, 0)
		for _, join := range stmt.Joins {
			values = append(values, collectAllPredicateValues(join.Predicate)...)
		}
		if stmt.Predicate != nil {
			return append(values, collectAllPredicateValues(stmt.Predicate)...)
		}
		return append(values, collectAllWhereValues(stmt.Where)...)
	case *InsertStmt:
		values := make([]*Value, 0, len(stmt.Values))
		for i := range stmt.Values {
			values = append(values, &stmt.Values[i])
		}
		return values
	case *UpdateStmt:
		values := make([]*Value, 0, len(stmt.Assignments))
		for i := range stmt.Assignments {
			values = append(values, &stmt.Assignments[i].Value)
		}
		if stmt.Predicate != nil {
			return append(values, collectAllPredicateValues(stmt.Predicate)...)
		}
		return append(values, collectAllWhereValues(stmt.Where)...)
	case *DeleteStmt:
		if stmt.Predicate != nil {
			return collectAllPredicateValues(stmt.Predicate)
		}
		return collectAllWhereValues(stmt.Where)
	default:
		return nil
	}
}

func collectPredicateValues(predicate *PredicateExpr) []*Value {
	if predicate == nil {
		return nil
	}

	values := make([]*Value, 0)
	switch predicate.Kind {
	case PredicateKindComparison:
		if predicate.Comparison != nil {
			values = append(values, collectValueExprPlaceholders(predicate.Comparison.LeftExpr)...)
			values = append(values, collectValueExprPlaceholders(predicate.Comparison.RightExpr)...)
		}
	case PredicateKindAnd, PredicateKindOr:
		values = append(values, collectPredicateValues(predicate.Left)...)
		values = append(values, collectPredicateValues(predicate.Right)...)
	case PredicateKindNot:
		values = append(values, collectPredicateValues(predicate.Inner)...)
	}
	return values
}

func collectAllPredicateValues(predicate *PredicateExpr) []*Value {
	if predicate == nil {
		return nil
	}

	values := make([]*Value, 0)
	switch predicate.Kind {
	case PredicateKindComparison:
		if predicate.Comparison != nil {
			values = append(values, collectAllValueExprValues(predicate.Comparison.LeftExpr)...)
			values = append(values, collectAllValueExprValues(predicate.Comparison.RightExpr)...)
		}
	case PredicateKindAnd, PredicateKindOr:
		values = append(values, collectAllPredicateValues(predicate.Left)...)
		values = append(values, collectAllPredicateValues(predicate.Right)...)
	case PredicateKindNot:
		values = append(values, collectAllPredicateValues(predicate.Inner)...)
	}
	return values
}

func collectAllWhereValues(where *WhereClause) []*Value {
	if where == nil {
		return nil
	}

	values := make([]*Value, 0, len(where.Items))
	for i := range where.Items {
		values = append(values, &where.Items[i].Condition.Right)
	}
	return values
}

func bindArgumentValue(arg any) (Value, error) {
	switch v := arg.(type) {
	case nil:
		return NullValue(), nil
	case int:
		return Int64Value(int64(v)), nil
	case string:
		return StringValue(v), nil
	case bool:
		return BoolValue(v), nil
	case float64:
		return RealValue(v), nil
	default:
		return Value{}, newBindError(fmt.Sprintf("unsupported placeholder argument type: %T", arg))
	}
}

func newBindError(msg string) error {
	return fmt.Errorf("bind: %s", msg)
}

func collectValueExprPlaceholders(expr *ValueExpr) []*Value {
	if expr == nil {
		return nil
	}

	values := make([]*Value, 0)
	switch expr.Kind {
	case ValueExprKindLiteral:
		if expr.Value.Kind == ValueKindPlaceholder {
			values = append(values, &expr.Value)
		}
	case ValueExprKindFunctionCall:
		values = append(values, collectValueExprPlaceholders(expr.Arg)...)
	case ValueExprKindParen:
		values = append(values, collectValueExprPlaceholders(expr.Inner)...)
	}
	return values
}

func collectAllValueExprValues(expr *ValueExpr) []*Value {
	if expr == nil {
		return nil
	}

	values := make([]*Value, 0)
	switch expr.Kind {
	case ValueExprKindLiteral:
		values = append(values, &expr.Value)
	case ValueExprKindFunctionCall:
		values = append(values, collectAllValueExprValues(expr.Arg)...)
	case ValueExprKindParen:
		values = append(values, collectAllValueExprValues(expr.Inner)...)
	}
	return values
}

func syncLegacyWhereFromPredicate(stmt any) {
	switch stmt := stmt.(type) {
	case *SelectExpr:
		if stmt.Predicate != nil {
			if where, ok := flattenPredicateExpr(stmt.Predicate); ok {
				stmt.Where = where
			} else {
				stmt.Where = nil
			}
		}
	case *UpdateStmt:
		if stmt.Predicate != nil {
			if where, ok := flattenPredicateExpr(stmt.Predicate); ok {
				stmt.Where = where
			} else {
				stmt.Where = nil
			}
		}
	case *DeleteStmt:
		if stmt.Predicate != nil {
			if where, ok := flattenPredicateExpr(stmt.Predicate); ok {
				stmt.Where = where
			} else {
				stmt.Where = nil
			}
		}
	}
}
