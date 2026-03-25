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

	return nil
}

func collectBindableValues(stmt any) []*Value {
	switch stmt := stmt.(type) {
	case *SelectExpr:
		return collectWhereValues(stmt.Where)
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
		return append(values, collectWhereValues(stmt.Where)...)
	case *DeleteStmt:
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
		return collectAllWhereValues(stmt.Where)
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
		return append(values, collectAllWhereValues(stmt.Where)...)
	case *DeleteStmt:
		return collectAllWhereValues(stmt.Where)
	default:
		return nil
	}
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
	case int8:
		return Int64Value(int64(v)), nil
	case int16:
		return Int64Value(int64(v)), nil
	case int32:
		return Int64Value(int64(v)), nil
	case int64:
		return Int64Value(v), nil
	case string:
		return StringValue(v), nil
	case bool:
		return BoolValue(v), nil
	case float32:
		return RealValue(float64(v)), nil
	case float64:
		return RealValue(v), nil
	default:
		return Value{}, newBindError(fmt.Sprintf("unsupported placeholder argument type: %T", arg))
	}
}

func newBindError(msg string) error {
	return fmt.Errorf("bind: %s", msg)
}
