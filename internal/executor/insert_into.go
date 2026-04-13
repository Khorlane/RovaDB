package executor

import (
	"fmt"
	"math"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func executeInsert(stmt *parser.InsertStmt, tables map[string]*Table) (int64, error) {
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, newTableNotFoundError(stmt.TableName)
	}

	values, err := evalInsertValues(stmt)
	if err != nil {
		return 0, err
	}

	row, err := buildInsertRow(table, stmt.Columns, values)
	if err != nil {
		return 0, err
	}

	candidateRows := append(cloneRows(table.Rows), row)
	if err := validateIndexedTextLimits(table, candidateRows); err != nil {
		return 0, err
	}
	if err := validateUniqueIndexes(table, candidateRows); err != nil {
		return 0, err
	}
	table.Rows = append(table.Rows, row)
	if err := rebuildIndexesForTable(table); err != nil {
		return 0, err
	}
	return 1, nil
}

func buildInsertRow(table *Table, columnNames []string, values []parser.Value) ([]parser.Value, error) {
	if len(columnNames) == 0 {
		if len(values) != len(table.Columns) {
			return nil, errWrongValueCount
		}
		row := make([]parser.Value, len(table.Columns))
		for i, value := range values {
			normalized, err := normalizeColumnValue(table, i, value)
			if err != nil {
				return nil, err
			}
			row[i] = normalized
		}
		return row, nil
	}

	if len(values) != len(columnNames) {
		return nil, errWrongValueCount
	}

	row := make([]parser.Value, len(table.Columns))
	seen := make(map[int]struct{}, len(columnNames))
	for i, name := range columnNames {
		idx, err := resolveColumnIndex(name, table)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[idx]; ok {
			return nil, errWrongValueCount
		}
		normalized, err := normalizeColumnValue(table, idx, values[i])
		if err != nil {
			return nil, err
		}
		seen[idx] = struct{}{}
		row[idx] = normalized
	}

	for i, column := range table.Columns {
		if _, ok := seen[i]; ok {
			continue
		}
		if column.HasDefault {
			normalized, err := normalizeColumnValue(table, i, column.DefaultValue)
			if err != nil {
				return nil, err
			}
			row[i] = normalized
			continue
		}
		if column.NotNull {
			return nil, newNotNullConstraintError(table.Name, column.Name)
		}
		row[i] = parser.NullValue()
	}

	return row, nil
}

func evalInsertValues(stmt *parser.InsertStmt) ([]parser.Value, error) {
	if stmt == nil {
		return nil, errUnsupportedStatement
	}
	if len(stmt.ValueExprs) == 0 {
		return append([]parser.Value(nil), stmt.Values...), nil
	}
	values := make([]parser.Value, 0, len(stmt.ValueExprs))
	for _, expr := range stmt.ValueExprs {
		if err := validateInsertValueExpr(expr); err != nil {
			return nil, err
		}
		value, err := evalInsertValueExpr(expr)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func validateInsertValueExpr(expr *parser.ValueExpr) error {
	if expr == nil {
		return errUnsupportedStatement
	}
	switch expr.Kind {
	case parser.ValueExprKindLiteral:
		return nil
	case parser.ValueExprKindParen:
		return validateInsertValueExpr(expr.Inner)
	case parser.ValueExprKindBinary:
		if err := validateInsertValueExpr(expr.Left); err != nil {
			return err
		}
		return validateInsertValueExpr(expr.Right)
	case parser.ValueExprKindFunctionCall:
		return validateInsertValueExpr(expr.Arg)
	default:
		return errUnsupportedStatement
	}
}

func evalInsertValueExpr(expr *parser.ValueExpr) (parser.Value, error) {
	if expr == nil {
		return parser.Value{}, errUnsupportedStatement
	}
	switch expr.Kind {
	case parser.ValueExprKindLiteral:
		return expr.Value, nil
	case parser.ValueExprKindParen:
		return evalInsertValueExpr(expr.Inner)
	case parser.ValueExprKindBinary:
		left, err := evalInsertValueExpr(expr.Left)
		if err != nil {
			return parser.Value{}, err
		}
		right, err := evalInsertValueExpr(expr.Right)
		if err != nil {
			return parser.Value{}, err
		}
		return evalBinaryValueExpr(int(expr.Op), left, right)
	case parser.ValueExprKindFunctionCall:
		arg, err := evalInsertValueExpr(expr.Arg)
		if err != nil {
			return parser.Value{}, err
		}
		return evalScalarFunction(expr.FuncName, arg)
	default:
		return parser.Value{}, errUnsupportedStatement
	}
}

func valueMatchesColumnType(value parser.Value, typeName string) bool {
	if value.Kind == parser.ValueKindNull {
		return true
	}
	switch typeName {
	case parser.ColumnTypeSmallInt, parser.ColumnTypeInt, parser.ColumnTypeBigInt:
		return value.IsInteger()
	case parser.ColumnTypeText:
		return value.Kind == parser.ValueKindString
	case parser.ColumnTypeBool:
		return value.Kind == parser.ValueKindBool
	case parser.ColumnTypeReal:
		return value.Kind == parser.ValueKindReal
	default:
		return false
	}
}

func normalizeColumnValue(table *Table, columnIndex int, value parser.Value) (parser.Value, error) {
	if table == nil || columnIndex < 0 || columnIndex >= len(table.Columns) {
		return parser.Value{}, errColumnDoesNotExist
	}
	column := table.Columns[columnIndex]
	normalized, err := normalizeColumnValueForDef(column, value)
	if err != nil {
		return parser.Value{}, err
	}
	if column.NotNull && normalized.Kind == parser.ValueKindNull {
		return parser.Value{}, newNotNullConstraintError(table.Name, column.Name)
	}
	return normalized, nil
}

func normalizeColumnValueForDef(column parser.ColumnDef, value parser.Value) (parser.Value, error) {
	if !valueMatchesColumnType(value, column.Type) {
		return parser.Value{}, errTypeMismatch
	}
	if value.Kind == parser.ValueKindNull {
		return value, nil
	}
	switch column.Type {
	case parser.ColumnTypeSmallInt:
		return normalizeIntegerColumnValue(value, parser.BoundIntegerTypeInt16, math.MinInt16, math.MaxInt16)
	case parser.ColumnTypeInt:
		return normalizeIntegerColumnValue(value, parser.BoundIntegerTypeInt32, math.MinInt32, math.MaxInt32)
	case parser.ColumnTypeBigInt:
		return normalizeIntegerColumnValue(value, parser.BoundIntegerTypeInt64, math.MinInt64, math.MaxInt64)
	default:
		return value, nil
	}
}

func normalizeIntegerColumnValue(value parser.Value, exactType parser.BoundIntegerType, minValue, maxValue int64) (parser.Value, error) {
	if value.BoundIntegerType != parser.BoundIntegerTypeNone && value.BoundIntegerType != exactType {
		return parser.Value{}, errTypeMismatch
	}
	if value.IsTypedInteger() && integerBoundTypeForValue(value) != exactType {
		return parser.Value{}, errTypeMismatch
	}
	integerValue := value.IntegerValue()
	if integerValue < minValue || integerValue > maxValue {
		return parser.Value{}, errTypeMismatch
	}
	switch exactType {
	case parser.BoundIntegerTypeInt16:
		return parser.SmallIntValue(int16(integerValue)), nil
	case parser.BoundIntegerTypeInt, parser.BoundIntegerTypeInt32:
		return parser.IntValue(int32(integerValue)), nil
	case parser.BoundIntegerTypeInt64:
		return parser.BigIntValue(integerValue), nil
	default:
		return parser.Value{}, errTypeMismatch
	}
}

func integerBoundTypeForValue(value parser.Value) parser.BoundIntegerType {
	switch value.Kind {
	case parser.ValueKindSmallInt:
		return parser.BoundIntegerTypeInt16
	case parser.ValueKindInt:
		return parser.BoundIntegerTypeInt32
	case parser.ValueKindBigInt:
		return parser.BoundIntegerTypeInt64
	default:
		return parser.BoundIntegerTypeNone
	}
}

func validateColumnValue(table *Table, columnIndex int, value parser.Value) error {
	_, err := normalizeColumnValue(table, columnIndex, value)
	return err
}

func newNotNullConstraintError(tableName, columnName string) error {
	if tableName == "" && columnName == "" {
		return newExecError("NOT NULL constraint failed")
	}
	if tableName == "" {
		return newExecError(fmt.Sprintf("NOT NULL constraint failed: %s", columnName))
	}
	return newExecError(fmt.Sprintf("NOT NULL constraint failed: %s.%s", tableName, columnName))
}
