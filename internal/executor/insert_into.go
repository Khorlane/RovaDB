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
	return normalizeColumnScalarValue(column.Type, value)
}

func normalizeColumnScalarValue(typeName string, value parser.Value) (parser.Value, error) {
	if value.Kind == parser.ValueKindNull {
		return value, nil
	}
	switch typeName {
	case parser.ColumnTypeSmallInt:
		return normalizeExactWidthIntegerColumnValue(value, parser.BoundIntegerTypeInt16)
	case parser.ColumnTypeInt:
		return normalizeExactWidthIntegerColumnValue(value, parser.BoundIntegerTypeInt32)
	case parser.ColumnTypeBigInt:
		return normalizeExactWidthIntegerColumnValue(value, parser.BoundIntegerTypeInt64)
	case parser.ColumnTypeText:
		if value.Kind != parser.ValueKindString {
			return parser.Value{}, errTypeMismatch
		}
		return value, nil
	case parser.ColumnTypeBool:
		if value.Kind != parser.ValueKindBool {
			return parser.Value{}, errTypeMismatch
		}
		return value, nil
	case parser.ColumnTypeReal:
		if value.Kind != parser.ValueKindReal {
			return parser.Value{}, errTypeMismatch
		}
		return value, nil
	case parser.ColumnTypeDate:
		return normalizeExactTemporalColumnValue(value, parser.ValueKindDate)
	case parser.ColumnTypeTime:
		return normalizeExactTemporalColumnValue(value, parser.ValueKindTime)
	case parser.ColumnTypeTimestamp:
		if value.Kind == parser.ValueKindTimestampUnresolved {
			return parser.Value{}, errUnresolvedTimestamp
		}
		return normalizeExactTemporalColumnValue(value, parser.ValueKindTimestamp)
	default:
		return parser.Value{}, errTypeMismatch
	}
}

func normalizeExactTemporalColumnValue(value parser.Value, exactKind parser.ValueKind) (parser.Value, error) {
	if value.Kind != exactKind {
		return parser.Value{}, errTypeMismatch
	}
	return value, nil
}

func normalizeExactWidthIntegerColumnValue(value parser.Value, exactType parser.BoundIntegerType) (parser.Value, error) {
	if value.IsIntegerLiteral() {
		return normalizeUntypedIntegerLiteralForColumn(value.I64, exactType)
	}
	if value.BoundIntegerType != parser.BoundIntegerTypeNone && value.BoundIntegerType != exactType {
		return parser.Value{}, errTypeMismatch
	}
	return normalizeTypedIntegerValueForColumn(value, exactType)
}

func normalizeUntypedIntegerLiteralForColumn(value int64, exactType parser.BoundIntegerType) (parser.Value, error) {
	switch exactType {
	case parser.BoundIntegerTypeInt16:
		if value < math.MinInt16 || value > math.MaxInt16 {
			return parser.Value{}, errTypeMismatch
		}
		return parser.SmallIntValue(int16(value)), nil
	case parser.BoundIntegerTypeInt32:
		if value < math.MinInt32 || value > math.MaxInt32 {
			return parser.Value{}, errTypeMismatch
		}
		return parser.IntValue(int32(value)), nil
	case parser.BoundIntegerTypeInt64:
		return parser.BigIntValue(value), nil
	default:
		return parser.Value{}, errTypeMismatch
	}
}

func normalizeTypedIntegerValueForColumn(value parser.Value, exactType parser.BoundIntegerType) (parser.Value, error) {
	switch exactType {
	case parser.BoundIntegerTypeInt16:
		if value.Kind != parser.ValueKindSmallInt {
			return parser.Value{}, errTypeMismatch
		}
		return parser.SmallIntValue(value.I16), nil
	case parser.BoundIntegerTypeInt32:
		if value.Kind != parser.ValueKindInt {
			return parser.Value{}, errTypeMismatch
		}
		return parser.IntValue(value.I32), nil
	case parser.BoundIntegerTypeInt64:
		if value.Kind != parser.ValueKindBigInt {
			return parser.Value{}, errTypeMismatch
		}
		return parser.BigIntValue(value.I64), nil
	default:
		return parser.Value{}, errTypeMismatch
	}
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
