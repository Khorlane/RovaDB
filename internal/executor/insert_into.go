package executor

import (
	"github.com/Khorlane/RovaDB/internal/parser"
)

func executeInsert(stmt *parser.InsertStmt, tables map[string]*Table) (int64, error) {
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, errTableDoesNotExist
	}

	values, err := evalInsertValues(stmt)
	if err != nil {
		return 0, err
	}

	if len(stmt.Columns) == 0 {
		if len(values) != len(table.Columns) {
			return 0, errWrongValueCount
		}
		for i, value := range values {
			if !valueMatchesColumnType(value, table.Columns[i].Type) {
				return 0, errTypeMismatch
			}
		}

		row := append([]parser.Value(nil), values...)
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

	if len(stmt.Columns) != len(table.Columns) || len(values) != len(table.Columns) {
		return 0, errWrongValueCount
	}

	row := make([]parser.Value, len(table.Columns))
	seen := make(map[int]struct{}, len(stmt.Columns))
	for i, name := range stmt.Columns {
		idx := -1
		for j, column := range table.Columns {
			if column.Name == name {
				idx = j
				break
			}
		}
		if idx < 0 {
			return 0, errColumnDoesNotExist
		}
		if _, ok := seen[idx]; ok {
			return 0, errWrongValueCount
		}
		if !valueMatchesColumnType(values[i], table.Columns[idx].Type) {
			return 0, errTypeMismatch
		}
		seen[idx] = struct{}{}
		row[idx] = values[i]
	}
	if len(seen) != len(table.Columns) {
		return 0, errWrongValueCount
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
		return evalBinaryValueExpr(expr.Op, left, right)
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
	case parser.ColumnTypeInt:
		return value.Kind == parser.ValueKindInt64
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
