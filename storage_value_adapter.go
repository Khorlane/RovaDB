package rovadb

import (
	"time"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
	"github.com/Khorlane/RovaDB/internal/storage"
)

func storageValueFromParser(value parser.Value) storage.Value {
	switch value.Kind {
	case parser.ValueKindNull:
		return storage.NullValue()
	case parser.ValueKindIntegerLiteral:
		return storage.IntegerLiteralValue(value.I64)
	case parser.ValueKindSmallInt:
		return storage.SmallIntValue(value.I16)
	case parser.ValueKindInt:
		return storage.IntValue(value.I32)
	case parser.ValueKindBigInt:
		return storage.BigIntValue(value.I64)
	case parser.ValueKindString:
		return storage.StringValue(value.Str)
	case parser.ValueKindBool:
		return storage.BoolValue(value.Bool)
	case parser.ValueKindReal:
		return storage.RealValue(value.F64)
	case parser.ValueKindDate:
		return storage.DateValue(value.DateDays)
	case parser.ValueKindTime:
		return storage.TimeValue(value.TimeSeconds)
	case parser.ValueKindTimestamp:
		return storage.TimestampValue(value.TimestampMillis, value.TimestampZoneID)
	default:
		return storage.Value{}
	}
}

func storageValuesFromParser(values []parser.Value) []storage.Value {
	converted := make([]storage.Value, 0, len(values))
	for _, value := range values {
		converted = append(converted, storageValueFromParser(value))
	}
	return converted
}

func parserValueFromStorage(value storage.Value) parser.Value {
	switch value.Kind {
	case storage.ValueKindNull:
		return parser.NullValue()
	case storage.ValueKindIntegerLiteral:
		return parser.IntegerLiteralValue(value.I64)
	case storage.ValueKindSmallInt:
		return parser.SmallIntValue(value.I16)
	case storage.ValueKindInt:
		return parser.IntValue(value.I32)
	case storage.ValueKindBigInt:
		return parser.BigIntValue(value.I64)
	case storage.ValueKindString:
		return parser.StringValue(value.Str)
	case storage.ValueKindBool:
		return parser.BoolValue(value.Bool)
	case storage.ValueKindReal:
		return parser.RealValue(value.F64)
	case storage.ValueKindDate:
		return parser.DateValue(value.DateDays)
	case storage.ValueKindTime:
		return parser.TimeValue(value.TimeSeconds)
	case storage.ValueKindTimestamp:
		return parser.TimestampValue(value.TimestampMillis, value.TimestampZoneID)
	default:
		return parser.Value{}
	}
}

func parserValuesFromStorage(values []storage.Value) []parser.Value {
	converted := make([]parser.Value, 0, len(values))
	for _, value := range values {
		converted = append(converted, parserValueFromStorage(value))
	}
	return converted
}

func parserRowsFromStorage(rows [][]storage.Value) [][]parser.Value {
	converted := make([][]parser.Value, 0, len(rows))
	for _, row := range rows {
		converted = append(converted, parserValuesFromStorage(row))
	}
	return converted
}

func parserRowsToStorage(rows [][]parser.Value) [][]storage.Value {
	converted := make([][]storage.Value, 0, len(rows))
	for _, row := range rows {
		converted = append(converted, storageValuesFromParser(row))
	}
	return converted
}

func publicValueFromParser(value parser.Value) any {
	switch value.Kind {
	case parser.ValueKindNull:
		return nil
	case parser.ValueKindIntegerLiteral, parser.ValueKindSmallInt, parser.ValueKindInt, parser.ValueKindBigInt:
		return value.Any()
	case parser.ValueKindString:
		return value.Str
	case parser.ValueKindBool:
		return value.Bool
	case parser.ValueKindReal:
		return value.F64
	default:
		return nil
	}
}

func normalizeStatementTimestampValues(stmt any, location *time.Location) {
	switch stmt := stmt.(type) {
	case *parser.CreateTableStmt:
		for i := range stmt.Columns {
			normalizeParserValueTimestamp(&stmt.Columns[i].DefaultValue, location)
		}
	case *parser.AlterTableAddColumnStmt:
		normalizeParserValueTimestamp(&stmt.Column.DefaultValue, location)
	case *parser.InsertStmt:
		for i := range stmt.Values {
			normalizeParserValueTimestamp(&stmt.Values[i], location)
		}
		for _, expr := range stmt.ValueExprs {
			normalizeParserValueExprTimestamps(expr, location)
		}
	case *parser.UpdateStmt:
		for i := range stmt.Assignments {
			normalizeParserValueTimestamp(&stmt.Assignments[i].Value, location)
			normalizeParserValueExprTimestamps(stmt.Assignments[i].Expr, location)
		}
		normalizeParserPredicateTimestamps(stmt.Predicate, location)
		normalizeParserWhereTimestamps(stmt.Where, location)
	case *parser.DeleteStmt:
		normalizeParserPredicateTimestamps(stmt.Predicate, location)
		normalizeParserWhereTimestamps(stmt.Where, location)
	}
}

func normalizeSelectPlanTimestampValues(plan *planner.SelectPlan, location *time.Location) {
	if plan == nil {
		return
	}
	if plan.Query != nil {
		normalizePlannerQueryTimestampValues(plan.Query, location)
	}
	if plan.IndexScan != nil {
		normalizePlannerValueTimestamp(&plan.IndexScan.LookupValue, location)
	}
}

func normalizePlannerQueryTimestampValues(query *planner.SelectQuery, location *time.Location) {
	if query == nil {
		return
	}
	for _, expr := range query.ProjectionExprs {
		normalizePlannerValueExprTimestamps(expr, location)
	}
	normalizePlannerPredicateTimestamps(query.Predicate, location)
	normalizePlannerWhereTimestamps(query.Where, location)
}

func normalizePlannerWhereTimestamps(where *planner.WhereClause, location *time.Location) {
	if where == nil {
		return
	}
	for i := range where.Items {
		normalizePlannerValueTimestamp(&where.Items[i].Condition.Right, location)
		normalizePlannerValueExprTimestamps(where.Items[i].Condition.LeftExpr, location)
		normalizePlannerValueExprTimestamps(where.Items[i].Condition.RightExpr, location)
	}
}

func normalizePlannerPredicateTimestamps(predicate *planner.PredicateExpr, location *time.Location) {
	if predicate == nil {
		return
	}
	if predicate.Comparison != nil {
		normalizePlannerValueTimestamp(&predicate.Comparison.Right, location)
		normalizePlannerValueExprTimestamps(predicate.Comparison.LeftExpr, location)
		normalizePlannerValueExprTimestamps(predicate.Comparison.RightExpr, location)
	}
	normalizePlannerPredicateTimestamps(predicate.Left, location)
	normalizePlannerPredicateTimestamps(predicate.Right, location)
	normalizePlannerPredicateTimestamps(predicate.Inner, location)
}

func normalizePlannerValueExprTimestamps(expr *planner.ValueExpr, location *time.Location) {
	if expr == nil {
		return
	}
	normalizePlannerValueTimestamp(&expr.Value, location)
	normalizePlannerValueExprTimestamps(expr.Left, location)
	normalizePlannerValueExprTimestamps(expr.Right, location)
	normalizePlannerValueExprTimestamps(expr.Arg, location)
	normalizePlannerValueExprTimestamps(expr.Inner, location)
}

func normalizePlannerValueTimestamp(value *planner.Value, location *time.Location) {
	if value == nil || value.Kind != planner.ValueKindTimestampUnresolved {
		return
	}
	millis := resolveTimestampComponentsMillis(
		value.TimestampYear,
		value.TimestampMonth,
		value.TimestampDay,
		value.TimestampHour,
		value.TimestampMinute,
		value.TimestampSecond,
		location,
	)
	*value = planner.TimestampValue(millis, 0)
}

func normalizeParserWhereTimestamps(where *parser.WhereClause, location *time.Location) {
	if where == nil {
		return
	}
	for i := range where.Items {
		normalizeParserValueTimestamp(&where.Items[i].Condition.Right, location)
		normalizeParserValueExprTimestamps(where.Items[i].Condition.LeftExpr, location)
		normalizeParserValueExprTimestamps(where.Items[i].Condition.RightExpr, location)
	}
}

func normalizeParserPredicateTimestamps(predicate *parser.PredicateExpr, location *time.Location) {
	if predicate == nil {
		return
	}
	if predicate.Comparison != nil {
		normalizeParserValueTimestamp(&predicate.Comparison.Right, location)
		normalizeParserValueExprTimestamps(predicate.Comparison.LeftExpr, location)
		normalizeParserValueExprTimestamps(predicate.Comparison.RightExpr, location)
	}
	normalizeParserPredicateTimestamps(predicate.Left, location)
	normalizeParserPredicateTimestamps(predicate.Right, location)
	normalizeParserPredicateTimestamps(predicate.Inner, location)
}

func normalizeParserValueExprTimestamps(expr *parser.ValueExpr, location *time.Location) {
	if expr == nil {
		return
	}
	normalizeParserValueTimestamp(&expr.Value, location)
	normalizeParserValueExprTimestamps(expr.Left, location)
	normalizeParserValueExprTimestamps(expr.Right, location)
	normalizeParserValueExprTimestamps(expr.Arg, location)
	normalizeParserValueExprTimestamps(expr.Inner, location)
}

func normalizeParserValueTimestamp(value *parser.Value, location *time.Location) {
	if value == nil || value.Kind != parser.ValueKindTimestampUnresolved {
		return
	}
	millis := resolveTimestampComponentsMillis(
		value.TimestampYear,
		value.TimestampMonth,
		value.TimestampDay,
		value.TimestampHour,
		value.TimestampMinute,
		value.TimestampSecond,
		location,
	)
	*value = parser.TimestampValue(millis, 0)
}

func resolveTimestampComponentsMillis(year, month, day, hour, minute, second int32, _ *time.Location) int64 {
	return time.Date(
		int(year),
		time.Month(month),
		int(day),
		int(hour),
		int(minute),
		int(second),
		0,
		time.UTC,
	).UnixMilli()
}
