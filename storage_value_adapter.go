package rovadb

import (
	"fmt"
	"time"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
	"github.com/Khorlane/RovaDB/internal/storage"
	"github.com/Khorlane/RovaDB/internal/temporal"
)

type timestampNormalizationContext struct {
	defaultTimezone string
	location        *time.Location
	dictionary      []string
	resolvedZoneID  int16
	zoneIDReady     bool
}

type timestampMaterializationContext struct {
	dictionary []string
	locations  map[int16]*time.Location
}

func newTimestampNormalizationContext(defaultTimezone string, location *time.Location, dictionary []string) *timestampNormalizationContext {
	return &timestampNormalizationContext{
		defaultTimezone: defaultTimezone,
		location:        location,
		dictionary:      dictionary,
	}
}

func newTimestampMaterializationContext(dictionary []string) *timestampMaterializationContext {
	return &timestampMaterializationContext{
		dictionary: append([]string(nil), dictionary...),
		locations:  make(map[int16]*time.Location),
	}
}

func (ctx *timestampNormalizationContext) defaultZoneID() (int16, error) {
	if ctx == nil || ctx.defaultTimezone == "" || ctx.location == nil {
		return 0, newExecError("unresolved TIMESTAMP requires configured database timezone")
	}
	if ctx.zoneIDReady {
		return ctx.resolvedZoneID, nil
	}
	for i, zone := range ctx.dictionary {
		if zone != ctx.defaultTimezone {
			continue
		}
		if i > 32767 {
			return 0, fmt.Errorf("timestamp normalization: timezone dictionary exceeds zone_id range: %w", newStorageError("corrupted catalog page"))
		}
		ctx.resolvedZoneID = int16(i)
		ctx.zoneIDReady = true
		return ctx.resolvedZoneID, nil
	}
	return 0, fmt.Errorf("timestamp normalization: default timezone %q missing from catalog dictionary: %w", ctx.defaultTimezone, newStorageError("corrupted catalog page"))
}

func (ctx *timestampMaterializationContext) materializeTimestamp(millisecondsSinceEpoch int64, zoneID int16) (time.Time, error) {
	if ctx == nil {
		return time.UnixMilli(millisecondsSinceEpoch).UTC(), nil
	}
	location, err := ctx.locationForZoneID(zoneID)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(millisecondsSinceEpoch).In(location), nil
}

func (ctx *timestampMaterializationContext) locationForZoneID(zoneID int16) (*time.Location, error) {
	if ctx == nil {
		return nil, fmt.Errorf("timestamp materialization: timezone dictionary is required")
	}
	if zoneID < 0 {
		return nil, fmt.Errorf("timestamp materialization: zone_id %d out of range for timezone dictionary: %w", zoneID, newStorageError("corrupted catalog page"))
	}
	index := int(zoneID)
	if index >= len(ctx.dictionary) {
		return nil, fmt.Errorf("timestamp materialization: zone_id %d out of range for timezone dictionary: %w", zoneID, newStorageError("corrupted catalog page"))
	}
	if location, ok := ctx.locations[zoneID]; ok {
		return location, nil
	}
	zoneName := ctx.dictionary[index]
	if zoneName == "" {
		return nil, fmt.Errorf("timestamp materialization: timezone dictionary entry for zone_id %d is empty: %w", zoneID, newStorageError("corrupted catalog page"))
	}
	location, err := temporal.LoadLocation(zoneName)
	if err != nil {
		return nil, fmt.Errorf("timestamp materialization: timezone dictionary entry for zone_id %d (%q) could not be loaded: %v", zoneID, zoneName, err)
	}
	ctx.locations[zoneID] = location
	return location, nil
}

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

func normalizeStatementTimestampValues(stmt any, ctx *timestampNormalizationContext) error {
	switch stmt := stmt.(type) {
	case *parser.CreateTableStmt:
		for i := range stmt.Columns {
			if err := normalizeParserValueTimestamp(&stmt.Columns[i].DefaultValue, ctx); err != nil {
				return err
			}
		}
	case *parser.AlterTableAddColumnStmt:
		if err := normalizeParserValueTimestamp(&stmt.Column.DefaultValue, ctx); err != nil {
			return err
		}
	case *parser.InsertStmt:
		for i := range stmt.Values {
			if err := normalizeParserValueTimestamp(&stmt.Values[i], ctx); err != nil {
				return err
			}
		}
		for _, expr := range stmt.ValueExprs {
			if err := normalizeParserValueExprTimestamps(expr, ctx); err != nil {
				return err
			}
		}
	case *parser.UpdateStmt:
		for i := range stmt.Assignments {
			if err := normalizeParserValueTimestamp(&stmt.Assignments[i].Value, ctx); err != nil {
				return err
			}
			if err := normalizeParserValueExprTimestamps(stmt.Assignments[i].Expr, ctx); err != nil {
				return err
			}
		}
		if err := normalizeParserPredicateTimestamps(stmt.Predicate, ctx); err != nil {
			return err
		}
		if err := normalizeParserWhereTimestamps(stmt.Where, ctx); err != nil {
			return err
		}
	case *parser.DeleteStmt:
		if err := normalizeParserPredicateTimestamps(stmt.Predicate, ctx); err != nil {
			return err
		}
		if err := normalizeParserWhereTimestamps(stmt.Where, ctx); err != nil {
			return err
		}
	}
	return nil
}

func normalizeSelectPlanTimestampValues(plan *planner.SelectPlan, ctx *timestampNormalizationContext) error {
	if plan == nil {
		return nil
	}
	if plan.Query != nil {
		if err := normalizePlannerQueryTimestampValues(plan.Query, ctx); err != nil {
			return err
		}
	}
	if plan.IndexScan != nil {
		if err := normalizePlannerValueTimestamp(&plan.IndexScan.LookupValue, ctx); err != nil {
			return err
		}
	}
	return nil
}

func normalizePlannerQueryTimestampValues(query *planner.SelectQuery, ctx *timestampNormalizationContext) error {
	if query == nil {
		return nil
	}
	for _, expr := range query.ProjectionExprs {
		if err := normalizePlannerValueExprTimestamps(expr, ctx); err != nil {
			return err
		}
	}
	if err := normalizePlannerPredicateTimestamps(query.Predicate, ctx); err != nil {
		return err
	}
	if err := normalizePlannerWhereTimestamps(query.Where, ctx); err != nil {
		return err
	}
	return nil
}

func normalizePlannerWhereTimestamps(where *planner.WhereClause, ctx *timestampNormalizationContext) error {
	if where == nil {
		return nil
	}
	for i := range where.Items {
		if err := normalizePlannerValueTimestamp(&where.Items[i].Condition.Right, ctx); err != nil {
			return err
		}
		if err := normalizePlannerValueExprTimestamps(where.Items[i].Condition.LeftExpr, ctx); err != nil {
			return err
		}
		if err := normalizePlannerValueExprTimestamps(where.Items[i].Condition.RightExpr, ctx); err != nil {
			return err
		}
	}
	return nil
}

func normalizePlannerPredicateTimestamps(predicate *planner.PredicateExpr, ctx *timestampNormalizationContext) error {
	if predicate == nil {
		return nil
	}
	if predicate.Comparison != nil {
		if err := normalizePlannerValueTimestamp(&predicate.Comparison.Right, ctx); err != nil {
			return err
		}
		if err := normalizePlannerValueExprTimestamps(predicate.Comparison.LeftExpr, ctx); err != nil {
			return err
		}
		if err := normalizePlannerValueExprTimestamps(predicate.Comparison.RightExpr, ctx); err != nil {
			return err
		}
	}
	if err := normalizePlannerPredicateTimestamps(predicate.Left, ctx); err != nil {
		return err
	}
	if err := normalizePlannerPredicateTimestamps(predicate.Right, ctx); err != nil {
		return err
	}
	if err := normalizePlannerPredicateTimestamps(predicate.Inner, ctx); err != nil {
		return err
	}
	return nil
}

func normalizePlannerValueExprTimestamps(expr *planner.ValueExpr, ctx *timestampNormalizationContext) error {
	if expr == nil {
		return nil
	}
	if err := normalizePlannerValueTimestamp(&expr.Value, ctx); err != nil {
		return err
	}
	if err := normalizePlannerValueExprTimestamps(expr.Left, ctx); err != nil {
		return err
	}
	if err := normalizePlannerValueExprTimestamps(expr.Right, ctx); err != nil {
		return err
	}
	if err := normalizePlannerValueExprTimestamps(expr.Arg, ctx); err != nil {
		return err
	}
	if err := normalizePlannerValueExprTimestamps(expr.Inner, ctx); err != nil {
		return err
	}
	return nil
}

func normalizePlannerValueTimestamp(value *planner.Value, ctx *timestampNormalizationContext) error {
	if value == nil || value.Kind != planner.ValueKindTimestampUnresolved {
		return nil
	}
	zoneID, err := ctx.defaultZoneID()
	if err != nil {
		return err
	}
	millis := resolveTimestampComponentsMillis(
		value.TimestampYear,
		value.TimestampMonth,
		value.TimestampDay,
		value.TimestampHour,
		value.TimestampMinute,
		value.TimestampSecond,
		ctx.location,
	)
	*value = planner.TimestampValue(millis, zoneID)
	return nil
}

func normalizeParserWhereTimestamps(where *parser.WhereClause, ctx *timestampNormalizationContext) error {
	if where == nil {
		return nil
	}
	for i := range where.Items {
		if err := normalizeParserValueTimestamp(&where.Items[i].Condition.Right, ctx); err != nil {
			return err
		}
		if err := normalizeParserValueExprTimestamps(where.Items[i].Condition.LeftExpr, ctx); err != nil {
			return err
		}
		if err := normalizeParserValueExprTimestamps(where.Items[i].Condition.RightExpr, ctx); err != nil {
			return err
		}
	}
	return nil
}

func normalizeParserPredicateTimestamps(predicate *parser.PredicateExpr, ctx *timestampNormalizationContext) error {
	if predicate == nil {
		return nil
	}
	if predicate.Comparison != nil {
		if err := normalizeParserValueTimestamp(&predicate.Comparison.Right, ctx); err != nil {
			return err
		}
		if err := normalizeParserValueExprTimestamps(predicate.Comparison.LeftExpr, ctx); err != nil {
			return err
		}
		if err := normalizeParserValueExprTimestamps(predicate.Comparison.RightExpr, ctx); err != nil {
			return err
		}
	}
	if err := normalizeParserPredicateTimestamps(predicate.Left, ctx); err != nil {
		return err
	}
	if err := normalizeParserPredicateTimestamps(predicate.Right, ctx); err != nil {
		return err
	}
	if err := normalizeParserPredicateTimestamps(predicate.Inner, ctx); err != nil {
		return err
	}
	return nil
}

func normalizeParserValueExprTimestamps(expr *parser.ValueExpr, ctx *timestampNormalizationContext) error {
	if expr == nil {
		return nil
	}
	if err := normalizeParserValueTimestamp(&expr.Value, ctx); err != nil {
		return err
	}
	if err := normalizeParserValueExprTimestamps(expr.Left, ctx); err != nil {
		return err
	}
	if err := normalizeParserValueExprTimestamps(expr.Right, ctx); err != nil {
		return err
	}
	if err := normalizeParserValueExprTimestamps(expr.Arg, ctx); err != nil {
		return err
	}
	if err := normalizeParserValueExprTimestamps(expr.Inner, ctx); err != nil {
		return err
	}
	return nil
}

func normalizeParserValueTimestamp(value *parser.Value, ctx *timestampNormalizationContext) error {
	if value == nil || value.Kind != parser.ValueKindTimestampUnresolved {
		return nil
	}
	zoneID, err := ctx.defaultZoneID()
	if err != nil {
		return err
	}
	millis := resolveTimestampComponentsMillis(
		value.TimestampYear,
		value.TimestampMonth,
		value.TimestampDay,
		value.TimestampHour,
		value.TimestampMinute,
		value.TimestampSecond,
		ctx.location,
	)
	*value = parser.TimestampValue(millis, zoneID)
	return nil
}

func resolveTimestampComponentsMillis(year, month, day, hour, minute, second int32, location *time.Location) int64 {
	if location == nil {
		location = time.UTC
	}
	return time.Date(
		int(year),
		time.Month(month),
		int(day),
		int(hour),
		int(minute),
		int(second),
		0,
		location,
	).UnixMilli()
}
