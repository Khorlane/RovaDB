package rovadb

import (
	"github.com/Khorlane/RovaDB/internal/parser"
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
