package storage

// ValueKind identifies a storage-owned scalar value kind.
type ValueKind int

const (
	ValueKindInvalid ValueKind = iota
	ValueKindNull
	ValueKindIntegerLiteral
	ValueKindSmallInt
	ValueKindInt
	ValueKindBigInt
	ValueKindString
	ValueKindBool
	ValueKindReal
	ValueKindDate
	ValueKindTime
	ValueKindTimestamp
)

// Value is the storage/runtime-neutral scalar representation used by storage
// row and index codecs.
type Value struct {
	Kind            ValueKind
	I16             int16
	I32             int32
	I64             int64
	DateDays        int32
	TimeSeconds     int32
	TimestampMillis int64
	TimestampZoneID int16
	Str             string
	Bool            bool
	F64             float64
}

func NullValue() Value {
	return Value{Kind: ValueKindNull}
}

func IntegerLiteralValue(v int64) Value {
	return Value{Kind: ValueKindIntegerLiteral, I64: v}
}

func SmallIntValue(v int16) Value {
	return Value{Kind: ValueKindSmallInt, I16: v}
}

func IntValue(v int32) Value {
	return Value{Kind: ValueKindInt, I32: v}
}

func BigIntValue(v int64) Value {
	return Value{Kind: ValueKindBigInt, I64: v}
}

func Int64Value(v int64) Value {
	return IntegerLiteralValue(v)
}

func StringValue(v string) Value {
	return Value{Kind: ValueKindString, Str: v}
}

func BoolValue(v bool) Value {
	return Value{Kind: ValueKindBool, Bool: v}
}

func RealValue(v float64) Value {
	return Value{Kind: ValueKindReal, F64: v}
}

func DateValue(daysSinceEpoch int32) Value {
	return Value{Kind: ValueKindDate, DateDays: daysSinceEpoch}
}

func TimeValue(secondsSinceMidnight int32) Value {
	return Value{Kind: ValueKindTime, TimeSeconds: secondsSinceMidnight}
}

func TimestampValue(millisecondsSinceEpoch int64, zoneID int16) Value {
	return Value{
		Kind:            ValueKindTimestamp,
		TimestampMillis: millisecondsSinceEpoch,
		TimestampZoneID: zoneID,
	}
}

func (v Value) IsInteger() bool {
	switch v.Kind {
	case ValueKindIntegerLiteral, ValueKindSmallInt, ValueKindInt, ValueKindBigInt:
		return true
	default:
		return false
	}
}

func (v Value) IntegerValue() int64 {
	switch v.Kind {
	case ValueKindSmallInt:
		return int64(v.I16)
	case ValueKindInt:
		return int64(v.I32)
	default:
		return v.I64
	}
}
