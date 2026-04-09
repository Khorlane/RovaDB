package storage

// ValueKind identifies a storage-owned scalar value kind.
type ValueKind int

const (
	ValueKindInvalid ValueKind = iota
	ValueKindNull
	ValueKindInt64
	ValueKindString
	ValueKindBool
	ValueKindReal
)

// Value is the storage/runtime-neutral scalar representation used by storage
// row and index codecs.
type Value struct {
	Kind ValueKind
	I64  int64
	Str  string
	Bool bool
	F64  float64
}

func NullValue() Value {
	return Value{Kind: ValueKindNull}
}

func Int64Value(v int64) Value {
	return Value{Kind: ValueKindInt64, I64: v}
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
