package parser

// ValueKind identifies the stored value type.
type ValueKind int

const (
	ValueKindInvalid ValueKind = iota
	ValueKindInt64
	ValueKindString
)

// Value is the tiny internal value representation for Stage 1 literals.
type Value struct {
	Kind ValueKind
	I64  int64
	Str  string
}

// Int64Value builds an int64 Value.
func Int64Value(v int64) Value {
	return Value{Kind: ValueKindInt64, I64: v}
}

// StringValue builds a string Value.
func StringValue(v string) Value {
	return Value{Kind: ValueKindString, Str: v}
}

// Any converts the internal value to its Go representation.
func (v Value) Any() any {
	switch v.Kind {
	case ValueKindInt64:
		return v.I64
	case ValueKindString:
		return v.Str
	default:
		return nil
	}
}
