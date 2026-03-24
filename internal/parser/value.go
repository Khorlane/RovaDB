package parser

// ValueKind identifies the stored value type.
type ValueKind int

const (
	ValueKindInvalid ValueKind = iota
	ValueKindNull
	ValueKindInt64
	ValueKindString
	ValueKindBool
)

/*
--- BOOL DESIGN (LOCKED) ---

Schema type:
- Name: BOOL

Runtime value:
- New value kind: Bool
- Go type: bool
- NULL remains separate (existing nil handling unchanged)

Literal forms:
- TRUE, FALSE (case-insensitive if parser already supports it)
- Quoted 'true'/'false' remain TEXT
- No numeric coercion (0/1 are INT, not BOOL)

Type enforcement:
- BOOL columns accept: TRUE, FALSE, NULL
- Reject: INT (0/1), TEXT ('true', etc.)

Storage encoding:
- Introduce a new value kind tag for BOOL
- Encoding:
    TRUE  -> BOOL tag + 1 byte (1)
    FALSE -> BOOL tag + 1 byte (0)
- Must NOT reuse INT or TEXT encoding
- Must remain backward-compatible with existing rows

Comparison semantics:
- TRUE == TRUE only
- FALSE == FALSE only
- No cross-type equality with INT/TEXT
*/

// Value is the tiny internal value representation for Stage 1 literals.
type Value struct {
	Kind ValueKind
	I64  int64
	Str  string
}

// NullValue builds a NULL Value.
func NullValue() Value {
	return Value{Kind: ValueKindNull}
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
	case ValueKindNull:
		return nil
	case ValueKindInt64:
		return v.I64
	case ValueKindString:
		return v.Str
	default:
		return nil
	}
}
