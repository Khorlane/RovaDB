package parser

import (
	"strconv"
	"strings"
)

// ValueKind identifies the stored value type.
type ValueKind int

const (
	ValueKindInvalid ValueKind = iota
	ValueKindNull
	ValueKindInt64
	ValueKindString
	ValueKindBool
	ValueKindReal
	ValueKindPlaceholder
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
	Kind             ValueKind
	I64              int64
	Str              string
	Bool             bool
	F64              float64
	PlaceholderIndex int
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

// BoolValue builds a bool Value.
func BoolValue(v bool) Value {
	return Value{Kind: ValueKindBool, Bool: v}
}

// RealValue builds a float64 Value.
func RealValue(v float64) Value {
	return Value{Kind: ValueKindReal, F64: v}
}

// PlaceholderValue builds a positional placeholder Value.
func PlaceholderValue() Value {
	return Value{Kind: ValueKindPlaceholder, PlaceholderIndex: -1}
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
	case ValueKindBool:
		return v.Bool
	case ValueKindReal:
		return v.F64
	case ValueKindPlaceholder:
		return nil
	default:
		return nil
	}
}

func parseRealLiteral(token string) (float64, bool) {
	if strings.HasPrefix(token, "+") {
		return 0, false
	}
	if token == "" {
		return 0, false
	}

	start := 0
	if token[0] == '-' {
		start = 1
		if len(token) < 4 {
			return 0, false
		}
	}

	dotIndex := -1
	for i := start; i < len(token); i++ {
		ch := token[i]
		if ch == '.' {
			if dotIndex >= 0 {
				return 0, false
			}
			dotIndex = i
			continue
		}
		if ch < '0' || ch > '9' {
			return 0, false
		}
	}

	if dotIndex <= start || dotIndex == len(token)-1 {
		return 0, false
	}

	value, err := strconv.ParseFloat(token, 64)
	if err != nil {
		return 0, false
	}

	return value, true
}
