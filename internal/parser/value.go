package parser

import (
	"strconv"
	"strings"
	"time"
)

// ValueKind identifies the stored value type.
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
	ValueKindTimestampUnresolved
	ValueKindPlaceholder
)

// BoundIntegerType records the original Go integer type for placeholder-bound
// public arguments so write-path validation can require exact integer widths
// without changing engine-owned SQL literal behavior.
type BoundIntegerType int

const (
	BoundIntegerTypeNone BoundIntegerType = iota
	BoundIntegerTypeInt16
	BoundIntegerTypeInt32
	BoundIntegerTypeInt64
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
	I16              int16
	I32              int32
	I64              int64
	DateDays         int32
	TimeSeconds      int32
	TimestampMillis  int64
	TimestampZoneID  int16
	TimestampYear    int32
	TimestampMonth   int32
	TimestampDay     int32
	TimestampHour    int32
	TimestampMinute  int32
	TimestampSecond  int32
	Str              string
	Bool             bool
	F64              float64
	BoundIntegerType BoundIntegerType
	PlaceholderIndex int
}

// NullValue builds a NULL Value.
func NullValue() Value {
	return Value{Kind: ValueKindNull}
}

// IntegerLiteralValue builds an untyped integer literal value.
func IntegerLiteralValue(v int64) Value {
	return Value{Kind: ValueKindIntegerLiteral, I64: v}
}

func boundIntegerValue(v int64, boundType BoundIntegerType) Value {
	value := IntegerLiteralValue(v)
	switch boundType {
	case BoundIntegerTypeInt16:
		value = SmallIntValue(int16(v))
	case BoundIntegerTypeInt32:
		value = IntValue(int32(v))
	case BoundIntegerTypeInt64:
		value = BigIntValue(v)
	}
	value.BoundIntegerType = boundType
	return value
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

// Int64Value builds an untyped integer literal value.
func Int64Value(v int64) Value {
	return IntegerLiteralValue(v)
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

func TimestampUnresolvedValue(year, month, day, hour, minute, second int) Value {
	return Value{
		Kind:            ValueKindTimestampUnresolved,
		TimestampYear:   int32(year),
		TimestampMonth:  int32(month),
		TimestampDay:    int32(day),
		TimestampHour:   int32(hour),
		TimestampMinute: int32(minute),
		TimestampSecond: int32(second),
	}
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
	case ValueKindIntegerLiteral:
		return v.I64
	case ValueKindSmallInt:
		return v.I16
	case ValueKindInt:
		return v.I32
	case ValueKindBigInt:
		return v.I64
	case ValueKindString:
		return v.Str
	case ValueKindBool:
		return v.Bool
	case ValueKindReal:
		return v.F64
	case ValueKindDate:
		return v.DateDays
	case ValueKindTime:
		return v.TimeSeconds
	case ValueKindTimestamp:
		return v.TimestampMillis
	case ValueKindTimestampUnresolved:
		return nil
	case ValueKindPlaceholder:
		return nil
	default:
		return nil
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

func (v Value) IsIntegerLiteral() bool {
	return v.Kind == ValueKindIntegerLiteral
}

func (v Value) IsTypedInteger() bool {
	switch v.Kind {
	case ValueKindSmallInt, ValueKindInt, ValueKindBigInt:
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

func parseInt64Literal(token string) (int64, error) {
	return strconv.ParseInt(token, 10, 64)
}

type temporalLiteralStatus int

const (
	temporalLiteralNotRecognized temporalLiteralStatus = iota
	temporalLiteralRecognized
	temporalLiteralInvalid
)

func parseStringLiteralPayload(payload string) (Value, error) {
	value, status := parseTemporalLiteralPayload(payload)
	switch status {
	case temporalLiteralRecognized:
		return value, nil
	case temporalLiteralInvalid:
		return Value{}, newParseError("unsupported query form")
	default:
		return StringValue(payload), nil
	}
}

func parseTemporalLiteralPayload(payload string) (Value, temporalLiteralStatus) {
	if strings.TrimSpace(payload) != payload {
		trimmed := strings.TrimSpace(payload)
		if looksTemporalLiteral(trimmed) || looksTemporalNearMiss(trimmed) {
			return Value{}, temporalLiteralInvalid
		}
		return Value{}, temporalLiteralNotRecognized
	}

	switch {
	case isCanonicalTimestampLiteral(payload):
		value, ok := parseCanonicalTimestampLiteral(payload)
		if !ok {
			return Value{}, temporalLiteralInvalid
		}
		return value, temporalLiteralRecognized
	case isCanonicalDateLiteral(payload):
		value, ok := parseCanonicalDateLiteral(payload)
		if !ok {
			return Value{}, temporalLiteralInvalid
		}
		return value, temporalLiteralRecognized
	case isCanonicalTimeLiteral(payload):
		value, ok := parseCanonicalTimeLiteral(payload)
		if !ok {
			return Value{}, temporalLiteralInvalid
		}
		return value, temporalLiteralRecognized
	case looksTemporalNearMiss(payload):
		return Value{}, temporalLiteralInvalid
	default:
		return Value{}, temporalLiteralNotRecognized
	}
}

func looksTemporalLiteral(payload string) bool {
	return isCanonicalDateLiteral(payload) || isCanonicalTimeLiteral(payload) || isCanonicalTimestampLiteral(payload)
}

func looksTemporalNearMiss(payload string) bool {
	switch {
	case isDateNearMiss(payload):
		return true
	case isTimeNearMiss(payload):
		return true
	case isTimestampNearMiss(payload):
		return true
	default:
		return false
	}
}

func isCanonicalDateLiteral(payload string) bool {
	return len(payload) == len("2006-01-02") &&
		isAllDigits(payload[0:4]) &&
		payload[4] == '-' &&
		isAllDigits(payload[5:7]) &&
		payload[7] == '-' &&
		isAllDigits(payload[8:10])
}

func isCanonicalTimeLiteral(payload string) bool {
	return len(payload) == len("15:04:05") &&
		isAllDigits(payload[0:2]) &&
		payload[2] == ':' &&
		isAllDigits(payload[3:5]) &&
		payload[5] == ':' &&
		isAllDigits(payload[6:8])
}

func isCanonicalTimestampLiteral(payload string) bool {
	return len(payload) == len("2006-01-02 15:04:05") &&
		isCanonicalDateLiteral(payload[:10]) &&
		payload[10] == ' ' &&
		isCanonicalTimeLiteral(payload[11:])
}

func parseCanonicalDateLiteral(payload string) (Value, bool) {
	year, ok := parseFixedWidthDigits(payload[0:4])
	if !ok {
		return Value{}, false
	}
	month, ok := parseFixedWidthDigits(payload[5:7])
	if !ok {
		return Value{}, false
	}
	day, ok := parseFixedWidthDigits(payload[8:10])
	if !ok {
		return Value{}, false
	}
	if !validCalendarDate(year, month, day) {
		return Value{}, false
	}

	ts := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	return DateValue(int32(ts.Unix() / secondsPerDayUnix)), true
}

func parseCanonicalTimeLiteral(payload string) (Value, bool) {
	hour, ok := parseFixedWidthDigits(payload[0:2])
	if !ok {
		return Value{}, false
	}
	minute, ok := parseFixedWidthDigits(payload[3:5])
	if !ok {
		return Value{}, false
	}
	second, ok := parseFixedWidthDigits(payload[6:8])
	if !ok {
		return Value{}, false
	}
	if hour < 0 || hour >= 24 || minute < 0 || minute >= 60 || second < 0 || second >= 60 {
		return Value{}, false
	}
	return TimeValue(int32(hour*3600 + minute*60 + second)), true
}

func parseCanonicalTimestampLiteral(payload string) (Value, bool) {
	year, ok := parseFixedWidthDigits(payload[0:4])
	if !ok {
		return Value{}, false
	}
	month, ok := parseFixedWidthDigits(payload[5:7])
	if !ok {
		return Value{}, false
	}
	day, ok := parseFixedWidthDigits(payload[8:10])
	if !ok {
		return Value{}, false
	}
	hour, ok := parseFixedWidthDigits(payload[11:13])
	if !ok {
		return Value{}, false
	}
	minute, ok := parseFixedWidthDigits(payload[14:16])
	if !ok {
		return Value{}, false
	}
	second, ok := parseFixedWidthDigits(payload[17:19])
	if !ok {
		return Value{}, false
	}
	if !validCalendarDate(year, month, day) {
		return Value{}, false
	}
	if hour < 0 || hour >= 24 || minute < 0 || minute >= 60 || second < 0 || second >= 60 {
		return Value{}, false
	}
	return TimestampUnresolvedValue(year, month, day, hour, minute, second), true
}

func parseFixedWidthDigits(value string) (int, bool) {
	if !isAllDigits(value) {
		return 0, false
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, false
	}
	return n, true
}

func isAllDigits(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func validCalendarDate(year, month, day int) bool {
	if month < 1 || month > 12 || day < 1 || day > 31 {
		return false
	}
	ts := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	return ts.Year() == year && int(ts.Month()) == month && ts.Day() == day
}

func isDateNearMiss(payload string) bool {
	if strings.ContainsAny(payload, ":Tt ") {
		return false
	}
	if strings.Count(payload, "-")+strings.Count(payload, "/") != 2 {
		return false
	}
	return splitTemporalNumericParts(payload, '-', '/') == 3
}

func isTimeNearMiss(payload string) bool {
	if strings.ContainsAny(payload, "-/Tt ") {
		return false
	}
	if strings.Count(payload, ":") != 2 {
		return false
	}
	return splitTemporalNumericParts(payload, ':') == 3
}

func isTimestampNearMiss(payload string) bool {
	if strings.Contains(strings.ToUpper(payload), "AM") || strings.Contains(strings.ToUpper(payload), "PM") {
		fields := strings.Fields(payload)
		if len(fields) >= 2 && isDateNearMiss(fields[0]) {
			return true
		}
	}

	for _, sep := range []string{" ", "T", "t"} {
		parts := strings.Split(payload, sep)
		if len(parts) != 2 {
			continue
		}
		if (isCanonicalDateLiteral(parts[0]) || isDateNearMiss(parts[0])) &&
			(isCanonicalTimeLiteral(parts[1]) || isTimeNearMiss(parts[1])) {
			return true
		}
	}

	return false
}

func splitTemporalNumericParts(payload string, separators ...byte) int {
	parts := make([]string, 0, 3)
	start := 0
	for i := 0; i < len(payload); i++ {
		for _, sep := range separators {
			if payload[i] != sep {
				continue
			}
			part := payload[start:i]
			if !isAllDigits(part) {
				return 0
			}
			parts = append(parts, part)
			start = i + 1
			goto nextCharacter
		}
		if payload[i] < '0' || payload[i] > '9' {
			return 0
		}
	nextCharacter:
	}
	last := payload[start:]
	if !isAllDigits(last) {
		return 0
	}
	parts = append(parts, last)
	return len(parts)
}

const (
	secondsPerDayUnix = 24 * 60 * 60
)
