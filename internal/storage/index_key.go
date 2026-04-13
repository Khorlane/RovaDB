package storage

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
)

const (
	indexKeyTypeNull   = 0
	indexKeyTypeInt    = 1
	indexKeyTypeBool   = 2
	indexKeyTypeReal   = 3
	indexKeyTypeString = 4
)

func EncodeIndexKey(values []Value) ([]byte, error) {
	if len(values) > math.MaxUint16 {
		return nil, errCorruptedIndexPage
	}

	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(values)))

	for _, value := range values {
		switch value.Kind {
		case ValueKindNull:
			buf = append(buf, indexKeyTypeNull)
		case ValueKindIntegerLiteral, ValueKindSmallInt, ValueKindInt, ValueKindBigInt:
			integerValue := value.IntegerValue()
			if !publicIntInRange(integerValue) {
				return nil, errCorruptedIndexPage
			}
			var raw [4]byte
			buf = append(buf, indexKeyTypeInt)
			binary.LittleEndian.PutUint32(raw[:], uint32(int32(integerValue)))
			buf = append(buf, raw[:]...)
		case ValueKindBool:
			buf = append(buf, indexKeyTypeBool)
			if value.Bool {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case ValueKindReal:
			var raw [8]byte
			buf = append(buf, indexKeyTypeReal)
			binary.LittleEndian.PutUint64(raw[:], math.Float64bits(value.F64))
			buf = append(buf, raw[:]...)
		case ValueKindString:
			text := []byte(value.Str)
			if len(text) > math.MaxUint16 {
				return nil, errCorruptedIndexPage
			}
			var raw [2]byte
			buf = append(buf, indexKeyTypeString)
			binary.LittleEndian.PutUint16(raw[:], uint16(len(text)))
			buf = append(buf, raw[:]...)
			buf = append(buf, text...)
		default:
			return nil, errCorruptedIndexPage
		}
	}

	return buf, nil
}

func decodeIndexKey(data []byte) ([]Value, error) {
	if len(data) < 2 {
		return nil, errCorruptedIndexPage
	}

	offset := 0
	count := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2

	values := make([]Value, 0, count)
	for i := 0; i < count; i++ {
		if offset >= len(data) {
			return nil, errCorruptedIndexPage
		}

		tag := data[offset]
		offset++

		switch tag {
		case indexKeyTypeNull:
			values = append(values, NullValue())
		case indexKeyTypeInt:
			if offset+4 > len(data) {
				return nil, errCorruptedIndexPage
			}
			value := int64(int32(binary.LittleEndian.Uint32(data[offset : offset+4])))
			offset += 4
			values = append(values, IntegerLiteralValue(value))
		case indexKeyTypeBool:
			if offset >= len(data) {
				return nil, errCorruptedIndexPage
			}
			switch data[offset] {
			case 0:
				values = append(values, BoolValue(false))
			case 1:
				values = append(values, BoolValue(true))
			default:
				return nil, errCorruptedIndexPage
			}
			offset++
		case indexKeyTypeReal:
			if offset+8 > len(data) {
				return nil, errCorruptedIndexPage
			}
			values = append(values, RealValue(math.Float64frombits(binary.LittleEndian.Uint64(data[offset:offset+8]))))
			offset += 8
		case indexKeyTypeString:
			if offset+2 > len(data) {
				return nil, errCorruptedIndexPage
			}
			length := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
			offset += 2
			if offset+length > len(data) {
				return nil, errCorruptedIndexPage
			}
			values = append(values, StringValue(string(data[offset:offset+length])))
			offset += length
		default:
			return nil, errCorruptedIndexPage
		}
	}

	if offset != len(data) {
		return nil, errCorruptedIndexPage
	}
	return values, nil
}

func CompareIndexKeys(left, right []byte) (int, error) {
	leftValues, err := decodeIndexKey(left)
	leftValid := err == nil
	rightValues, err := decodeIndexKey(right)
	rightValid := err == nil

	if !leftValid || !rightValid {
		return bytes.Compare(left, right), nil
	}

	limit := len(leftValues)
	if len(rightValues) < limit {
		limit = len(rightValues)
	}
	for i := 0; i < limit; i++ {
		if cmp := compareIndexKeyValue(leftValues[i], rightValues[i]); cmp != 0 {
			return cmp, nil
		}
	}
	switch {
	case len(leftValues) < len(rightValues):
		return -1, nil
	case len(leftValues) > len(rightValues):
		return 1, nil
	default:
		return 0, nil
	}
}

func compareIndexKeyValue(left, right Value) int {
	if left.Kind != right.Kind {
		switch {
		case left.Kind < right.Kind:
			return -1
		case left.Kind > right.Kind:
			return 1
		default:
			return 0
		}
	}

	switch left.Kind {
	case ValueKindNull:
		return 0
	case ValueKindIntegerLiteral, ValueKindSmallInt, ValueKindInt, ValueKindBigInt:
		switch {
		case left.IntegerValue() < right.IntegerValue():
			return -1
		case left.IntegerValue() > right.IntegerValue():
			return 1
		default:
			return 0
		}
	case ValueKindBool:
		switch {
		case !left.Bool && right.Bool:
			return -1
		case left.Bool && !right.Bool:
			return 1
		default:
			return 0
		}
	case ValueKindReal:
		switch {
		case left.F64 < right.F64:
			return -1
		case left.F64 > right.F64:
			return 1
		default:
			return 0
		}
	case ValueKindString:
		return strings.Compare(left.Str, right.Str)
	default:
		return 0
	}
}
