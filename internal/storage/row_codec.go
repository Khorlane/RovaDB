package storage

import (
	"encoding/binary"
	"math"
)

const (
	rowTypeNull   = 0
	rowTypeInt64  = 1
	rowTypeString = 2
	rowTypeBool   = 3
	rowTypeReal   = 4
)

// EncodeRow encodes one row payload using the current row storage format.

func EncodeRow(values []Value) ([]byte, error) {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf[:2], uint16(len(values)))

	for _, value := range values {
		switch value.Kind {
		case ValueKindNull:
			buf = append(buf, rowTypeNull)
		case ValueKindIntegerLiteral, ValueKindSmallInt, ValueKindInt, ValueKindBigInt:
			integerValue := value.IntegerValue()
			if !publicIntInRange(integerValue) {
				return nil, errCorruptedRowData
			}
			var raw [8]byte
			buf = append(buf, rowTypeInt64)
			binary.LittleEndian.PutUint64(raw[:], uint64(integerValue))
			buf = append(buf, raw[:]...)
		case ValueKindString:
			var raw [4]byte
			buf = append(buf, rowTypeString)
			binary.LittleEndian.PutUint32(raw[:], uint32(len(value.Str)))
			buf = append(buf, raw[:]...)
			buf = append(buf, value.Str...)
		case ValueKindBool:
			buf = append(buf, rowTypeBool)
			if value.Bool {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case ValueKindReal:
			var raw [8]byte
			buf = append(buf, rowTypeReal)
			binary.LittleEndian.PutUint64(raw[:], math.Float64bits(value.F64))
			buf = append(buf, raw[:]...)
		default:
			return nil, errCorruptedRowData
		}
	}

	return buf, nil
}

// DecodeRow decodes one row payload using the current row storage format.
func DecodeRow(data []byte) ([]Value, error) {
	if len(data) < 2 {
		return nil, errCorruptedRowData
	}

	offset := 0
	count := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2

	values := make([]Value, 0, count)
	for i := 0; i < count; i++ {
		if offset >= len(data) {
			return nil, errCorruptedRowData
		}

		tag := data[offset]
		offset++

		switch tag {
		case rowTypeNull:
			values = append(values, NullValue())
		case rowTypeInt64:
			if offset+8 > len(data) {
				return nil, errCorruptedRowData
			}
			value := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
			offset += 8
			if !publicIntInRange(value) {
				return nil, errCorruptedRowData
			}
			values = append(values, IntegerLiteralValue(value))
		case rowTypeString:
			if offset+4 > len(data) {
				return nil, errCorruptedRowData
			}
			length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
			offset += 4
			if offset+length > len(data) {
				return nil, errCorruptedRowData
			}
			values = append(values, StringValue(string(data[offset:offset+length])))
			offset += length
		case rowTypeBool:
			if offset >= len(data) {
				return nil, errCorruptedRowData
			}
			switch data[offset] {
			case 0:
				values = append(values, BoolValue(false))
			case 1:
				values = append(values, BoolValue(true))
			default:
				return nil, errCorruptedRowData
			}
			offset++
		case rowTypeReal:
			if offset+8 > len(data) {
				return nil, errCorruptedRowData
			}
			value := math.Float64frombits(binary.LittleEndian.Uint64(data[offset : offset+8]))
			offset += 8
			values = append(values, RealValue(value))
		default:
			return nil, errCorruptedRowData
		}
	}

	if offset != len(data) {
		return nil, errCorruptedRowData
	}

	return values, nil
}

// EncodeSlottedRow encodes one row payload using the slotted-page row format.
func EncodeSlottedRow(values []Value, columnTypes []uint8) ([]byte, error) {
	columnCount := len(values)
	if columnCount != len(columnTypes) {
		return nil, errInvalidRowData
	}
	nullBitmapByteCount := (columnCount + 7) / 8

	buf := make([]byte, 4+nullBitmapByteCount)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(columnCount))
	binary.LittleEndian.PutUint16(buf[2:4], uint16(nullBitmapByteCount))

	for i, value := range values {
		if value.Kind == ValueKindNull {
			buf[4+i/8] |= 1 << (i % 8)
			continue
		}

		switch columnTypes[i] {
		case CatalogColumnTypeSmallInt:
			if value.Kind != ValueKindSmallInt {
				return nil, errInvalidRowData
			}
			var raw [2]byte
			binary.LittleEndian.PutUint16(raw[:], uint16(value.I16))
			buf = append(buf, raw[:]...)
		case CatalogColumnTypeInt:
			if value.Kind != ValueKindInt {
				return nil, errInvalidRowData
			}
			var raw [4]byte
			binary.LittleEndian.PutUint32(raw[:], uint32(value.I32))
			buf = append(buf, raw[:]...)
		case CatalogColumnTypeBigInt:
			if value.Kind != ValueKindBigInt {
				return nil, errInvalidRowData
			}
			var raw [8]byte
			binary.LittleEndian.PutUint64(raw[:], uint64(value.I64))
			buf = append(buf, raw[:]...)
		case CatalogColumnTypeBool:
			if value.Kind != ValueKindBool {
				return nil, errInvalidRowData
			}
			if value.Bool {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case CatalogColumnTypeReal:
			if value.Kind != ValueKindReal {
				return nil, errInvalidRowData
			}
			var raw [8]byte
			binary.LittleEndian.PutUint64(raw[:], math.Float64bits(value.F64))
			buf = append(buf, raw[:]...)
		case CatalogColumnTypeText:
			if value.Kind != ValueKindString {
				return nil, errInvalidRowData
			}
			text := []byte(value.Str)
			if len(text) > math.MaxUint16 {
				return nil, errInvalidRowData
			}
			var raw [2]byte
			binary.LittleEndian.PutUint16(raw[:], uint16(len(text)))
			buf = append(buf, raw[:]...)
			buf = append(buf, text...)
		default:
			return nil, errInvalidRowData
		}
	}

	return buf, nil
}

// DecodeSlottedRow decodes one slotted-page row payload using the expected
// storage-side column types.
func DecodeSlottedRow(data []byte, columnTypes []uint8) ([]Value, error) {
	if len(data) < 4 {
		return nil, errInvalidRowData
	}

	offset := 0
	columnCount := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2
	nullBitmapByteCount := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2

	if columnCount != len(columnTypes) {
		return nil, errInvalidRowData
	}
	expectedNullBitmapByteCount := (columnCount + 7) / 8
	if nullBitmapByteCount != expectedNullBitmapByteCount || offset+nullBitmapByteCount > len(data) {
		return nil, errInvalidRowData
	}

	nullBitmap := data[offset : offset+nullBitmapByteCount]
	offset += nullBitmapByteCount

	values := make([]Value, 0, columnCount)
	for i, columnType := range columnTypes {
		if nullBitmap[i/8]&(1<<(i%8)) != 0 {
			values = append(values, NullValue())
			continue
		}

		switch columnType {
		case CatalogColumnTypeSmallInt:
			if offset+2 > len(data) {
				return nil, errInvalidRowData
			}
			value := int16(binary.LittleEndian.Uint16(data[offset : offset+2]))
			offset += 2
			values = append(values, SmallIntValue(value))
		case CatalogColumnTypeInt:
			if offset+4 > len(data) {
				return nil, errInvalidRowData
			}
			value := int32(binary.LittleEndian.Uint32(data[offset : offset+4]))
			offset += 4
			values = append(values, IntValue(value))
		case CatalogColumnTypeBigInt:
			if offset+8 > len(data) {
				return nil, errInvalidRowData
			}
			value := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
			offset += 8
			values = append(values, BigIntValue(value))
		case CatalogColumnTypeBool:
			if offset >= len(data) {
				return nil, errInvalidRowData
			}
			switch data[offset] {
			case 0:
				values = append(values, BoolValue(false))
			case 1:
				values = append(values, BoolValue(true))
			default:
				return nil, errInvalidRowData
			}
			offset++
		case CatalogColumnTypeReal:
			if offset+8 > len(data) {
				return nil, errInvalidRowData
			}
			value := math.Float64frombits(binary.LittleEndian.Uint64(data[offset : offset+8]))
			offset += 8
			values = append(values, RealValue(value))
		case CatalogColumnTypeText:
			if offset+2 > len(data) {
				return nil, errInvalidRowData
			}
			length := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
			offset += 2
			if offset+length > len(data) {
				return nil, errInvalidRowData
			}
			values = append(values, StringValue(string(data[offset:offset+length])))
			offset += length
		default:
			return nil, errInvalidRowData
		}
	}

	if offset != len(data) {
		return nil, errInvalidRowData
	}
	return values, nil
}

func publicIntInRange(v int64) bool {
	return v >= math.MinInt32 && v <= math.MaxInt32
}
