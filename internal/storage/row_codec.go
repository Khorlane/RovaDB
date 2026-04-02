package storage

import (
	"encoding/binary"
	"math"

	"github.com/Khorlane/RovaDB/internal/parser"
)

const (
	rowTypeNull   = 0
	rowTypeInt64  = 1
	rowTypeString = 2
	rowTypeBool   = 3
	rowTypeReal   = 4
)

// EncodeRow encodes one row payload using the current row storage format.

func EncodeRow(values []parser.Value) ([]byte, error) {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf[:2], uint16(len(values)))

	for _, value := range values {
		switch value.Kind {
		case parser.ValueKindNull:
			buf = append(buf, rowTypeNull)
		case parser.ValueKindInt64:
			if !parser.PublicIntInRange(value.I64) {
				return nil, errCorruptedRowData
			}
			var raw [8]byte
			buf = append(buf, rowTypeInt64)
			binary.LittleEndian.PutUint64(raw[:], uint64(value.I64))
			buf = append(buf, raw[:]...)
		case parser.ValueKindString:
			var raw [4]byte
			buf = append(buf, rowTypeString)
			binary.LittleEndian.PutUint32(raw[:], uint32(len(value.Str)))
			buf = append(buf, raw[:]...)
			buf = append(buf, value.Str...)
		case parser.ValueKindBool:
			buf = append(buf, rowTypeBool)
			if value.Bool {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case parser.ValueKindReal:
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
func DecodeRow(data []byte) ([]parser.Value, error) {
	if len(data) < 2 {
		return nil, errCorruptedRowData
	}

	offset := 0
	count := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2

	values := make([]parser.Value, 0, count)
	for i := 0; i < count; i++ {
		if offset >= len(data) {
			return nil, errCorruptedRowData
		}

		tag := data[offset]
		offset++

		switch tag {
		case rowTypeNull:
			values = append(values, parser.NullValue())
		case rowTypeInt64:
			if offset+8 > len(data) {
				return nil, errCorruptedRowData
			}
			value := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
			if !parser.PublicIntInRange(value) {
				return nil, errCorruptedRowData
			}
			offset += 8
			values = append(values, parser.Int64Value(value))
		case rowTypeString:
			if offset+4 > len(data) {
				return nil, errCorruptedRowData
			}
			length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
			offset += 4
			if offset+length > len(data) {
				return nil, errCorruptedRowData
			}
			values = append(values, parser.StringValue(string(data[offset:offset+length])))
			offset += length
		case rowTypeBool:
			if offset >= len(data) {
				return nil, errCorruptedRowData
			}
			switch data[offset] {
			case 0:
				values = append(values, parser.BoolValue(false))
			case 1:
				values = append(values, parser.BoolValue(true))
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
			values = append(values, parser.RealValue(value))
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
func EncodeSlottedRow(values []parser.Value) ([]byte, error) {
	columnCount := len(values)
	nullBitmapByteCount := (columnCount + 7) / 8

	buf := make([]byte, 4+nullBitmapByteCount)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(columnCount))
	binary.LittleEndian.PutUint16(buf[2:4], uint16(nullBitmapByteCount))

	for i, value := range values {
		if value.Kind == parser.ValueKindNull {
			buf[4+i/8] |= 1 << (i % 8)
			continue
		}

		switch value.Kind {
		case parser.ValueKindInt64:
			if !parser.PublicIntInRange(value.I64) {
				return nil, errInvalidRowData
			}
			var raw [4]byte
			binary.LittleEndian.PutUint32(raw[:], uint32(int32(value.I64)))
			buf = append(buf, raw[:]...)
		case parser.ValueKindBool:
			if value.Bool {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case parser.ValueKindReal:
			var raw [8]byte
			binary.LittleEndian.PutUint64(raw[:], math.Float64bits(value.F64))
			buf = append(buf, raw[:]...)
		case parser.ValueKindString:
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
func DecodeSlottedRow(data []byte, columnTypes []uint8) ([]parser.Value, error) {
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

	values := make([]parser.Value, 0, columnCount)
	for i, columnType := range columnTypes {
		if nullBitmap[i/8]&(1<<(i%8)) != 0 {
			values = append(values, parser.NullValue())
			continue
		}

		switch columnType {
		case CatalogColumnTypeInt:
			if offset+4 > len(data) {
				return nil, errInvalidRowData
			}
			value := int64(int32(binary.LittleEndian.Uint32(data[offset : offset+4])))
			offset += 4
			values = append(values, parser.Int64Value(value))
		case CatalogColumnTypeBool:
			if offset >= len(data) {
				return nil, errInvalidRowData
			}
			switch data[offset] {
			case 0:
				values = append(values, parser.BoolValue(false))
			case 1:
				values = append(values, parser.BoolValue(true))
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
			values = append(values, parser.RealValue(value))
		case CatalogColumnTypeText:
			if offset+2 > len(data) {
				return nil, errInvalidRowData
			}
			length := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
			offset += 2
			if offset+length > len(data) {
				return nil, errInvalidRowData
			}
			values = append(values, parser.StringValue(string(data[offset:offset+length])))
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
