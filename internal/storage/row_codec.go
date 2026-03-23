package storage

import (
	"encoding/binary"
	"errors"

	"github.com/Khorlane/RovaDB/internal/parser"
)

const (
	rowTypeInt64  = 1
	rowTypeString = 2
)

var errInvalidRowData = errors.New("storage: invalid row data")

// EncodeRow encodes one row payload using the storage row format.
func EncodeRow(values []parser.Value) ([]byte, error) {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf[:2], uint16(len(values)))

	for _, value := range values {
		switch value.Kind {
		case parser.ValueKindInt64:
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
		default:
			return nil, errInvalidRowData
		}
	}

	return buf, nil
}

// DecodeRow decodes one row payload using the storage row format.
func DecodeRow(data []byte) ([]parser.Value, error) {
	if len(data) < 2 {
		return nil, errInvalidRowData
	}

	offset := 0
	count := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2

	values := make([]parser.Value, 0, count)
	for i := 0; i < count; i++ {
		if offset >= len(data) {
			return nil, errInvalidRowData
		}

		tag := data[offset]
		offset++

		switch tag {
		case rowTypeInt64:
			if offset+8 > len(data) {
				return nil, errInvalidRowData
			}
			value := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
			offset += 8
			values = append(values, parser.Int64Value(value))
		case rowTypeString:
			if offset+4 > len(data) {
				return nil, errInvalidRowData
			}
			length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
			offset += 4
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
