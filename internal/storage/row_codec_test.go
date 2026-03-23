package storage

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestEncodeDecodeRowInts(t *testing.T) {
	values := []parser.Value{
		parser.Int64Value(1),
		parser.Int64Value(-42),
	}

	encoded, err := EncodeRow(values)
	if err != nil {
		t.Fatalf("EncodeRow() error = %v", err)
	}

	decoded, err := DecodeRow(encoded)
	if err != nil {
		t.Fatalf("DecodeRow() error = %v", err)
	}

	assertRowValuesEqual(t, decoded, values)
}

func TestEncodeDecodeRowStrings(t *testing.T) {
	values := []parser.Value{
		parser.StringValue("hello"),
		parser.StringValue("rovadb"),
	}

	encoded, err := EncodeRow(values)
	if err != nil {
		t.Fatalf("EncodeRow() error = %v", err)
	}

	decoded, err := DecodeRow(encoded)
	if err != nil {
		t.Fatalf("DecodeRow() error = %v", err)
	}

	assertRowValuesEqual(t, decoded, values)
}

func TestEncodeDecodeRowMixed(t *testing.T) {
	values := []parser.Value{
		parser.Int64Value(7),
		parser.StringValue("alice"),
		parser.Int64Value(9),
	}

	encoded, err := EncodeRow(values)
	if err != nil {
		t.Fatalf("EncodeRow() error = %v", err)
	}

	decoded, err := DecodeRow(encoded)
	if err != nil {
		t.Fatalf("DecodeRow() error = %v", err)
	}

	assertRowValuesEqual(t, decoded, values)
}

func TestDecodeRowUnknownTag(t *testing.T) {
	data := []byte{1, 0, 99}

	if err := expectDecodeError(data); !errors.Is(err, errInvalidRowData) {
		t.Fatalf("DecodeRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestDecodeRowTruncatedInt(t *testing.T) {
	data := []byte{1, 0, rowTypeInt64, 1, 2, 3}

	if err := expectDecodeError(data); !errors.Is(err, errInvalidRowData) {
		t.Fatalf("DecodeRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestDecodeRowTruncatedStringLength(t *testing.T) {
	data := []byte{1, 0, rowTypeString, 3, 0}

	if err := expectDecodeError(data); !errors.Is(err, errInvalidRowData) {
		t.Fatalf("DecodeRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestDecodeRowTruncatedStringPayload(t *testing.T) {
	data := make([]byte, 0, 10)
	data = append(data, 1, 0, rowTypeString)
	var raw [4]byte
	binary.LittleEndian.PutUint32(raw[:], 5)
	data = append(data, raw[:]...)
	data = append(data, 'h', 'i')

	if err := expectDecodeError(data); !errors.Is(err, errInvalidRowData) {
		t.Fatalf("DecodeRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestDecodeRowTrailingJunk(t *testing.T) {
	encoded, err := EncodeRow([]parser.Value{parser.Int64Value(1)})
	if err != nil {
		t.Fatalf("EncodeRow() error = %v", err)
	}
	encoded = append(encoded, 0xff)

	if err := expectDecodeError(encoded); !errors.Is(err, errInvalidRowData) {
		t.Fatalf("DecodeRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func expectDecodeError(data []byte) error {
	_, err := DecodeRow(data)
	return err
}

func assertRowValuesEqual(t *testing.T, got, want []parser.Value) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("len(decoded) = %d, want %d", len(got), len(want))
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("decoded[%d] = %#v, want %#v", i, got[i], want[i])
		}
	}
}
