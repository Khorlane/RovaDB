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

func TestEncodeRowBoolTrue(t *testing.T) {
	encoded, err := EncodeRow([]parser.Value{parser.BoolValue(true)})
	if err != nil {
		t.Fatalf("EncodeRow() error = %v", err)
	}

	want := []byte{1, 0, rowTypeBool, 1}
	if len(encoded) != len(want) {
		t.Fatalf("len(encoded) = %d, want %d", len(encoded), len(want))
	}
	for i := range want {
		if encoded[i] != want[i] {
			t.Fatalf("encoded[%d] = %d, want %d", i, encoded[i], want[i])
		}
	}
}

func TestEncodeRowBoolFalse(t *testing.T) {
	encoded, err := EncodeRow([]parser.Value{parser.BoolValue(false)})
	if err != nil {
		t.Fatalf("EncodeRow() error = %v", err)
	}

	want := []byte{1, 0, rowTypeBool, 0}
	if len(encoded) != len(want) {
		t.Fatalf("len(encoded) = %d, want %d", len(encoded), len(want))
	}
	for i := range want {
		if encoded[i] != want[i] {
			t.Fatalf("encoded[%d] = %d, want %d", i, encoded[i], want[i])
		}
	}
}

func TestDecodeRowBoolTrue(t *testing.T) {
	decoded, err := DecodeRow([]byte{1, 0, rowTypeBool, 1})
	if err != nil {
		t.Fatalf("DecodeRow() error = %v", err)
	}

	assertRowValuesEqual(t, decoded, []parser.Value{parser.BoolValue(true)})
}

func TestDecodeRowBoolFalse(t *testing.T) {
	decoded, err := DecodeRow([]byte{1, 0, rowTypeBool, 0})
	if err != nil {
		t.Fatalf("DecodeRow() error = %v", err)
	}

	assertRowValuesEqual(t, decoded, []parser.Value{parser.BoolValue(false)})
}

func TestEncodeDecodeRowWithNull(t *testing.T) {
	values := []parser.Value{
		parser.Int64Value(7),
		parser.NullValue(),
		parser.StringValue("alice"),
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

func TestEncodeDecodeRowWithNullAndBool(t *testing.T) {
	values := []parser.Value{
		parser.BoolValue(true),
		parser.NullValue(),
		parser.BoolValue(false),
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

func TestEncodeDecodeRowMixedWithBool(t *testing.T) {
	values := []parser.Value{
		parser.Int64Value(7),
		parser.StringValue("alice"),
		parser.BoolValue(true),
		parser.BoolValue(false),
		parser.NullValue(),
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

func TestEncodeDecodeRowExistingIntStringRegression(t *testing.T) {
	values := []parser.Value{
		parser.Int64Value(1),
		parser.StringValue("legacy"),
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

func TestDecodeRowTruncatedBool(t *testing.T) {
	data := []byte{1, 0, rowTypeBool}

	if err := expectDecodeError(data); !errors.Is(err, errInvalidRowData) {
		t.Fatalf("DecodeRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestDecodeRowInvalidBoolPayload(t *testing.T) {
	data := []byte{1, 0, rowTypeBool, 2}

	if err := expectDecodeError(data); !errors.Is(err, errInvalidRowData) {
		t.Fatalf("DecodeRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestBoolEncodingIsDistinctFromIntAndString(t *testing.T) {
	boolEncoded, err := EncodeRow([]parser.Value{parser.BoolValue(true)})
	if err != nil {
		t.Fatalf("EncodeRow(bool) error = %v", err)
	}
	intEncoded, err := EncodeRow([]parser.Value{parser.Int64Value(1)})
	if err != nil {
		t.Fatalf("EncodeRow(int) error = %v", err)
	}
	stringEncoded, err := EncodeRow([]parser.Value{parser.StringValue("true")})
	if err != nil {
		t.Fatalf("EncodeRow(string) error = %v", err)
	}

	if binary.LittleEndian.Uint16(boolEncoded[:2]) != 1 {
		t.Fatalf("bool row count header = %d, want 1", binary.LittleEndian.Uint16(boolEncoded[:2]))
	}
	if boolEncoded[2] != rowTypeBool {
		t.Fatalf("bool tag = %d, want %d", boolEncoded[2], rowTypeBool)
	}
	if boolEncoded[2] == intEncoded[2] || boolEncoded[2] == stringEncoded[2] {
		t.Fatalf("bool tag %d must differ from int tag %d and string tag %d", boolEncoded[2], intEncoded[2], stringEncoded[2])
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
