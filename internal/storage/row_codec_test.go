package storage

import (
	"encoding/binary"
	"errors"
	"math"
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

func TestEncodeRowReal(t *testing.T) {
	encoded, err := EncodeRow([]parser.Value{parser.RealValue(3.14)})
	if err != nil {
		t.Fatalf("EncodeRow() error = %v", err)
	}

	if len(encoded) != 11 {
		t.Fatalf("len(encoded) = %d, want 11", len(encoded))
	}
	if encoded[2] != rowTypeReal {
		t.Fatalf("encoded tag = %d, want %d", encoded[2], rowTypeReal)
	}
}

func TestDecodeRowReal(t *testing.T) {
	encoded, err := EncodeRow([]parser.Value{parser.RealValue(3.14)})
	if err != nil {
		t.Fatalf("EncodeRow() error = %v", err)
	}

	decoded, err := DecodeRow(encoded)
	if err != nil {
		t.Fatalf("DecodeRow() error = %v", err)
	}

	assertRowValuesEqual(t, decoded, []parser.Value{parser.RealValue(3.14)})
}

func TestEncodeDecodeMultipleRealValues(t *testing.T) {
	values := []parser.Value{
		parser.RealValue(0.0),
		parser.RealValue(3.14),
		parser.RealValue(-2.5),
		parser.RealValue(10.25),
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

func TestEncodeDecodeRowMixedWithReal(t *testing.T) {
	values := []parser.Value{
		parser.Int64Value(7),
		parser.StringValue("alice"),
		parser.BoolValue(true),
		parser.RealValue(10.25),
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

func TestEncodeRowRejectsOutOfRangeInt(t *testing.T) {
	_, err := EncodeRow([]parser.Value{parser.Int64Value(2147483648)})
	if !errors.Is(err, errInvalidRowData) {
		t.Fatalf("EncodeRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestDecodeRowRejectsOutOfRangeInt(t *testing.T) {
	data := make([]byte, 0, 11)
	data = append(data, 1, 0, rowTypeInt64)
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], uint64(int64(math.MaxInt32)+1))
	data = append(data, raw[:]...)

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

func TestDecodeRowTruncatedReal(t *testing.T) {
	data := []byte{1, 0, rowTypeReal, 1, 2, 3}

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

func TestRealEncodingIsDistinctFromIntStringAndBool(t *testing.T) {
	realEncoded, err := EncodeRow([]parser.Value{parser.RealValue(1.25)})
	if err != nil {
		t.Fatalf("EncodeRow(real) error = %v", err)
	}
	intEncoded, err := EncodeRow([]parser.Value{parser.Int64Value(1)})
	if err != nil {
		t.Fatalf("EncodeRow(int) error = %v", err)
	}
	stringEncoded, err := EncodeRow([]parser.Value{parser.StringValue("1.25")})
	if err != nil {
		t.Fatalf("EncodeRow(string) error = %v", err)
	}
	boolEncoded, err := EncodeRow([]parser.Value{parser.BoolValue(true)})
	if err != nil {
		t.Fatalf("EncodeRow(bool) error = %v", err)
	}

	if binary.LittleEndian.Uint16(realEncoded[:2]) != 1 {
		t.Fatalf("real row count header = %d, want 1", binary.LittleEndian.Uint16(realEncoded[:2]))
	}
	if realEncoded[2] != rowTypeReal {
		t.Fatalf("real tag = %d, want %d", realEncoded[2], rowTypeReal)
	}
	if realEncoded[2] == intEncoded[2] || realEncoded[2] == stringEncoded[2] || realEncoded[2] == boolEncoded[2] {
		t.Fatalf("real tag %d must differ from int tag %d, string tag %d, and bool tag %d", realEncoded[2], intEncoded[2], stringEncoded[2], boolEncoded[2])
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

func TestEncodeDecodeSlottedRowSingleInt(t *testing.T) {
	values := []parser.Value{parser.Int64Value(42)}

	encoded, err := EncodeSlottedRow(values)
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}
	decoded, err := DecodeSlottedRow(encoded, []uint8{CatalogColumnTypeInt})
	if err != nil {
		t.Fatalf("DecodeSlottedRow() error = %v", err)
	}

	assertRowValuesEqual(t, decoded, values)
}

func TestEncodeDecodeSlottedRowSingleBool(t *testing.T) {
	values := []parser.Value{parser.BoolValue(true)}

	encoded, err := EncodeSlottedRow(values)
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}
	decoded, err := DecodeSlottedRow(encoded, []uint8{CatalogColumnTypeBool})
	if err != nil {
		t.Fatalf("DecodeSlottedRow() error = %v", err)
	}

	assertRowValuesEqual(t, decoded, values)
}

func TestEncodeDecodeSlottedRowSingleReal(t *testing.T) {
	values := []parser.Value{parser.RealValue(3.14)}

	encoded, err := EncodeSlottedRow(values)
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}
	decoded, err := DecodeSlottedRow(encoded, []uint8{CatalogColumnTypeReal})
	if err != nil {
		t.Fatalf("DecodeSlottedRow() error = %v", err)
	}

	assertRowValuesEqual(t, decoded, values)
}

func TestEncodeDecodeSlottedRowSingleText(t *testing.T) {
	values := []parser.Value{parser.StringValue("hello")}

	encoded, err := EncodeSlottedRow(values)
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}
	decoded, err := DecodeSlottedRow(encoded, []uint8{CatalogColumnTypeText})
	if err != nil {
		t.Fatalf("DecodeSlottedRow() error = %v", err)
	}

	assertRowValuesEqual(t, decoded, values)
}

func TestEncodeDecodeSlottedRowMixed(t *testing.T) {
	values := []parser.Value{
		parser.Int64Value(7),
		parser.BoolValue(true),
		parser.RealValue(10.25),
		parser.StringValue("alice"),
	}
	columnTypes := []uint8{
		CatalogColumnTypeInt,
		CatalogColumnTypeBool,
		CatalogColumnTypeReal,
		CatalogColumnTypeText,
	}

	encoded, err := EncodeSlottedRow(values)
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}
	decoded, err := DecodeSlottedRow(encoded, columnTypes)
	if err != nil {
		t.Fatalf("DecodeSlottedRow() error = %v", err)
	}

	assertRowValuesEqual(t, decoded, values)
}

func TestEncodeDecodeSlottedRowWithNulls(t *testing.T) {
	values := []parser.Value{
		parser.NullValue(),
		parser.StringValue("name"),
		parser.NullValue(),
		parser.BoolValue(false),
	}
	columnTypes := []uint8{
		CatalogColumnTypeInt,
		CatalogColumnTypeText,
		CatalogColumnTypeReal,
		CatalogColumnTypeBool,
	}

	encoded, err := EncodeSlottedRow(values)
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}
	if got := binary.LittleEndian.Uint16(encoded[0:2]); got != uint16(len(values)) {
		t.Fatalf("column count = %d, want %d", got, len(values))
	}
	if got := binary.LittleEndian.Uint16(encoded[2:4]); got != 1 {
		t.Fatalf("null bitmap byte count = %d, want 1", got)
	}
	if encoded[4] != 0b00000101 {
		t.Fatalf("null bitmap = %08b, want 00000101", encoded[4])
	}

	decoded, err := DecodeSlottedRow(encoded, columnTypes)
	if err != nil {
		t.Fatalf("DecodeSlottedRow() error = %v", err)
	}

	assertRowValuesEqual(t, decoded, values)
}

func TestEncodeDecodeSlottedRowIntBoundaries(t *testing.T) {
	values := []parser.Value{
		parser.Int64Value(math.MinInt32),
		parser.Int64Value(math.MaxInt32),
	}

	encoded, err := EncodeSlottedRow(values)
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}
	decoded, err := DecodeSlottedRow(encoded, []uint8{CatalogColumnTypeInt, CatalogColumnTypeInt})
	if err != nil {
		t.Fatalf("DecodeSlottedRow() error = %v", err)
	}

	assertRowValuesEqual(t, decoded, values)
}

func TestDecodeSlottedRowRejectsInvalidBoolByte(t *testing.T) {
	data := []byte{1, 0, 1, 0, 0, 2}

	_, err := DecodeSlottedRow(data, []uint8{CatalogColumnTypeBool})
	if !errors.Is(err, errInvalidRowData) {
		t.Fatalf("DecodeSlottedRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestDecodeSlottedRowRejectsTruncatedInt(t *testing.T) {
	data := []byte{1, 0, 1, 0, 0, 1, 2, 3}

	_, err := DecodeSlottedRow(data, []uint8{CatalogColumnTypeInt})
	if !errors.Is(err, errInvalidRowData) {
		t.Fatalf("DecodeSlottedRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestDecodeSlottedRowRejectsTruncatedReal(t *testing.T) {
	data := []byte{1, 0, 1, 0, 0, 1, 2, 3}

	_, err := DecodeSlottedRow(data, []uint8{CatalogColumnTypeReal})
	if !errors.Is(err, errInvalidRowData) {
		t.Fatalf("DecodeSlottedRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestDecodeSlottedRowRejectsTruncatedTextLength(t *testing.T) {
	data := []byte{1, 0, 1, 0, 0, 1}

	_, err := DecodeSlottedRow(data, []uint8{CatalogColumnTypeText})
	if !errors.Is(err, errInvalidRowData) {
		t.Fatalf("DecodeSlottedRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestDecodeSlottedRowRejectsTruncatedTextPayload(t *testing.T) {
	data := []byte{1, 0, 1, 0, 0, 5, 0, 'h', 'i'}

	_, err := DecodeSlottedRow(data, []uint8{CatalogColumnTypeText})
	if !errors.Is(err, errInvalidRowData) {
		t.Fatalf("DecodeSlottedRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestDecodeSlottedRowRejectsColumnCountMismatch(t *testing.T) {
	encoded, err := EncodeSlottedRow([]parser.Value{parser.Int64Value(1)})
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}

	_, err = DecodeSlottedRow(encoded, []uint8{CatalogColumnTypeInt, CatalogColumnTypeText})
	if !errors.Is(err, errInvalidRowData) {
		t.Fatalf("DecodeSlottedRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestDecodeSlottedRowRejectsUnsupportedType(t *testing.T) {
	encoded, err := EncodeSlottedRow([]parser.Value{parser.Int64Value(1)})
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}

	_, err = DecodeSlottedRow(encoded, []uint8{99})
	if !errors.Is(err, errInvalidRowData) {
		t.Fatalf("DecodeSlottedRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestEncodeSlottedRowRejectsTextLengthOverflow(t *testing.T) {
	values := []parser.Value{parser.StringValue(string(make([]byte, math.MaxUint16+1)))}

	_, err := EncodeSlottedRow(values)
	if !errors.Is(err, errInvalidRowData) {
		t.Fatalf("EncodeSlottedRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestEncodeSlottedRowRejectsOutOfRangeInt(t *testing.T) {
	_, err := EncodeSlottedRow([]parser.Value{parser.Int64Value(math.MaxInt32 + 1)})
	if !errors.Is(err, errInvalidRowData) {
		t.Fatalf("EncodeSlottedRow() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestEncodeInsertDecodeSlottedRowRoundTrip(t *testing.T) {
	values := []parser.Value{
		parser.Int64Value(5),
		parser.StringValue("slot"),
		parser.BoolValue(true),
		parser.RealValue(1.5),
	}
	columnTypes := []uint8{
		CatalogColumnTypeInt,
		CatalogColumnTypeText,
		CatalogColumnTypeBool,
		CatalogColumnTypeReal,
	}

	row, err := EncodeSlottedRow(values)
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}
	page := InitializeTablePage(1)
	slotID, err := InsertRowIntoTablePage(page, row)
	if err != nil {
		t.Fatalf("InsertRowIntoTablePage() error = %v", err)
	}
	offset, length, err := TablePageSlot(page, slotID)
	if err != nil {
		t.Fatalf("TablePageSlot() error = %v", err)
	}
	decoded, err := DecodeSlottedRow(page[offset:offset+length], columnTypes)
	if err != nil {
		t.Fatalf("DecodeSlottedRow() error = %v", err)
	}

	assertRowValuesEqual(t, decoded, values)
}
