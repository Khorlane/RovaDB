package storage

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestInitTableRootPage(t *testing.T) {
	page := NewPage(1)

	InitTableRootPage(page)

	if got := TablePageRowCount(page); got != 0 {
		t.Fatalf("TablePageRowCount() = %d, want 0", got)
	}
	if got := binary.LittleEndian.Uint32(page.Data()[4:8]); got != tablePageHeaderSize {
		t.Fatalf("free offset = %d, want %d", got, tablePageHeaderSize)
	}
}

func TestAppendRowToTablePage(t *testing.T) {
	page := NewPage(1)
	row, err := EncodeRow([]parser.Value{parser.Int64Value(1), parser.StringValue("steve")})
	if err != nil {
		t.Fatalf("EncodeRow() error = %v", err)
	}

	if err := AppendRowToTablePage(page, row); err != nil {
		t.Fatalf("AppendRowToTablePage() error = %v", err)
	}

	if got := TablePageRowCount(page); got != 1 {
		t.Fatalf("TablePageRowCount() = %d, want 1", got)
	}
	wantOffset := uint32(tablePageHeaderSize + 4 + len(row))
	if got := binary.LittleEndian.Uint32(page.Data()[4:8]); got != wantOffset {
		t.Fatalf("free offset = %d, want %d", got, wantOffset)
	}
	if !page.Dirty() {
		t.Fatal("page.Dirty() = false, want true")
	}

	storedLen := binary.LittleEndian.Uint32(page.Data()[8:12])
	if storedLen != uint32(len(row)) {
		t.Fatalf("stored row length = %d, want %d", storedLen, len(row))
	}
}

func TestAppendRowToTablePageFull(t *testing.T) {
	page := NewPage(1)
	oversized := make([]byte, PageSize)

	err := AppendRowToTablePage(page, oversized)
	if !errors.Is(err, errTablePageFull) {
		t.Fatalf("AppendRowToTablePage() error = %v, want %v", err, errTablePageFull)
	}
}

func TestRewriteTablePage(t *testing.T) {
	page := NewPage(1)

	row1, err := EncodeRow([]parser.Value{parser.Int64Value(1), parser.StringValue("alice")})
	if err != nil {
		t.Fatalf("EncodeRow(row1) error = %v", err)
	}
	row2, err := EncodeRow([]parser.Value{parser.Int64Value(2), parser.StringValue("bob")})
	if err != nil {
		t.Fatalf("EncodeRow(row2) error = %v", err)
	}
	row3, err := EncodeRow([]parser.Value{parser.Int64Value(3), parser.StringValue("carol")})
	if err != nil {
		t.Fatalf("EncodeRow(row3) error = %v", err)
	}

	if err := AppendRowToTablePage(page, row1); err != nil {
		t.Fatalf("AppendRowToTablePage(initial) error = %v", err)
	}
	if err := AppendRowToTablePage(page, row2); err != nil {
		t.Fatalf("AppendRowToTablePage(initial second) error = %v", err)
	}

	if err := RewriteTablePage(page, [][]byte{row3}); err != nil {
		t.Fatalf("RewriteTablePage() error = %v", err)
	}

	if got := TablePageRowCount(page); got != 1 {
		t.Fatalf("TablePageRowCount() = %d, want 1", got)
	}
	wantOffset := uint32(tablePageHeaderSize + 4 + len(row3))
	if got := binary.LittleEndian.Uint32(page.Data()[4:8]); got != wantOffset {
		t.Fatalf("free offset = %d, want %d", got, wantOffset)
	}

	payloads, err := ReadRowsFromTablePage(page)
	if err != nil {
		t.Fatalf("ReadRowsFromTablePage() error = %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("len(payloads) = %d, want 1", len(payloads))
	}
	values, err := DecodeRow(payloads[0])
	if err != nil {
		t.Fatalf("DecodeRow() error = %v", err)
	}
	wantValues := []parser.Value{parser.Int64Value(3), parser.StringValue("carol")}
	if len(values) != len(wantValues) {
		t.Fatalf("len(values) = %d, want %d", len(values), len(wantValues))
	}
	for i := range wantValues {
		if values[i] != wantValues[i] {
			t.Fatalf("values[%d] = %#v, want %#v", i, values[i], wantValues[i])
		}
	}
}

func TestReadRowsFromTablePageMalformedFreeOffset(t *testing.T) {
	page := NewPage(1)
	binary.LittleEndian.PutUint32(page.Data()[4:8], 7)

	_, err := ReadRowsFromTablePage(page)
	if !errors.Is(err, errCorruptedTablePage) {
		t.Fatalf("ReadRowsFromTablePage() error = %v, want %v", err, errCorruptedTablePage)
	}
}

func TestReadRowsFromTablePageTruncatedRow(t *testing.T) {
	page := NewPage(1)
	InitTableRootPage(page)
	binary.LittleEndian.PutUint32(page.Data()[0:4], 1)
	binary.LittleEndian.PutUint32(page.Data()[4:8], 20)
	binary.LittleEndian.PutUint32(page.Data()[8:12], 12)

	_, err := ReadRowsFromTablePage(page)
	if !errors.Is(err, errCorruptedTablePage) {
		t.Fatalf("ReadRowsFromTablePage() error = %v, want %v", err, errCorruptedTablePage)
	}
}

func TestReadRowsFromTablePageCountMismatch(t *testing.T) {
	page := NewPage(1)
	row, err := EncodeRow([]parser.Value{parser.Int64Value(1)})
	if err != nil {
		t.Fatalf("EncodeRow() error = %v", err)
	}
	if err := AppendRowToTablePage(page, row); err != nil {
		t.Fatalf("AppendRowToTablePage() error = %v", err)
	}
	binary.LittleEndian.PutUint32(page.Data()[0:4], 2)

	_, err = ReadRowsFromTablePage(page)
	if !errors.Is(err, errCorruptedTablePage) {
		t.Fatalf("ReadRowsFromTablePage() error = %v, want %v", err, errCorruptedTablePage)
	}
}
