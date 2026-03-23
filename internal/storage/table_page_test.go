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

func TestReadRowsFromTablePageMalformedFreeOffset(t *testing.T) {
	page := NewPage(1)
	binary.LittleEndian.PutUint32(page.Data()[4:8], 7)

	_, err := ReadRowsFromTablePage(page)
	if !errors.Is(err, errInvalidRowData) {
		t.Fatalf("ReadRowsFromTablePage() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestReadRowsFromTablePageTruncatedRow(t *testing.T) {
	page := NewPage(1)
	InitTableRootPage(page)
	binary.LittleEndian.PutUint32(page.Data()[0:4], 1)
	binary.LittleEndian.PutUint32(page.Data()[4:8], 20)
	binary.LittleEndian.PutUint32(page.Data()[8:12], 12)

	_, err := ReadRowsFromTablePage(page)
	if !errors.Is(err, errInvalidRowData) {
		t.Fatalf("ReadRowsFromTablePage() error = %v, want %v", err, errInvalidRowData)
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
	if !errors.Is(err, errInvalidRowData) {
		t.Fatalf("ReadRowsFromTablePage() error = %v, want %v", err, errInvalidRowData)
	}
}
