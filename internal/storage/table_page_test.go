package storage

import (
	"bytes"
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

func TestInitializeTablePage(t *testing.T) {
	page := InitializeTablePage(7)

	if got := len(page); got != PageSize {
		t.Fatalf("len(page) = %d, want %d", got, PageSize)
	}
	if got := binary.LittleEndian.Uint32(page[tablePageHeaderOffsetPageID : tablePageHeaderOffsetPageID+4]); got != 7 {
		t.Fatalf("pageID = %d, want 7", got)
	}
	if got := binary.LittleEndian.Uint16(page[tablePageHeaderOffsetPageType : tablePageHeaderOffsetPageType+2]); got != tablePageType {
		t.Fatalf("pageType = %d, want %d", got, tablePageType)
	}
	slotCount, err := TablePageSlotCount(page)
	if err != nil {
		t.Fatalf("TablePageSlotCount() error = %v", err)
	}
	if slotCount != 0 {
		t.Fatalf("TablePageSlotCount() = %d, want 0", slotCount)
	}
	freeStart, err := TablePageFreeStart(page)
	if err != nil {
		t.Fatalf("TablePageFreeStart() error = %v", err)
	}
	if freeStart != tablePageBodyStart {
		t.Fatalf("TablePageFreeStart() = %d, want %d", freeStart, tablePageBodyStart)
	}
	freeEnd, err := TablePageFreeEnd(page)
	if err != nil {
		t.Fatalf("TablePageFreeEnd() error = %v", err)
	}
	if freeEnd != PageSize {
		t.Fatalf("TablePageFreeEnd() = %d, want %d", freeEnd, PageSize)
	}
	freeSpace, err := TablePageFreeSpace(page)
	if err != nil {
		t.Fatalf("TablePageFreeSpace() error = %v", err)
	}
	if freeSpace != PageSize-tablePageBodyStart {
		t.Fatalf("TablePageFreeSpace() = %d, want %d", freeSpace, PageSize-tablePageBodyStart)
	}
}

func TestInsertRowIntoTablePage(t *testing.T) {
	page := InitializeTablePage(1)
	row := []byte{0xAA, 0xBB, 0xCC}

	slotID, err := InsertRowIntoTablePage(page, row)
	if err != nil {
		t.Fatalf("InsertRowIntoTablePage() error = %v", err)
	}
	if slotID != 0 {
		t.Fatalf("slotID = %d, want 0", slotID)
	}

	slotCount, err := TablePageSlotCount(page)
	if err != nil {
		t.Fatalf("TablePageSlotCount() error = %v", err)
	}
	if slotCount != 1 {
		t.Fatalf("TablePageSlotCount() = %d, want 1", slotCount)
	}
	freeStart, err := TablePageFreeStart(page)
	if err != nil {
		t.Fatalf("TablePageFreeStart() error = %v", err)
	}
	if freeStart != tablePageBodyStart+tablePageSlotEntrySize {
		t.Fatalf("TablePageFreeStart() = %d, want %d", freeStart, tablePageBodyStart+tablePageSlotEntrySize)
	}
	freeEnd, err := TablePageFreeEnd(page)
	if err != nil {
		t.Fatalf("TablePageFreeEnd() error = %v", err)
	}
	if freeEnd != PageSize-len(row) {
		t.Fatalf("TablePageFreeEnd() = %d, want %d", freeEnd, PageSize-len(row))
	}

	offset, length, err := TablePageSlot(page, 0)
	if err != nil {
		t.Fatalf("TablePageSlot() error = %v", err)
	}
	if length != len(row) {
		t.Fatalf("slot length = %d, want %d", length, len(row))
	}
	if !bytes.Equal(page[offset:offset+length], row) {
		t.Fatalf("stored row = %v, want %v", page[offset:offset+length], row)
	}
}

func TestInsertMultipleRowsIntoTablePage(t *testing.T) {
	page := InitializeTablePage(2)
	row1 := []byte{0x10, 0x11}
	row2 := []byte{0x20, 0x21, 0x22}

	slot1, err := InsertRowIntoTablePage(page, row1)
	if err != nil {
		t.Fatalf("InsertRowIntoTablePage(row1) error = %v", err)
	}
	slot2, err := InsertRowIntoTablePage(page, row2)
	if err != nil {
		t.Fatalf("InsertRowIntoTablePage(row2) error = %v", err)
	}
	if slot1 != 0 || slot2 != 1 {
		t.Fatalf("slots = (%d, %d), want (0, 1)", slot1, slot2)
	}

	offset1, length1, err := TablePageSlot(page, slot1)
	if err != nil {
		t.Fatalf("TablePageSlot(slot1) error = %v", err)
	}
	offset2, length2, err := TablePageSlot(page, slot2)
	if err != nil {
		t.Fatalf("TablePageSlot(slot2) error = %v", err)
	}
	if length1 != len(row1) || length2 != len(row2) {
		t.Fatalf("slot lengths = (%d, %d), want (%d, %d)", length1, length2, len(row1), len(row2))
	}
	if offset2 >= offset1 {
		t.Fatalf("offset2 = %d, want less than offset1 = %d", offset2, offset1)
	}
	if !bytes.Equal(page[offset1:offset1+length1], row1) {
		t.Fatalf("row1 bytes mismatch")
	}
	if !bytes.Equal(page[offset2:offset2+length2], row2) {
		t.Fatalf("row2 bytes mismatch")
	}
}

func TestCanFitRowFalseWhenInsufficientSpace(t *testing.T) {
	page := InitializeTablePage(3)
	rowLen := PageSize - tablePageBodyStart - tablePageSlotEntrySize + 1

	fit, err := CanFitRow(page, rowLen)
	if err != nil {
		t.Fatalf("CanFitRow() error = %v", err)
	}
	if fit {
		t.Fatal("CanFitRow() = true, want false")
	}
}

func TestSlottedTablePageRejectsInvalidPageSize(t *testing.T) {
	page := make([]byte, PageSize-1)

	if _, err := TablePageSlotCount(page); !errors.Is(err, errCorruptedTablePage) {
		t.Fatalf("TablePageSlotCount() error = %v, want %v", err, errCorruptedTablePage)
	}
}

func TestSlottedTablePageRejectsWrongPageType(t *testing.T) {
	page := InitializeTablePage(4)
	binary.LittleEndian.PutUint16(page[tablePageHeaderOffsetPageType:tablePageHeaderOffsetPageType+2], 99)

	if _, err := TablePageSlotCount(page); !errors.Is(err, errCorruptedTablePage) {
		t.Fatalf("TablePageSlotCount() error = %v, want %v", err, errCorruptedTablePage)
	}
}

func TestTablePageSlotRejectsOutOfRange(t *testing.T) {
	page := InitializeTablePage(5)

	if _, _, err := TablePageSlot(page, 0); !errors.Is(err, errCorruptedTablePage) {
		t.Fatalf("TablePageSlot() error = %v, want %v", err, errCorruptedTablePage)
	}
}
