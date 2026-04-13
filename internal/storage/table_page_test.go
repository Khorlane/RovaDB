package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
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
	row, err := EncodeRow([]Value{Int64Value(1), StringValue("steve")})
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

	row1, err := EncodeRow([]Value{Int64Value(1), StringValue("alice")})
	if err != nil {
		t.Fatalf("EncodeRow(row1) error = %v", err)
	}
	row2, err := EncodeRow([]Value{Int64Value(2), StringValue("bob")})
	if err != nil {
		t.Fatalf("EncodeRow(row2) error = %v", err)
	}
	row3, err := EncodeRow([]Value{Int64Value(3), StringValue("carol")})
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
	wantValues := []Value{Int64Value(3), StringValue("carol")}
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
	row, err := EncodeRow([]Value{Int64Value(1)})
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
	if got := binary.LittleEndian.Uint16(page[tablePageHeaderOffsetPageType : tablePageHeaderOffsetPageType+2]); got != uint16(PageTypeTable) {
		t.Fatalf("pageType = %d, want %d", got, PageTypeTable)
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

func TestUpdateRowBySlotKeepsLocatorStableWhenReplacementFits(t *testing.T) {
	page := InitializeTablePage(21)
	slot0, err := InsertRowIntoTablePage(page, []byte{0x10, 0x11, 0x12})
	if err != nil {
		t.Fatalf("InsertRowIntoTablePage(slot0) error = %v", err)
	}
	slot1, err := InsertRowIntoTablePage(page, []byte{0x20, 0x21})
	if err != nil {
		t.Fatalf("InsertRowIntoTablePage(slot1) error = %v", err)
	}

	fit, err := CanUpdateRowInPlace(page, slot0, 6)
	if err != nil {
		t.Fatalf("CanUpdateRowInPlace() error = %v", err)
	}
	if !fit {
		t.Fatal("CanUpdateRowInPlace() = false, want true")
	}

	if err := UpdateRowBySlot(page, slot0, []byte{0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF}); err != nil {
		t.Fatalf("UpdateRowBySlot() error = %v", err)
	}

	payload, err := ExtractSlottedRowPayload(page, slot0)
	if err != nil {
		t.Fatalf("ExtractSlottedRowPayload(slot0) error = %v", err)
	}
	if !bytes.Equal(payload, []byte{0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF}) {
		t.Fatalf("slot0 payload = %v, want replacement bytes", payload)
	}
	locators, err := TablePageLocators(page, 21)
	if err != nil {
		t.Fatalf("TablePageLocators() error = %v", err)
	}
	want := []RowLocator{{PageID: 21, SlotID: 0}, {PageID: 21, SlotID: 1}}
	if len(locators) != len(want) {
		t.Fatalf("len(locators) = %d, want %d", len(locators), len(want))
	}
	for i := range want {
		if locators[i] != want[i] {
			t.Fatalf("locators[%d] = %#v, want %#v", i, locators[i], want[i])
		}
	}
	if _, err := ExtractSlottedRowPayload(page, slot1); err != nil {
		t.Fatalf("ExtractSlottedRowPayload(slot1) error = %v", err)
	}
}

func TestDeleteRowBySlotPreservesOtherLocatorsAndSkipsDeletedSlot(t *testing.T) {
	page := InitializeTablePage(22)
	for _, row := range [][]byte{{0x01}, {0x02, 0x03}, {0x04}} {
		if _, err := InsertRowIntoTablePage(page, row); err != nil {
			t.Fatalf("InsertRowIntoTablePage() error = %v", err)
		}
	}

	if err := DeleteRowBySlot(page, 1); err != nil {
		t.Fatalf("DeleteRowBySlot() error = %v", err)
	}
	if _, err := ExtractSlottedRowPayload(page, 1); !errors.Is(err, errCorruptedTablePage) {
		t.Fatalf("ExtractSlottedRowPayload(deleted) error = %v, want %v", err, errCorruptedTablePage)
	}

	locators, err := TablePageLocators(page, 22)
	if err != nil {
		t.Fatalf("TablePageLocators() error = %v", err)
	}
	want := []RowLocator{{PageID: 22, SlotID: 0}, {PageID: 22, SlotID: 2}}
	if len(locators) != len(want) {
		t.Fatalf("len(locators) = %d, want %d", len(locators), len(want))
	}
	for i := range want {
		if locators[i] != want[i] {
			t.Fatalf("locators[%d] = %#v, want %#v", i, locators[i], want[i])
		}
	}

	liveRows, err := TablePageLiveRowCount(page)
	if err != nil {
		t.Fatalf("TablePageLiveRowCount() error = %v", err)
	}
	if liveRows != 2 {
		t.Fatalf("TablePageLiveRowCount() = %d, want 2", liveRows)
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

func TestSlotLocator(t *testing.T) {
	locator, err := SlotLocator(7, 3)
	if err != nil {
		t.Fatalf("SlotLocator() error = %v", err)
	}
	if locator.PageID != 7 || locator.SlotID != 3 {
		t.Fatalf("locator = %#v, want PageID=7 SlotID=3", locator)
	}
}

func TestSlotLocatorRejectsInvalidSlotIndex(t *testing.T) {
	if _, err := SlotLocator(1, -1); !errors.Is(err, errCorruptedTablePage) {
		t.Fatalf("SlotLocator() error = %v, want %v", err, errCorruptedTablePage)
	}
}

func TestTablePageLocators(t *testing.T) {
	page, err := BuildSlottedTablePageData(8, []uint8{CatalogColumnTypeInt}, [][]Value{
		{Int64Value(1)},
		{Int64Value(2)},
	})
	if err != nil {
		t.Fatalf("BuildSlottedTablePageData() error = %v", err)
	}

	locators, err := TablePageLocators(page, 8)
	if err != nil {
		t.Fatalf("TablePageLocators() error = %v", err)
	}
	if len(locators) != 2 {
		t.Fatalf("len(locators) = %d, want 2", len(locators))
	}
	want := []RowLocator{{PageID: 8, SlotID: 0}, {PageID: 8, SlotID: 1}}
	for i := range want {
		if locators[i] != want[i] {
			t.Fatalf("locators[%d] = %#v, want %#v", i, locators[i], want[i])
		}
	}
}

func TestTablePageLocatorsRejectWrongPageType(t *testing.T) {
	page := InitializeTablePage(9)
	binary.LittleEndian.PutUint16(page[tablePageHeaderOffsetPageType:tablePageHeaderOffsetPageType+2], 99)

	_, err := TablePageLocators(page, 9)
	if !errors.Is(err, errCorruptedTablePage) {
		t.Fatalf("TablePageLocators() error = %v, want %v", err, errCorruptedTablePage)
	}
}

func TestBuildSlottedTablePageDataStoresEncodedRows(t *testing.T) {
	rows := [][]Value{
		{IntValue(1), StringValue("alice")},
		{IntValue(2), StringValue("bob")},
	}
	columnTypes := []uint8{CatalogColumnTypeInt, CatalogColumnTypeText}

	page, err := BuildSlottedTablePageData(9, columnTypes, rows)
	if err != nil {
		t.Fatalf("BuildSlottedTablePageData() error = %v", err)
	}
	if !IsSlottedTablePage(page) {
		t.Fatal("page is not slotted table page")
	}
	slotCount, err := TablePageSlotCount(page)
	if err != nil {
		t.Fatalf("TablePageSlotCount() error = %v", err)
	}
	if slotCount != len(rows) {
		t.Fatalf("TablePageSlotCount() = %d, want %d", slotCount, len(rows))
	}

	decodedRows, err := ReadSlottedRowsFromTablePageData(page, columnTypes)
	if err != nil {
		t.Fatalf("ReadSlottedRowsFromTablePageData() error = %v", err)
	}
	if len(decodedRows) != len(rows) {
		t.Fatalf("len(decodedRows) = %d, want %d", len(decodedRows), len(rows))
	}
	for i := range rows {
		payload, err := ExtractSlottedRowPayload(page, i)
		if err != nil {
			t.Fatalf("ExtractSlottedRowPayload() error = %v", err)
		}
		want, err := EncodeSlottedRow(rows[i], columnTypes)
		if err != nil {
			t.Fatalf("EncodeSlottedRow() error = %v", err)
		}
		if !bytes.Equal(payload, want) {
			t.Fatalf("payload[%d] mismatch", i)
		}
		for j := range rows[i] {
			if decodedRows[i][j] != rows[i][j] {
				t.Fatalf("decoded[%d][%d] = %#v, want %#v", i, j, decodedRows[i][j], rows[i][j])
			}
		}
	}
}

func TestBuildSlottedTablePageDataUsesDeclaredIntegerWidths(t *testing.T) {
	columnTypes := []uint8{
		CatalogColumnTypeSmallInt,
		CatalogColumnTypeInt,
		CatalogColumnTypeBigInt,
	}
	rows := [][]Value{
		{SmallIntValue(-7), IntValue(-8), BigIntValue(-9)},
	}

	page, err := BuildSlottedTablePageData(10, columnTypes, rows)
	if err != nil {
		t.Fatalf("BuildSlottedTablePageData() error = %v", err)
	}

	payload, err := ExtractSlottedRowPayload(page, 0)
	if err != nil {
		t.Fatalf("ExtractSlottedRowPayload() error = %v", err)
	}
	if got, want := len(payload), 4+1+2+4+8; got != want {
		t.Fatalf("len(payload) = %d, want %d", got, want)
	}

	decoded, err := ReadSlottedRowsFromTablePageData(page, columnTypes)
	if err != nil {
		t.Fatalf("ReadSlottedRowsFromTablePageData() error = %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("len(decoded) = %d, want 1", len(decoded))
	}
	for i := range rows[0] {
		if decoded[0][i] != rows[0][i] {
			t.Fatalf("decoded[0][%d] = %#v, want %#v", i, decoded[0][i], rows[0][i])
		}
	}
}

func TestBuildSlottedTablePageDataOverflow(t *testing.T) {
	row := []Value{StringValue(string(bytes.Repeat([]byte("x"), PageSize)))}

	_, err := BuildSlottedTablePageData(10, []uint8{CatalogColumnTypeText}, [][]Value{row})
	if !errors.Is(err, errTablePageFull) {
		t.Fatalf("BuildSlottedTablePageData() error = %v, want %v", err, errTablePageFull)
	}
}

func TestReadSlottedRowsFromTablePageDataPadsTrailingNulls(t *testing.T) {
	page, err := BuildSlottedTablePageData(11, []uint8{CatalogColumnTypeInt, CatalogColumnTypeText}, [][]Value{
		{Int64Value(1), StringValue("alice")},
	})
	if err != nil {
		t.Fatalf("BuildSlottedTablePageData() error = %v", err)
	}

	rows, err := ReadSlottedRowsFromTablePageData(page, []uint8{
		CatalogColumnTypeInt,
		CatalogColumnTypeText,
		CatalogColumnTypeInt,
	})
	if err != nil {
		t.Fatalf("ReadSlottedRowsFromTablePageData() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1", len(rows))
	}
	want := []Value{IntValue(1), StringValue("alice"), NullValue()}
	for i := range want {
		if rows[0][i] != want[i] {
			t.Fatalf("rows[0][%d] = %#v, want %#v", i, rows[0][i], want[i])
		}
	}
}

func TestReadSlottedRowsFromTablePageDataRejectsTruncatedPayload(t *testing.T) {
	page, err := BuildSlottedTablePageData(12, []uint8{CatalogColumnTypeInt}, [][]Value{
		{Int64Value(1)},
	})
	if err != nil {
		t.Fatalf("BuildSlottedTablePageData() error = %v", err)
	}

	offset, _, err := TablePageSlot(page, 0)
	if err != nil {
		t.Fatalf("TablePageSlot() error = %v", err)
	}
	page[offset] = 0x02
	page[offset+1] = 0x00

	_, err = ReadSlottedRowsFromTablePageData(page, []uint8{CatalogColumnTypeInt})
	if !errors.Is(err, errInvalidRowData) {
		t.Fatalf("ReadSlottedRowsFromTablePageData() error = %v, want %v", err, errInvalidRowData)
	}
}

func TestReadSlottedRowsFromTablePageDataRejectsWrongPageType(t *testing.T) {
	page := InitializeTablePage(13)
	binary.LittleEndian.PutUint16(page[tablePageHeaderOffsetPageType:tablePageHeaderOffsetPageType+2], 99)

	_, err := ReadSlottedRowsFromTablePageData(page, []uint8{CatalogColumnTypeInt})
	if !errors.Is(err, errCorruptedTablePage) {
		t.Fatalf("ReadSlottedRowsFromTablePageData() error = %v, want %v", err, errCorruptedTablePage)
	}
}

func TestReadSlottedRowsWithLocators(t *testing.T) {
	rows := [][]Value{
		{IntValue(1), StringValue("alice")},
		{IntValue(2), StringValue("bob")},
	}
	page, err := BuildSlottedTablePageData(14, []uint8{CatalogColumnTypeInt, CatalogColumnTypeText}, rows)
	if err != nil {
		t.Fatalf("BuildSlottedTablePageData() error = %v", err)
	}

	locators, decodedRows, err := ReadSlottedRowsWithLocators(page, 14, []uint8{
		CatalogColumnTypeInt,
		CatalogColumnTypeText,
	})
	if err != nil {
		t.Fatalf("ReadSlottedRowsWithLocators() error = %v", err)
	}
	if len(locators) != len(rows) || len(decodedRows) != len(rows) {
		t.Fatalf("lens = (%d, %d), want (%d, %d)", len(locators), len(decodedRows), len(rows), len(rows))
	}
	for i := range rows {
		wantLocator := RowLocator{PageID: 14, SlotID: uint16(i)}
		if locators[i] != wantLocator {
			t.Fatalf("locators[%d] = %#v, want %#v", i, locators[i], wantLocator)
		}
		for j := range rows[i] {
			if decodedRows[i][j] != rows[i][j] {
				t.Fatalf("decodedRows[%d][%d] = %#v, want %#v", i, j, decodedRows[i][j], rows[i][j])
			}
		}
	}
}

func TestReadSlottedRowsWithLocatorsRejectsInvalidMetadata(t *testing.T) {
	page := InitializeTablePage(15)
	binary.LittleEndian.PutUint16(page[tablePageBodyOffsetFreeStart:tablePageBodyOffsetFreeStart+2], tablePageBodyStart+1)

	_, _, err := ReadSlottedRowsWithLocators(page, 15, []uint8{CatalogColumnTypeInt})
	if !errors.Is(err, errCorruptedTablePage) {
		t.Fatalf("ReadSlottedRowsWithLocators() error = %v, want %v", err, errCorruptedTablePage)
	}
}

func TestReadRowByLocatorFromTablePageData(t *testing.T) {
	page, err := BuildSlottedTablePageData(16, []uint8{CatalogColumnTypeInt, CatalogColumnTypeText}, [][]Value{
		{Int64Value(1), StringValue("alice")},
		{Int64Value(2), StringValue("bob")},
	})
	if err != nil {
		t.Fatalf("BuildSlottedTablePageData() error = %v", err)
	}

	row, err := ReadRowByLocatorFromTablePageData(page, RowLocator{PageID: 16, SlotID: 1}, []uint8{
		CatalogColumnTypeInt,
		CatalogColumnTypeText,
	})
	if err != nil {
		t.Fatalf("ReadRowByLocatorFromTablePageData() error = %v", err)
	}
	want := []Value{IntValue(2), StringValue("bob")}
	for i := range want {
		if row[i] != want[i] {
			t.Fatalf("row[%d] = %#v, want %#v", i, row[i], want[i])
		}
	}
}

func TestReadRowByLocatorFromTablePageDataPadsTrailingNulls(t *testing.T) {
	page, err := BuildSlottedTablePageData(17, []uint8{CatalogColumnTypeInt, CatalogColumnTypeText}, [][]Value{
		{Int64Value(1), StringValue("alice")},
	})
	if err != nil {
		t.Fatalf("BuildSlottedTablePageData() error = %v", err)
	}

	row, err := ReadRowByLocatorFromTablePageData(page, RowLocator{PageID: 17, SlotID: 0}, []uint8{
		CatalogColumnTypeInt,
		CatalogColumnTypeText,
		CatalogColumnTypeInt,
	})
	if err != nil {
		t.Fatalf("ReadRowByLocatorFromTablePageData() error = %v", err)
	}
	want := []Value{IntValue(1), StringValue("alice"), NullValue()}
	for i := range want {
		if row[i] != want[i] {
			t.Fatalf("row[%d] = %#v, want %#v", i, row[i], want[i])
		}
	}
}

func TestReadRowByLocatorFromTablePageDataRejectsInvalidSlot(t *testing.T) {
	page, err := BuildSlottedTablePageData(18, []uint8{CatalogColumnTypeInt}, [][]Value{
		{Int64Value(1)},
	})
	if err != nil {
		t.Fatalf("BuildSlottedTablePageData() error = %v", err)
	}

	_, err = ReadRowByLocatorFromTablePageData(page, RowLocator{PageID: 18, SlotID: 1}, []uint8{CatalogColumnTypeInt})
	if !errors.Is(err, errCorruptedTablePage) {
		t.Fatalf("ReadRowByLocatorFromTablePageData() error = %v, want %v", err, errCorruptedTablePage)
	}
}

func TestReadRowByLocatorFromTablePageDataRejectsWrongPageType(t *testing.T) {
	page := InitializeTablePage(19)
	binary.LittleEndian.PutUint16(page[tablePageHeaderOffsetPageType:tablePageHeaderOffsetPageType+2], 99)

	_, err := ReadRowByLocatorFromTablePageData(page, RowLocator{PageID: 19, SlotID: 0}, []uint8{CatalogColumnTypeInt})
	if !errors.Is(err, errCorruptedTablePage) {
		t.Fatalf("ReadRowByLocatorFromTablePageData() error = %v, want %v", err, errCorruptedTablePage)
	}
}

func TestReadRowByLocatorFromTablePageDataRejectsMalformedPayload(t *testing.T) {
	page, err := BuildSlottedTablePageData(20, []uint8{CatalogColumnTypeInt}, [][]Value{
		{Int64Value(1)},
	})
	if err != nil {
		t.Fatalf("BuildSlottedTablePageData() error = %v", err)
	}

	offset, _, err := TablePageSlot(page, 0)
	if err != nil {
		t.Fatalf("TablePageSlot() error = %v", err)
	}
	page[offset] = 0x02
	page[offset+1] = 0x00

	_, err = ReadRowByLocatorFromTablePageData(page, RowLocator{PageID: 20, SlotID: 0}, []uint8{CatalogColumnTypeInt})
	if !errors.Is(err, errInvalidRowData) {
		t.Fatalf("ReadRowByLocatorFromTablePageData() error = %v, want %v", err, errInvalidRowData)
	}
}
