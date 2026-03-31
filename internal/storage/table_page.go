package storage

import (
	"encoding/binary"

	"github.com/Khorlane/RovaDB/internal/parser"
)

const tablePageHeaderSize = 8

const (
	tablePageCommonHeaderSize = 32
	tablePageBodyHeaderSize   = 8
	tablePageSlotEntrySize    = 4
	tablePageBodyStart        = tablePageCommonHeaderSize + tablePageBodyHeaderSize

	tablePageHeaderOffsetPageID   = 0
	tablePageHeaderOffsetPageType = 4
	tablePageHeaderOffsetPageLSN  = 8
	tablePageHeaderOffsetChecksum = 16

	tablePageBodyOffsetSlotCount = tablePageCommonHeaderSize
	tablePageBodyOffsetFreeStart = tablePageCommonHeaderSize + 4
	tablePageBodyOffsetFreeEnd   = tablePageCommonHeaderSize + 6
)

// InitTableRootPage initializes a blank table root page header.
func InitTableRootPage(page *Page) {
	if page == nil {
		return
	}
	if TablePageRowCount(page) != 0 || tablePageFreeOffset(page) != 0 {
		return
	}

	binary.LittleEndian.PutUint32(page.data[0:4], 0)
	binary.LittleEndian.PutUint32(page.data[4:8], tablePageHeaderSize)
	// Table page mutation sites must explicitly dirty the page so commit-oriented
	// flush decisions can be driven by dirty tracking.
	page.MarkDirty()
}

// AppendRowToTablePage appends one encoded row to a table root page.
func AppendRowToTablePage(page *Page, row []byte) error {
	if page == nil {
		return errCorruptedTablePage
	}
	InitTableRootPage(page)

	freeOffset := tablePageFreeOffset(page)
	if freeOffset < tablePageHeaderSize || freeOffset > PageSize {
		return errCorruptedTablePage
	}

	required := 4 + len(row)
	if int(freeOffset)+required > PageSize {
		return errTablePageFull
	}

	binary.LittleEndian.PutUint32(page.data[freeOffset:freeOffset+4], uint32(len(row)))
	copy(page.data[freeOffset+4:freeOffset+4+uint32(len(row))], row)

	binary.LittleEndian.PutUint32(page.data[0:4], TablePageRowCount(page)+1)
	binary.LittleEndian.PutUint32(page.data[4:8], freeOffset+uint32(required))
	// Table page mutation sites must explicitly dirty the page so commit-oriented
	// flush decisions can be driven by dirty tracking.
	page.MarkDirty()
	return nil
}

// TablePageRowCount reports the row count stored in a table root page.
func TablePageRowCount(page *Page) uint32 {
	if page == nil || len(page.data) < tablePageHeaderSize {
		return 0
	}
	if IsSlottedTablePage(page.data) {
		slotCount, err := TablePageSlotCount(page.data)
		if err != nil {
			return 0
		}
		return uint32(slotCount)
	}
	return binary.LittleEndian.Uint32(page.data[0:4])
}

// ReadRowsFromTablePage reads all encoded row payloads from a table root page.
func ReadRowsFromTablePage(page *Page) ([][]byte, error) {
	if page == nil {
		return nil, errCorruptedTablePage
	}
	if IsSlottedTablePage(page.data) {
		return readSlottedRowPayloads(page.data)
	}
	return ReadRowsFromTablePageData(page.data)
}

// ReadRowsFromTablePageData reads all encoded row payloads from a table root
// page image.
func ReadRowsFromTablePageData(pageData []byte) ([][]byte, error) {
	if len(pageData) < tablePageHeaderSize {
		return nil, errCorruptedTablePage
	}

	rowCount := binary.LittleEndian.Uint32(pageData[0:4])
	freeOffset := binary.LittleEndian.Uint32(pageData[4:8])
	if freeOffset < tablePageHeaderSize || freeOffset > PageSize {
		return nil, errCorruptedTablePage
	}

	rows := make([][]byte, 0, rowCount)
	offset := uint32(tablePageHeaderSize)
	for offset < freeOffset {
		if offset+4 > freeOffset {
			return nil, errCorruptedTablePage
		}
		rowLen := binary.LittleEndian.Uint32(pageData[offset : offset+4])
		offset += 4
		if offset+rowLen > freeOffset {
			return nil, errCorruptedTablePage
		}

		payload := append([]byte(nil), pageData[offset:offset+rowLen]...)
		rows = append(rows, payload)
		offset += rowLen
	}

	if offset != freeOffset || uint32(len(rows)) != rowCount {
		return nil, errCorruptedTablePage
	}

	return rows, nil
}

// BuildTablePageData builds a full table page image from encoded rows.
func BuildTablePageData(encodedRows [][]byte) ([]byte, error) {
	page := NewPage(0)

	clear(page.data)
	binary.LittleEndian.PutUint32(page.data[0:4], 0)
	binary.LittleEndian.PutUint32(page.data[4:8], tablePageHeaderSize)
	page.MarkDirty()

	for _, row := range encodedRows {
		if err := AppendRowToTablePage(page, row); err != nil {
			return nil, err
		}
	}

	return append([]byte(nil), page.data...), nil
}

// RewriteTablePage rebuilds a table root page from the provided encoded rows.
func RewriteTablePage(page *Page, encodedRows [][]byte) error {
	if page == nil {
		return errCorruptedTablePage
	}

	data, err := BuildTablePageData(encodedRows)
	if err != nil {
		return err
	}
	clear(page.data)
	copy(page.data, data)
	// Table page mutation sites must explicitly dirty the page so commit-oriented
	// flush decisions can be driven by dirty tracking.
	page.MarkDirty()
	return nil
}

func tablePageFreeOffset(page *Page) uint32 {
	if page == nil || len(page.data) < tablePageHeaderSize {
		return 0
	}
	return binary.LittleEndian.Uint32(page.data[4:8])
}

// InitializeTablePage builds a blank slotted table page.
func InitializeTablePage(pageID uint32) []byte {
	page := make([]byte, PageSize)
	binary.LittleEndian.PutUint32(page[tablePageHeaderOffsetPageID:tablePageHeaderOffsetPageID+4], pageID)
	binary.LittleEndian.PutUint16(page[tablePageHeaderOffsetPageType:tablePageHeaderOffsetPageType+2], uint16(PageTypeTable))
	binary.LittleEndian.PutUint16(page[tablePageBodyOffsetSlotCount:tablePageBodyOffsetSlotCount+2], 0)
	binary.LittleEndian.PutUint16(page[tablePageBodyOffsetFreeStart:tablePageBodyOffsetFreeStart+2], tablePageBodyStart)
	binary.LittleEndian.PutUint16(page[tablePageBodyOffsetFreeEnd:tablePageBodyOffsetFreeEnd+2], PageSize)
	return page
}

func TablePageSlotCount(page []byte) (int, error) {
	if err := validateSlottedTablePage(page); err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint16(page[tablePageBodyOffsetSlotCount : tablePageBodyOffsetSlotCount+2])), nil
}

func TablePageFreeStart(page []byte) (int, error) {
	if err := validateSlottedTablePage(page); err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint16(page[tablePageBodyOffsetFreeStart : tablePageBodyOffsetFreeStart+2])), nil
}

func TablePageFreeEnd(page []byte) (int, error) {
	if err := validateSlottedTablePage(page); err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint16(page[tablePageBodyOffsetFreeEnd : tablePageBodyOffsetFreeEnd+2])), nil
}

func TablePageFreeSpace(page []byte) (int, error) {
	freeStart, err := TablePageFreeStart(page)
	if err != nil {
		return 0, err
	}
	freeEnd, err := TablePageFreeEnd(page)
	if err != nil {
		return 0, err
	}
	return freeEnd - freeStart, nil
}

func TablePageSlot(page []byte, slotID int) (offset int, length int, err error) {
	if err := validateSlottedTablePage(page); err != nil {
		return 0, 0, err
	}
	slotCount, err := TablePageSlotCount(page)
	if err != nil {
		return 0, 0, err
	}
	if slotID < 0 || slotID >= slotCount {
		return 0, 0, errCorruptedTablePage
	}

	entryOffset := tablePageBodyStart + slotID*tablePageSlotEntrySize
	offset = int(binary.LittleEndian.Uint16(page[entryOffset : entryOffset+2]))
	length = int(binary.LittleEndian.Uint16(page[entryOffset+2 : entryOffset+4]))
	if offset < tablePageBodyStart || length < 0 || offset+length > PageSize {
		return 0, 0, errCorruptedTablePage
	}
	return offset, length, nil
}

func SlotLocator(pageID uint32, slotID int) (RowLocator, error) {
	if slotID < 0 || slotID > int(^uint16(0)) {
		return RowLocator{}, errCorruptedTablePage
	}
	return RowLocator{PageID: pageID, SlotID: uint16(slotID)}, nil
}

func TablePageLocators(page []byte, pageID uint32) ([]RowLocator, error) {
	slotCount, err := TablePageSlotCount(page)
	if err != nil {
		return nil, err
	}

	locators := make([]RowLocator, 0, slotCount)
	for slotID := 0; slotID < slotCount; slotID++ {
		if _, _, err := TablePageSlot(page, slotID); err != nil {
			return nil, err
		}
		locator, err := SlotLocator(pageID, slotID)
		if err != nil {
			return nil, err
		}
		locators = append(locators, locator)
	}
	return locators, nil
}

func IsSlottedTablePage(page []byte) bool {
	return len(page) == PageSize && PageType(binary.LittleEndian.Uint16(page[tablePageHeaderOffsetPageType:tablePageHeaderOffsetPageType+2])) == PageTypeTable
}

func CanFitRow(page []byte, rowLen int) (bool, error) {
	if err := validateSlottedTablePage(page); err != nil {
		return false, err
	}
	if rowLen < 0 {
		return false, errCorruptedTablePage
	}
	freeSpace, err := TablePageFreeSpace(page)
	if err != nil {
		return false, err
	}
	return freeSpace >= rowLen+tablePageSlotEntrySize, nil
}

func InsertRowIntoTablePage(page []byte, row []byte) (slotID int, err error) {
	if err := validateSlottedTablePage(page); err != nil {
		return 0, err
	}

	fit, err := CanFitRow(page, len(row))
	if err != nil {
		return 0, err
	}
	if !fit {
		return 0, errTablePageFull
	}

	slotCount, err := TablePageSlotCount(page)
	if err != nil {
		return 0, err
	}
	freeStart, err := TablePageFreeStart(page)
	if err != nil {
		return 0, err
	}
	freeEnd, err := TablePageFreeEnd(page)
	if err != nil {
		return 0, err
	}

	rowOffset := freeEnd - len(row)
	copy(page[rowOffset:freeEnd], row)

	entryOffset := tablePageBodyStart + slotCount*tablePageSlotEntrySize
	binary.LittleEndian.PutUint16(page[entryOffset:entryOffset+2], uint16(rowOffset))
	binary.LittleEndian.PutUint16(page[entryOffset+2:entryOffset+4], uint16(len(row)))
	binary.LittleEndian.PutUint16(page[tablePageBodyOffsetSlotCount:tablePageBodyOffsetSlotCount+2], uint16(slotCount+1))
	binary.LittleEndian.PutUint16(page[tablePageBodyOffsetFreeStart:tablePageBodyOffsetFreeStart+2], uint16(freeStart+tablePageSlotEntrySize))
	binary.LittleEndian.PutUint16(page[tablePageBodyOffsetFreeEnd:tablePageBodyOffsetFreeEnd+2], uint16(rowOffset))
	return slotCount, nil
}

func ExtractSlottedRowPayload(page []byte, slotID int) ([]byte, error) {
	if err := validateSlottedTablePage(page); err != nil {
		return nil, err
	}
	offset, length, err := TablePageSlot(page, slotID)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), page[offset:offset+length]...), nil
}

func ReadSlottedRowsFromTablePageData(page []byte, columnTypes []uint8) ([][]parser.Value, error) {
	if err := validateSlottedTablePage(page); err != nil {
		return nil, err
	}
	pageID := binary.LittleEndian.Uint32(page[tablePageHeaderOffsetPageID : tablePageHeaderOffsetPageID+4])
	_, rows, err := ReadSlottedRowsWithLocators(page, pageID, columnTypes)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func ReadSlottedRowsWithLocators(page []byte, pageID uint32, columnTypes []uint8) ([]RowLocator, [][]parser.Value, error) {
	payloads, err := readSlottedRowPayloads(page)
	if err != nil {
		return nil, nil, err
	}
	locators, err := TablePageLocators(page, pageID)
	if err != nil {
		return nil, nil, err
	}

	rows := make([][]parser.Value, 0, len(payloads))
	for _, payload := range payloads {
		if len(payload) < 2 {
			return nil, nil, errInvalidRowData
		}
		encodedColumnCount := int(binary.LittleEndian.Uint16(payload[0:2]))
		if encodedColumnCount > len(columnTypes) {
			return nil, nil, errInvalidRowData
		}
		row, err := DecodeSlottedRow(payload, columnTypes[:encodedColumnCount])
		if err != nil {
			return nil, nil, err
		}
		for len(row) < len(columnTypes) {
			row = append(row, parser.NullValue())
		}
		rows = append(rows, row)
	}
	return locators, rows, nil
}

func readSlottedRowPayloads(page []byte) ([][]byte, error) {
	slotCount, err := TablePageSlotCount(page)
	if err != nil {
		return nil, err
	}

	rows := make([][]byte, 0, slotCount)
	for slotID := 0; slotID < slotCount; slotID++ {
		payload, err := ExtractSlottedRowPayload(page, slotID)
		if err != nil {
			return nil, err
		}
		rows = append(rows, payload)
	}
	return rows, nil
}

func BuildSlottedTablePageData(pageID uint32, rows [][]parser.Value) ([]byte, error) {
	page := InitializeTablePage(pageID)
	for _, row := range rows {
		encoded, err := EncodeSlottedRow(row)
		if err != nil {
			return nil, err
		}
		if _, err := InsertRowIntoTablePage(page, encoded); err != nil {
			return nil, err
		}
	}
	return page, nil
}

func validateSlottedTablePage(page []byte) error {
	if len(page) != PageSize {
		return errCorruptedTablePage
	}
	pageType := PageType(binary.LittleEndian.Uint16(page[tablePageHeaderOffsetPageType : tablePageHeaderOffsetPageType+2]))
	if !IsValidPageType(pageType) || pageType != PageTypeTable {
		return errCorruptedTablePage
	}

	slotCount := int(binary.LittleEndian.Uint16(page[tablePageBodyOffsetSlotCount : tablePageBodyOffsetSlotCount+2]))
	freeStart := int(binary.LittleEndian.Uint16(page[tablePageBodyOffsetFreeStart : tablePageBodyOffsetFreeStart+2]))
	freeEnd := int(binary.LittleEndian.Uint16(page[tablePageBodyOffsetFreeEnd : tablePageBodyOffsetFreeEnd+2]))
	expectedFreeStart := tablePageBodyStart + slotCount*tablePageSlotEntrySize

	if slotCount < 0 || freeStart < tablePageBodyStart || freeEnd < tablePageBodyStart || freeEnd > PageSize {
		return errCorruptedTablePage
	}
	if freeStart != expectedFreeStart || freeStart > freeEnd {
		return errCorruptedTablePage
	}
	return nil
}
