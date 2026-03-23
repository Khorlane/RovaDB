package storage

import (
	"encoding/binary"
	"errors"
)

const tablePageHeaderSize = 8

var errTablePageFull = errors.New("storage: table page full")

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
	page.MarkDirty()
}

// AppendRowToTablePage appends one encoded row to a table root page.
func AppendRowToTablePage(page *Page, row []byte) error {
	if page == nil {
		return errInvalidRowData
	}
	InitTableRootPage(page)

	freeOffset := tablePageFreeOffset(page)
	if freeOffset < tablePageHeaderSize || freeOffset > PageSize {
		return errInvalidRowData
	}

	required := 4 + len(row)
	if int(freeOffset)+required > PageSize {
		return errTablePageFull
	}

	binary.LittleEndian.PutUint32(page.data[freeOffset:freeOffset+4], uint32(len(row)))
	copy(page.data[freeOffset+4:freeOffset+4+uint32(len(row))], row)

	binary.LittleEndian.PutUint32(page.data[0:4], TablePageRowCount(page)+1)
	binary.LittleEndian.PutUint32(page.data[4:8], freeOffset+uint32(required))
	page.MarkDirty()
	return nil
}

// TablePageRowCount reports the row count stored in a table root page.
func TablePageRowCount(page *Page) uint32 {
	if page == nil || len(page.data) < tablePageHeaderSize {
		return 0
	}
	return binary.LittleEndian.Uint32(page.data[0:4])
}

func tablePageFreeOffset(page *Page) uint32 {
	if page == nil || len(page.data) < tablePageHeaderSize {
		return 0
	}
	return binary.LittleEndian.Uint32(page.data[4:8])
}
