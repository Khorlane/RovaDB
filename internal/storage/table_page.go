package storage

import (
	"encoding/binary"
)

const tablePageHeaderSize = 8

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
	return binary.LittleEndian.Uint32(page.data[0:4])
}

// ReadRowsFromTablePage reads all encoded row payloads from a table root page.
func ReadRowsFromTablePage(page *Page) ([][]byte, error) {
	if page == nil {
		return nil, errCorruptedTablePage
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
