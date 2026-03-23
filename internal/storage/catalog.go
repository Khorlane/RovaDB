package storage

import (
	"encoding/binary"
	"errors"
)

const (
	catalogVersion = 1
	catalogPageID  = 0

	CatalogColumnTypeInt  = 1
	CatalogColumnTypeText = 2
)

var (
	errCatalogTooLarge = errors.New("storage: catalog too large")
	errInvalidCatalog  = errors.New("storage: invalid catalog")
)

// CatalogData is the tiny storage-side catalog DTO persisted in page 0.
type CatalogData struct {
	Tables []CatalogTable
}

// CatalogTable is a persisted table schema entry.
type CatalogTable struct {
	Name       string
	RootPageID uint32
	RowCount   uint32
	Columns    []CatalogColumn
}

// CatalogColumn is a persisted typed column entry.
type CatalogColumn struct {
	Name string
	Type uint8
}

// LoadCatalog decodes the catalog stored in page 0.
func LoadCatalog(pager *Pager) (*CatalogData, error) {
	page, err := pager.Get(catalogPageID)
	if err != nil {
		return nil, err
	}
	if isZeroPage(page.data) {
		return &CatalogData{}, nil
	}

	offset := 0
	version, ok := readUint32(page.data, &offset)
	if !ok || version != catalogVersion {
		return nil, errInvalidCatalog
	}
	tableCount, ok := readUint32(page.data, &offset)
	if !ok {
		return nil, errInvalidCatalog
	}

	cat := &CatalogData{Tables: make([]CatalogTable, 0, tableCount)}
	for i := uint32(0); i < tableCount; i++ {
		name, ok := readString(page.data, &offset)
		if !ok || name == "" {
			return nil, errInvalidCatalog
		}
		rootPageID, ok := readUint32(page.data, &offset)
		if !ok || rootPageID < 1 {
			return nil, errInvalidCatalog
		}
		rowCount, ok := readUint32(page.data, &offset)
		if !ok {
			return nil, errInvalidCatalog
		}
		columnCount, ok := readUint16(page.data, &offset)
		if !ok {
			return nil, errInvalidCatalog
		}

		table := CatalogTable{
			Name:       name,
			RootPageID: rootPageID,
			RowCount:   rowCount,
			Columns:    make([]CatalogColumn, 0, columnCount),
		}
		for j := uint16(0); j < columnCount; j++ {
			columnName, ok := readString(page.data, &offset)
			if !ok || columnName == "" {
				return nil, errInvalidCatalog
			}
			if offset >= len(page.data) {
				return nil, errInvalidCatalog
			}
			columnType := page.data[offset]
			offset++
			if columnType != CatalogColumnTypeInt && columnType != CatalogColumnTypeText {
				return nil, errInvalidCatalog
			}

			table.Columns = append(table.Columns, CatalogColumn{
				Name: columnName,
				Type: columnType,
			})
		}

		cat.Tables = append(cat.Tables, table)
	}

	return cat, nil
}

// SaveCatalog encodes the catalog into page 0.
func SaveCatalog(pager *Pager, cat *CatalogData) error {
	buf, err := BuildCatalogPageData(cat)
	if err != nil {
		return err
	}

	page, err := pager.Get(catalogPageID)
	if err != nil {
		return err
	}
	clear(page.data)
	copy(page.data, buf)
	page.MarkDirty()
	return nil
}

// BuildCatalogPageData encodes the catalog into a full catalog page image.
func BuildCatalogPageData(cat *CatalogData) ([]byte, error) {
	if cat == nil {
		cat = &CatalogData{}
	}

	buf := make([]byte, 0, PageSize)
	buf = appendUint32(buf, catalogVersion)
	buf = appendUint32(buf, uint32(len(cat.Tables)))

	for _, table := range cat.Tables {
		if table.Name == "" || table.RootPageID < 1 || len(table.Columns) == 0 {
			return nil, errInvalidCatalog
		}
		buf = appendString(buf, table.Name)
		buf = appendUint32(buf, table.RootPageID)
		buf = appendUint32(buf, table.RowCount)
		buf = appendUint16(buf, uint16(len(table.Columns)))

		for _, column := range table.Columns {
			if column.Name == "" {
				return nil, errInvalidCatalog
			}
			if column.Type != CatalogColumnTypeInt && column.Type != CatalogColumnTypeText {
				return nil, errInvalidCatalog
			}
			buf = appendString(buf, column.Name)
			buf = append(buf, column.Type)
			if len(buf) > PageSize {
				return nil, errCatalogTooLarge
			}
		}
		if len(buf) > PageSize {
			return nil, errCatalogTooLarge
		}
	}

	pageData := make([]byte, PageSize)
	copy(pageData, buf)
	return pageData, nil
}

func isZeroPage(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

func appendUint32(buf []byte, value uint32) []byte {
	var raw [4]byte
	binary.LittleEndian.PutUint32(raw[:], value)
	return append(buf, raw[:]...)
}

func appendUint16(buf []byte, value uint16) []byte {
	var raw [2]byte
	binary.LittleEndian.PutUint16(raw[:], value)
	return append(buf, raw[:]...)
}

func appendString(buf []byte, value string) []byte {
	buf = appendUint16(buf, uint16(len(value)))
	return append(buf, value...)
}

func readUint32(data []byte, offset *int) (uint32, bool) {
	if *offset+4 > len(data) {
		return 0, false
	}
	value := binary.LittleEndian.Uint32(data[*offset : *offset+4])
	*offset += 4
	return value, true
}

func readUint16(data []byte, offset *int) (uint16, bool) {
	if *offset+2 > len(data) {
		return 0, false
	}
	value := binary.LittleEndian.Uint16(data[*offset : *offset+2])
	*offset += 2
	return value, true
}

func readString(data []byte, offset *int) (string, bool) {
	length, ok := readUint16(data, offset)
	if !ok {
		return "", false
	}
	if *offset+int(length) > len(data) {
		return "", false
	}
	value := string(data[*offset : *offset+int(length)])
	*offset += int(length)
	return value, true
}
