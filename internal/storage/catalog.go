package storage

import (
	"encoding/binary"
)

const (
	catalogVersionV1 = 1
	catalogVersionV2 = 2
	catalogVersion   = 3
	catalogPageID    = 0

	CatalogColumnTypeInt  = 1
	CatalogColumnTypeText = 2
	CatalogColumnTypeBool = 3
	CatalogColumnTypeReal = 4
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
	Indexes    []CatalogIndex
}

// CatalogColumn is a persisted typed column entry.
type CatalogColumn struct {
	Name string
	Type uint8
}

// CatalogIndex is a persisted index definition.
type CatalogIndex struct {
	Name    string
	Unique  bool
	Columns []CatalogIndexColumn
}

// CatalogIndexColumn is a persisted indexed column entry.
type CatalogIndexColumn struct {
	Name string
	Desc bool
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
	if !ok || (version != catalogVersionV1 && version != catalogVersionV2 && version != catalogVersion) {
		return nil, errCorruptedCatalogPage
	}
	tableCount, ok := readUint32(page.data, &offset)
	if !ok {
		return nil, errCorruptedCatalogPage
	}

	cat := &CatalogData{Tables: make([]CatalogTable, 0, tableCount)}
	for i := uint32(0); i < tableCount; i++ {
		name, ok := readString(page.data, &offset)
		if !ok || name == "" {
			return nil, errCorruptedCatalogPage
		}
		rootPageID, ok := readUint32(page.data, &offset)
		if !ok || rootPageID < 1 {
			return nil, errCorruptedCatalogPage
		}
		rowCount, ok := readUint32(page.data, &offset)
		if !ok {
			return nil, errCorruptedCatalogPage
		}
		columnCount, ok := readUint16(page.data, &offset)
		if !ok {
			return nil, errCorruptedCatalogPage
		}

		table := CatalogTable{
			Name:       name,
			RootPageID: rootPageID,
			RowCount:   rowCount,
			Columns:    make([]CatalogColumn, 0, columnCount),
		}
		columnNames := make(map[string]struct{}, columnCount)
		for j := uint16(0); j < columnCount; j++ {
			columnName, ok := readString(page.data, &offset)
			if !ok || columnName == "" {
				return nil, errCorruptedCatalogPage
			}
			if offset >= len(page.data) {
				return nil, errCorruptedCatalogPage
			}
			columnType := page.data[offset]
			offset++
			if columnType != CatalogColumnTypeInt && columnType != CatalogColumnTypeText && columnType != CatalogColumnTypeBool && columnType != CatalogColumnTypeReal {
				return nil, errCorruptedCatalogPage
			}
			if _, exists := columnNames[columnName]; exists {
				return nil, errCorruptedCatalogPage
			}
			columnNames[columnName] = struct{}{}

			table.Columns = append(table.Columns, CatalogColumn{
				Name: columnName,
				Type: columnType,
			})
		}
		if version >= catalogVersionV2 {
			indexCount, ok := readUint16(page.data, &offset)
			if !ok {
				return nil, errCorruptedCatalogPage
			}
			table.Indexes = make([]CatalogIndex, 0, indexCount)
			indexNames := make(map[string]struct{}, indexCount)
			for j := uint16(0); j < indexCount; j++ {
				index, err := readCatalogIndex(page.data, &offset, version, columnNames, indexNames)
				if err != nil {
					return nil, err
				}
				table.Indexes = append(table.Indexes, index)
			}
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
	pager.MarkDirtyWithOriginal(page)
	clear(page.data)
	copy(page.data, buf)
	// Catalog mutation requires explicit dirty marking; later flush eligibility
	// is driven by dirty tracking rather than implicit full flushes.
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
			return nil, errCorruptedCatalogPage
		}
		buf = appendString(buf, table.Name)
		buf = appendUint32(buf, table.RootPageID)
		buf = appendUint32(buf, table.RowCount)
		buf = appendUint16(buf, uint16(len(table.Columns)))

		for _, column := range table.Columns {
			if column.Name == "" {
				return nil, errCorruptedCatalogPage
			}
			if column.Type != CatalogColumnTypeInt && column.Type != CatalogColumnTypeText && column.Type != CatalogColumnTypeBool && column.Type != CatalogColumnTypeReal {
				return nil, errCorruptedCatalogPage
			}
			buf = appendString(buf, column.Name)
			buf = append(buf, column.Type)
			if len(buf) > PageSize {
				return nil, errCatalogTooLarge
			}
		}
		buf = appendUint16(buf, uint16(len(table.Indexes)))
		for _, index := range table.Indexes {
			var err error
			buf, err = appendCatalogIndex(buf, index, table.Columns)
			if err != nil {
				return nil, err
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

func readCatalogIndex(data []byte, offset *int, version uint32, columnNames map[string]struct{}, indexNames map[string]struct{}) (CatalogIndex, error) {
	if version == catalogVersionV2 {
		columnName, ok := readString(data, offset)
		if !ok || columnName == "" {
			return CatalogIndex{}, errCorruptedCatalogPage
		}
		if _, exists := columnNames[columnName]; !exists {
			return CatalogIndex{}, errCorruptedIndexMetadata
		}
		if _, exists := indexNames[columnName]; exists {
			return CatalogIndex{}, errCorruptedIndexMetadata
		}
		indexNames[columnName] = struct{}{}
		return CatalogIndex{
			Name:   columnName,
			Unique: false,
			Columns: []CatalogIndexColumn{
				{Name: columnName},
			},
		}, nil
	}

	name, ok := readString(data, offset)
	if !ok || name == "" {
		return CatalogIndex{}, errCorruptedCatalogPage
	}
	if _, exists := indexNames[name]; exists {
		return CatalogIndex{}, errCorruptedIndexMetadata
	}
	if *offset >= len(data) {
		return CatalogIndex{}, errCorruptedCatalogPage
	}
	unique := data[*offset] != 0
	*offset++
	columnCount, ok := readUint16(data, offset)
	if !ok || columnCount == 0 {
		return CatalogIndex{}, errCorruptedIndexMetadata
	}
	columns := make([]CatalogIndexColumn, 0, columnCount)
	seenColumns := make(map[string]struct{}, columnCount)
	for i := uint16(0); i < columnCount; i++ {
		columnName, ok := readString(data, offset)
		if !ok || columnName == "" {
			return CatalogIndex{}, errCorruptedCatalogPage
		}
		if _, exists := columnNames[columnName]; !exists {
			return CatalogIndex{}, errCorruptedIndexMetadata
		}
		if _, exists := seenColumns[columnName]; exists {
			return CatalogIndex{}, errCorruptedIndexMetadata
		}
		seenColumns[columnName] = struct{}{}
		if *offset >= len(data) {
			return CatalogIndex{}, errCorruptedCatalogPage
		}
		desc := data[*offset] != 0
		*offset++
		columns = append(columns, CatalogIndexColumn{Name: columnName, Desc: desc})
	}
	indexNames[name] = struct{}{}
	return CatalogIndex{Name: name, Unique: unique, Columns: columns}, nil
}

func appendCatalogIndex(buf []byte, index CatalogIndex, columns []CatalogColumn) ([]byte, error) {
	if index.Name == "" || len(index.Columns) == 0 {
		return nil, errCorruptedIndexMetadata
	}
	validColumns := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		validColumns[column.Name] = struct{}{}
	}
	seenColumns := make(map[string]struct{}, len(index.Columns))

	buf = appendString(buf, index.Name)
	if index.Unique {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = appendUint16(buf, uint16(len(index.Columns)))
	for _, column := range index.Columns {
		if column.Name == "" {
			return nil, errCorruptedIndexMetadata
		}
		if _, exists := validColumns[column.Name]; !exists {
			return nil, errCorruptedIndexMetadata
		}
		if _, exists := seenColumns[column.Name]; exists {
			return nil, errCorruptedIndexMetadata
		}
		seenColumns[column.Name] = struct{}{}
		buf = appendString(buf, column.Name)
		if column.Desc {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
		if len(buf) > PageSize {
			return nil, errCatalogTooLarge
		}
	}
	return buf, nil
}
