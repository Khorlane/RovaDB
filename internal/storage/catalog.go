package storage

import (
	"encoding/binary"
)

const (
	catalogVersionV1 = 1
	catalogVersionV2 = 2
	catalogVersionV3 = 3
	catalogVersion   = 4
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
	Name       string
	Unique     bool
	RootPageID uint32
	Columns    []CatalogIndexColumn
}

// CatalogIndexColumn is a persisted indexed column entry.
type CatalogIndexColumn struct {
	Name string
	Desc bool
}

// PageReader provides durable page reads without exposing pager internals.
type PageReader interface {
	ReadPage(pageID PageID) ([]byte, error)
}

// PageReaderFunc adapts a function to PageReader.
type PageReaderFunc func(PageID) ([]byte, error)

func (f PageReaderFunc) ReadPage(pageID PageID) ([]byte, error) {
	return f(pageID)
}

// LoadCatalog decodes the catalog stored in page 0.
func LoadCatalog(reader PageReader) (*CatalogData, error) {
	pageData, err := reader.ReadPage(catalogPageID)
	if err != nil {
		return nil, err
	}
	return LoadCatalogPageData(pageData)
}

// LoadCatalogPageData decodes a catalog page image.
func LoadCatalogPageData(pageData []byte) (*CatalogData, error) {
	if ValidateDirectoryPage(pageData) == nil {
		payload, err := directoryCatalogPayload(pageData)
		if err != nil {
			return nil, err
		}
		return loadCatalogPayload(payload)
	}
	return loadCatalogPayload(pageData)
}

func loadCatalogPayload(pageData []byte) (*CatalogData, error) {
	_, cat, err := decodeCatalogPayload(pageData)
	return cat, err
}

func decodeCatalogPayload(pageData []byte) (int, *CatalogData, error) {
	if isZeroPage(pageData) {
		return 0, &CatalogData{}, nil
	}

	offset := 0
	version, ok := readUint32(pageData, &offset)
	if !ok || (version != catalogVersionV1 && version != catalogVersionV2 && version != catalogVersionV3 && version != catalogVersion) {
		return 0, nil, errCorruptedCatalogPage
	}
	tableCount, ok := readUint32(pageData, &offset)
	if !ok {
		return 0, nil, errCorruptedCatalogPage
	}

	cat := &CatalogData{Tables: make([]CatalogTable, 0, tableCount)}
	for i := uint32(0); i < tableCount; i++ {
		name, ok := readString(pageData, &offset)
		if !ok || name == "" {
			return 0, nil, errCorruptedCatalogPage
		}
		rootPageID, ok := readUint32(pageData, &offset)
		if !ok || rootPageID < 1 {
			return 0, nil, errCorruptedCatalogPage
		}
		rowCount, ok := readUint32(pageData, &offset)
		if !ok {
			return 0, nil, errCorruptedCatalogPage
		}
		columnCount, ok := readUint16(pageData, &offset)
		if !ok {
			return 0, nil, errCorruptedCatalogPage
		}

		table := CatalogTable{
			Name:       name,
			RootPageID: rootPageID,
			RowCount:   rowCount,
			Columns:    make([]CatalogColumn, 0, columnCount),
		}
		columnNames := make(map[string]struct{}, columnCount)
		for j := uint16(0); j < columnCount; j++ {
			columnName, ok := readString(pageData, &offset)
			if !ok || columnName == "" {
				return 0, nil, errCorruptedCatalogPage
			}
			if offset >= len(pageData) {
				return 0, nil, errCorruptedCatalogPage
			}
			columnType := pageData[offset]
			offset++
			if columnType != CatalogColumnTypeInt && columnType != CatalogColumnTypeText && columnType != CatalogColumnTypeBool && columnType != CatalogColumnTypeReal {
				return 0, nil, errCorruptedCatalogPage
			}
			if _, exists := columnNames[columnName]; exists {
				return 0, nil, errCorruptedCatalogPage
			}
			columnNames[columnName] = struct{}{}

			table.Columns = append(table.Columns, CatalogColumn{
				Name: columnName,
				Type: columnType,
			})
		}
		if version >= catalogVersionV2 {
			indexCount, ok := readUint16(pageData, &offset)
			if !ok {
				return 0, nil, errCorruptedCatalogPage
			}
			table.Indexes = make([]CatalogIndex, 0, indexCount)
			indexNames := make(map[string]struct{}, indexCount)
			for j := uint16(0); j < indexCount; j++ {
				index, err := readCatalogIndex(pageData, &offset, version, columnNames, indexNames)
				if err != nil {
					return 0, nil, err
				}
				table.Indexes = append(table.Indexes, index)
			}
		}

		cat.Tables = append(cat.Tables, table)
	}

	return offset, cat, nil
}

// SaveCatalog encodes the catalog into page 0.
func SaveCatalog(pager *Pager, cat *CatalogData) error {
	page, err := pager.Get(catalogPageID)
	if err != nil {
		return err
	}
	freeListHead := uint32(0)
	checkpointMeta := DirectoryCheckpointMetadata{}
	if ValidateDirectoryPage(page.Data()) == nil {
		freeListHead, err = DirectoryFreeListHead(page.Data())
		if err != nil {
			return err
		}
		checkpointMeta, err = directoryCheckpointMetadata(page.Data())
		if err != nil {
			return err
		}
	}
	buf, err := BuildCatalogPageDataWithDirectoryState(cat, freeListHead, checkpointMeta)
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
	return BuildCatalogPageDataWithDirectoryState(cat, 0, DirectoryCheckpointMetadata{})
}

// BuildCatalogPageDataWithFreeListHead encodes the catalog into the directory-wrapped page 0 image.
func BuildCatalogPageDataWithFreeListHead(cat *CatalogData, freeListHead uint32) ([]byte, error) {
	return BuildCatalogPageDataWithDirectoryState(cat, freeListHead, DirectoryCheckpointMetadata{})
}

// BuildCatalogPageDataWithDirectoryState encodes the wrapped page 0 image with directory state.
func BuildCatalogPageDataWithDirectoryState(cat *CatalogData, freeListHead uint32, checkpointMeta DirectoryCheckpointMetadata) ([]byte, error) {
	if cat == nil {
		cat = &CatalogData{}
	}
	rootMappings := BuildDirectoryRootMappings(cat)
	rootMapPayload, err := encodeDirectoryRootMappings(rootMappings)
	if err != nil {
		return nil, err
	}

	maxCatalogPayloadSize := PageSize - directoryCatalogOffset - len(rootMapPayload) - directoryCheckpointMetadataSize
	buf := make([]byte, 0, maxCatalogPayloadSize)
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
			if len(buf) > maxCatalogPayloadSize {
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
		if len(buf) > maxCatalogPayloadSize {
			return nil, errCatalogTooLarge
		}
	}

	return buildDirectoryCatalogPage(buf, version, freeListHead, rootMappings, checkpointMeta)
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
			Name:       columnName,
			Unique:     false,
			RootPageID: 0,
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
	rootPageID := uint32(0)
	if version >= catalogVersion {
		var ok bool
		rootPageID, ok = readUint32(data, offset)
		if !ok {
			return CatalogIndex{}, errCorruptedCatalogPage
		}
	}
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
	return CatalogIndex{Name: name, Unique: unique, RootPageID: rootPageID, Columns: columns}, nil
}

func appendCatalogIndex(buf []byte, index CatalogIndex, columns []CatalogColumn) ([]byte, error) {
	if index.Name == "" || len(index.Columns) == 0 {
		return nil, errCorruptedIndexMetadata
	}
	maxCatalogPayloadSize := PageSize - directoryCatalogOffset
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
	buf = appendUint32(buf, index.RootPageID)
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
		if len(buf) > maxCatalogPayloadSize {
			return nil, errCatalogTooLarge
		}
	}
	return buf, nil
}
