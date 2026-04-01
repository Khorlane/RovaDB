package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

const (
	// DirectoryControlPageID remains page 0 so existing catalog/root-page numbering stays intact.
	DirectoryControlPageID PageID = catalogPageID

	directoryPageHeaderSize = 32

	directoryBodyOffsetFormatVersion = directoryPageHeaderSize
	directoryBodyOffsetFreeListHead  = directoryPageHeaderSize + 4
	directoryBodyOffsetRootMapCount  = directoryPageHeaderSize + 8
	directoryBodyOffsetRootMapBytes  = directoryPageHeaderSize + 12
	directoryCatalogOffset           = directoryPageHeaderSize + 16
)

const (
	DirectoryRootMappingObjectTable uint8 = 1 + iota
	DirectoryRootMappingObjectIndex
)

// DirectoryRootMapping is the durable physical root-page mapping entry.
type DirectoryRootMapping struct {
	ObjectType uint8
	TableName  string
	IndexName  string
	RootPageID uint32
}

// InitDirectoryPage initializes the durable directory/control page.
func InitDirectoryPage(pageID uint32, formatVersion uint32) []byte {
	page := make([]byte, PageSize)
	binary.LittleEndian.PutUint32(page[pageHeaderOffsetPageID:pageHeaderOffsetPageID+4], pageID)
	binary.LittleEndian.PutUint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2], uint16(PageTypeDirectory))
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetFormatVersion:directoryBodyOffsetFormatVersion+4], formatVersion)
	return page
}

// DirectoryFormatVersion returns the durable directory format version.
func DirectoryFormatVersion(page []byte) (uint32, error) {
	if err := ValidateDirectoryPage(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(page[directoryBodyOffsetFormatVersion : directoryBodyOffsetFormatVersion+4]), nil
}

// SetDirectoryFormatVersion updates the durable directory format version.
func SetDirectoryFormatVersion(page []byte, formatVersion uint32) error {
	if err := ValidateDirectoryPage(page); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetFormatVersion:directoryBodyOffsetFormatVersion+4], formatVersion)
	return nil
}

// DirectoryFreeListHead returns the durable free-list head pointer.
func DirectoryFreeListHead(page []byte) (uint32, error) {
	if err := ValidateDirectoryPage(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(page[directoryBodyOffsetFreeListHead : directoryBodyOffsetFreeListHead+4]), nil
}

// SetDirectoryFreeListHead updates the durable free-list head pointer.
func SetDirectoryFreeListHead(page []byte, head uint32) error {
	if err := ValidateDirectoryPage(page); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetFreeListHead:directoryBodyOffsetFreeListHead+4], head)
	return nil
}

// ValidateDirectoryPage validates the shared header and fixed directory body.
func ValidateDirectoryPage(page []byte) error {
	if len(page) != PageSize {
		return errCorruptedDirectoryPage
	}
	if binary.LittleEndian.Uint32(page[pageHeaderOffsetPageID:pageHeaderOffsetPageID+4]) != uint32(DirectoryControlPageID) {
		return errCorruptedDirectoryPage
	}
	if PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2])) != PageTypeDirectory {
		return errCorruptedDirectoryPage
	}
	formatVersion := binary.LittleEndian.Uint32(page[directoryBodyOffsetFormatVersion : directoryBodyOffsetFormatVersion+4])
	if formatVersion != version {
		return errCorruptedDirectoryPage
	}
	rootMapBytes := binary.LittleEndian.Uint32(page[directoryBodyOffsetRootMapBytes : directoryBodyOffsetRootMapBytes+4])
	if directoryCatalogOffset+int(rootMapBytes) > PageSize {
		return errCorruptedDirectoryPage
	}
	return nil
}

// EnsureDirectoryPage initializes or upgrades the durable directory page in-place.
func EnsureDirectoryPage(file *os.File) error {
	if file == nil {
		return errCorruptedDirectoryPage
	}

	page, n, err := readDirectoryPage(file)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	if n == 0 || isZeroPage(page) {
		return writeDirectoryPage(file, InitDirectoryPage(uint32(DirectoryControlPageID), version))
	}
	if ValidateDirectoryPage(page) == nil {
		return nil
	}

	cat, err := loadCatalogPayload(page)
	if err != nil {
		return err
	}
	upgraded, err := BuildCatalogPageData(cat)
	if err != nil {
		return err
	}
	return writeDirectoryPage(file, upgraded)
}

// ReadDirectoryPage reads and validates the durable directory page.
func ReadDirectoryPage(file *os.File) ([]byte, error) {
	page, _, err := readDirectoryPage(file)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	if err := ValidateDirectoryPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// WriteDirectoryPage rewrites the durable directory page in place.
func WriteDirectoryPage(file *os.File, page []byte) error {
	if err := ValidateDirectoryPage(page); err != nil {
		return err
	}
	return writeDirectoryPage(file, page)
}

// ReadDirectoryFreeListHead reads the durable free-list head from the directory page.
func ReadDirectoryFreeListHead(file *os.File) (uint32, error) {
	page, err := ReadDirectoryPage(file)
	if err != nil {
		return 0, err
	}
	return DirectoryFreeListHead(page)
}

// WriteDirectoryFreeListHead persists the durable free-list head in the directory page.
func WriteDirectoryFreeListHead(file *os.File, head uint32) error {
	page, err := ReadDirectoryPage(file)
	if err != nil {
		return err
	}
	if err := SetDirectoryFreeListHead(page, head); err != nil {
		return err
	}
	return WriteDirectoryPage(file, page)
}

// ReadDirectoryRootMappings reads the durable root mappings from the directory page.
func ReadDirectoryRootMappings(file *os.File) ([]DirectoryRootMapping, error) {
	page, err := ReadDirectoryPage(file)
	if err != nil {
		return nil, err
	}
	return directoryRootMappings(page)
}

// WriteDirectoryRootMappings rewrites the durable root mappings while preserving other directory state.
func WriteDirectoryRootMappings(file *os.File, mappings []DirectoryRootMapping) error {
	page, err := ReadDirectoryPage(file)
	if err != nil {
		return err
	}
	catalogPayload, err := directoryCatalogPayload(page)
	if err != nil {
		return err
	}
	freeListHead, err := DirectoryFreeListHead(page)
	if err != nil {
		return err
	}
	rebuilt, err := buildDirectoryCatalogPage(catalogPayload, version, freeListHead, mappings)
	if err != nil {
		return err
	}
	return WriteDirectoryPage(file, rebuilt)
}

// BuildDirectoryRootMappings derives the durable physical root mappings from catalog metadata.
func BuildDirectoryRootMappings(cat *CatalogData) []DirectoryRootMapping {
	if cat == nil || len(cat.Tables) == 0 {
		return nil
	}

	mappings := make([]DirectoryRootMapping, 0, len(cat.Tables))
	for _, table := range cat.Tables {
		if table.RootPageID != 0 {
			mappings = append(mappings, DirectoryRootMapping{
				ObjectType: DirectoryRootMappingObjectTable,
				TableName:  table.Name,
				RootPageID: table.RootPageID,
			})
		}
		for _, index := range table.Indexes {
			if index.RootPageID == 0 {
				continue
			}
			mappings = append(mappings, DirectoryRootMapping{
				ObjectType: DirectoryRootMappingObjectIndex,
				TableName:  table.Name,
				IndexName:  index.Name,
				RootPageID: index.RootPageID,
			})
		}
	}
	return mappings
}

// ApplyDirectoryRootMappings overlays directory-owned physical roots onto catalog metadata.
func ApplyDirectoryRootMappings(cat *CatalogData, mappings []DirectoryRootMapping) (*CatalogData, error) {
	if cat == nil || len(mappings) == 0 {
		return cat, nil
	}

	tableMappings := make(map[string]uint32)
	indexMappings := make(map[string]uint32)
	for _, mapping := range mappings {
		switch mapping.ObjectType {
		case DirectoryRootMappingObjectTable:
			if mapping.TableName == "" || mapping.IndexName != "" || mapping.RootPageID == 0 {
				return nil, errCorruptedDirectoryPage
			}
			if _, exists := tableMappings[mapping.TableName]; exists {
				return nil, errCorruptedDirectoryPage
			}
			tableMappings[mapping.TableName] = mapping.RootPageID
		case DirectoryRootMappingObjectIndex:
			if mapping.TableName == "" || mapping.IndexName == "" || mapping.RootPageID == 0 {
				return nil, errCorruptedDirectoryPage
			}
			key := mapping.TableName + "\x00" + mapping.IndexName
			if _, exists := indexMappings[key]; exists {
				return nil, errCorruptedDirectoryPage
			}
			indexMappings[key] = mapping.RootPageID
		default:
			return nil, errCorruptedDirectoryPage
		}
	}

	applied := &CatalogData{Tables: make([]CatalogTable, 0, len(cat.Tables))}
	for _, table := range cat.Tables {
		cloned := CatalogTable{
			Name:       table.Name,
			RootPageID: table.RootPageID,
			RowCount:   table.RowCount,
			Columns:    append([]CatalogColumn(nil), table.Columns...),
			Indexes:    make([]CatalogIndex, 0, len(table.Indexes)),
		}
		mappedRootPageID, hasTableMapping := tableMappings[table.Name]
		if hasTableMapping {
			if cloned.RootPageID != mappedRootPageID {
				return nil, errCorruptedDirectoryPage
			}
			cloned.RootPageID = mappedRootPageID
			delete(tableMappings, table.Name)
		}

		for _, index := range table.Indexes {
			clonedIndex := CatalogIndex{
				Name:       index.Name,
				Unique:     index.Unique,
				RootPageID: index.RootPageID,
				Columns:    append([]CatalogIndexColumn(nil), index.Columns...),
			}
			key := table.Name + "\x00" + index.Name
			mappedRootPageID, hasIndexMapping := indexMappings[key]
			if hasIndexMapping {
				if clonedIndex.RootPageID != mappedRootPageID {
					return nil, errCorruptedDirectoryPage
				}
				clonedIndex.RootPageID = mappedRootPageID
				delete(indexMappings, key)
			}
			cloned.Indexes = append(cloned.Indexes, clonedIndex)
		}
		applied.Tables = append(applied.Tables, cloned)
	}

	if len(tableMappings) != 0 || len(indexMappings) != 0 {
		return nil, errCorruptedDirectoryPage
	}
	return applied, nil
}

func buildDirectoryCatalogPage(catalogPayload []byte, formatVersion uint32, freeListHead uint32, mappings []DirectoryRootMapping) ([]byte, error) {
	rootMapPayload, err := encodeDirectoryRootMappings(mappings)
	if err != nil {
		return nil, err
	}
	if len(catalogPayload)+len(rootMapPayload) > PageSize-directoryCatalogOffset {
		return nil, errCatalogTooLarge
	}
	page := InitDirectoryPage(uint32(DirectoryControlPageID), formatVersion)
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetFreeListHead:directoryBodyOffsetFreeListHead+4], freeListHead)
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetRootMapCount:directoryBodyOffsetRootMapCount+4], uint32(len(mappings)))
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetRootMapBytes:directoryBodyOffsetRootMapBytes+4], uint32(len(rootMapPayload)))
	copy(page[directoryCatalogOffset:], rootMapPayload)
	copy(page[directoryCatalogOffset+len(rootMapPayload):], catalogPayload)
	return page, nil
}

func directoryCatalogPayload(page []byte) ([]byte, error) {
	if err := ValidateDirectoryPage(page); err != nil {
		return nil, err
	}
	rootMapBytes := binary.LittleEndian.Uint32(page[directoryBodyOffsetRootMapBytes : directoryBodyOffsetRootMapBytes+4])
	start := directoryCatalogOffset + int(rootMapBytes)
	length, _, err := decodeCatalogPayload(page[start:])
	if err != nil {
		return nil, err
	}
	return page[start : start+length], nil
}

func writeDirectoryPage(file *os.File, page []byte) error {
	if _, err := file.WriteAt(page, pageOffset(DirectoryControlPageID)); err != nil {
		return err
	}
	return file.Sync()
}

func readDirectoryPage(file *os.File) ([]byte, int, error) {
	page := make([]byte, PageSize)
	n, err := file.ReadAt(page, pageOffset(DirectoryControlPageID))
	return page, n, err
}

func directoryRootMappings(page []byte) ([]DirectoryRootMapping, error) {
	if err := ValidateDirectoryPage(page); err != nil {
		return nil, err
	}

	rootMapCount := binary.LittleEndian.Uint32(page[directoryBodyOffsetRootMapCount : directoryBodyOffsetRootMapCount+4])
	rootMapBytes := binary.LittleEndian.Uint32(page[directoryBodyOffsetRootMapBytes : directoryBodyOffsetRootMapBytes+4])
	if rootMapCount == 0 && rootMapBytes == 0 {
		return nil, nil
	}
	if rootMapCount == 0 || rootMapBytes == 0 {
		return nil, errCorruptedDirectoryPage
	}

	payload := page[directoryCatalogOffset : directoryCatalogOffset+int(rootMapBytes)]
	mappings := make([]DirectoryRootMapping, 0, rootMapCount)
	offset := 0
	for i := uint32(0); i < rootMapCount; i++ {
		if offset >= len(payload) {
			return nil, errCorruptedDirectoryPage
		}
		objectType := payload[offset]
		offset++
		tableName, ok := readString(payload, &offset)
		if !ok || tableName == "" {
			return nil, errCorruptedDirectoryPage
		}
		indexName, ok := readString(payload, &offset)
		if !ok {
			return nil, errCorruptedDirectoryPage
		}
		rootPageID, ok := readUint32(payload, &offset)
		if !ok || rootPageID == 0 {
			return nil, errCorruptedDirectoryPage
		}
		mapping := DirectoryRootMapping{
			ObjectType: objectType,
			TableName:  tableName,
			IndexName:  indexName,
			RootPageID: rootPageID,
		}
		switch objectType {
		case DirectoryRootMappingObjectTable:
			if indexName != "" {
				return nil, errCorruptedDirectoryPage
			}
		case DirectoryRootMappingObjectIndex:
			if indexName == "" {
				return nil, errCorruptedDirectoryPage
			}
		default:
			return nil, errCorruptedDirectoryPage
		}
		mappings = append(mappings, mapping)
	}
	if offset != len(payload) {
		return nil, errCorruptedDirectoryPage
	}
	return mappings, nil
}

func encodeDirectoryRootMappings(mappings []DirectoryRootMapping) ([]byte, error) {
	if len(mappings) == 0 {
		return nil, nil
	}

	buf := make([]byte, 0, len(mappings)*16)
	for _, mapping := range mappings {
		if mapping.TableName == "" || mapping.RootPageID == 0 {
			return nil, errCorruptedDirectoryPage
		}
		switch mapping.ObjectType {
		case DirectoryRootMappingObjectTable:
			if mapping.IndexName != "" {
				return nil, errCorruptedDirectoryPage
			}
		case DirectoryRootMappingObjectIndex:
			if mapping.IndexName == "" {
				return nil, errCorruptedDirectoryPage
			}
		default:
			return nil, errCorruptedDirectoryPage
		}
		buf = append(buf, mapping.ObjectType)
		buf = appendString(buf, mapping.TableName)
		buf = appendString(buf, mapping.IndexName)
		buf = appendUint32(buf, mapping.RootPageID)
	}
	return buf, nil
}
