package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
)

const (
	// DirectoryControlPageID remains page 0 so existing catalog/root-page numbering stays intact.
	DirectoryControlPageID PageID = catalogPageID

	directoryPageHeaderSize = 32

	directoryBodyOffsetFormatVersion         = directoryPageHeaderSize
	directoryBodyOffsetFreeListHead          = directoryPageHeaderSize + 4
	directoryBodyOffsetCATDIRStorageMode     = directoryPageHeaderSize + 8
	directoryBodyOffsetCATDIROverflowHead    = directoryPageHeaderSize + 12
	directoryBodyOffsetCATDIROverflowCount   = directoryPageHeaderSize + 16
	directoryBodyOffsetCATDIRPayloadByteSize = directoryPageHeaderSize + 20
	directoryCatalogOffset                   = directoryPageHeaderSize + 24
	legacyDirectoryCatalogOffset             = directoryPageHeaderSize + 16
	directoryCheckpointMetadataSize          = 16
	directoryRootIDTrailerHeaderSize         = 8
)

const (
	DirectoryRootMappingObjectTable uint8 = 1 + iota
	DirectoryRootMappingObjectIndex
)

const (
	DirectoryCATDIRStorageModeEmbedded uint32 = iota
	DirectoryCATDIRStorageModeOverflow
)

// DirectoryRootIDMapping is the durable physical root-page mapping keyed by stable logical ID.
type DirectoryRootIDMapping struct {
	ObjectType uint8
	ObjectID   uint32
	RootPageID uint32
}

// DirectoryCheckpointMetadata is the durable checkpoint control state.
type DirectoryCheckpointMetadata struct {
	LastCheckpointLSN       uint64
	LastCheckpointPageCount uint32
	ReservedCheckpoint      uint32
}

// DirectoryControlState is the decoded durable control-plane state from page 0.
type DirectoryControlState struct {
	FreeListHead        uint32
	CATDIRStorageMode   uint32
	CATDIROverflowHead  uint32
	CATDIROverflowCount uint32
	CATDIRPayloadBytes  uint32
	RootIDMappings      []DirectoryRootIDMapping
	CheckpointMeta      DirectoryCheckpointMetadata
}

// InitDirectoryPage initializes the durable directory/control page.
func InitDirectoryPage(pageID uint32, formatVersion uint32) []byte {
	page := make([]byte, PageSize)
	binary.LittleEndian.PutUint32(page[pageHeaderOffsetPageID:pageHeaderOffsetPageID+4], pageID)
	binary.LittleEndian.PutUint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2], uint16(PageTypeDirectory))
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetFormatVersion:directoryBodyOffsetFormatVersion+4], formatVersion)
	_ = FinalizePageImage(page)
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

// DirectoryCATDIRStorageMode returns the durable CAT/DIR storage mode.
func DirectoryCATDIRStorageMode(page []byte) (uint32, error) {
	control, err := directoryCATDIRControl(page)
	if err != nil {
		return 0, err
	}
	return control.mode, nil
}

// DirectoryCATDIROverflowHeadPageID returns the durable CAT/DIR overflow head page ID.
func DirectoryCATDIROverflowHeadPageID(page []byte) (uint32, error) {
	control, err := directoryCATDIRControl(page)
	if err != nil {
		return 0, err
	}
	return control.overflowHeadPageID, nil
}

// DirectoryCATDIROverflowPageCount returns the durable CAT/DIR overflow page count.
func DirectoryCATDIROverflowPageCount(page []byte) (uint32, error) {
	control, err := directoryCATDIRControl(page)
	if err != nil {
		return 0, err
	}
	return control.overflowPageCount, nil
}

// DirectoryCATDIRPayloadByteLength returns the durable CAT/DIR payload byte length.
func DirectoryCATDIRPayloadByteLength(page []byte) (uint32, error) {
	control, err := directoryCATDIRControl(page)
	if err != nil {
		return 0, err
	}
	return control.payloadByteLength, nil
}

// ValidateDirectoryPage validates the shared header and fixed directory body.
func ValidateDirectoryPage(page []byte) error {
	if err := validateDirectoryPageHeader(page); err != nil {
		return err
	}
	_, err := directoryCATDIRControl(page)
	return err
}

// EnsureDirectoryPage initializes the durable directory page in-place.
func EnsureDirectoryPage(file *os.File) error {
	if file == nil {
		return errCorruptedDirectoryPage
	}

	page, n, err := readDirectoryPage(file)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	if n == 0 || isZeroPage(page) {
		return writeDirectoryPage(file, InitDirectoryPage(uint32(DirectoryControlPageID), CurrentDBFormatVersion))
	}
	if err := ValidateDirectoryPage(page); err == nil {
		return nil
	} else if looksLikeWrappedDirectoryPage(page) {
		return err
	}
	return errCorruptedDirectoryPage
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
	if err := FinalizePageImage(page); err != nil {
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

// DirectoryLastCheckpointLSN returns the durable last checkpoint LSN.
func DirectoryLastCheckpointLSN(page []byte) (uint64, error) {
	meta, err := directoryCheckpointMetadata(page)
	if err != nil {
		return 0, err
	}
	return meta.LastCheckpointLSN, nil
}

// SetDirectoryLastCheckpointLSN updates the durable last checkpoint LSN.
func SetDirectoryLastCheckpointLSN(page []byte, lsn uint64) error {
	meta, err := directoryCheckpointMetadata(page)
	if err != nil {
		return err
	}
	meta.LastCheckpointLSN = lsn
	return rewriteDirectoryCheckpointMetadata(page, meta)
}

// DirectoryLastCheckpointPageCount returns the durable last checkpoint page count.
func DirectoryLastCheckpointPageCount(page []byte) (uint32, error) {
	meta, err := directoryCheckpointMetadata(page)
	if err != nil {
		return 0, err
	}
	return meta.LastCheckpointPageCount, nil
}

// SetDirectoryLastCheckpointPageCount updates the durable last checkpoint page count.
func SetDirectoryLastCheckpointPageCount(page []byte, n uint32) error {
	meta, err := directoryCheckpointMetadata(page)
	if err != nil {
		return err
	}
	meta.LastCheckpointPageCount = n
	return rewriteDirectoryCheckpointMetadata(page, meta)
}

// ReadDirectoryCheckpointMetadata reads the durable checkpoint metadata from the directory page.
func ReadDirectoryCheckpointMetadata(file *os.File) (DirectoryCheckpointMetadata, error) {
	page, err := ReadDirectoryPage(file)
	if err != nil {
		return DirectoryCheckpointMetadata{}, err
	}
	return directoryCheckpointMetadata(page)
}

// ReadDirectoryRootIDMappings reads the durable ID-based root mappings from the directory page.
func ReadDirectoryRootIDMappings(file *os.File) ([]DirectoryRootIDMapping, error) {
	page, err := ReadDirectoryPage(file)
	if err != nil {
		return nil, err
	}
	return directoryRootIDMappings(page)
}

// ValidateDirectoryControlState validates the currently loaded directory/control metadata against disk pages.
func ValidateDirectoryControlState(file *os.File, state DirectoryControlState) error {
	if file == nil {
		return errCorruptedDirectoryPage
	}
	if state.CATDIRStorageMode == DirectoryCATDIRStorageModeEmbedded {
		if state.CATDIROverflowHead != 0 || state.CATDIROverflowCount != 0 {
			return errCorruptedDirectoryPage
		}
	} else if state.CATDIRStorageMode == DirectoryCATDIRStorageModeOverflow {
		return errUnsupportedDirectoryPage
	} else if state.CATDIRStorageMode != 0 {
		return errCorruptedDirectoryPage
	}
	if state.FreeListHead != 0 {
		page, err := readStoragePage(file, PageID(state.FreeListHead))
		if err != nil {
			return err
		}
		pageType := PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType : pageHeaderOffsetPageType+2]))
		if IsValidPageType(pageType) && pageType != PageTypeFreePage {
			return errCorruptedDirectoryPage
		}
	}

	seenTableIDMappings := make(map[uint32]struct{}, len(state.RootIDMappings))
	seenIndexIDMappings := make(map[uint32]struct{}, len(state.RootIDMappings))
	for _, mapping := range state.RootIDMappings {
		if mapping.ObjectID == 0 || mapping.RootPageID == 0 {
			return errCorruptedDirectoryPage
		}
		if mapping.RootPageID == uint32(DirectoryControlPageID) {
			return errCorruptedDirectoryPage
		}

		page, err := readStoragePage(file, PageID(mapping.RootPageID))
		if err != nil {
			return err
		}
		pageType := PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType : pageHeaderOffsetPageType+2]))

		switch mapping.ObjectType {
		case DirectoryRootMappingObjectTable:
			if IsValidPageType(pageType) && pageType != PageTypeTable {
				return errCorruptedTablePage
			}
			if _, exists := seenTableIDMappings[mapping.ObjectID]; exists {
				return errCorruptedDirectoryPage
			}
			seenTableIDMappings[mapping.ObjectID] = struct{}{}
		case DirectoryRootMappingObjectIndex:
			if IsValidPageType(pageType) && pageType != PageTypeIndexLeaf && pageType != PageTypeIndexInternal {
				return errCorruptedIndexPage
			}
			if _, exists := seenIndexIDMappings[mapping.ObjectID]; exists {
				return errCorruptedDirectoryPage
			}
			seenIndexIDMappings[mapping.ObjectID] = struct{}{}
		default:
			return errCorruptedDirectoryPage
		}
	}

	if state.CheckpointMeta.LastCheckpointPageCount == 0 && state.CheckpointMeta.ReservedCheckpoint != 0 {
		return errCorruptedDirectoryPage
	}
	return nil
}

// WriteDirectoryRootIDMappings rewrites the durable ID-based root mappings while preserving other directory state.
func WriteDirectoryRootIDMappings(file *os.File, mappings []DirectoryRootIDMapping) error {
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
	checkpointMeta, err := directoryCheckpointMetadata(page)
	if err != nil {
		return err
	}
	rebuilt, err := buildDirectoryCatalogPage(catalogPayload, CurrentDBFormatVersion, freeListHead, mappings, checkpointMeta)
	if err != nil {
		return err
	}
	return WriteDirectoryPage(file, rebuilt)
}

// BuildDirectoryRootIDMappings derives the durable physical root mappings keyed by stable logical IDs.
func BuildDirectoryRootIDMappings(cat *CatalogData) []DirectoryRootIDMapping {
	if cat == nil || len(cat.Tables) == 0 {
		return nil
	}

	type tableRoot struct {
		tableID    uint32
		rootPageID uint32
	}
	type indexRoot struct {
		indexID    uint32
		rootPageID uint32
	}

	tableRoots := make([]tableRoot, 0, len(cat.Tables))
	indexRoots := make([]indexRoot, 0, len(cat.Tables))
	for _, table := range cat.Tables {
		if table.RootPageID != 0 {
			tableRoots = append(tableRoots, tableRoot{
				tableID:    table.TableID,
				rootPageID: table.RootPageID,
			})
		}
		for _, index := range table.Indexes {
			if index.RootPageID == 0 {
				continue
			}
			indexRoots = append(indexRoots, indexRoot{
				indexID:    index.IndexID,
				rootPageID: index.RootPageID,
			})
		}
	}

	idMappings := make([]DirectoryRootIDMapping, 0, len(tableRoots)+len(indexRoots))
	sort.Slice(tableRoots, func(i, j int) bool {
		return tableRoots[i].tableID < tableRoots[j].tableID
	})
	for _, table := range tableRoots {
		if table.tableID == 0 {
			continue
		}
		idMappings = append(idMappings, DirectoryRootIDMapping{
			ObjectType: DirectoryRootMappingObjectTable,
			ObjectID:   table.tableID,
			RootPageID: table.rootPageID,
		})
	}
	sort.Slice(indexRoots, func(i, j int) bool {
		return indexRoots[i].indexID < indexRoots[j].indexID
	})
	for _, index := range indexRoots {
		if index.indexID == 0 {
			continue
		}
		idMappings = append(idMappings, DirectoryRootIDMapping{
			ObjectType: DirectoryRootMappingObjectIndex,
			ObjectID:   index.indexID,
			RootPageID: index.rootPageID,
		})
	}
	return idMappings
}

// ApplyDirectoryRootIDMappings overlays directory-owned physical roots onto catalog metadata by stable logical ID.
func ApplyDirectoryRootIDMappings(cat *CatalogData, mappings []DirectoryRootIDMapping) (*CatalogData, error) {
	if cat == nil {
		return cat, nil
	}
	strictRoots := cat.Version >= catalogVersion
	if len(mappings) == 0 {
		if strictRoots && len(cat.Tables) != 0 {
			return nil, errCorruptedDirectoryPage
		}
		return cat, nil
	}

	tableMappings := make(map[uint32]uint32)
	indexMappings := make(map[uint32]uint32)
	for _, mapping := range mappings {
		if mapping.ObjectID == 0 || mapping.RootPageID == 0 {
			return nil, errCorruptedDirectoryPage
		}
		switch mapping.ObjectType {
		case DirectoryRootMappingObjectTable:
			if _, exists := tableMappings[mapping.ObjectID]; exists {
				return nil, errCorruptedDirectoryPage
			}
			tableMappings[mapping.ObjectID] = mapping.RootPageID
		case DirectoryRootMappingObjectIndex:
			if _, exists := indexMappings[mapping.ObjectID]; exists {
				return nil, errCorruptedDirectoryPage
			}
			indexMappings[mapping.ObjectID] = mapping.RootPageID
		default:
			return nil, errCorruptedDirectoryPage
		}
	}

	applied := &CatalogData{Tables: make([]CatalogTable, 0, len(cat.Tables))}
	for _, table := range cat.Tables {
		cloned := CatalogTable{
			Name:       table.Name,
			TableID:    table.TableID,
			RootPageID: 0,
			RowCount:   table.RowCount,
			Columns:    append([]CatalogColumn(nil), table.Columns...),
			Indexes:    make([]CatalogIndex, 0, len(table.Indexes)),
		}
		if table.TableID != 0 {
			if mappedRootPageID, ok := tableMappings[table.TableID]; ok {
				cloned.RootPageID = mappedRootPageID
				delete(tableMappings, table.TableID)
			} else if strictRoots {
				return nil, errCorruptedDirectoryPage
			}
		}

		for _, index := range table.Indexes {
			clonedIndex := CatalogIndex{
				Name:       index.Name,
				Unique:     index.Unique,
				IndexID:    index.IndexID,
				RootPageID: 0,
				Columns:    append([]CatalogIndexColumn(nil), index.Columns...),
			}
			if index.IndexID != 0 {
				if mappedRootPageID, ok := indexMappings[index.IndexID]; ok {
					clonedIndex.RootPageID = mappedRootPageID
					delete(indexMappings, index.IndexID)
				} else if strictRoots {
					return nil, errCorruptedDirectoryPage
				}
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

func buildDirectoryCatalogPage(catalogPayload []byte, formatVersion uint32, freeListHead uint32, idMappings []DirectoryRootIDMapping, checkpointMeta DirectoryCheckpointMetadata) ([]byte, error) {
	rootIDPayload, err := encodeDirectoryRootIDMappings(idMappings)
	if err != nil {
		return nil, err
	}
	rootIDTrailerSize := 0
	if len(rootIDPayload) > 0 {
		rootIDTrailerSize = directoryRootIDTrailerHeaderSize + len(rootIDPayload)
	}
	if len(catalogPayload)+directoryCheckpointMetadataSize+rootIDTrailerSize > PageSize-directoryCatalogOffset {
		return nil, errCatalogTooLarge
	}
	page := InitDirectoryPage(uint32(DirectoryControlPageID), formatVersion)
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetFreeListHead:directoryBodyOffsetFreeListHead+4], freeListHead)
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetCATDIRStorageMode:directoryBodyOffsetCATDIRStorageMode+4], DirectoryCATDIRStorageModeEmbedded)
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetCATDIROverflowHead:directoryBodyOffsetCATDIROverflowHead+4], 0)
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetCATDIROverflowCount:directoryBodyOffsetCATDIROverflowCount+4], 0)
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetCATDIRPayloadByteSize:directoryBodyOffsetCATDIRPayloadByteSize+4], uint32(len(catalogPayload)))
	catalogStart := directoryCatalogOffset
	copy(page[catalogStart:], catalogPayload)
	checkpointOffset := catalogStart + len(catalogPayload)
	binary.LittleEndian.PutUint64(page[checkpointOffset:checkpointOffset+8], checkpointMeta.LastCheckpointLSN)
	binary.LittleEndian.PutUint32(page[checkpointOffset+8:checkpointOffset+12], checkpointMeta.LastCheckpointPageCount)
	binary.LittleEndian.PutUint32(page[checkpointOffset+12:checkpointOffset+16], checkpointMeta.ReservedCheckpoint)
	if len(rootIDPayload) > 0 {
		trailerOffset := checkpointOffset + directoryCheckpointMetadataSize
		binary.LittleEndian.PutUint32(page[trailerOffset:trailerOffset+4], uint32(len(idMappings)))
		binary.LittleEndian.PutUint32(page[trailerOffset+4:trailerOffset+8], uint32(len(rootIDPayload)))
		copy(page[trailerOffset+8:], rootIDPayload)
	}
	if err := FinalizePageImage(page); err != nil {
		return nil, err
	}
	return page, nil
}

func directoryCatalogPayload(page []byte) ([]byte, error) {
	control, err := directoryCATDIRControl(page)
	if err != nil {
		return nil, err
	}
	length, _, err := decodeCatalogPayload(page[control.catalogOffset:])
	if err != nil {
		return nil, err
	}
	return page[control.catalogOffset : control.catalogOffset+length], nil
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

func readStoragePage(file *os.File, pageID PageID) ([]byte, error) {
	if file == nil || pageID == 0 {
		return nil, errCorruptedDirectoryPage
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	offset := pageOffset(pageID)
	if info.Size() < offset+PageSize {
		return nil, errCorruptedDirectoryPage
	}

	page := make([]byte, PageSize)
	if _, err := file.ReadAt(page, offset); err != nil {
		return nil, errCorruptedDirectoryPage
	}
	return page, nil
}

func looksLikeWrappedDirectoryPage(page []byte) bool {
	if len(page) != PageSize {
		return false
	}
	return binary.LittleEndian.Uint32(page[pageHeaderOffsetPageID:pageHeaderOffsetPageID+4]) == uint32(DirectoryControlPageID) &&
		PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2])) == PageTypeDirectory
}

func directoryRootIDMappings(page []byte) ([]DirectoryRootIDMapping, error) {
	if err := ValidateDirectoryPage(page); err != nil {
		return nil, err
	}
	checkpointOffset, err := directoryCheckpointOffset(page)
	if err != nil {
		return nil, err
	}
	trailerOffset := checkpointOffset + directoryCheckpointMetadataSize
	if trailerOffset >= len(page) || len(page)-trailerOffset < directoryRootIDTrailerHeaderSize {
		return nil, nil
	}

	rootIDCount := binary.LittleEndian.Uint32(page[trailerOffset : trailerOffset+4])
	rootIDBytes := binary.LittleEndian.Uint32(page[trailerOffset+4 : trailerOffset+8])
	if rootIDCount == 0 && rootIDBytes == 0 {
		return nil, nil
	}
	if rootIDCount == 0 || rootIDBytes == 0 {
		return nil, errCorruptedDirectoryPage
	}
	if trailerOffset+directoryRootIDTrailerHeaderSize+int(rootIDBytes) > len(page) {
		return nil, errCorruptedDirectoryPage
	}

	payload := page[trailerOffset+directoryRootIDTrailerHeaderSize : trailerOffset+directoryRootIDTrailerHeaderSize+int(rootIDBytes)]
	mappings := make([]DirectoryRootIDMapping, 0, rootIDCount)
	offset := 0
	for i := uint32(0); i < rootIDCount; i++ {
		if offset >= len(payload) {
			return nil, errCorruptedDirectoryPage
		}
		objectType := payload[offset]
		offset++
		objectID, ok := readUint32(payload, &offset)
		if !ok || objectID == 0 {
			return nil, errCorruptedDirectoryPage
		}
		rootPageID, ok := readUint32(payload, &offset)
		if !ok || rootPageID == 0 {
			return nil, errCorruptedDirectoryPage
		}
		switch objectType {
		case DirectoryRootMappingObjectTable, DirectoryRootMappingObjectIndex:
		default:
			return nil, errCorruptedDirectoryPage
		}
		mappings = append(mappings, DirectoryRootIDMapping{
			ObjectType: objectType,
			ObjectID:   objectID,
			RootPageID: rootPageID,
		})
	}
	if offset != len(payload) {
		return nil, errCorruptedDirectoryPage
	}
	return mappings, nil
}

func encodeDirectoryRootIDMappings(mappings []DirectoryRootIDMapping) ([]byte, error) {
	if len(mappings) == 0 {
		return nil, nil
	}

	buf := make([]byte, 0, len(mappings)*9)
	for _, mapping := range mappings {
		if mapping.ObjectID == 0 || mapping.RootPageID == 0 {
			return nil, errCorruptedDirectoryPage
		}
		switch mapping.ObjectType {
		case DirectoryRootMappingObjectTable, DirectoryRootMappingObjectIndex:
		default:
			return nil, errCorruptedDirectoryPage
		}
		buf = append(buf, mapping.ObjectType)
		buf = appendUint32(buf, mapping.ObjectID)
		buf = appendUint32(buf, mapping.RootPageID)
	}
	return buf, nil
}

func directoryCheckpointMetadata(page []byte) (DirectoryCheckpointMetadata, error) {
	if err := ValidateDirectoryPage(page); err != nil {
		return DirectoryCheckpointMetadata{}, err
	}
	offset, err := directoryCheckpointOffset(page)
	if err != nil {
		return DirectoryCheckpointMetadata{}, err
	}
	if offset < 0 || PageSize-offset < directoryCheckpointMetadataSize {
		return DirectoryCheckpointMetadata{}, nil
	}
	return DirectoryCheckpointMetadata{
		LastCheckpointLSN:       binary.LittleEndian.Uint64(page[offset : offset+8]),
		LastCheckpointPageCount: binary.LittleEndian.Uint32(page[offset+8 : offset+12]),
		ReservedCheckpoint:      binary.LittleEndian.Uint32(page[offset+12 : offset+16]),
	}, nil
}

func rewriteDirectoryCheckpointMetadata(page []byte, meta DirectoryCheckpointMetadata) error {
	if err := ValidateDirectoryPage(page); err != nil {
		return err
	}
	offset, err := directoryCheckpointOffset(page)
	if err != nil {
		return err
	}
	if offset < 0 || PageSize-offset < directoryCheckpointMetadataSize {
		return errCatalogTooLarge
	}
	binary.LittleEndian.PutUint64(page[offset:offset+8], meta.LastCheckpointLSN)
	binary.LittleEndian.PutUint32(page[offset+8:offset+12], meta.LastCheckpointPageCount)
	binary.LittleEndian.PutUint32(page[offset+12:offset+16], meta.ReservedCheckpoint)
	return nil
}

func directoryCheckpointOffset(page []byte) (int, error) {
	control, err := directoryCATDIRControl(page)
	if err != nil {
		return 0, err
	}
	return control.catalogOffset + int(control.payloadByteLength), nil
}

func validateDirectoryPageHeader(page []byte) error {
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
	if !SupportedDBFormatVersion(formatVersion) {
		return errCorruptedDirectoryPage
	}
	return nil
}

type directoryCATDIRControlState struct {
	catalogOffset      int
	mode               uint32
	overflowHeadPageID uint32
	overflowPageCount  uint32
	payloadByteLength  uint32
}

func directoryCATDIRControl(page []byte) (directoryCATDIRControlState, error) {
	if err := validateDirectoryPageHeader(page); err != nil {
		return directoryCATDIRControlState{}, err
	}
	if control, ok, err := decodeCurrentCATDIRControl(page); ok {
		return control, err
	}
	if control, err := decodeLegacyEmbeddedCATDIRControl(page); err == nil {
		return control, nil
	}
	return directoryCATDIRControlState{}, errCorruptedDirectoryPage
}

func decodeCurrentCATDIRControl(page []byte) (directoryCATDIRControlState, bool, error) {
	mode := binary.LittleEndian.Uint32(page[directoryBodyOffsetCATDIRStorageMode : directoryBodyOffsetCATDIRStorageMode+4])
	overflowHead := binary.LittleEndian.Uint32(page[directoryBodyOffsetCATDIROverflowHead : directoryBodyOffsetCATDIROverflowHead+4])
	overflowCount := binary.LittleEndian.Uint32(page[directoryBodyOffsetCATDIROverflowCount : directoryBodyOffsetCATDIROverflowCount+4])
	payloadBytes := binary.LittleEndian.Uint32(page[directoryBodyOffsetCATDIRPayloadByteSize : directoryBodyOffsetCATDIRPayloadByteSize+4])

	switch mode {
	case DirectoryCATDIRStorageModeEmbedded:
		if overflowHead != 0 || overflowCount != 0 {
			if overflowCount == 0 {
				return directoryCATDIRControlState{}, true, errCorruptedDirectoryPage
			}
			return directoryCATDIRControlState{}, false, nil
		}
		length, _, err := decodeCatalogPayload(page[directoryCatalogOffset:])
		if err != nil {
			if payloadBytes != 0 {
				return directoryCATDIRControlState{}, true, err
			}
			return directoryCATDIRControlState{}, false, nil
		}
		if payloadBytes != uint32(length) {
			return directoryCATDIRControlState{}, true, errCorruptedDirectoryPage
		}
		if directoryCatalogOffset+length+directoryCheckpointMetadataSize > PageSize {
			return directoryCATDIRControlState{}, true, errCorruptedDirectoryPage
		}
		return directoryCATDIRControlState{
			catalogOffset:      directoryCatalogOffset,
			mode:               mode,
			overflowHeadPageID: overflowHead,
			overflowPageCount:  overflowCount,
			payloadByteLength:  payloadBytes,
		}, true, nil
	case DirectoryCATDIRStorageModeOverflow:
		if overflowHead == 0 || overflowCount == 0 {
			return directoryCATDIRControlState{}, true, errCorruptedDirectoryPage
		}
		return directoryCATDIRControlState{}, true, errUnsupportedDirectoryPage
	default:
		return directoryCATDIRControlState{}, false, nil
	}
}

func decodeLegacyEmbeddedCATDIRControl(page []byte) (directoryCATDIRControlState, error) {
	if binary.LittleEndian.Uint32(page[directoryBodyOffsetCATDIRStorageMode:directoryBodyOffsetCATDIRStorageMode+4]) != 0 {
		return directoryCATDIRControlState{}, errCorruptedDirectoryPage
	}
	if binary.LittleEndian.Uint32(page[directoryBodyOffsetCATDIROverflowHead:directoryBodyOffsetCATDIROverflowHead+4]) != 0 {
		return directoryCATDIRControlState{}, errCorruptedDirectoryPage
	}
	length, _, err := decodeCatalogPayload(page[legacyDirectoryCatalogOffset:])
	if err != nil {
		return directoryCATDIRControlState{}, err
	}
	if legacyDirectoryCatalogOffset+length+directoryCheckpointMetadataSize > PageSize {
		return directoryCATDIRControlState{}, errCorruptedDirectoryPage
	}
	return directoryCATDIRControlState{
		catalogOffset:      legacyDirectoryCatalogOffset,
		mode:               DirectoryCATDIRStorageModeEmbedded,
		overflowHeadPageID: 0,
		overflowPageCount:  0,
		payloadByteLength:  uint32(length),
	}, nil
}

func validateDirectoryPageImage(page []byte) error {
	if err := ValidateDirectoryPage(page); err != nil {
		return err
	}
	storedChecksum := binary.LittleEndian.Uint32(page[pageHeaderOffsetChecksum : pageHeaderOffsetChecksum+4])
	if storedChecksum != 0 && storedChecksum != pageChecksum(page) {
		return errCorruptedDirectoryPage
	}
	return nil
}
