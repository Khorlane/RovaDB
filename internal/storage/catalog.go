package storage

import (
	"encoding/binary"
)

const (
	catalogVersion = 6
	catalogPageID  = 0

	CatalogColumnTypeInt  = 1
	CatalogColumnTypeText = 2
	CatalogColumnTypeBool = 3
	CatalogColumnTypeReal = 4
)

// CatalogData is the tiny storage-side catalog DTO persisted through CAT/DIR storage.
type CatalogData struct {
	Version uint32
	Tables  []CatalogTable
}

// CatalogTable is a persisted table schema entry.
type CatalogTable struct {
	Name       string
	TableID    uint32
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
	IndexID    uint32
	RootPageID uint32
	Columns    []CatalogIndexColumn
}

// CatalogIndexColumn is a persisted indexed column entry.
type CatalogIndexColumn struct {
	Name string
	Desc bool
}

type CatalogWritePage struct {
	PageID PageID
	Data   []byte
	IsNew  bool
}

type CatalogWritePlan struct {
	DirectoryPage     []byte
	OverflowPages     []CatalogWritePage
	ReclaimedPages    []CatalogWritePage
	FreeListHead      uint32
	CATDIRStorageMode uint32
}

type CatalogOverflowPageAllocator func() (PageID, bool, error)

// PageReader provides durable page reads without exposing pager internals.
type PageReader interface {
	ReadPage(pageID PageID) ([]byte, error)
}

// PageReaderFunc adapts a function to PageReader.
type PageReaderFunc func(PageID) ([]byte, error)

func (f PageReaderFunc) ReadPage(pageID PageID) ([]byte, error) {
	return f(pageID)
}

// LoadCatalog decodes the committed catalog from embedded or overflow CAT/DIR storage.
func LoadCatalog(reader PageReader) (*CatalogData, error) {
	payload, err := readCommittedCatalogPayload(reader)
	if err != nil {
		return nil, err
	}
	return loadCatalogPayload(payload)
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

func readCommittedCatalogPayload(reader PageReader) ([]byte, error) {
	pageData, err := reader.ReadPage(catalogPageID)
	if err != nil {
		return nil, err
	}
	if ValidateDirectoryPage(pageData) != nil {
		return pageData, nil
	}
	return readDirectoryCatalogPayload(reader, pageData)
}

func readDirectoryCatalogPayload(reader PageReader, pageData []byte) ([]byte, error) {
	control, err := directoryCATDIRControl(pageData)
	if err != nil {
		return nil, err
	}
	if control.mode == DirectoryCATDIRStorageModeEmbedded {
		return directoryCatalogPayload(pageData)
	}
	return ReadCatalogOverflowPayload(
		reader,
		PageID(control.overflowHeadPageID),
		control.overflowPageCount,
		control.payloadByteLength,
	)
}

func loadCatalogPayload(pageData []byte) (*CatalogData, error) {
	_, cat, err := decodeCatalogPayload(pageData)
	return cat, err
}

func encodeCatalogPayload(cat *CatalogData) ([]byte, error) {
	if cat == nil {
		cat = &CatalogData{}
	}
	buf := make([]byte, 0, embeddedDirectoryCatalogPayloadCapacity(0))
	buf = appendUint32(buf, catalogVersion)
	buf = appendUint32(buf, uint32(len(cat.Tables)))

	for _, table := range cat.Tables {
		if table.Name == "" || len(table.Columns) == 0 {
			return nil, errCorruptedCatalogPage
		}
		buf = appendString(buf, table.Name)
		buf = appendUint32(buf, table.TableID)
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
		}
		buf = appendUint16(buf, uint16(len(table.Indexes)))
		for _, index := range table.Indexes {
			var err error
			buf, err = appendCatalogIndex(buf, index, table.Columns)
			if err != nil {
				return nil, err
			}
		}
	}
	return buf, nil
}

func decodeCatalogPayload(pageData []byte) (int, *CatalogData, error) {
	if isZeroPage(pageData) {
		return 0, &CatalogData{}, nil
	}

	offset := 0
	version, ok := readUint32(pageData, &offset)
	if !ok {
		return 0, nil, errCorruptedCatalogPage
	}
	if version != catalogVersion {
		return 0, nil, errUnsupportedCatalogPage
	}
	tableCount, ok := readUint32(pageData, &offset)
	if !ok {
		return 0, nil, errCorruptedCatalogPage
	}

	cat := &CatalogData{Version: version, Tables: make([]CatalogTable, 0, tableCount)}
	for i := uint32(0); i < tableCount; i++ {
		name, ok := readString(pageData, &offset)
		if !ok || name == "" {
			return 0, nil, errCorruptedCatalogPage
		}
		tableID, ok := readUint32(pageData, &offset)
		if !ok || tableID == 0 {
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
			TableID:    tableID,
			RootPageID: 0,
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
		indexCount, ok := readUint16(pageData, &offset)
		if !ok {
			return 0, nil, errCorruptedCatalogPage
		}
		table.Indexes = make([]CatalogIndex, 0, indexCount)
		indexNames := make(map[string]struct{}, indexCount)
		for j := uint16(0); j < indexCount; j++ {
			index, err := readCatalogIndex(pageData, &offset, columnNames, indexNames)
			if err != nil {
				return 0, nil, err
			}
			table.Indexes = append(table.Indexes, index)
		}

		cat.Tables = append(cat.Tables, table)
	}

	return offset, cat, nil
}

// SaveCatalog encodes the catalog into the smallest valid CAT/DIR representation.
func SaveCatalog(pager *Pager, cat *CatalogData) error {
	page, err := pager.Get(catalogPageID)
	if err != nil {
		return err
	}
	freeListHead := uint32(0)
	checkpointMeta := DirectoryCheckpointMetadata{}
	currentMode := DirectoryCATDIRStorageModeEmbedded
	currentOverflowHead := PageID(0)
	currentOverflowCount := uint32(0)
	if ValidateDirectoryPage(page.Data()) == nil {
		freeListHead, err = DirectoryFreeListHead(page.Data())
		if err != nil {
			return err
		}
		currentMode, err = DirectoryCATDIRStorageMode(page.Data())
		if err != nil {
			return err
		}
		checkpointMeta, err = directoryCheckpointMetadata(page.Data())
		if err != nil {
			return err
		}
		if currentMode == DirectoryCATDIRStorageModeOverflow {
			overflowHead, err := DirectoryCATDIROverflowHeadPageID(page.Data())
			if err != nil {
				return err
			}
			currentOverflowHead = PageID(overflowHead)
			currentOverflowCount, err = DirectoryCATDIROverflowPageCount(page.Data())
			if err != nil {
				return err
			}
		}
	}
	allocator := newCatalogOverflowAllocator(pager, &freeListHead)
	plan, err := PrepareCatalogWritePlan(cat, currentMode, currentOverflowHead, currentOverflowCount, pager, CurrentDBFormatVersion, &freeListHead, checkpointMeta, allocator.Allocate)
	if err != nil {
		return err
	}
	if err := applyCatalogWritePlanToPager(pager, plan); err != nil {
		return err
	}
	// Catalog mutation requires explicit dirty marking; later flush eligibility
	// is driven by dirty tracking rather than implicit full flushes.
	return nil
}

// BuildCatalogPageData encodes the catalog into a full embedded directory page image.
func BuildCatalogPageData(cat *CatalogData) ([]byte, error) {
	return BuildCatalogPageDataWithDirectoryState(cat, 0, DirectoryCheckpointMetadata{})
}

// BuildCatalogPageDataWithFreeListHead encodes the catalog into the directory-wrapped page 0 image.
func BuildCatalogPageDataWithFreeListHead(cat *CatalogData, freeListHead uint32) ([]byte, error) {
	return BuildCatalogPageDataWithDirectoryState(cat, freeListHead, DirectoryCheckpointMetadata{})
}

// BuildCatalogPageDataWithDirectoryState encodes the wrapped page 0 image with directory state.
func BuildCatalogPageDataWithDirectoryState(cat *CatalogData, freeListHead uint32, checkpointMeta DirectoryCheckpointMetadata) ([]byte, error) {
	plan, err := PrepareCatalogWritePlan(cat, DirectoryCATDIRStorageModeEmbedded, 0, 0, nil, CurrentDBFormatVersion, &freeListHead, checkpointMeta, nil)
	if err != nil {
		return nil, err
	}
	return plan.DirectoryPage, nil
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

func readCatalogIndex(data []byte, offset *int, columnNames map[string]struct{}, indexNames map[string]struct{}) (CatalogIndex, error) {
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
	indexID, ok := readUint32(data, offset)
	if !ok || indexID == 0 {
		return CatalogIndex{}, errCorruptedCatalogPage
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
	return CatalogIndex{Name: name, Unique: unique, IndexID: indexID, RootPageID: 0, Columns: columns}, nil
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
	buf = appendUint32(buf, index.IndexID)
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
	}
	return buf, nil
}

func PrepareCatalogWritePlan(cat *CatalogData, currentMode uint32, currentOverflowHead PageID, currentOverflowPageCount uint32, reader PageReader, formatVersion uint32, freeListHead *uint32, checkpointMeta DirectoryCheckpointMetadata, allocate CatalogOverflowPageAllocator) (*CatalogWritePlan, error) {
	if freeListHead == nil {
		return nil, errCorruptedDirectoryPage
	}
	rootIDMappings := BuildDirectoryRootIDMappings(cat)
	rootIDPayload, err := encodeDirectoryRootIDMappings(rootIDMappings)
	if err != nil {
		return nil, err
	}
	payload, err := encodeCatalogPayload(cat)
	if err != nil {
		return nil, err
	}

	newMode := DirectoryCATDIRStorageModeEmbedded
	var pageAlloc []CatalogWritePage
	newOverflowHead := PageID(0)
	newOverflowCount := uint32(0)
	if !canStoreCatalogPayloadEmbedded(payload, rootIDPayload) {
		if allocate == nil {
			return nil, errCATDIRExceedsEmbeddedWrite
		}
		requiredPages := catalogOverflowRequiredPageCount(len(payload))
		pageIDs := make([]PageID, 0, requiredPages)
		pageAlloc = make([]CatalogWritePage, 0, requiredPages)
		for i := 0; i < requiredPages; i++ {
			pageID, isNew, err := allocate()
			if err != nil {
				return nil, err
			}
			pageIDs = append(pageIDs, pageID)
			pageAlloc = append(pageAlloc, CatalogWritePage{PageID: pageID, IsNew: isNew})
		}
		overflowImages, err := BuildCatalogOverflowPageChain(payload, pageIDs)
		if err != nil {
			return nil, err
		}
		for i := range overflowImages {
			pageAlloc[i].Data = overflowImages[i].Data
		}
		newMode = DirectoryCATDIRStorageModeOverflow
		newOverflowHead = overflowImages[0].PageID
		newOverflowCount = uint32(len(overflowImages))
	}

	var reclaimedPages []CatalogWritePage
	if currentMode == DirectoryCATDIRStorageModeOverflow {
		if currentOverflowHead == 0 || currentOverflowPageCount == 0 || reader == nil {
			return nil, errCorruptedCatalogOverflow
		}
		reclaimedPages, *freeListHead, err = BuildCatalogOverflowReclaimPages(reader, currentOverflowHead, currentOverflowPageCount, *freeListHead)
		if err != nil {
			return nil, err
		}
	}

	var directoryPage []byte
	if newMode == DirectoryCATDIRStorageModeEmbedded {
		directoryPage, err = buildEmbeddedDirectoryCatalogPage(payload, formatVersion, *freeListHead, rootIDMappings, checkpointMeta)
		if err != nil {
			return nil, err
		}
	} else {
		directoryPage, err = buildOverflowDirectoryCatalogPage(uint32(len(payload)), newOverflowHead, newOverflowCount, formatVersion, *freeListHead, rootIDMappings, checkpointMeta)
		if err != nil {
			return nil, err
		}
	}
	return &CatalogWritePlan{
		DirectoryPage:     directoryPage,
		OverflowPages:     pageAlloc,
		ReclaimedPages:    reclaimedPages,
		FreeListHead:      *freeListHead,
		CATDIRStorageMode: newMode,
	}, nil
}

type catalogOverflowPagerAllocator struct {
	pager        *Pager
	nextFreshID  PageID
	freeListHead *uint32
}

var newCatalogOverflowAllocator = newCatalogOverflowPagerAllocator

func newCatalogOverflowPagerAllocator(pager *Pager, freeListHead *uint32) *catalogOverflowPagerAllocator {
	return &catalogOverflowPagerAllocator{
		pager:        pager,
		nextFreshID:  pager.NextPageID(),
		freeListHead: freeListHead,
	}
}

func (a *catalogOverflowPagerAllocator) Allocate() (PageID, bool, error) {
	if a == nil || a.pager == nil || a.freeListHead == nil {
		return 0, false, errCorruptedDirectoryPage
	}
	allocator := PageAllocator{
		NextPageID: uint32(a.nextFreshID),
		FreePage: FreePageState{
			HeadPageID: *a.freeListHead,
		},
		ReadFreeNext: func(pageID uint32) (uint32, error) {
			pageData, err := a.pager.ReadPage(PageID(pageID))
			if err != nil {
				return 0, err
			}
			return FreePageNext(pageData)
		},
	}
	allocated, reused, err := allocator.Allocate()
	if err != nil {
		return 0, false, err
	}
	a.nextFreshID = PageID(allocator.NextPageID)
	*a.freeListHead = allocator.FreePage.HeadPageID
	return PageID(allocated), !reused, nil
}

func applyCatalogWritePlanToPager(pager *Pager, plan *CatalogWritePlan) error {
	if pager == nil || plan == nil {
		return errCorruptedDirectoryPage
	}
	for _, overflowPage := range plan.OverflowPages {
		var page *Page
		var err error
		if overflowPage.IsNew {
			page = pager.NewPage()
			if page.ID() != overflowPage.PageID {
				pager.DiscardNewPage(page.ID())
				return errCorruptedCatalogOverflow
			}
		} else {
			page, err = pager.Get(overflowPage.PageID)
			if err != nil {
				return err
			}
		}
		pager.MarkDirtyWithOriginal(page)
		clear(page.data)
		copy(page.data, overflowPage.Data)
	}
	for _, reclaimedPage := range plan.ReclaimedPages {
		page, err := pager.Get(reclaimedPage.PageID)
		if err != nil {
			return err
		}
		pager.MarkDirtyWithOriginal(page)
		clear(page.data)
		copy(page.data, reclaimedPage.Data)
	}
	page, err := pager.Get(catalogPageID)
	if err != nil {
		return err
	}
	pager.MarkDirtyWithOriginal(page)
	clear(page.data)
	copy(page.data, plan.DirectoryPage)
	return nil
}
