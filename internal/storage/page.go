package storage

import "encoding/binary"

const (
	// HeaderSize is the size of the reserved database file header at bytes [0:HeaderSize).
	HeaderSize = 16
	// PageSize is the fixed size of each future page after the file header.
	PageSize = 4096

	pageHeaderOffsetPageID   = 0
	pageHeaderOffsetPageType = 4
	pageHeaderOffsetPageLSN  = 8
	pageHeaderOffsetChecksum = 16
	pageHeaderSize           = 20
)

// PageID identifies a fixed-size page in the database file.
type PageID uint32

// PageType identifies the physical page layout stored in a page header.
type PageType uint16

const (
	PageTypeTable PageType = 1 + iota
	PageTypeIndexLeaf
	PageTypeIndexInternal
	PageTypeFreePage
	PageTypeDirectory
	PageTypeCatalogOverflow
)

const (
	PageTypeHeader   PageType = 64
	PageTypeSpaceMap PageType = 65
)

// PageTypeData is the physical-family name for the existing slotted table data
// page format. It intentionally aliases the current table page type so normal
// table runtime behavior and on-disk compatibility remain unchanged.
const PageTypeData PageType = PageTypeTable

// HeaderPageRole identifies the logical role stored within a physical Header page.
type HeaderPageRole uint16

const (
	HeaderPageRoleDatabase HeaderPageRole = 1 + iota
	HeaderPageRoleTable
)

// Page is a fixed-size in-memory page buffer. Dirty/original tracking is used
// to stage autocommit writes and to restore pre-commit page bytes on rollback.
type Page struct {
	id             PageID
	data           []byte
	dirty          bool
	originalData   []byte
	hasOriginal    bool
	newlyAllocated bool
}

// NewPage allocates a zeroed fixed-size page buffer.
func NewPage(id PageID) *Page {
	return &Page{
		id:   id,
		data: make([]byte, PageSize),
	}
}

// ID returns the page identifier.
func (p *Page) ID() PageID {
	return p.id
}

// Data returns the fixed-size page buffer.
func (p *Page) Data() []byte {
	return p.data
}

// MarkDirty marks the page as modified and eligible for commit-time flush.
func (p *Page) MarkDirty() {
	if p == nil {
		return
	}
	p.dirty = true
}

// ClearDirty marks the page as clean.
func (p *Page) ClearDirty() {
	if p == nil {
		return
	}
	p.dirty = false
}

// Dirty reports whether the page has been modified.
func (p *Page) Dirty() bool {
	if p == nil {
		return false
	}
	return p.dirty
}

// HasOriginal reports whether the page still has a saved original durable image.
func (p *Page) HasOriginal() bool {
	if p == nil {
		return false
	}
	return p.hasOriginal
}

// pageOffset returns the file offset for the given page.
// Bytes [0:HeaderSize) are reserved for the file header, and page space begins at HeaderSize.
func pageOffset(id PageID) int64 {
	return HeaderSize + int64(id)*PageSize
}

func IsValidPageType(pageType PageType) bool {
	switch pageType {
	case PageTypeTable, PageTypeIndexLeaf, PageTypeIndexInternal, PageTypeFreePage, PageTypeDirectory, PageTypeCatalogOverflow, PageTypeHeader, PageTypeSpaceMap:
		return true
	default:
		return false
	}
}

func IsHeaderPageType(pageType PageType) bool {
	return pageType == PageTypeHeader
}

func IsSpaceMapPageType(pageType PageType) bool {
	return pageType == PageTypeSpaceMap
}

func IsDataPageType(pageType PageType) bool {
	return pageType == PageTypeData
}

func IsValidHeaderPageRole(role HeaderPageRole) bool {
	switch role {
	case HeaderPageRoleDatabase, HeaderPageRoleTable:
		return true
	default:
		return false
	}
}

func IsIndexPageType(pageType PageType) bool {
	return pageType == PageTypeIndexLeaf || pageType == PageTypeIndexInternal
}

func IsLeafIndexPageType(pageType PageType) bool {
	return pageType == PageTypeIndexLeaf
}

func IsInternalIndexPageType(pageType PageType) bool {
	return pageType == PageTypeIndexInternal
}

func PageLSN(page []byte) (uint64, error) {
	if err := validateStampedPageHeader(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(page[pageHeaderOffsetPageLSN : pageHeaderOffsetPageLSN+8]), nil
}

func SetPageLSN(page []byte, lsn uint64) error {
	if err := validateStampedPageHeader(page); err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(page[pageHeaderOffsetPageLSN:pageHeaderOffsetPageLSN+8], lsn)
	return nil
}

func PageChecksum(page []byte) (uint32, error) {
	if err := validateStampedPageHeader(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(page[pageHeaderOffsetChecksum : pageHeaderOffsetChecksum+4]), nil
}

func RecomputePageChecksum(page []byte) error {
	if err := validateChecksumPageHeader(page); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(page[pageHeaderOffsetChecksum:pageHeaderOffsetChecksum+4], 0)
	binary.LittleEndian.PutUint32(page[pageHeaderOffsetChecksum:pageHeaderOffsetChecksum+4], pageChecksum(page))
	return nil
}

// FinalizePageImage stamps the checksum for a page image that is ready for persistence.
func FinalizePageImage(page []byte) error {
	if err := validateChecksumPageHeader(page); err != nil {
		return err
	}
	if err := RecomputePageChecksum(page); err != nil {
		return err
	}
	return ValidatePageImage(page)
}

// ValidatePageImage validates one full persisted page image.
func ValidatePageImage(page []byte) error {
	if len(page) != PageSize {
		return errCorruptedPageHeader
	}

	pageType := PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType : pageHeaderOffsetPageType+2]))
	switch pageType {
	case PageTypeTable:
		if err := validateStoredPageChecksum(page); err != nil {
			return errCorruptedTablePage
		}
		return validateSlottedTablePage(page)
	case PageTypeHeader:
		if err := validateStoredPageChecksum(page); err != nil {
			return errCorruptedHeaderPage
		}
		return ValidateHeaderPage(page)
	case PageTypeSpaceMap:
		if err := validateStoredPageChecksum(page); err != nil {
			return errCorruptedSpaceMapPage
		}
		return ValidateSpaceMapPage(page)
	case PageTypeIndexLeaf, PageTypeIndexInternal:
		if err := validateStoredPageChecksum(page); err != nil {
			return errCorruptedIndexPage
		}
		return validateIndexPage(page)
	case PageTypeFreePage:
		if err := validateStoredPageChecksum(page); err != nil {
			return errCorruptedPageHeader
		}
		return validateFreePage(page)
	case PageTypeDirectory:
		return validateDirectoryPageImage(page)
	case PageTypeCatalogOverflow:
		if err := validateStoredPageChecksum(page); err != nil {
			return errCorruptedCatalogOverflow
		}
		return validateCatalogOverflowPage(page)
	default:
		return errCorruptedPageHeader
	}
}

func validateStampedPageHeader(page []byte) error {
	if len(page) != PageSize {
		return errCorruptedPageHeader
	}
	pageType := PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType : pageHeaderOffsetPageType+2]))
	if !IsValidPageType(pageType) {
		return errCorruptedPageHeader
	}
	pageID := binary.LittleEndian.Uint32(page[pageHeaderOffsetPageID : pageHeaderOffsetPageID+4])
	if pageID == 0 && pageType != PageTypeDirectory && pageType != PageTypeHeader {
		return errCorruptedPageHeader
	}
	return nil
}

func validateChecksumPageHeader(page []byte) error {
	if len(page) != PageSize {
		return errCorruptedPageHeader
	}
	pageType := PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType : pageHeaderOffsetPageType+2]))
	if !IsValidPageType(pageType) {
		return errCorruptedPageHeader
	}
	pageID := binary.LittleEndian.Uint32(page[pageHeaderOffsetPageID : pageHeaderOffsetPageID+4])
	if pageID == 0 && pageType != PageTypeDirectory && pageType != PageTypeHeader {
		return errCorruptedPageHeader
	}
	return nil
}

func validateStoredPageChecksum(page []byte) error {
	if err := validateChecksumPageHeader(page); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(page[pageHeaderOffsetChecksum:pageHeaderOffsetChecksum+4]) != pageChecksum(page) {
		return errCorruptedPageHeader
	}
	return nil
}

func pageChecksum(page []byte) uint32 {
	var checksum uint32
	for i, b := range page {
		if i >= pageHeaderOffsetChecksum && i < pageHeaderOffsetChecksum+4 {
			continue
		}
		checksum = checksum*16777619 ^ uint32(b)
	}
	return checksum
}
