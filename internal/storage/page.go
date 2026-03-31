package storage

const (
	// HeaderSize is the size of the reserved database file header at bytes [0:HeaderSize).
	HeaderSize = 16
	// PageSize is the fixed size of each future page after the file header.
	PageSize = 4096
)

// PageID identifies a fixed-size page in the database file.
type PageID uint32

// PageType identifies the physical page layout stored in a page header.
type PageType uint16

const (
	PageTypeTable PageType = 1 + iota
	PageTypeIndexLeaf
	PageTypeIndexInternal
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
	case PageTypeTable, PageTypeIndexLeaf, PageTypeIndexInternal:
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
