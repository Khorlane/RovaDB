package storage

const (
	// HeaderSize is the size of the reserved database file header at bytes [0:HeaderSize).
	HeaderSize = 16
	// PageSize is the fixed size of each future page after the file header.
	PageSize = 4096
)

// PageID identifies a fixed-size page in the database file.
type PageID uint32

// Page is a fixed-size page buffer. Page data always has length PageSize.
type Page struct {
	id    PageID
	data  []byte
	dirty bool
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

// MarkDirty marks the page as modified.
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

// pageOffset returns the file offset for the given page.
// Bytes [0:HeaderSize) are reserved for the file header, and page space begins at HeaderSize.
func pageOffset(id PageID) int64 {
	return HeaderSize + int64(id)*PageSize
}
