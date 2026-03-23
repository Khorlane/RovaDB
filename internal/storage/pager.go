package storage

import (
	"errors"
	"io"
	"os"
	"sort"
)

var errInvalidPageFileSize = errors.New("storage: invalid page-aligned file size")

// Pager is the minimal page loader and flusher for a database file.
type Pager struct {
	file       *os.File
	pages      map[PageID]*Page
	nextPageID PageID
}

// NewPager creates a pager over an open database file.
func NewPager(f *os.File) (*Pager, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := info.Size()
	if size < HeaderSize {
		return nil, errFileTooSmall
	}
	if size > HeaderSize && (size-HeaderSize)%PageSize != 0 {
		return nil, errInvalidPageFileSize
	}

	nextPageID := PageID(1)
	if size > HeaderSize {
		nextPageID = PageID((size - HeaderSize) / PageSize)
		if nextPageID < 1 {
			nextPageID = 1
		}
	}

	return &Pager{
		file:       f,
		pages:      make(map[PageID]*Page),
		nextPageID: nextPageID,
	}, nil
}

// Get returns a page, loading it from disk if necessary.
func (p *Pager) Get(id PageID) (*Page, error) {
	if page, ok := p.pages[id]; ok {
		return page, nil
	}

	page := NewPage(id)
	_, err := p.file.ReadAt(page.data, pageOffset(id))
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	p.pages[id] = page
	return page, nil
}

// NewPage allocates a new dirty page.
func (p *Pager) NewPage() *Page {
	id := p.nextPageID
	p.nextPageID++

	page := NewPage(id)
	p.pages[id] = page
	p.MarkDirty(page)
	return page
}

// NextPageID reports the next page identifier that NewPage would allocate.
func (p *Pager) NextPageID() PageID {
	if p == nil {
		return 0
	}
	return p.nextPageID
}

// DiscardNewPage removes the most recently allocated unflushed page.
func (p *Pager) DiscardNewPage(id PageID) {
	if p == nil {
		return
	}
	if p.nextPageID == 0 || id+1 != p.nextPageID {
		return
	}
	delete(p.pages, id)
	p.nextPageID = id
}

// MarkDirty marks a loaded page dirty. Page mutation requires explicit dirty
// registration so later commit-oriented flushing can be driven by dirty state.
func (p *Pager) MarkDirty(page *Page) {
	if p == nil || page == nil {
		return
	}
	page.MarkDirty()
}

// IsDirty reports whether a loaded page is currently dirty.
func (p *Pager) IsDirty(page *Page) bool {
	if p == nil || page == nil {
		return false
	}
	return page.Dirty()
}

// ClearDirty marks a loaded page clean.
func (p *Pager) ClearDirty(page *Page) {
	if p == nil || page == nil {
		return
	}
	page.ClearDirty()
}

// DirtyPages returns loaded dirty pages in ascending page-number order.
func (p *Pager) DirtyPages() []*Page {
	if p == nil {
		return nil
	}

	ids := make([]int, 0, len(p.pages))
	for id, page := range p.pages {
		if page == nil || !page.Dirty() {
			continue
		}
		ids = append(ids, int(id))
	}
	sort.Ints(ids)

	pages := make([]*Page, 0, len(ids))
	for _, id := range ids {
		pages = append(pages, p.pages[PageID(id)])
	}
	return pages
}

// FlushDirty writes dirty pages to disk and clears their dirty flags on
// successful write. Flush eligibility is driven entirely by dirty tracking.
func (p *Pager) FlushDirty() error {
	for _, page := range p.DirtyPages() {
		if _, err := p.file.WriteAt(page.data, pageOffset(page.id)); err != nil {
			return err
		}
		p.ClearDirty(page)
	}

	return p.file.Sync()
}

// Flush writes all currently dirty pages to disk.
func (p *Pager) Flush() error {
	return p.FlushDirty()
}

// Close flushes dirty pages.
func (p *Pager) Close() error {
	if p == nil || p.file == nil {
		return nil
	}
	return p.FlushDirty()
}
