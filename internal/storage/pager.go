package storage

import (
	"io"
	"os"
	"sort"
)

// Pager loads pages, tracks dirty/original images for one in-process
// transaction window, and flushes pages in a deterministic order.
type Pager struct {
	file                *os.File
	pages               map[PageID]*Page
	nextPageID          PageID
	lastCommittedPageID PageID
}

// NewPager creates a pager over an open database file.
func NewPager(f *os.File) (*Pager, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := info.Size()
	if size < HeaderSize {
		return nil, errCorruptedDatabaseHeader
	}
	if size > HeaderSize && (size-HeaderSize)%PageSize != 0 {
		return nil, errCorruptedPageHeader
	}

	nextPageID := PageID(1)
	if size > HeaderSize {
		nextPageID = PageID((size - HeaderSize) / PageSize)
		if nextPageID < 1 {
			nextPageID = 1
		}
	}

	return &Pager{
		file:                f,
		pages:               make(map[PageID]*Page),
		nextPageID:          nextPageID,
		lastCommittedPageID: nextPageID,
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
	page.newlyAllocated = true
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

// MarkDirty marks a loaded page dirty. Mutations must register explicitly so
// commit is the only place that decides which pages flush.
func (p *Pager) MarkDirty(page *Page) {
	p.MarkDirtyWithOriginal(page)
}

// MarkDirtyWithOriginal marks a page dirty and captures its pre-mutation image
// the first time it is dirtied within the current transaction window.
func (p *Pager) MarkDirtyWithOriginal(page *Page) {
	if p == nil || page == nil {
		return
	}
	if !page.hasOriginal && !page.newlyAllocated {
		page.originalData = append([]byte(nil), page.data...)
		page.hasOriginal = true
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

// HasOriginal reports whether rollback restoration is tracked for a page.
func (p *Pager) HasOriginal(page *Page) bool {
	if p == nil || page == nil {
		return false
	}
	return page.HasOriginal()
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

// DirtyPagesWithOriginals returns dirty pages that still have saved
// pre-mutation images, ordered by ascending page number. Newly allocated pages
// are excluded because they have no prior durable image to journal.
func (p *Pager) DirtyPagesWithOriginals() []*Page {
	if p == nil {
		return nil
	}

	ids := make([]int, 0, len(p.pages))
	for id, page := range p.pages {
		if page == nil || !page.Dirty() || !page.HasOriginal() {
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
	return nil
}

// PageSize reports the fixed storage page size used by the pager.
func (p *Pager) PageSize() uint32 {
	return PageSize
}

// Sync fsyncs the underlying database file.
func (p *Pager) Sync() error {
	if p == nil || p.file == nil {
		return nil
	}
	return p.file.Sync()
}

// RestoreDirtyPages restores pre-commit in-memory page images and discards
// newly allocated uncommitted pages.
func (p *Pager) RestoreDirtyPages() {
	if p == nil {
		return
	}

	for _, page := range p.pages {
		if page == nil {
			continue
		}
		if !page.newlyAllocated && !page.hasOriginal {
			continue
		}
		if page.newlyAllocated {
			delete(p.pages, page.id)
			continue
		}
		if page.hasOriginal {
			clear(page.data)
			copy(page.data, page.originalData)
		}
		page.ClearDirty()
		page.originalData = nil
		page.hasOriginal = false
	}

	p.nextPageID = p.lastCommittedPageID
}

// ClearDirtyTracking clears rollback snapshots after a successful commit.
func (p *Pager) ClearDirtyTracking() {
	if p == nil {
		return
	}
	for _, page := range p.pages {
		if page == nil {
			continue
		}
		page.originalData = nil
		page.hasOriginal = false
		page.newlyAllocated = false
		page.ClearDirty()
	}
	p.lastCommittedPageID = p.nextPageID
}

// Flush writes all currently dirty pages to disk.
func (p *Pager) Flush() error {
	if err := p.FlushDirty(); err != nil {
		return err
	}
	return p.Sync()
}

// Close flushes dirty pages.
func (p *Pager) Close() error {
	if p == nil || p.file == nil {
		return nil
	}
	return p.Flush()
}
