package storage

import (
	"errors"
	"io"
	"os"
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
	page.MarkDirty()
	p.pages[id] = page
	return page
}

// Flush writes all dirty pages to disk.
func (p *Pager) Flush() error {
	for _, page := range p.pages {
		if !page.dirty {
			continue
		}
		if _, err := p.file.WriteAt(page.data, pageOffset(page.id)); err != nil {
			return err
		}
		page.dirty = false
	}

	return p.file.Sync()
}

// Close flushes dirty pages.
func (p *Pager) Close() error {
	if p == nil || p.file == nil {
		return nil
	}
	return p.Flush()
}
