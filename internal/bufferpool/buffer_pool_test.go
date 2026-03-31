package bufferpool

import (
	"bytes"
	"errors"
	"testing"
)

type stubLoader struct {
	pages map[PageID][]byte
	reads map[PageID]int
	err   error
}

func (l *stubLoader) ReadPage(pageID PageID) ([]byte, error) {
	if l.err != nil {
		return nil, l.err
	}
	if l.reads == nil {
		l.reads = make(map[PageID]int)
	}
	l.reads[pageID]++
	return append([]byte(nil), l.pages[pageID]...), nil
}

func TestGetCommittedPageLoadsViaLoader(t *testing.T) {
	loader := &stubLoader{
		pages: map[PageID][]byte{
			7: bytes.Repeat([]byte{0xAB}, PageSize),
		},
	}
	pool := New(1, loader)

	frame, err := pool.GetCommittedPage(7)
	if err != nil {
		t.Fatalf("GetCommittedPage() error = %v", err)
	}
	if frame == nil {
		t.Fatal("GetCommittedPage() = nil, want frame")
	}
	if frame.FrameType != FrameCommitted {
		t.Fatalf("frame.FrameType = %d, want %d", frame.FrameType, FrameCommitted)
	}
	if frame.PageID != 7 {
		t.Fatalf("frame.PageID = %d, want 7", frame.PageID)
	}
	if !bytes.Equal(frame.Data[:], loader.pages[7]) {
		t.Fatal("frame.Data mismatch")
	}
	if got := pool.committedFrameCount(); got != 1 {
		t.Fatalf("committedFrameCount() = %d, want 1", got)
	}

	again, err := pool.GetCommittedPage(7)
	if err != nil {
		t.Fatalf("second GetCommittedPage() error = %v", err)
	}
	if again != frame {
		t.Fatal("second GetCommittedPage() returned different frame")
	}
	if got := loader.reads[7]; got != 1 {
		t.Fatalf("loader reads = %d, want 1", got)
	}
	if got := pool.committedFrameCount(); got != 1 {
		t.Fatalf("committedFrameCount() after second read = %d, want 1", got)
	}
}

func TestGetCommittedPagePropagatesLoaderError(t *testing.T) {
	want := errors.New("boom")
	pool := New(1, &stubLoader{err: want})

	_, err := pool.GetCommittedPage(1)
	if !errors.Is(err, want) {
		t.Fatalf("GetCommittedPage() error = %v, want %v", err, want)
	}
}

func TestGetCommittedPageRejectsInvalidPageSize(t *testing.T) {
	pool := New(1, &stubLoader{
		pages: map[PageID][]byte{
			3: []byte{0x01, 0x02},
		},
	})

	_, err := pool.GetCommittedPage(3)
	if !errors.Is(err, errInvalidPageSize) {
		t.Fatalf("GetCommittedPage() error = %v, want %v", err, errInvalidPageSize)
	}
	if got := pool.committedFrameCount(); got != 0 {
		t.Fatalf("committedFrameCount() = %d, want 0", got)
	}
}

func TestGetCommittedPageCopiesDataIntoFrame(t *testing.T) {
	pageData := bytes.Repeat([]byte{0xCD}, PageSize)
	loader := &stubLoader{
		pages: map[PageID][]byte{
			9: pageData,
		},
	}
	pool := New(1, loader)

	frame, err := pool.GetCommittedPage(9)
	if err != nil {
		t.Fatalf("GetCommittedPage() error = %v", err)
	}
	pageData[0] = 0x00
	loader.pages[9][1] = 0x00

	if frame.Data[0] != 0xCD {
		t.Fatalf("frame.Data[0] = 0x%02x, want 0xCD", frame.Data[0])
	}
	if frame.Data[1] != 0xCD {
		t.Fatalf("frame.Data[1] = 0x%02x, want 0xCD", frame.Data[1])
	}
}
