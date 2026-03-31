package bufferpool

import (
	"bytes"
	"errors"
	"runtime"
	"testing"
	"time"
)

type stubLoader struct {
	pages map[PageID][]byte
	reads map[PageID]int
	err   error
	errs  map[PageID]error
}

func (l *stubLoader) ReadPage(pageID PageID) ([]byte, error) {
	if l.err != nil {
		return nil, l.err
	}
	if l.errs != nil {
		if err := l.errs[pageID]; err != nil {
			if l.reads == nil {
				l.reads = make(map[PageID]int)
			}
			l.reads[pageID]++
			return nil, err
		}
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
	if pool.IsDirty(frame) {
		t.Fatal("newly loaded frame is dirty, want clean")
	}
	if got := frame.PinCount; got != 1 {
		t.Fatalf("frame.PinCount = %d, want 1", got)
	}
	if got := pool.committedFrameCount(); got != 1 {
		t.Fatalf("committedFrameCount() = %d, want 1", got)
	}
	pool.UnlatchShared(frame)

	again, err := pool.GetCommittedPage(7)
	if err != nil {
		t.Fatalf("second GetCommittedPage() error = %v", err)
	}
	if again != frame {
		t.Fatal("second GetCommittedPage() returned different frame")
	}
	if got := frame.PinCount; got != 2 {
		t.Fatalf("frame.PinCount after second read = %d, want 2", got)
	}
	if got := loader.reads[7]; got != 1 {
		t.Fatalf("loader reads = %d, want 1", got)
	}
	if got := pool.committedFrameCount(); got != 1 {
		t.Fatalf("committedFrameCount() after second read = %d, want 1", got)
	}
	pool.UnlatchShared(frame)
}

func TestGetCommittedPagePropagatesLoaderError(t *testing.T) {
	want := errors.New("boom")
	pool := New(1, &stubLoader{err: want})

	_, err := pool.GetCommittedPage(1)
	if !errors.Is(err, want) {
		t.Fatalf("GetCommittedPage() error = %v, want %v", err, want)
	}
}

func TestGetCommittedPageLoadsDistinctPagesSeparately(t *testing.T) {
	loader := &stubLoader{
		pages: map[PageID][]byte{
			1: bytes.Repeat([]byte{0x11}, PageSize),
			2: bytes.Repeat([]byte{0x22}, PageSize),
		},
	}
	pool := New(1, loader)

	frame1, err := pool.GetCommittedPage(1)
	if err != nil {
		t.Fatalf("GetCommittedPage(1) error = %v", err)
	}
	frame2, err := pool.GetCommittedPage(2)
	if err != nil {
		t.Fatalf("GetCommittedPage(2) error = %v", err)
	}

	if frame1 == frame2 {
		t.Fatal("distinct page reads returned same frame pointer")
	}
	if got := frame1.PinCount; got != 1 {
		t.Fatalf("frame1.PinCount = %d, want 1", got)
	}
	if got := frame2.PinCount; got != 1 {
		t.Fatalf("frame2.PinCount = %d, want 1", got)
	}
	if got := loader.reads[1]; got != 1 {
		t.Fatalf("loader reads for page 1 = %d, want 1", got)
	}
	if got := loader.reads[2]; got != 1 {
		t.Fatalf("loader reads for page 2 = %d, want 1", got)
	}
	if got := pool.committedFrameCount(); got != 2 {
		t.Fatalf("committedFrameCount() = %d, want 2", got)
	}
	pool.UnlatchShared(frame1)
	pool.UnlatchShared(frame2)
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

func TestGetCommittedPageFailedLoadDoesNotPopulateCache(t *testing.T) {
	want := errors.New("load failed")
	loader := &stubLoader{
		errs: map[PageID]error{
			5: want,
		},
	}
	pool := New(1, loader)

	_, err := pool.GetCommittedPage(5)
	if !errors.Is(err, want) {
		t.Fatalf("GetCommittedPage() error = %v, want %v", err, want)
	}
	if got := loader.reads[5]; got != 1 {
		t.Fatalf("loader reads = %d, want 1", got)
	}
	if got := pool.committedFrameCount(); got != 0 {
		t.Fatalf("committedFrameCount() = %d, want 0", got)
	}

	_, err = pool.GetCommittedPage(5)
	if !errors.Is(err, want) {
		t.Fatalf("second GetCommittedPage() error = %v, want %v", err, want)
	}
	if got := loader.reads[5]; got != 2 {
		t.Fatalf("loader reads after second miss = %d, want 2", got)
	}
	if got := pool.committedFrameCount(); got != 0 {
		t.Fatalf("committedFrameCount() after second miss = %d, want 0", got)
	}
}

func TestGetCommittedPageRetryAfterFailedLoadCanPopulateCache(t *testing.T) {
	want := errors.New("load failed")
	loader := &stubLoader{
		pages: map[PageID][]byte{
			6: bytes.Repeat([]byte{0x66}, PageSize),
		},
		errs: map[PageID]error{
			6: want,
		},
	}
	pool := New(1, loader)

	_, err := pool.GetCommittedPage(6)
	if !errors.Is(err, want) {
		t.Fatalf("GetCommittedPage() error = %v, want %v", err, want)
	}
	delete(loader.errs, 6)

	frame, err := pool.GetCommittedPage(6)
	if err != nil {
		t.Fatalf("retry GetCommittedPage() error = %v", err)
	}
	if frame == nil {
		t.Fatal("retry GetCommittedPage() = nil, want frame")
	}
	if got := loader.reads[6]; got != 2 {
		t.Fatalf("loader reads after retry = %d, want 2", got)
	}
	if got := pool.committedFrameCount(); got != 1 {
		t.Fatalf("committedFrameCount() after retry = %d, want 1", got)
	}
	if got := frame.PinCount; got != 1 {
		t.Fatalf("frame.PinCount after retry = %d, want 1", got)
	}

	again, err := pool.GetCommittedPage(6)
	if err != nil {
		t.Fatalf("cached GetCommittedPage() error = %v", err)
	}
	if again != frame {
		t.Fatal("cached GetCommittedPage() returned different frame")
	}
	if got := loader.reads[6]; got != 2 {
		t.Fatalf("loader reads after cache hit = %d, want 2", got)
	}
	if got := frame.PinCount; got != 2 {
		t.Fatalf("frame.PinCount after cache hit = %d, want 2", got)
	}
	pool.UnlatchShared(frame)
	pool.UnlatchShared(frame)
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
	pool.UnlatchShared(frame)
}

func TestUnpinDecrementsButNeverGoesNegative(t *testing.T) {
	pool := New(1, &stubLoader{
		pages: map[PageID][]byte{
			4: bytes.Repeat([]byte{0x44}, PageSize),
		},
	})

	frame, err := pool.GetCommittedPage(4)
	if err != nil {
		t.Fatalf("GetCommittedPage() error = %v", err)
	}
	if _, err := pool.GetCommittedPage(4); err != nil {
		t.Fatalf("second GetCommittedPage() error = %v", err)
	}
	if got := frame.PinCount; got != 2 {
		t.Fatalf("frame.PinCount = %d, want 2", got)
	}

	pool.Unpin(frame)
	if got := frame.PinCount; got != 1 {
		t.Fatalf("frame.PinCount after first Unpin() = %d, want 1", got)
	}
	pool.Unpin(frame)
	if got := frame.PinCount; got != 0 {
		t.Fatalf("frame.PinCount after second Unpin() = %d, want 0", got)
	}
	pool.Unpin(frame)
	if got := frame.PinCount; got != 0 {
		t.Fatalf("frame.PinCount after extra Unpin() = %d, want 0", got)
	}
	pool.UnlatchShared(frame)
	pool.UnlatchShared(frame)
}

func TestPinAndUnpinNilAreNoOps(t *testing.T) {
	pool := New(1, nil)

	pool.Pin(nil)
	pool.Unpin(nil)
}

func TestGetCommittedPageReturnsSharedLatchedFrame(t *testing.T) {
	pool := New(1, &stubLoader{
		pages: map[PageID][]byte{
			8: bytes.Repeat([]byte{0x88}, PageSize),
		},
	})

	frame, err := pool.GetCommittedPage(8)
	if err != nil {
		t.Fatalf("GetCommittedPage() error = %v", err)
	}
	if got := frame.PinCount; got != 1 {
		t.Fatalf("frame.PinCount = %d, want 1", got)
	}

	started := make(chan struct{})
	acquired := make(chan struct{})
	go func() {
		close(started)
		pool.LatchExclusive(frame)
		close(acquired)
		pool.UnlatchExclusive(frame)
	}()

	<-started
	runtime.Gosched()
	select {
	case <-acquired:
		t.Fatal("exclusive latch acquired before shared latch release")
	default:
	}

	pool.UnlatchShared(frame)

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("exclusive latch not acquired after shared latch release")
	}
}

func TestRepeatedGetCommittedPageReturnsSharedLatchedFrame(t *testing.T) {
	pool := New(1, &stubLoader{
		pages: map[PageID][]byte{
			10: bytes.Repeat([]byte{0xAA}, PageSize),
		},
	})

	frame, err := pool.GetCommittedPage(10)
	if err != nil {
		t.Fatalf("first GetCommittedPage() error = %v", err)
	}
	again, err := pool.GetCommittedPage(10)
	if err != nil {
		t.Fatalf("second GetCommittedPage() error = %v", err)
	}
	if again != frame {
		t.Fatal("second GetCommittedPage() returned different frame")
	}

	pool.UnlatchShared(frame)
	pool.UnlatchShared(frame)
}

func TestUnpinDoesNotReleaseLatch(t *testing.T) {
	pool := New(1, &stubLoader{
		pages: map[PageID][]byte{
			11: bytes.Repeat([]byte{0xBB}, PageSize),
		},
	})

	frame, err := pool.GetCommittedPage(11)
	if err != nil {
		t.Fatalf("GetCommittedPage() error = %v", err)
	}
	pool.Unpin(frame)
	if got := frame.PinCount; got != 0 {
		t.Fatalf("frame.PinCount after Unpin() = %d, want 0", got)
	}

	started := make(chan struct{})
	acquired := make(chan struct{})
	go func() {
		close(started)
		pool.LatchExclusive(frame)
		close(acquired)
		pool.UnlatchExclusive(frame)
	}()

	<-started
	runtime.Gosched()
	select {
	case <-acquired:
		t.Fatal("exclusive latch acquired while shared latch should still be held")
	default:
	}

	pool.UnlatchShared(frame)

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("exclusive latch not acquired after shared latch release")
	}
}

func TestUnlatchSharedNilIsNoOp(t *testing.T) {
	pool := New(1, nil)

	pool.UnlatchShared(nil)
}

func TestMarkDirtyAndMarkCleanUpdateTrackedFrameState(t *testing.T) {
	pool := New(1, &stubLoader{
		pages: map[PageID][]byte{
			12: bytes.Repeat([]byte{0x12}, PageSize),
		},
	})

	frame, err := pool.GetCommittedPage(12)
	if err != nil {
		t.Fatalf("GetCommittedPage() error = %v", err)
	}
	if pool.IsDirty(frame) {
		t.Fatal("frame starts dirty, want clean")
	}

	pool.MarkDirty(frame)
	if !pool.IsDirty(frame) {
		t.Fatal("frame not dirty after MarkDirty()")
	}

	pool.MarkClean(frame)
	if pool.IsDirty(frame) {
		t.Fatal("frame still dirty after MarkClean()")
	}
	pool.UnlatchShared(frame)
}

func TestDirtyHelpersAreNilSafe(t *testing.T) {
	pool := New(1, nil)

	pool.MarkDirty(nil)
	pool.MarkClean(nil)
	if pool.IsDirty(nil) {
		t.Fatal("IsDirty(nil) = true, want false")
	}
}

func TestDirtyFramesReturnsOnlyTrackedDirtyFrames(t *testing.T) {
	pool := New(1, &stubLoader{
		pages: map[PageID][]byte{
			13: bytes.Repeat([]byte{0x13}, PageSize),
			14: bytes.Repeat([]byte{0x14}, PageSize),
			15: bytes.Repeat([]byte{0x15}, PageSize),
		},
	})

	frame13, err := pool.GetCommittedPage(13)
	if err != nil {
		t.Fatalf("GetCommittedPage(13) error = %v", err)
	}
	frame14, err := pool.GetCommittedPage(14)
	if err != nil {
		t.Fatalf("GetCommittedPage(14) error = %v", err)
	}
	frame15, err := pool.GetCommittedPage(15)
	if err != nil {
		t.Fatalf("GetCommittedPage(15) error = %v", err)
	}

	pool.MarkDirty(frame14)
	pool.MarkDirty(frame13)

	dirty := pool.DirtyFrames()
	if len(dirty) != 2 {
		t.Fatalf("len(DirtyFrames()) = %d, want 2", len(dirty))
	}
	if dirty[0] != frame13 {
		t.Fatal("DirtyFrames()[0] is not tracked frame13")
	}
	if dirty[1] != frame14 {
		t.Fatal("DirtyFrames()[1] is not tracked frame14")
	}

	pool.MarkDirty(frame13)
	dirty = pool.DirtyFrames()
	if len(dirty) != 2 {
		t.Fatalf("len(DirtyFrames()) after repeat dirty = %d, want 2", len(dirty))
	}

	pool.MarkClean(frame13)
	dirty = pool.DirtyFrames()
	if len(dirty) != 1 {
		t.Fatalf("len(DirtyFrames()) after clean = %d, want 1", len(dirty))
	}
	if dirty[0] != frame14 {
		t.Fatal("DirtyFrames()[0] is not tracked frame14 after clean")
	}

	pool.UnlatchShared(frame13)
	pool.UnlatchShared(frame14)
	pool.UnlatchShared(frame15)
}
