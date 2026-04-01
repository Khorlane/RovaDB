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
	pool := New(2, loader)

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
	pool := New(2, &stubLoader{err: want})

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
	pool := New(2, loader)

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
	pool := New(2, &stubLoader{
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
	pool := New(2, loader)

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
	pool := New(2, loader)

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
	pool := New(2, loader)

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
	pool := New(2, &stubLoader{
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
	pool := New(2, nil)

	pool.Pin(nil)
	pool.Unpin(nil)
}

func TestGetCommittedPageReturnsSharedLatchedFrame(t *testing.T) {
	pool := New(2, &stubLoader{
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
	pool := New(2, &stubLoader{
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
	pool := New(2, &stubLoader{
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
	pool := New(2, nil)

	pool.UnlatchShared(nil)
}

func TestMarkDirtyAndMarkCleanUpdateTrackedFrameState(t *testing.T) {
	pool := New(2, &stubLoader{
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
	pool := New(2, nil)

	pool.MarkDirty(nil)
	pool.MarkClean(nil)
	if pool.IsDirty(nil) {
		t.Fatal("IsDirty(nil) = true, want false")
	}
}

func TestDirtyFramesReturnsOnlyTrackedDirtyFrames(t *testing.T) {
	pool := New(3, &stubLoader{
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

func TestEvictsOldestCleanUnpinnedFrameAtCapacity(t *testing.T) {
	loader := &stubLoader{
		pages: map[PageID][]byte{
			1: bytes.Repeat([]byte{0x01}, PageSize),
			2: bytes.Repeat([]byte{0x02}, PageSize),
		},
	}
	pool := New(1, loader)

	frame1, err := pool.GetCommittedPage(1)
	if err != nil {
		t.Fatalf("GetCommittedPage(1) error = %v", err)
	}
	pool.UnlatchShared(frame1)
	pool.Unpin(frame1)

	frame2, err := pool.GetCommittedPage(2)
	if err != nil {
		t.Fatalf("GetCommittedPage(2) error = %v", err)
	}
	if frame2 == frame1 {
		t.Fatal("GetCommittedPage(2) returned evicted frame1 pointer")
	}
	if got := pool.committedFrameCount(); got != 1 {
		t.Fatalf("committedFrameCount() = %d, want 1", got)
	}
	if _, ok := pool.getCommittedFrame(1); ok {
		t.Fatal("page 1 still tracked after eviction")
	}
	if got := loader.reads[1]; got != 1 {
		t.Fatalf("loader reads for page 1 = %d, want 1", got)
	}
	if got := loader.reads[2]; got != 1 {
		t.Fatalf("loader reads for page 2 = %d, want 1", got)
	}
	pool.UnlatchShared(frame2)
	pool.Unpin(frame2)

	reloaded1, err := pool.GetCommittedPage(1)
	if err != nil {
		t.Fatalf("GetCommittedPage(1) reload error = %v", err)
	}
	if reloaded1 == frame1 {
		t.Fatal("reloaded page 1 reused evicted frame pointer")
	}
	if got := loader.reads[1]; got != 2 {
		t.Fatalf("loader reads for page 1 after reload = %d, want 2", got)
	}
	pool.UnlatchShared(reloaded1)
}

func TestPinnedFrameIsNotEvictable(t *testing.T) {
	loader := &stubLoader{
		pages: map[PageID][]byte{
			1: bytes.Repeat([]byte{0x01}, PageSize),
			2: bytes.Repeat([]byte{0x02}, PageSize),
		},
	}
	pool := New(1, loader)

	frame1, err := pool.GetCommittedPage(1)
	if err != nil {
		t.Fatalf("GetCommittedPage(1) error = %v", err)
	}
	pool.UnlatchShared(frame1)

	_, err = pool.GetCommittedPage(2)
	if !errors.Is(err, errNoEvictableFrame) {
		t.Fatalf("GetCommittedPage(2) error = %v, want %v", err, errNoEvictableFrame)
	}
	if got := loader.reads[2]; got != 0 {
		t.Fatalf("loader reads for page 2 = %d, want 0", got)
	}
	if _, ok := pool.getCommittedFrame(1); !ok {
		t.Fatal("page 1 missing after failed eviction")
	}
}

func TestDirtyFrameIsNotEvictable(t *testing.T) {
	loader := &stubLoader{
		pages: map[PageID][]byte{
			1: bytes.Repeat([]byte{0x01}, PageSize),
			2: bytes.Repeat([]byte{0x02}, PageSize),
		},
	}
	pool := New(1, loader)

	frame1, err := pool.GetCommittedPage(1)
	if err != nil {
		t.Fatalf("GetCommittedPage(1) error = %v", err)
	}
	pool.MarkDirty(frame1)
	pool.UnlatchShared(frame1)
	pool.Unpin(frame1)

	_, err = pool.GetCommittedPage(2)
	if !errors.Is(err, errNoEvictableFrame) {
		t.Fatalf("GetCommittedPage(2) error = %v, want %v", err, errNoEvictableFrame)
	}
	if got := loader.reads[2]; got != 0 {
		t.Fatalf("loader reads for page 2 = %d, want 0", got)
	}
	if _, ok := pool.getCommittedFrame(1); !ok {
		t.Fatal("page 1 missing after dirty eviction attempt")
	}
}

func TestCacheHitDoesNotTriggerEviction(t *testing.T) {
	loader := &stubLoader{
		pages: map[PageID][]byte{
			1: bytes.Repeat([]byte{0x01}, PageSize),
		},
	}
	pool := New(1, loader)

	frame1, err := pool.GetCommittedPage(1)
	if err != nil {
		t.Fatalf("GetCommittedPage(1) error = %v", err)
	}
	pool.UnlatchShared(frame1)

	again, err := pool.GetCommittedPage(1)
	if err != nil {
		t.Fatalf("second GetCommittedPage(1) error = %v", err)
	}
	if again != frame1 {
		t.Fatal("cache hit returned different frame")
	}
	if got := loader.reads[1]; got != 1 {
		t.Fatalf("loader reads for page 1 = %d, want 1", got)
	}
	if len(pool.order) != 1 {
		t.Fatalf("len(order) = %d, want 1", len(pool.order))
	}
	pool.UnlatchShared(frame1)
}

func TestCapacityNormalizesToAtLeastOne(t *testing.T) {
	pool := New(0, nil)
	if pool.capacity != 1 {
		t.Fatalf("capacity = %d, want 1", pool.capacity)
	}
}

func TestGetPrivatePageCreatesIndependentPrivateFrame(t *testing.T) {
	loader := &stubLoader{
		pages: map[PageID][]byte{
			21: bytes.Repeat([]byte{0x21}, PageSize),
		},
	}
	pool := New(2, loader)

	committed, err := pool.GetCommittedPage(21)
	if err != nil {
		t.Fatalf("GetCommittedPage() error = %v", err)
	}
	pool.UnlatchShared(committed)
	pool.Unpin(committed)

	private, err := pool.GetPrivatePage(21)
	if err != nil {
		t.Fatalf("GetPrivatePage() error = %v", err)
	}
	if private == nil {
		t.Fatal("GetPrivatePage() = nil, want frame")
	}
	if private == committed {
		t.Fatal("private frame pointer matches committed frame")
	}
	if private.FrameType != FramePrivate {
		t.Fatalf("private.FrameType = %d, want %d", private.FrameType, FramePrivate)
	}
	if private.Dirty {
		t.Fatal("new private frame starts dirty, want clean")
	}
	if private.PinCount != 1 {
		t.Fatalf("private.PinCount = %d, want 1", private.PinCount)
	}
	if !bytes.Equal(private.Data[:], committed.Data[:]) {
		t.Fatal("private frame data mismatch at creation")
	}
	if got := pool.privateFrameCount(); got != 1 {
		t.Fatalf("privateFrameCount() = %d, want 1", got)
	}

	private.Data[0] = 0x99
	if committed.Data[0] != 0x21 {
		t.Fatalf("committed.Data[0] = 0x%02x, want 0x21", committed.Data[0])
	}

	pool.UnlatchExclusive(private)
	pool.Unpin(private)
}

func TestGetPrivatePageReturnsTrackedPrivateFrameOnRepeat(t *testing.T) {
	loader := &stubLoader{
		pages: map[PageID][]byte{
			22: bytes.Repeat([]byte{0x22}, PageSize),
		},
	}
	pool := New(2, loader)

	private, err := pool.GetPrivatePage(22)
	if err != nil {
		t.Fatalf("first GetPrivatePage() error = %v", err)
	}
	pool.UnlatchExclusive(private)

	again, err := pool.GetPrivatePage(22)
	if err != nil {
		t.Fatalf("second GetPrivatePage() error = %v", err)
	}
	if again != private {
		t.Fatal("second GetPrivatePage() returned different private frame")
	}
	if private.PinCount != 2 {
		t.Fatalf("private.PinCount = %d, want 2", private.PinCount)
	}
	if got := loader.reads[22]; got != 1 {
		t.Fatalf("loader reads = %d, want 1", got)
	}

	pool.UnlatchExclusive(private)
	pool.Unpin(private)
	pool.Unpin(private)
}

func TestCommittedReadsStillReturnCommittedFrameWhenPrivateExists(t *testing.T) {
	loader := &stubLoader{
		pages: map[PageID][]byte{
			23: bytes.Repeat([]byte{0x23}, PageSize),
		},
	}
	pool := New(2, loader)

	private, err := pool.GetPrivatePage(23)
	if err != nil {
		t.Fatalf("GetPrivatePage() error = %v", err)
	}
	private.Data[1] = 0xFE
	pool.MarkDirty(private)
	pool.UnlatchExclusive(private)
	pool.Unpin(private)

	committed, err := pool.GetCommittedPage(23)
	if err != nil {
		t.Fatalf("GetCommittedPage() error = %v", err)
	}
	if committed.FrameType != FrameCommitted {
		t.Fatalf("committed.FrameType = %d, want %d", committed.FrameType, FrameCommitted)
	}
	if committed == private {
		t.Fatal("GetCommittedPage() returned private frame")
	}
	if committed.Data[1] != 0x23 {
		t.Fatalf("committed.Data[1] = 0x%02x, want 0x23", committed.Data[1])
	}
	if got := pool.committedFrameCount(); got != 1 {
		t.Fatalf("committedFrameCount() = %d, want 1", got)
	}
	if len(pool.DirtyFrames()) != 0 {
		t.Fatalf("len(DirtyFrames()) = %d, want 0 for committed-only enumeration", len(pool.DirtyFrames()))
	}

	pool.UnlatchShared(committed)
	pool.Unpin(committed)
}

func TestPrivateFramesAreNotEvictionCandidates(t *testing.T) {
	loader := &stubLoader{
		pages: map[PageID][]byte{
			24: bytes.Repeat([]byte{0x24}, PageSize),
			25: bytes.Repeat([]byte{0x25}, PageSize),
		},
	}
	pool := New(1, loader)

	private, err := pool.GetPrivatePage(24)
	if err != nil {
		t.Fatalf("GetPrivatePage(24) error = %v", err)
	}
	pool.UnlatchExclusive(private)
	pool.Unpin(private)

	committed25, err := pool.GetCommittedPage(25)
	if err != nil {
		t.Fatalf("GetCommittedPage(25) error = %v", err)
	}
	if _, ok := pool.getPrivateFrame(24); !ok {
		t.Fatal("private frame for page 24 missing after committed eviction path")
	}
	if _, ok := pool.getCommittedFrame(24); ok {
		t.Fatal("committed frame for page 24 still tracked after eviction")
	}
	pool.UnlatchShared(committed25)
	pool.Unpin(committed25)
}

func TestUnlatchExclusiveNilIsNoOp(t *testing.T) {
	pool := New(2, nil)

	pool.UnlatchExclusive(nil)
}

func TestDiscardPrivatePagesRemovesPrivateFramesButKeepsCommitted(t *testing.T) {
	loader := &stubLoader{
		pages: map[PageID][]byte{
			26: bytes.Repeat([]byte{0x26}, PageSize),
		},
	}
	pool := New(2, loader)

	committed, err := pool.GetCommittedPage(26)
	if err != nil {
		t.Fatalf("GetCommittedPage() error = %v", err)
	}
	pool.UnlatchShared(committed)
	pool.Unpin(committed)

	private, err := pool.GetPrivatePage(26)
	if err != nil {
		t.Fatalf("GetPrivatePage() error = %v", err)
	}
	private.Data[0] = 0x99
	pool.MarkDirty(private)
	pool.UnlatchExclusive(private)
	pool.Unpin(private)

	pool.DiscardPrivatePages()
	if got := pool.privateFrameCount(); got != 0 {
		t.Fatalf("privateFrameCount() = %d, want 0", got)
	}

	committedAgain, err := pool.GetCommittedPage(26)
	if err != nil {
		t.Fatalf("GetCommittedPage() after discard error = %v", err)
	}
	if committedAgain != committed {
		t.Fatal("GetCommittedPage() returned different committed frame after discard")
	}
	if committedAgain.Data[0] != 0x26 {
		t.Fatalf("committedAgain.Data[0] = 0x%02x, want 0x26", committedAgain.Data[0])
	}
	pool.UnlatchShared(committedAgain)
	pool.Unpin(committedAgain)

	privateAgain, err := pool.GetPrivatePage(26)
	if err != nil {
		t.Fatalf("GetPrivatePage() recreate error = %v", err)
	}
	if privateAgain.Data[0] != 0x26 {
		t.Fatalf("privateAgain.Data[0] = 0x%02x, want recreated committed byte 0x26", privateAgain.Data[0])
	}
	pool.UnlatchExclusive(privateAgain)
	pool.Unpin(privateAgain)
}

func TestPromotePrivatePagesReplacesCommittedContentAndClearsPrivate(t *testing.T) {
	loader := &stubLoader{
		pages: map[PageID][]byte{
			27: bytes.Repeat([]byte{0x27}, PageSize),
		},
	}
	pool := New(2, loader)

	committed, err := pool.GetCommittedPage(27)
	if err != nil {
		t.Fatalf("GetCommittedPage() error = %v", err)
	}
	pool.UnlatchShared(committed)
	pool.Unpin(committed)

	private, err := pool.GetPrivatePage(27)
	if err != nil {
		t.Fatalf("GetPrivatePage() error = %v", err)
	}
	private.Data[0] = 0x99
	pool.MarkDirty(private)
	pool.UnlatchExclusive(private)
	pool.Unpin(private)

	pool.PromotePrivatePages()
	if got := pool.privateFrameCount(); got != 0 {
		t.Fatalf("privateFrameCount() = %d, want 0", got)
	}

	promoted, ok := pool.getCommittedFrame(27)
	if !ok || promoted == nil {
		t.Fatal("committed frame missing after promotion")
	}
	if promoted == private {
		t.Fatal("committed frame aliases private frame after promotion")
	}
	if promoted.FrameType != FrameCommitted {
		t.Fatalf("promoted.FrameType = %d, want %d", promoted.FrameType, FrameCommitted)
	}
	if promoted.Dirty {
		t.Fatal("promoted frame is dirty, want clean")
	}
	if promoted.PinCount != 0 {
		t.Fatalf("promoted.PinCount = %d, want 0", promoted.PinCount)
	}
	if promoted.Data[0] != 0x99 {
		t.Fatalf("promoted.Data[0] = 0x%02x, want 0x99", promoted.Data[0])
	}

	committedAgain, err := pool.GetCommittedPage(27)
	if err != nil {
		t.Fatalf("GetCommittedPage() after promotion error = %v", err)
	}
	if committedAgain.Data[0] != 0x99 {
		t.Fatalf("GetCommittedPage() after promotion saw 0x%02x, want 0x99", committedAgain.Data[0])
	}
	pool.UnlatchShared(committedAgain)
	pool.Unpin(committedAgain)
}

func TestRequirePrivateFrameRejectsCommittedFrame(t *testing.T) {
	loader := &stubLoader{
		pages: map[PageID][]byte{
			28: bytes.Repeat([]byte{0x28}, PageSize),
		},
	}
	pool := New(2, loader)

	committed, err := pool.GetCommittedPage(28)
	if err != nil {
		t.Fatalf("GetCommittedPage() error = %v", err)
	}
	if err := RequirePrivateFrame(committed); !errors.Is(err, errFrameNotPrivate) {
		t.Fatalf("RequirePrivateFrame(committed) error = %v, want %v", err, errFrameNotPrivate)
	}
	pool.UnlatchShared(committed)
	pool.Unpin(committed)

	private, err := pool.GetPrivatePage(28)
	if err != nil {
		t.Fatalf("GetPrivatePage() error = %v", err)
	}
	if err := RequirePrivateFrame(private); err != nil {
		t.Fatalf("RequirePrivateFrame(private) error = %v, want nil", err)
	}
	pool.UnlatchExclusive(private)
	pool.Unpin(private)
}
