package bufferpool

import (
	"errors"
	"sort"
)

var errNoEvictableFrame = errors.New("bufferpool: no evictable frame available")

type BufferPool struct {
	committed map[PageID]*Frame
	order     []PageID
	capacity  int
	loader    PageLoader
}

type PageLoader interface {
	ReadPage(pageID PageID) ([]byte, error)
}

func New(size int, loader PageLoader) *BufferPool {
	if size <= 0 {
		size = 1
	}
	return &BufferPool{
		committed: make(map[PageID]*Frame),
		order:     make([]PageID, 0, size),
		capacity:  size,
		loader:    loader,
	}
}

func (bp *BufferPool) getCommittedFrame(pageID PageID) (*Frame, bool) {
	if bp == nil {
		return nil, false
	}
	frame, ok := bp.committed[pageID]
	return frame, ok
}

func (bp *BufferPool) trackCommittedFrame(frame *Frame) *Frame {
	if bp == nil || frame == nil {
		return nil
	}
	if _, exists := bp.committed[frame.PageID]; !exists {
		bp.order = append(bp.order, frame.PageID)
	}
	bp.committed[frame.PageID] = frame
	return frame
}

func (bp *BufferPool) committedFrameCount() int {
	if bp == nil {
		return 0
	}
	return len(bp.committed)
}

func (bp *BufferPool) DirtyFrames() []*Frame {
	if bp == nil || len(bp.committed) == 0 {
		return nil
	}

	ids := make([]int, 0, len(bp.committed))
	for pageID, frame := range bp.committed {
		if !bp.IsDirty(frame) {
			continue
		}
		ids = append(ids, int(pageID))
	}
	sort.Ints(ids)

	frames := make([]*Frame, 0, len(ids))
	for _, id := range ids {
		frames = append(frames, bp.committed[PageID(id)])
	}
	return frames
}

func (bp *BufferPool) ensureCapacity() error {
	if bp == nil || bp.capacity <= 0 || len(bp.committed) < bp.capacity {
		return nil
	}

	candidates := len(bp.order)
	for i := 0; i < candidates; i++ {
		pageID := bp.order[0]
		bp.order = bp.order[1:]

		frame, ok := bp.committed[pageID]
		if !ok || frame == nil {
			continue
		}
		if frame.FrameType != FrameCommitted || bp.IsDirty(frame) || frame.PinCount > 0 {
			bp.order = append(bp.order, pageID)
			continue
		}

		delete(bp.committed, pageID)
		return nil
	}

	return errNoEvictableFrame
}
