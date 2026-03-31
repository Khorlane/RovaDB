package bufferpool

type BufferPool struct {
	committed map[PageID]*Frame
	loader    PageLoader
}

type PageLoader interface {
	ReadPage(pageID PageID) ([]byte, error)
}

func New(size int, loader PageLoader) *BufferPool {
	_ = size
	return &BufferPool{
		committed: make(map[PageID]*Frame),
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
	bp.committed[frame.PageID] = frame
	return frame
}

func (bp *BufferPool) committedFrameCount() int {
	if bp == nil {
		return 0
	}
	return len(bp.committed)
}
