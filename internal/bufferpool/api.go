package bufferpool

func (bp *BufferPool) GetCommittedPage(pageID PageID) (*Frame, error) {
	if bp == nil {
		return nil, nil
	}
	if f, ok := bp.getCommittedFrame(pageID); ok {
		return f, nil
	}
	frame, err := bp.loadCommittedFrame(pageID)
	if err != nil {
		return nil, err
	}
	return bp.trackCommittedFrame(frame), nil
}

func (bp *BufferPool) GetPage(pageID PageID) (*Frame, error) {
	return bp.GetCommittedPage(pageID)
}

func (bp *BufferPool) PutPage(f *Frame) {
	if bp == nil || f == nil {
		return
	}
	if f.FrameType == FrameCommitted {
		bp.trackCommittedFrame(f)
	}
}

func (bp *BufferPool) Pin(f *Frame) {
	if f == nil {
		return
	}
	f.PinCount++
}

func (bp *BufferPool) Unpin(f *Frame) {
	if f == nil {
		return
	}
	if f.PinCount > 0 {
		f.PinCount--
	}
}
