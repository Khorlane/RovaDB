package bufferpool

func (bp *BufferPool) GetCommittedPage(pageID PageID) (*Frame, error) {
	return bp.getOrLoadCommittedFrame(pageID)
}

func (bp *BufferPool) getOrLoadCommittedFrame(pageID PageID) (*Frame, error) {
	frame, err := bp.getOrLoadCommittedFrameUnlatched(pageID)
	if err != nil {
		return nil, err
	}
	return bp.pinSharedLatchAndReturn(frame), nil
}

func (bp *BufferPool) getOrLoadCommittedFrameUnlatched(pageID PageID) (*Frame, error) {
	if bp == nil {
		return nil, nil
	}
	if f, ok := bp.getCommittedFrame(pageID); ok {
		return f, nil
	}
	if err := bp.ensureCapacity(); err != nil {
		return nil, err
	}
	frame, err := bp.loadCommittedFrame(pageID)
	if err != nil {
		return nil, err
	}
	return bp.trackCommittedFrame(frame), nil
}

func (bp *BufferPool) GetPrivatePage(pageID PageID) (*Frame, error) {
	if bp == nil {
		return nil, nil
	}
	frame, err := bp.createPrivateFrame(pageID)
	if err != nil {
		return nil, err
	}
	return bp.pinExclusiveLatchAndReturn(frame), nil
}

func (bp *BufferPool) GetPage(pageID PageID) (*Frame, error) {
	return bp.GetCommittedPage(pageID)
}

func (bp *BufferPool) PutPage(f *Frame) {
	if bp == nil || f == nil {
		return
	}
	switch f.FrameType {
	case FrameCommitted:
		bp.trackCommittedFrame(f)
	case FramePrivate:
		bp.trackPrivateFrame(f)
	}
}

func (bp *BufferPool) pinSharedLatchAndReturn(f *Frame) *Frame {
	bp.Pin(f)
	bp.LatchShared(f)
	return f
}

func (bp *BufferPool) pinExclusiveLatchAndReturn(f *Frame) *Frame {
	bp.Pin(f)
	bp.LatchExclusive(f)
	return f
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

func (bp *BufferPool) MarkDirty(f *Frame) {
	if f == nil {
		return
	}
	f.Dirty = true
}

func (bp *BufferPool) MarkClean(f *Frame) {
	if f == nil {
		return
	}
	f.Dirty = false
}

func (bp *BufferPool) IsDirty(f *Frame) bool {
	if f == nil {
		return false
	}
	return f.Dirty
}

func (bp *BufferPool) LatchShared(f *Frame) {
	if f == nil {
		return
	}
	f.latch.RLock()
}

func (bp *BufferPool) UnlatchShared(f *Frame) {
	if f == nil {
		return
	}
	f.latch.RUnlock()
}

func (bp *BufferPool) LatchExclusive(f *Frame) {
	if f == nil {
		return
	}
	f.latch.Lock()
}

func (bp *BufferPool) UnlatchExclusive(f *Frame) {
	if f == nil {
		return
	}
	f.latch.Unlock()
}
