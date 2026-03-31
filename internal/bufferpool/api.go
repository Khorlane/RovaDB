package bufferpool

func (bp *BufferPool) GetCommittedPage(pageID PageID) (*Frame, error) {
	if bp == nil {
		return nil, nil
	}
	if f, ok := bp.frames[pageID]; ok {
		return f, nil
	}
	if bp.loader == nil {
		return nil, nil
	}

	data, err := bp.loader.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	frame := &Frame{
		PageID:    pageID,
		FrameType: FrameCommitted,
	}
	copy(frame.Data[:], data)
	bp.frames[pageID] = frame
	return frame, nil
}

func (bp *BufferPool) GetPage(pageID PageID) (*Frame, error) {
	return bp.GetCommittedPage(pageID)
}

func (bp *BufferPool) PutPage(f *Frame) {
	if bp == nil || f == nil {
		return
	}
	bp.frames[f.PageID] = f
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
