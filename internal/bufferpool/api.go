package bufferpool

func (bp *BufferPool) GetPage(pageID PageID) (*Frame, error) {
	// Skeleton: no real loading yet
	if f, ok := bp.frames[pageID]; ok {
		return f, nil
	}
	return nil, nil
}

func (bp *BufferPool) PutPage(f *Frame) {
	bp.frames[f.PageID] = f
}

func (bp *BufferPool) Pin(f *Frame) {
	f.PinCount++
}

func (bp *BufferPool) Unpin(f *Frame) {
	if f.PinCount > 0 {
		f.PinCount--
	}
}
