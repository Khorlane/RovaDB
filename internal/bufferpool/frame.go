package bufferpool

import (
	"errors"
	"sync"
)

var errInvalidPageSize = errors.New("bufferpool: invalid page size")

type Frame struct {
	PageID   PageID
	PageType PageType

	Data [PageSize]byte

	PageLSN uint64

	Dirty bool

	PinCount int

	FrameType FrameType

	latch sync.RWMutex
}

func newCommittedFrame(pageID PageID, data []byte) (*Frame, error) {
	if len(data) != PageSize {
		return nil, errInvalidPageSize
	}

	frame := &Frame{
		PageID:    pageID,
		FrameType: FrameCommitted,
	}
	copy(frame.Data[:], data)
	// TODO(buffer-pool.5): populate PageType/PageLSN when frame metadata can be
	// extracted without duplicating storage page parsing logic.
	return frame, nil
}

func newPrivateFrame(committed *Frame) (*Frame, error) {
	if committed == nil {
		return nil, nil
	}

	frame := &Frame{
		PageID:    committed.PageID,
		PageType:  committed.PageType,
		PageLSN:   committed.PageLSN,
		FrameType: FramePrivate,
	}
	copy(frame.Data[:], committed.Data[:])
	return frame, nil
}

func newPrivateFrameFromData(pageID PageID, data []byte) (*Frame, error) {
	if len(data) != PageSize {
		return nil, errInvalidPageSize
	}

	frame := &Frame{
		PageID:    pageID,
		FrameType: FramePrivate,
	}
	copy(frame.Data[:], data)
	return frame, nil
}

func (bp *BufferPool) loadCommittedFrame(pageID PageID) (*Frame, error) {
	if bp == nil || bp.loader == nil {
		return nil, nil
	}

	data, err := bp.loader.ReadPage(pageID)
	if err != nil {
		return nil, err
	}
	return newCommittedFrame(pageID, data)
}

func (bp *BufferPool) createPrivateFrame(pageID PageID) (*Frame, error) {
	if bp == nil {
		return nil, nil
	}
	if frame, ok := bp.getPrivateFrame(pageID); ok {
		return frame, nil
	}

	committed, err := bp.getOrLoadCommittedFrameUnlatched(pageID)
	if err != nil {
		return nil, err
	}
	private, err := newPrivateFrame(committed)
	if err != nil {
		return nil, err
	}
	return bp.trackPrivateFrame(private), nil
}
