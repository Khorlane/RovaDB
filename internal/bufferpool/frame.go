package bufferpool

type Frame struct {
	PageID   PageID
	PageType PageType

	Data [PageSize]byte

	PageLSN uint64

	Dirty bool

	PinCount int

	FrameType FrameType
}
