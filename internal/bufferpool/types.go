package bufferpool

type PageID uint32

type PageType uint8

const PageSize = 4096

type FrameType uint8

const (
	FrameCommitted FrameType = iota
	FramePrivate
)

type LatchMode uint8

const (
	LatchShared LatchMode = iota
	LatchExclusive
)
