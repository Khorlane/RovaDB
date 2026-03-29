package bufferpool

type BufferPool struct {
	frames map[PageID]*Frame
}

func New(size int) *BufferPool {
	return &BufferPool{
		frames: make(map[PageID]*Frame),
	}
}
