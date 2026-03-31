package bufferpool

type BufferPool struct {
	frames map[PageID]*Frame
	loader PageLoader
}

type PageLoader interface {
	ReadPage(pageID PageID) ([]byte, error)
}

func New(size int, loader PageLoader) *BufferPool {
	_ = size
	return &BufferPool{
		frames: make(map[PageID]*Frame),
		loader: loader,
	}
}
