package storage

import "encoding/binary"

const (
	freePageOffsetNext = pageHeaderSize
	freePageSize       = PageSize
)

// FreePageState tracks the head of the reusable free-page chain.
type FreePageState struct {
	HeadPageID uint32
}

// PageAllocator prefers reusable pages before extending the file.
type PageAllocator struct {
	NextPageID   uint32
	FreePage     FreePageState
	ReadFreeNext func(pageID uint32) (uint32, error)
}

// Allocate returns a reusable free page first, then falls back to the next fresh page.
func (a *PageAllocator) Allocate() (allocated uint32, reused bool, err error) {
	if a == nil {
		return 0, false, nil
	}
	if a.FreePage.HeadPageID != 0 {
		if a.ReadFreeNext == nil {
			return 0, false, errCorruptedPageHeader
		}
		head := a.FreePage.HeadPageID
		next, err := a.ReadFreeNext(head)
		if err != nil {
			return 0, false, err
		}
		a.FreePage.HeadPageID = next
		return head, true, nil
	}

	allocated = a.NextPageID
	if allocated == 0 {
		allocated = 1
	}
	a.NextPageID = allocated + 1
	return allocated, false, nil
}

// InitFreePage initializes a free-page image with the next-free pointer.
func InitFreePage(pageID uint32, nextFreePageID uint32) []byte {
	page := make([]byte, freePageSize)
	binary.LittleEndian.PutUint32(page[pageHeaderOffsetPageID:pageHeaderOffsetPageID+4], pageID)
	binary.LittleEndian.PutUint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2], uint16(PageTypeFreePage))
	binary.LittleEndian.PutUint32(page[freePageOffsetNext:freePageOffsetNext+4], nextFreePageID)
	_ = FinalizePageImage(page)
	return page
}

// FreePageNext returns the next-free pointer stored in a free page.
func FreePageNext(page []byte) (uint32, error) {
	if err := validateFreePage(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(page[freePageOffsetNext : freePageOffsetNext+4]), nil
}

// SetFreePageNext updates the next-free pointer in a free page.
func SetFreePageNext(page []byte, next uint32) error {
	if err := validateFreePage(page); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(page[freePageOffsetNext:freePageOffsetNext+4], next)
	return FinalizePageImage(page)
}

func validateFreePage(page []byte) error {
	if err := validateStampedPageHeader(page); err != nil {
		return err
	}
	if PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2])) != PageTypeFreePage {
		return errCorruptedPageHeader
	}
	if err := validateStoredPageChecksum(page); err != nil {
		return errCorruptedPageHeader
	}
	return nil
}
