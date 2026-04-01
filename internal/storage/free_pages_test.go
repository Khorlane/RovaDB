package storage

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestInitFreePageCreatesValidFreePage(t *testing.T) {
	page := InitFreePage(7, 11)

	if len(page) != PageSize {
		t.Fatalf("len(page) = %d, want %d", len(page), PageSize)
	}
	if got := PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType : pageHeaderOffsetPageType+2])); got != PageTypeFreePage {
		t.Fatalf("pageType = %d, want %d", got, PageTypeFreePage)
	}
	if next, err := FreePageNext(page); err != nil {
		t.Fatalf("FreePageNext() error = %v", err)
	} else if next != 11 {
		t.Fatalf("FreePageNext() = %d, want 11", next)
	}
}

func TestFreePageNextRoundTrip(t *testing.T) {
	page := InitFreePage(5, 9)

	if err := SetFreePageNext(page, 13); err != nil {
		t.Fatalf("SetFreePageNext() error = %v", err)
	}
	next, err := FreePageNext(page)
	if err != nil {
		t.Fatalf("FreePageNext() error = %v", err)
	}
	if next != 13 {
		t.Fatalf("FreePageNext() = %d, want 13", next)
	}
}

func TestPageAllocatorPrefersFreeListHead(t *testing.T) {
	allocator := &PageAllocator{
		NextPageID: 20,
		FreePage:   FreePageState{HeadPageID: 7},
		ReadFreeNext: func(pageID uint32) (uint32, error) {
			if pageID != 7 {
				t.Fatalf("ReadFreeNext pageID = %d, want 7", pageID)
			}
			return 11, nil
		},
	}

	allocated, reused, err := allocator.Allocate()
	if err != nil {
		t.Fatalf("Allocate() error = %v", err)
	}
	if allocated != 7 {
		t.Fatalf("Allocate() pageID = %d, want 7", allocated)
	}
	if !reused {
		t.Fatal("Allocate() reused = false, want true")
	}
	if allocator.FreePage.HeadPageID != 11 {
		t.Fatalf("allocator.FreePage.HeadPageID = %d, want 11", allocator.FreePage.HeadPageID)
	}
	if allocator.NextPageID != 20 {
		t.Fatalf("allocator.NextPageID = %d, want 20", allocator.NextPageID)
	}
}

func TestPageAllocatorFallsBackToFreshPageID(t *testing.T) {
	allocator := &PageAllocator{NextPageID: 20}

	allocated, reused, err := allocator.Allocate()
	if err != nil {
		t.Fatalf("Allocate() error = %v", err)
	}
	if allocated != 20 {
		t.Fatalf("Allocate() pageID = %d, want 20", allocated)
	}
	if reused {
		t.Fatal("Allocate() reused = true, want false")
	}
	if allocator.NextPageID != 21 {
		t.Fatalf("allocator.NextPageID = %d, want 21", allocator.NextPageID)
	}
}

func TestPageAllocatorUpdatesFreeListHeadCorrectly(t *testing.T) {
	allocator := &PageAllocator{
		NextPageID: 30,
		FreePage:   FreePageState{HeadPageID: 4},
		ReadFreeNext: func(pageID uint32) (uint32, error) {
			switch pageID {
			case 4:
				return 6, nil
			case 6:
				return 0, nil
			default:
				t.Fatalf("ReadFreeNext pageID = %d, want 4 or 6", pageID)
				return 0, nil
			}
		},
	}

	first, reused, err := allocator.Allocate()
	if err != nil {
		t.Fatalf("first Allocate() error = %v", err)
	}
	if first != 4 || !reused {
		t.Fatalf("first Allocate() = (%d, %t), want (4, true)", first, reused)
	}

	second, reused, err := allocator.Allocate()
	if err != nil {
		t.Fatalf("second Allocate() error = %v", err)
	}
	if second != 6 || !reused {
		t.Fatalf("second Allocate() = (%d, %t), want (6, true)", second, reused)
	}
	if allocator.FreePage.HeadPageID != 0 {
		t.Fatalf("allocator.FreePage.HeadPageID = %d, want 0", allocator.FreePage.HeadPageID)
	}
}

func TestFreePageRejectsInvalidTypeOrSize(t *testing.T) {
	short := make([]byte, PageSize-1)
	if _, err := FreePageNext(short); !errors.Is(err, errCorruptedPageHeader) {
		t.Fatalf("FreePageNext(short) error = %v, want %v", err, errCorruptedPageHeader)
	}

	page := InitFreePage(5, 0)
	page[pageHeaderOffsetPageType] = byte(PageTypeTable)
	if _, err := FreePageNext(page); !errors.Is(err, errCorruptedPageHeader) {
		t.Fatalf("FreePageNext(wrong type) error = %v, want %v", err, errCorruptedPageHeader)
	}
}

func TestPageAllocatorPropagatesReadFreeNextError(t *testing.T) {
	wantErr := errors.New("boom")
	allocator := &PageAllocator{
		FreePage: FreePageState{HeadPageID: 9},
		ReadFreeNext: func(pageID uint32) (uint32, error) {
			return 0, wantErr
		},
	}

	_, _, err := allocator.Allocate()
	if !errors.Is(err, wantErr) {
		t.Fatalf("Allocate() error = %v, want %v", err, wantErr)
	}
}
