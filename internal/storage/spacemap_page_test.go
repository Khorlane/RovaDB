package storage

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestInitSpaceMapPageEmptyState(t *testing.T) {
	page := InitSpaceMapPage(31, 7)

	if err := ValidateSpaceMapPage(page); err != nil {
		t.Fatalf("ValidateSpaceMapPage() error = %v", err)
	}
	if got := PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType : pageHeaderOffsetPageType+2])); got != PageTypeSpaceMap {
		t.Fatalf("pageType = %d, want %d", got, PageTypeSpaceMap)
	}
}

func TestSpaceMapEntryAppendReadUpdateRoundTrip(t *testing.T) {
	page := InitSpaceMapPage(32, 8)

	entryID, err := AppendSpaceMapEntry(page, SpaceMapEntry{DataPageID: 100, FreeSpaceBucket: SpaceMapBucketHigh})
	if err != nil {
		t.Fatalf("AppendSpaceMapEntry(first) error = %v", err)
	}
	if entryID != 0 {
		t.Fatalf("entryID = %d, want 0", entryID)
	}
	if _, err := AppendSpaceMapEntry(page, SpaceMapEntry{DataPageID: 101, FreeSpaceBucket: SpaceMapBucketLow}); err != nil {
		t.Fatalf("AppendSpaceMapEntry(second) error = %v", err)
	}

	entry, err := SpaceMapPageEntry(page, 0)
	if err != nil {
		t.Fatalf("SpaceMapPageEntry(0) error = %v", err)
	}
	if entry.DataPageID != 100 || entry.FreeSpaceBucket != SpaceMapBucketHigh {
		t.Fatalf("entry0 = %#v, want DataPageID=100 bucket=High", entry)
	}

	if err := UpdateSpaceMapEntry(page, 1, SpaceMapEntry{DataPageID: 101, FreeSpaceBucket: SpaceMapBucketMedium}); err != nil {
		t.Fatalf("UpdateSpaceMapEntry() error = %v", err)
	}
	updated, err := SpaceMapPageEntry(page, 1)
	if err != nil {
		t.Fatalf("SpaceMapPageEntry(1) error = %v", err)
	}
	if updated.DataPageID != 101 || updated.FreeSpaceBucket != SpaceMapBucketMedium {
		t.Fatalf("updated entry = %#v, want DataPageID=101 bucket=Medium", updated)
	}
}

func TestSpaceMapNextPageLinkageRoundTrip(t *testing.T) {
	page := InitSpaceMapPage(33, 9)

	if err := SetSpaceMapNextPageID(page, 45); err != nil {
		t.Fatalf("SetSpaceMapNextPageID() error = %v", err)
	}
	nextPageID, err := SpaceMapNextPageID(page)
	if err != nil {
		t.Fatalf("SpaceMapNextPageID() error = %v", err)
	}
	if nextPageID != 45 {
		t.Fatalf("SpaceMapNextPageID() = %d, want 45", nextPageID)
	}
}

func TestRemoveSpaceMapEntryCompactsRemainingEntries(t *testing.T) {
	page := InitSpaceMapPage(40, 18)
	for _, entry := range []SpaceMapEntry{
		{DataPageID: 500, FreeSpaceBucket: SpaceMapBucketHigh},
		{DataPageID: 501, FreeSpaceBucket: SpaceMapBucketMedium},
		{DataPageID: 502, FreeSpaceBucket: SpaceMapBucketLow},
	} {
		if _, err := AppendSpaceMapEntry(page, entry); err != nil {
			t.Fatalf("AppendSpaceMapEntry() error = %v", err)
		}
	}

	if err := RemoveSpaceMapEntry(page, 1); err != nil {
		t.Fatalf("RemoveSpaceMapEntry() error = %v", err)
	}
	entryCount, err := SpaceMapEntryCount(page)
	if err != nil {
		t.Fatalf("SpaceMapEntryCount() error = %v", err)
	}
	if entryCount != 2 {
		t.Fatalf("SpaceMapEntryCount() = %d, want 2", entryCount)
	}
	entry, err := SpaceMapPageEntry(page, 1)
	if err != nil {
		t.Fatalf("SpaceMapPageEntry(1) error = %v", err)
	}
	if entry.DataPageID != 502 || entry.FreeSpaceBucket != SpaceMapBucketLow {
		t.Fatalf("entry after removal = %#v, want DataPageID=502 bucket=Low", entry)
	}
}

func TestValidateSpaceMapPageRejectsWrongType(t *testing.T) {
	page := InitTableHeaderPage(34, 12)

	if err := ValidateSpaceMapPage(page); !errors.Is(err, errCorruptedSpaceMapPage) {
		t.Fatalf("ValidateSpaceMapPage() error = %v, want %v", err, errCorruptedSpaceMapPage)
	}
}

func TestValidateSpaceMapPageRejectsBadBucket(t *testing.T) {
	page := InitSpaceMapPage(35, 13)
	if _, err := AppendSpaceMapEntry(page, SpaceMapEntry{DataPageID: 200, FreeSpaceBucket: SpaceMapBucketHigh}); err != nil {
		t.Fatalf("AppendSpaceMapEntry() error = %v", err)
	}
	page[spaceMapEntriesOffset+4] = 99

	if err := ValidateSpaceMapPage(page); !errors.Is(err, errCorruptedSpaceMapPage) {
		t.Fatalf("ValidateSpaceMapPage() error = %v, want %v", err, errCorruptedSpaceMapPage)
	}
}

func TestValidateSpaceMapPageRejectsEntryCountBeyondCapacity(t *testing.T) {
	page := InitSpaceMapPage(36, 14)
	binary.LittleEndian.PutUint16(page[spaceMapBodyOffsetEntryCount:spaceMapBodyOffsetEntryCount+2], uint16(SpaceMapPageEntryCapacity()+1))

	if err := ValidateSpaceMapPage(page); !errors.Is(err, errCorruptedSpaceMapPage) {
		t.Fatalf("ValidateSpaceMapPage() error = %v, want %v", err, errCorruptedSpaceMapPage)
	}
}

func TestSetSpaceMapNextPageIDRejectsSelfReference(t *testing.T) {
	page := InitSpaceMapPage(37, 15)

	if err := SetSpaceMapNextPageID(page, 37); !errors.Is(err, errCorruptedSpaceMapPage) {
		t.Fatalf("SetSpaceMapNextPageID() error = %v, want %v", err, errCorruptedSpaceMapPage)
	}
}

func TestAppendSpaceMapEntryRejectsInvalidBucket(t *testing.T) {
	page := InitSpaceMapPage(38, 16)

	if _, err := AppendSpaceMapEntry(page, SpaceMapEntry{DataPageID: 300, FreeSpaceBucket: SpaceMapFreeSpaceBucket(0)}); !errors.Is(err, errCorruptedSpaceMapPage) {
		t.Fatalf("AppendSpaceMapEntry() error = %v, want %v", err, errCorruptedSpaceMapPage)
	}
}

func TestAppendSpaceMapEntryRejectsWhenPageFull(t *testing.T) {
	page := InitSpaceMapPage(39, 17)

	for i := 0; i < SpaceMapPageEntryCapacity(); i++ {
		_, err := AppendSpaceMapEntry(page, SpaceMapEntry{
			DataPageID:      PageID(400 + i),
			FreeSpaceBucket: SpaceMapBucketLow,
		})
		if err != nil {
			t.Fatalf("AppendSpaceMapEntry(%d) error = %v", i, err)
		}
	}

	if _, err := AppendSpaceMapEntry(page, SpaceMapEntry{DataPageID: 9999, FreeSpaceBucket: SpaceMapBucketLow}); !errors.Is(err, errSpaceMapPageFull) {
		t.Fatalf("AppendSpaceMapEntry(full) error = %v, want %v", err, errSpaceMapPageFull)
	}
}
