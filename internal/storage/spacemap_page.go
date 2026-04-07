package storage

import "encoding/binary"

const (
	spaceMapPageCommonHeaderSize = 32
	spaceMapPageBodyHeaderSize   = 12
	spaceMapEntrySize            = 8

	spaceMapBodyOffsetTableID    = spaceMapPageCommonHeaderSize
	spaceMapBodyOffsetNextPageID = spaceMapPageCommonHeaderSize + 4
	spaceMapBodyOffsetEntryCount = spaceMapPageCommonHeaderSize + 8
	spaceMapBodyOffsetReserved   = spaceMapPageCommonHeaderSize + 10
	spaceMapEntriesOffset        = spaceMapPageCommonHeaderSize + spaceMapPageBodyHeaderSize
)

type SpaceMapFreeSpaceBucket uint8

const (
	SpaceMapBucketFull SpaceMapFreeSpaceBucket = 1 + iota
	SpaceMapBucketLow
	SpaceMapBucketMedium
	SpaceMapBucketHigh
)

type SpaceMapEntry struct {
	DataPageID      PageID
	FreeSpaceBucket SpaceMapFreeSpaceBucket
}

func IsValidSpaceMapBucket(bucket SpaceMapFreeSpaceBucket) bool {
	switch bucket {
	case SpaceMapBucketFull, SpaceMapBucketLow, SpaceMapBucketMedium, SpaceMapBucketHigh:
		return true
	default:
		return false
	}
}

func InitSpaceMapPage(pageID uint32, tableID uint32) []byte {
	page := make([]byte, PageSize)
	binary.LittleEndian.PutUint32(page[pageHeaderOffsetPageID:pageHeaderOffsetPageID+4], pageID)
	binary.LittleEndian.PutUint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2], uint16(PageTypeSpaceMap))
	binary.LittleEndian.PutUint32(page[spaceMapBodyOffsetTableID:spaceMapBodyOffsetTableID+4], tableID)
	_ = FinalizePageImage(page)
	return page
}

func ValidateSpaceMapPage(page []byte) error {
	if err := validateChecksumPageHeader(page); err != nil {
		return errCorruptedSpaceMapPage
	}
	if PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2])) != PageTypeSpaceMap {
		return errCorruptedSpaceMapPage
	}

	pageID := binary.LittleEndian.Uint32(page[pageHeaderOffsetPageID : pageHeaderOffsetPageID+4])
	if pageID == 0 {
		return errCorruptedSpaceMapPage
	}

	tableID := binary.LittleEndian.Uint32(page[spaceMapBodyOffsetTableID : spaceMapBodyOffsetTableID+4])
	if tableID == 0 {
		return errCorruptedSpaceMapPage
	}

	nextPageID := binary.LittleEndian.Uint32(page[spaceMapBodyOffsetNextPageID : spaceMapBodyOffsetNextPageID+4])
	if nextPageID == pageID {
		return errCorruptedSpaceMapPage
	}

	entryCount := int(binary.LittleEndian.Uint16(page[spaceMapBodyOffsetEntryCount : spaceMapBodyOffsetEntryCount+2]))
	if entryCount > SpaceMapPageEntryCapacity() {
		return errCorruptedSpaceMapPage
	}

	for entryID := 0; entryID < entryCount; entryID++ {
		entryOffset := spaceMapEntriesOffset + entryID*spaceMapEntrySize
		entry := SpaceMapEntry{
			DataPageID:      PageID(binary.LittleEndian.Uint32(page[entryOffset : entryOffset+4])),
			FreeSpaceBucket: SpaceMapFreeSpaceBucket(page[entryOffset+4]),
		}
		if entry.DataPageID == 0 || !IsValidSpaceMapBucket(entry.FreeSpaceBucket) {
			return errCorruptedSpaceMapPage
		}
	}
	return nil
}

func SpaceMapPageEntryCapacity() int {
	return (PageSize - spaceMapEntriesOffset) / spaceMapEntrySize
}

func SpaceMapOwningTableID(page []byte) (uint32, error) {
	if err := ValidateSpaceMapPage(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(page[spaceMapBodyOffsetTableID : spaceMapBodyOffsetTableID+4]), nil
}

func SetSpaceMapOwningTableID(page []byte, tableID uint32) error {
	if err := ValidateSpaceMapPage(page); err != nil {
		return err
	}
	if tableID == 0 {
		return errCorruptedSpaceMapPage
	}
	binary.LittleEndian.PutUint32(page[spaceMapBodyOffsetTableID:spaceMapBodyOffsetTableID+4], tableID)
	return FinalizePageImage(page)
}

func SpaceMapNextPageID(page []byte) (uint32, error) {
	if err := ValidateSpaceMapPage(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(page[spaceMapBodyOffsetNextPageID : spaceMapBodyOffsetNextPageID+4]), nil
}

func SetSpaceMapNextPageID(page []byte, nextPageID uint32) error {
	if err := ValidateSpaceMapPage(page); err != nil {
		return err
	}
	pageID := binary.LittleEndian.Uint32(page[pageHeaderOffsetPageID : pageHeaderOffsetPageID+4])
	if nextPageID == pageID {
		return errCorruptedSpaceMapPage
	}
	binary.LittleEndian.PutUint32(page[spaceMapBodyOffsetNextPageID:spaceMapBodyOffsetNextPageID+4], nextPageID)
	return FinalizePageImage(page)
}

func SpaceMapEntryCount(page []byte) (int, error) {
	if err := ValidateSpaceMapPage(page); err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint16(page[spaceMapBodyOffsetEntryCount : spaceMapBodyOffsetEntryCount+2])), nil
}

func SpaceMapPageEntry(page []byte, entryID int) (SpaceMapEntry, error) {
	if err := ValidateSpaceMapPage(page); err != nil {
		return SpaceMapEntry{}, err
	}
	entryCount := int(binary.LittleEndian.Uint16(page[spaceMapBodyOffsetEntryCount : spaceMapBodyOffsetEntryCount+2]))
	if entryID < 0 || entryID >= entryCount {
		return SpaceMapEntry{}, errCorruptedSpaceMapPage
	}
	entryOffset := spaceMapEntriesOffset + entryID*spaceMapEntrySize
	entry := SpaceMapEntry{
		DataPageID:      PageID(binary.LittleEndian.Uint32(page[entryOffset : entryOffset+4])),
		FreeSpaceBucket: SpaceMapFreeSpaceBucket(page[entryOffset+4]),
	}
	if entry.DataPageID == 0 || !IsValidSpaceMapBucket(entry.FreeSpaceBucket) {
		return SpaceMapEntry{}, errCorruptedSpaceMapPage
	}
	return entry, nil
}

func AppendSpaceMapEntry(page []byte, entry SpaceMapEntry) (int, error) {
	if err := ValidateSpaceMapPage(page); err != nil {
		return 0, err
	}
	if entry.DataPageID == 0 || !IsValidSpaceMapBucket(entry.FreeSpaceBucket) {
		return 0, errCorruptedSpaceMapPage
	}

	entryCount := int(binary.LittleEndian.Uint16(page[spaceMapBodyOffsetEntryCount : spaceMapBodyOffsetEntryCount+2]))
	if entryCount >= SpaceMapPageEntryCapacity() {
		return 0, errSpaceMapPageFull
	}

	entryOffset := spaceMapEntriesOffset + entryCount*spaceMapEntrySize
	binary.LittleEndian.PutUint32(page[entryOffset:entryOffset+4], uint32(entry.DataPageID))
	page[entryOffset+4] = byte(entry.FreeSpaceBucket)
	page[entryOffset+5] = 0
	page[entryOffset+6] = 0
	page[entryOffset+7] = 0
	binary.LittleEndian.PutUint16(page[spaceMapBodyOffsetEntryCount:spaceMapBodyOffsetEntryCount+2], uint16(entryCount+1))
	if err := FinalizePageImage(page); err != nil {
		return 0, err
	}
	return entryCount, nil
}

func UpdateSpaceMapEntry(page []byte, entryID int, entry SpaceMapEntry) error {
	if err := ValidateSpaceMapPage(page); err != nil {
		return err
	}
	if entry.DataPageID == 0 || !IsValidSpaceMapBucket(entry.FreeSpaceBucket) {
		return errCorruptedSpaceMapPage
	}
	entryCount := int(binary.LittleEndian.Uint16(page[spaceMapBodyOffsetEntryCount : spaceMapBodyOffsetEntryCount+2]))
	if entryID < 0 || entryID >= entryCount {
		return errCorruptedSpaceMapPage
	}
	entryOffset := spaceMapEntriesOffset + entryID*spaceMapEntrySize
	binary.LittleEndian.PutUint32(page[entryOffset:entryOffset+4], uint32(entry.DataPageID))
	page[entryOffset+4] = byte(entry.FreeSpaceBucket)
	page[entryOffset+5] = 0
	page[entryOffset+6] = 0
	page[entryOffset+7] = 0
	return FinalizePageImage(page)
}
