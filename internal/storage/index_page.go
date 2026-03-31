package storage

import "encoding/binary"

const (
	indexPageCommonHeaderSize = 32
	indexPageBodyHeaderSize   = 12
	indexPageEntrySize        = 4
	indexPageBodyStart        = indexPageCommonHeaderSize + indexPageBodyHeaderSize

	indexInternalEntryHeaderSize = 2
	indexLeafEntryHeaderSize     = 2

	indexPageHeaderOffsetPageID   = 0
	indexPageHeaderOffsetPageType = 4
	indexPageHeaderOffsetPageLSN  = 8
	indexPageHeaderOffsetChecksum = 16

	indexPageBodyOffsetEntryCount   = indexPageCommonHeaderSize
	indexPageBodyOffsetReserved     = indexPageCommonHeaderSize + 2
	indexPageBodyOffsetFreeStart    = indexPageCommonHeaderSize + 4
	indexPageBodyOffsetFreeEnd      = indexPageCommonHeaderSize + 6
	indexPageBodyOffsetRightSibling = indexPageCommonHeaderSize + 8
)

func InitIndexLeafPage(pageID uint32) []byte {
	return initIndexPage(pageID, PageTypeIndexLeaf)
}

func InitIndexInternalPage(pageID uint32) []byte {
	return initIndexPage(pageID, PageTypeIndexInternal)
}

func IndexPageEntryCount(page []byte) (int, error) {
	if err := validateIndexPage(page); err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint16(page[indexPageBodyOffsetEntryCount : indexPageBodyOffsetEntryCount+2])), nil
}

func IndexPageFreeStart(page []byte) (int, error) {
	if err := validateIndexPage(page); err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint16(page[indexPageBodyOffsetFreeStart : indexPageBodyOffsetFreeStart+2])), nil
}

func IndexPageFreeEnd(page []byte) (int, error) {
	if err := validateIndexPage(page); err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint16(page[indexPageBodyOffsetFreeEnd : indexPageBodyOffsetFreeEnd+2])), nil
}

func IndexPageFreeSpace(page []byte) (int, error) {
	freeStart, err := IndexPageFreeStart(page)
	if err != nil {
		return 0, err
	}
	freeEnd, err := IndexPageFreeEnd(page)
	if err != nil {
		return 0, err
	}
	return freeEnd - freeStart, nil
}

func IndexLeafRightSibling(page []byte) (uint32, error) {
	if err := validateLeafIndexPage(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(page[indexPageBodyOffsetRightSibling : indexPageBodyOffsetRightSibling+4]), nil
}

func SetIndexLeafRightSibling(page []byte, pageID uint32) error {
	if err := validateLeafIndexPage(page); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(page[indexPageBodyOffsetRightSibling:indexPageBodyOffsetRightSibling+4], pageID)
	return nil
}

func IndexPageEntry(page []byte, entryID int) (offset int, length int, err error) {
	if err := validateIndexPage(page); err != nil {
		return 0, 0, err
	}
	entryCount, err := IndexPageEntryCount(page)
	if err != nil {
		return 0, 0, err
	}
	if entryID < 0 || entryID >= entryCount {
		return 0, 0, errCorruptedIndexPage
	}

	entryOffset := indexPageBodyStart + entryID*indexPageEntrySize
	offset = int(binary.LittleEndian.Uint16(page[entryOffset : entryOffset+2]))
	length = int(binary.LittleEndian.Uint16(page[entryOffset+2 : entryOffset+4]))
	if offset < indexPageBodyStart || offset+length > PageSize {
		return 0, 0, errCorruptedIndexPage
	}
	return offset, length, nil
}

func IndexPageEntryPayload(page []byte, entryID int) ([]byte, error) {
	offset, length, err := IndexPageEntry(page, entryID)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), page[offset:offset+length]...), nil
}

func EncodeIndexInternalEntry(key []byte, childPageID uint32) ([]byte, error) {
	if len(key) > int(^uint16(0)) {
		return nil, errCorruptedIndexPage
	}

	payload := make([]byte, indexInternalEntryHeaderSize+len(key)+4)
	binary.LittleEndian.PutUint16(payload[0:2], uint16(len(key)))
	copy(payload[2:2+len(key)], key)
	binary.LittleEndian.PutUint32(payload[2+len(key):], childPageID)
	return payload, nil
}

func DecodeIndexInternalEntry(payload []byte) (key []byte, childPageID uint32, err error) {
	if len(payload) < indexInternalEntryHeaderSize+4 {
		return nil, 0, errCorruptedIndexPage
	}
	keyLength := int(binary.LittleEndian.Uint16(payload[0:2]))
	if len(payload) != indexInternalEntryHeaderSize+keyLength+4 {
		return nil, 0, errCorruptedIndexPage
	}

	key = append([]byte(nil), payload[2:2+keyLength]...)
	childPageID = binary.LittleEndian.Uint32(payload[2+keyLength:])
	return key, childPageID, nil
}

func EncodeIndexLeafEntry(key []byte, locator RowLocator) ([]byte, error) {
	if len(key) > int(^uint16(0)) {
		return nil, errCorruptedIndexPage
	}

	payload := make([]byte, indexLeafEntryHeaderSize+len(key)+4+2)
	binary.LittleEndian.PutUint16(payload[0:2], uint16(len(key)))
	copy(payload[2:2+len(key)], key)
	binary.LittleEndian.PutUint32(payload[2+len(key):2+len(key)+4], locator.PageID)
	binary.LittleEndian.PutUint16(payload[2+len(key)+4:], locator.SlotID)
	return payload, nil
}

func DecodeIndexLeafEntry(payload []byte) (key []byte, locator RowLocator, err error) {
	if len(payload) < indexLeafEntryHeaderSize+4+2 {
		return nil, RowLocator{}, errCorruptedIndexPage
	}
	keyLength := int(binary.LittleEndian.Uint16(payload[0:2]))
	if len(payload) != indexLeafEntryHeaderSize+keyLength+4+2 {
		return nil, RowLocator{}, errCorruptedIndexPage
	}

	key = append([]byte(nil), payload[2:2+keyLength]...)
	locator = RowLocator{
		PageID: binary.LittleEndian.Uint32(payload[2+keyLength : 2+keyLength+4]),
		SlotID: binary.LittleEndian.Uint16(payload[2+keyLength+4:]),
	}
	return key, locator, nil
}

func IndexLeafEntry(page []byte, entryID int) (key []byte, locator RowLocator, err error) {
	if err := validateLeafIndexPage(page); err != nil {
		return nil, RowLocator{}, err
	}
	payload, err := IndexPageEntryPayload(page, entryID)
	if err != nil {
		return nil, RowLocator{}, err
	}
	return DecodeIndexLeafEntry(payload)
}

func IndexInternalEntry(page []byte, entryID int) (key []byte, childPageID uint32, err error) {
	if err := validateInternalIndexPage(page); err != nil {
		return nil, 0, err
	}
	payload, err := IndexPageEntryPayload(page, entryID)
	if err != nil {
		return nil, 0, err
	}
	return DecodeIndexInternalEntry(payload)
}

func InsertIndexEntry(page []byte, payload []byte) (entryID int, err error) {
	if err := validateIndexPage(page); err != nil {
		return 0, err
	}

	freeSpace, err := IndexPageFreeSpace(page)
	if err != nil {
		return 0, err
	}
	if freeSpace < len(payload)+indexPageEntrySize {
		return 0, errTablePageFull
	}

	entryCount, err := IndexPageEntryCount(page)
	if err != nil {
		return 0, err
	}
	freeStart, err := IndexPageFreeStart(page)
	if err != nil {
		return 0, err
	}
	freeEnd, err := IndexPageFreeEnd(page)
	if err != nil {
		return 0, err
	}

	payloadOffset := freeEnd - len(payload)
	copy(page[payloadOffset:freeEnd], payload)

	entryOffset := indexPageBodyStart + entryCount*indexPageEntrySize
	binary.LittleEndian.PutUint16(page[entryOffset:entryOffset+2], uint16(payloadOffset))
	binary.LittleEndian.PutUint16(page[entryOffset+2:entryOffset+4], uint16(len(payload)))
	binary.LittleEndian.PutUint16(page[indexPageBodyOffsetEntryCount:indexPageBodyOffsetEntryCount+2], uint16(entryCount+1))
	binary.LittleEndian.PutUint16(page[indexPageBodyOffsetFreeStart:indexPageBodyOffsetFreeStart+2], uint16(freeStart+indexPageEntrySize))
	binary.LittleEndian.PutUint16(page[indexPageBodyOffsetFreeEnd:indexPageBodyOffsetFreeEnd+2], uint16(payloadOffset))
	return entryCount, nil
}

func initIndexPage(pageID uint32, pageType PageType) []byte {
	page := make([]byte, PageSize)
	binary.LittleEndian.PutUint32(page[indexPageHeaderOffsetPageID:indexPageHeaderOffsetPageID+4], pageID)
	binary.LittleEndian.PutUint16(page[indexPageHeaderOffsetPageType:indexPageHeaderOffsetPageType+2], uint16(pageType))
	binary.LittleEndian.PutUint16(page[indexPageBodyOffsetEntryCount:indexPageBodyOffsetEntryCount+2], 0)
	binary.LittleEndian.PutUint16(page[indexPageBodyOffsetReserved:indexPageBodyOffsetReserved+2], 0)
	binary.LittleEndian.PutUint16(page[indexPageBodyOffsetFreeStart:indexPageBodyOffsetFreeStart+2], indexPageBodyStart)
	binary.LittleEndian.PutUint16(page[indexPageBodyOffsetFreeEnd:indexPageBodyOffsetFreeEnd+2], PageSize)
	binary.LittleEndian.PutUint32(page[indexPageBodyOffsetRightSibling:indexPageBodyOffsetRightSibling+4], 0)
	return page
}

func validateIndexPage(page []byte) error {
	if len(page) != PageSize {
		return errCorruptedIndexPage
	}
	pageType := PageType(binary.LittleEndian.Uint16(page[indexPageHeaderOffsetPageType : indexPageHeaderOffsetPageType+2]))
	if !IsIndexPageType(pageType) {
		return errCorruptedIndexPage
	}

	entryCount := int(binary.LittleEndian.Uint16(page[indexPageBodyOffsetEntryCount : indexPageBodyOffsetEntryCount+2]))
	freeStart := int(binary.LittleEndian.Uint16(page[indexPageBodyOffsetFreeStart : indexPageBodyOffsetFreeStart+2]))
	freeEnd := int(binary.LittleEndian.Uint16(page[indexPageBodyOffsetFreeEnd : indexPageBodyOffsetFreeEnd+2]))
	expectedFreeStart := indexPageBodyStart + entryCount*indexPageEntrySize

	if freeStart < indexPageBodyStart || freeEnd < indexPageBodyStart || freeEnd > PageSize {
		return errCorruptedIndexPage
	}
	if freeStart != expectedFreeStart || freeStart > freeEnd {
		return errCorruptedIndexPage
	}
	return nil
}

func validateLeafIndexPage(page []byte) error {
	if err := validateIndexPage(page); err != nil {
		return err
	}
	pageType := PageType(binary.LittleEndian.Uint16(page[indexPageHeaderOffsetPageType : indexPageHeaderOffsetPageType+2]))
	if pageType != PageTypeIndexLeaf {
		return errCorruptedIndexPage
	}
	return nil
}

func validateInternalIndexPage(page []byte) error {
	if err := validateIndexPage(page); err != nil {
		return err
	}
	pageType := PageType(binary.LittleEndian.Uint16(page[indexPageHeaderOffsetPageType : indexPageHeaderOffsetPageType+2]))
	if pageType != PageTypeIndexInternal {
		return errCorruptedIndexPage
	}
	return nil
}
