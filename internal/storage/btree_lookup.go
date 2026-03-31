package storage

import (
	"bytes"
	"encoding/binary"
)

type IndexPageReader func(pageID uint32) ([]byte, error)

func FindIndexLeafPage(pageReader IndexPageReader, rootPageID uint32, searchKey []byte) (leafPageID uint32, leafPage []byte, err error) {
	if pageReader == nil || rootPageID == 0 {
		return 0, nil, errCorruptedIndexPage
	}

	currentPageID := rootPageID
	for {
		page, err := pageReader(currentPageID)
		if err != nil {
			return 0, nil, err
		}
		if err := validateIndexPage(page); err != nil {
			return 0, nil, err
		}

		pageType := PageType(binary.LittleEndian.Uint16(page[indexPageHeaderOffsetPageType : indexPageHeaderOffsetPageType+2]))
		if pageType == PageTypeIndexLeaf {
			return currentPageID, page, nil
		}
		if pageType != PageTypeIndexInternal {
			return 0, nil, errCorruptedIndexPage
		}

		nextPageID, err := chooseIndexChildPage(page, searchKey)
		if err != nil {
			return 0, nil, err
		}
		if nextPageID == 0 {
			return 0, nil, errCorruptedIndexPage
		}
		currentPageID = nextPageID
	}
}

func LookupIndexLeafExact(page []byte, searchKey []byte) ([]RowLocator, error) {
	if err := validateLeafIndexPage(page); err != nil {
		return nil, err
	}

	entryCount, err := IndexPageEntryCount(page)
	if err != nil {
		return nil, err
	}

	matches := make([]RowLocator, 0)
	for entryID := 0; entryID < entryCount; entryID++ {
		key, locator, err := IndexLeafEntry(page, entryID)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(key, searchKey) {
			matches = append(matches, locator)
		}
	}
	return matches, nil
}

func LookupIndexExact(pageReader IndexPageReader, rootPageID uint32, searchKey []byte) ([]RowLocator, error) {
	_, leafPage, err := FindIndexLeafPage(pageReader, rootPageID, searchKey)
	if err != nil {
		return nil, err
	}
	return LookupIndexLeafExact(leafPage, searchKey)
}

func chooseIndexChildPage(page []byte, searchKey []byte) (uint32, error) {
	entryCount, err := IndexPageEntryCount(page)
	if err != nil {
		return 0, err
	}
	if entryCount == 0 {
		return 0, errCorruptedIndexPage
	}

	var rightmostChildPageID uint32
	for entryID := 0; entryID < entryCount; entryID++ {
		key, childPageID, err := IndexInternalEntry(page, entryID)
		if err != nil {
			return 0, err
		}
		rightmostChildPageID = childPageID
		cmp, err := CompareIndexKeys(searchKey, key)
		if err != nil {
			return 0, err
		}
		if cmp < 0 {
			return childPageID, nil
		}
	}
	if rightmostChildPageID == 0 {
		return 0, errCorruptedIndexPage
	}
	return rightmostChildPageID, nil
}
