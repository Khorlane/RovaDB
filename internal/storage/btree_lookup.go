package storage

import (
	"bytes"
	"encoding/binary"
)

type IndexPageReader func(pageID uint32) ([]byte, error)

func CountAllIndexEntries(pageReader IndexPageReader, rootPageID uint32) (int, error) {
	_, leafPage, err := findLeftmostIndexLeafPage(pageReader, rootPageID)
	if err != nil {
		return 0, err
	}

	count := 0
	currentPage := leafPage
	for {
		entryCount, err := IndexPageEntryCount(currentPage)
		if err != nil {
			return 0, err
		}
		count += entryCount

		rightSibling, err := IndexLeafRightSibling(currentPage)
		if err != nil {
			return 0, err
		}
		if rightSibling == 0 {
			return count, nil
		}
		currentPage, err = pageReader(rightSibling)
		if err != nil {
			return 0, err
		}
		if err := validateLeafIndexPage(currentPage); err != nil {
			return 0, err
		}
	}
}

func ReadAllIndexLeafRecordsInOrder(pageReader IndexPageReader, rootPageID uint32) ([]IndexLeafRecord, error) {
	_, leafPage, err := findLeftmostIndexLeafPage(pageReader, rootPageID)
	if err != nil {
		return nil, err
	}

	records := make([]IndexLeafRecord, 0)
	currentPage := leafPage
	for {
		pageRecords, err := ReadAllIndexLeafRecords(currentPage)
		if err != nil {
			return nil, err
		}
		records = append(records, pageRecords...)

		rightSibling, err := IndexLeafRightSibling(currentPage)
		if err != nil {
			return nil, err
		}
		if rightSibling == 0 {
			return records, nil
		}
		currentPage, err = pageReader(rightSibling)
		if err != nil {
			return nil, err
		}
		if err := validateLeafIndexPage(currentPage); err != nil {
			return nil, err
		}
	}
}

func findIndexLeafPage(pageReader IndexPageReader, rootPageID uint32, searchKey []byte) (leafPageID uint32, leafPage []byte, err error) {
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

func findLeftmostIndexLeafPage(pageReader IndexPageReader, rootPageID uint32) (leafPageID uint32, leafPage []byte, err error) {
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

		records, err := ReadAllIndexInternalRecords(page)
		if err != nil {
			return 0, nil, err
		}
		if len(records) == 0 || records[0].ChildPageID == 0 {
			return 0, nil, errCorruptedIndexPage
		}
		currentPageID = records[0].ChildPageID
	}
}

func lookupIndexLeafExact(page []byte, searchKey []byte) ([]RowLocator, error) {
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
	_, leafPage, err := findIndexLeafPage(pageReader, rootPageID, searchKey)
	if err != nil {
		return nil, err
	}

	matches := make([]RowLocator, 0)
	currentPage := leafPage
	for {
		currentMatches, err := lookupIndexLeafExact(currentPage, searchKey)
		if err != nil {
			return nil, err
		}
		matches = append(matches, currentMatches...)

		rightSibling, err := IndexLeafRightSibling(currentPage)
		if err != nil {
			return nil, err
		}
		if rightSibling == 0 {
			break
		}

		nextPage, err := pageReader(rightSibling)
		if err != nil {
			return nil, err
		}
		records, err := ReadAllIndexLeafRecords(nextPage)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			break
		}
		cmp, err := CompareIndexKeys(records[0].Key, searchKey)
		if err != nil || cmp != 0 {
			break
		}
		currentPage = nextPage
	}
	return matches, nil
}

func chooseIndexChildPage(page []byte, searchKey []byte) (uint32, error) {
	records, err := ReadAllIndexInternalRecords(page)
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, errCorruptedIndexPage
	}

	var rightmostChildPageID uint32
	for _, record := range records {
		rightmostChildPageID = record.ChildPageID
		cmp, err := CompareIndexKeys(searchKey, record.Key)
		if err != nil {
			return 0, err
		}
		if cmp < 0 {
			return record.ChildPageID, nil
		}
	}
	if rightmostChildPageID == 0 {
		return 0, errCorruptedIndexPage
	}
	return rightmostChildPageID, nil
}
