package storage

import "encoding/binary"

const (
	catalogOverflowOffsetNextPageID  = pageHeaderSize
	catalogOverflowOffsetPayloadUsed = pageHeaderSize + 4
	catalogOverflowPayloadOffset     = pageHeaderSize + 8
)

// CatalogOverflowPayloadCapacity is the maximum CAT/DIR payload bytes stored in one overflow page.
const CatalogOverflowPayloadCapacity = PageSize - catalogOverflowPayloadOffset

// InitCatalogOverflowPage initializes one CAT/DIR overflow page image.
func InitCatalogOverflowPage(pageID uint32) []byte {
	page := make([]byte, PageSize)
	binary.LittleEndian.PutUint32(page[pageHeaderOffsetPageID:pageHeaderOffsetPageID+4], pageID)
	binary.LittleEndian.PutUint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2], uint16(PageTypeCatalogOverflow))
	_ = FinalizePageImage(page)
	return page
}

// CatalogOverflowNextPageID returns the next page pointer for one CAT/DIR overflow page.
func CatalogOverflowNextPageID(page []byte) (PageID, error) {
	if err := validateCatalogOverflowPage(page); err != nil {
		return 0, err
	}
	return PageID(binary.LittleEndian.Uint32(page[catalogOverflowOffsetNextPageID : catalogOverflowOffsetNextPageID+4])), nil
}

// SetCatalogOverflowNextPageID updates the next page pointer for one CAT/DIR overflow page.
func SetCatalogOverflowNextPageID(page []byte, nextPageID PageID) error {
	if err := validateCatalogOverflowPage(page); err != nil {
		return err
	}
	pageID := PageID(binary.LittleEndian.Uint32(page[pageHeaderOffsetPageID : pageHeaderOffsetPageID+4]))
	if nextPageID == pageID {
		return errCorruptedCatalogOverflow
	}
	binary.LittleEndian.PutUint32(page[catalogOverflowOffsetNextPageID:catalogOverflowOffsetNextPageID+4], uint32(nextPageID))
	return nil
}

// CatalogOverflowPayloadUsedBytes returns the used payload length for one CAT/DIR overflow page.
func CatalogOverflowPayloadUsedBytes(page []byte) (uint32, error) {
	if err := validateCatalogOverflowPage(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(page[catalogOverflowOffsetPayloadUsed : catalogOverflowOffsetPayloadUsed+4]), nil
}

// CatalogOverflowPayload returns the used payload bytes for one CAT/DIR overflow page.
func CatalogOverflowPayload(page []byte) ([]byte, error) {
	usedBytes, err := CatalogOverflowPayloadUsedBytes(page)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), page[catalogOverflowPayloadOffset:catalogOverflowPayloadOffset+int(usedBytes)]...), nil
}

// WriteCatalogOverflowPayload writes contiguous CAT/DIR payload bytes into one overflow page.
func WriteCatalogOverflowPayload(page []byte, payload []byte) error {
	if err := validateCatalogOverflowPage(page); err != nil {
		return err
	}
	if len(payload) > CatalogOverflowPayloadCapacity {
		return errCatalogTooLarge
	}
	binary.LittleEndian.PutUint32(page[catalogOverflowOffsetPayloadUsed:catalogOverflowOffsetPayloadUsed+4], uint32(len(payload)))
	clear(page[catalogOverflowPayloadOffset:])
	copy(page[catalogOverflowPayloadOffset:], payload)
	return nil
}

func validateCatalogOverflowPage(page []byte) error {
	if err := validateChecksumPageHeader(page); err != nil {
		return errCorruptedCatalogOverflow
	}
	if PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2])) != PageTypeCatalogOverflow {
		return errCorruptedCatalogOverflow
	}
	pageID := PageID(binary.LittleEndian.Uint32(page[pageHeaderOffsetPageID : pageHeaderOffsetPageID+4]))
	nextPageID := PageID(binary.LittleEndian.Uint32(page[catalogOverflowOffsetNextPageID : catalogOverflowOffsetNextPageID+4]))
	if nextPageID == pageID {
		return errCorruptedCatalogOverflow
	}
	usedBytes := binary.LittleEndian.Uint32(page[catalogOverflowOffsetPayloadUsed : catalogOverflowOffsetPayloadUsed+4])
	if usedBytes > CatalogOverflowPayloadCapacity {
		return errCorruptedCatalogOverflow
	}
	return nil
}
