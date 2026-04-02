package storage

import "encoding/binary"

type CatalogOverflowPageImage struct {
	PageID PageID
	Data   []byte
}

func catalogOverflowRequiredPageCount(payloadBytes int) int {
	if payloadBytes <= 0 {
		return 0
	}
	return (payloadBytes + CatalogOverflowPayloadCapacity - 1) / CatalogOverflowPayloadCapacity
}

// BuildCatalogOverflowPageChain encodes one CAT/DIR payload across supplied overflow pages.
func BuildCatalogOverflowPageChain(payload []byte, pageIDs []PageID) ([]CatalogOverflowPageImage, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	requiredPages := catalogOverflowRequiredPageCount(len(payload))
	if len(pageIDs) < requiredPages {
		return nil, errCatalogTooLarge
	}
	seen := make(map[PageID]struct{}, len(pageIDs))
	for _, pageID := range pageIDs {
		if pageID == 0 {
			return nil, errCorruptedCatalogOverflow
		}
		if _, ok := seen[pageID]; ok {
			return nil, errCorruptedCatalogOverflow
		}
		seen[pageID] = struct{}{}
	}

	pages := make([]CatalogOverflowPageImage, 0, requiredPages)
	offset := 0
	for i := 0; i < requiredPages; i++ {
		pageID := pageIDs[i]
		page := InitCatalogOverflowPage(uint32(pageID))
		end := offset + CatalogOverflowPayloadCapacity
		if end > len(payload) {
			end = len(payload)
		}
		if err := WriteCatalogOverflowPayload(page, payload[offset:end]); err != nil {
			return nil, err
		}
		nextPageID := PageID(0)
		if i+1 < requiredPages {
			nextPageID = pageIDs[i+1]
		}
		if err := SetCatalogOverflowNextPageID(page, nextPageID); err != nil {
			return nil, err
		}
		if err := FinalizePageImage(page); err != nil {
			return nil, err
		}
		pages = append(pages, CatalogOverflowPageImage{PageID: pageID, Data: page})
		offset = end
	}
	return pages, nil
}

// ReadCatalogOverflowPayload reconstructs one CAT/DIR payload from an overflow chain.
func ReadCatalogOverflowPayload(reader PageReader, headPageID PageID, expectedPageCount uint32, expectedPayloadBytes uint32) ([]byte, error) {
	if expectedPageCount == 0 || expectedPayloadBytes == 0 {
		if headPageID != 0 || expectedPageCount != 0 || expectedPayloadBytes != 0 {
			return nil, errCorruptedCatalogOverflow
		}
		return nil, nil
	}
	if headPageID == 0 {
		return nil, errCorruptedCatalogOverflow
	}

	payload := make([]byte, 0, expectedPayloadBytes)
	visited := make(map[PageID]struct{}, expectedPageCount)
	currentPageID := headPageID
	for i := uint32(0); i < expectedPageCount; i++ {
		if currentPageID == 0 {
			return nil, errCorruptedCatalogOverflow
		}
		if _, ok := visited[currentPageID]; ok {
			return nil, errCorruptedCatalogOverflow
		}
		visited[currentPageID] = struct{}{}

		page, err := reader.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}
		if err := ValidatePageImage(page); err != nil {
			return nil, err
		}
		if PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2])) != PageTypeCatalogOverflow {
			return nil, errCorruptedCatalogOverflow
		}
		pagePayload, err := CatalogOverflowPayload(page)
		if err != nil {
			return nil, err
		}
		payload = append(payload, pagePayload...)
		if uint32(len(payload)) > expectedPayloadBytes {
			return nil, errCorruptedCatalogOverflow
		}
		nextPageID, err := CatalogOverflowNextPageID(page)
		if err != nil {
			return nil, err
		}
		if i+1 < expectedPageCount {
			if nextPageID == 0 {
				return nil, errCorruptedCatalogOverflow
			}
		} else if nextPageID != 0 {
			return nil, errCorruptedCatalogOverflow
		}
		currentPageID = nextPageID
	}
	if currentPageID != 0 {
		return nil, errCorruptedCatalogOverflow
	}
	if uint32(len(payload)) != expectedPayloadBytes {
		return nil, errCorruptedCatalogOverflow
	}
	return payload, nil
}

func ReadCatalogOverflowChainPageIDs(reader PageReader, headPageID PageID, expectedPageCount uint32) ([]PageID, error) {
	if reader == nil || headPageID == 0 || expectedPageCount == 0 {
		return nil, errCorruptedCatalogOverflow
	}
	visited := make(map[PageID]struct{}, expectedPageCount)
	pageIDs := make([]PageID, 0, expectedPageCount)
	currentPageID := headPageID

	for i := uint32(0); i < expectedPageCount; i++ {
		if currentPageID == 0 {
			return nil, errCorruptedCatalogOverflow
		}
		if _, exists := visited[currentPageID]; exists {
			return nil, errCorruptedCatalogOverflow
		}
		visited[currentPageID] = struct{}{}
		pageIDs = append(pageIDs, currentPageID)

		pageData, err := reader.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}
		nextPageID, err := CatalogOverflowNextPageID(pageData)
		if err != nil {
			return nil, err
		}
		if i+1 < expectedPageCount && nextPageID == 0 {
			return nil, errCorruptedCatalogOverflow
		}
		currentPageID = nextPageID
	}
	if currentPageID != 0 {
		return nil, errCorruptedCatalogOverflow
	}
	return pageIDs, nil
}

func BuildCatalogOverflowReclaimPages(reader PageReader, headPageID PageID, expectedPageCount uint32, freeListHead uint32) ([]CatalogWritePage, uint32, error) {
	pageIDs, err := ReadCatalogOverflowChainPageIDs(reader, headPageID, expectedPageCount)
	if err != nil {
		return nil, freeListHead, err
	}
	reclaimedPages := make([]CatalogWritePage, 0, len(pageIDs))
	currentFreeListHead := freeListHead
	for _, pageID := range pageIDs {
		reclaimedPages = append(reclaimedPages, CatalogWritePage{
			PageID: pageID,
			Data:   InitFreePage(uint32(pageID), currentFreeListHead),
			IsNew:  false,
		})
		currentFreeListHead = uint32(pageID)
	}
	return reclaimedPages, currentFreeListHead, nil
}
