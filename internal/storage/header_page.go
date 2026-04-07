package storage

import "encoding/binary"

const (
	headerPageCommonHeaderSize = 32

	headerPageBodyOffsetRole                   = headerPageCommonHeaderSize
	headerPageBodyOffsetReserved               = headerPageCommonHeaderSize + 2
	headerPageBodyOffsetTableID                = headerPageCommonHeaderSize + 4
	headerPageBodyOffsetTableStorageVersion    = headerPageCommonHeaderSize + 8
	headerPageBodyOffsetFirstSpaceMapPageID    = headerPageCommonHeaderSize + 12
	headerPageBodyOffsetOwnedDataPageCount     = headerPageCommonHeaderSize + 16
	headerPageBodyOffsetOwnedSpaceMapPageCount = headerPageCommonHeaderSize + 20
)

const CurrentTableStorageFormatVersion uint32 = 1

func SupportedTableStorageFormatVersion(v uint32) bool {
	return v == CurrentTableStorageFormatVersion
}

func InitHeaderPage(pageID uint32, role HeaderPageRole) []byte {
	page := make([]byte, PageSize)
	binary.LittleEndian.PutUint32(page[pageHeaderOffsetPageID:pageHeaderOffsetPageID+4], pageID)
	binary.LittleEndian.PutUint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2], uint16(PageTypeHeader))
	binary.LittleEndian.PutUint16(page[headerPageBodyOffsetRole:headerPageBodyOffsetRole+2], uint16(role))
	_ = FinalizePageImage(page)
	return page
}

func HeaderPageRoleValue(page []byte) (HeaderPageRole, error) {
	if err := validateHeaderPage(page); err != nil {
		return 0, err
	}
	return HeaderPageRole(binary.LittleEndian.Uint16(page[headerPageBodyOffsetRole : headerPageBodyOffsetRole+2])), nil
}

func ValidateHeaderPage(page []byte) error {
	if err := validateHeaderPage(page); err != nil {
		return err
	}

	role := HeaderPageRole(binary.LittleEndian.Uint16(page[headerPageBodyOffsetRole : headerPageBodyOffsetRole+2]))
	switch role {
	case HeaderPageRoleDatabase:
		pageID := binary.LittleEndian.Uint32(page[pageHeaderOffsetPageID : pageHeaderOffsetPageID+4])
		if pageID != 0 {
			return errCorruptedHeaderPage
		}
		return nil
	case HeaderPageRoleTable:
		return ValidateTableHeaderPage(page)
	default:
		return errCorruptedHeaderPage
	}
}

func InitTableHeaderPage(pageID uint32, tableID uint32) []byte {
	page := InitHeaderPage(pageID, HeaderPageRoleTable)
	binary.LittleEndian.PutUint32(page[headerPageBodyOffsetTableID:headerPageBodyOffsetTableID+4], tableID)
	binary.LittleEndian.PutUint32(page[headerPageBodyOffsetTableStorageVersion:headerPageBodyOffsetTableStorageVersion+4], CurrentTableStorageFormatVersion)
	_ = FinalizePageImage(page)
	return page
}

func ValidateTableHeaderPage(page []byte) error {
	if err := validateHeaderPage(page); err != nil {
		return err
	}
	if HeaderPageRole(binary.LittleEndian.Uint16(page[headerPageBodyOffsetRole:headerPageBodyOffsetRole+2])) != HeaderPageRoleTable {
		return errCorruptedHeaderPage
	}

	pageID := binary.LittleEndian.Uint32(page[pageHeaderOffsetPageID : pageHeaderOffsetPageID+4])
	if pageID == 0 {
		return errCorruptedHeaderPage
	}

	tableID := binary.LittleEndian.Uint32(page[headerPageBodyOffsetTableID : headerPageBodyOffsetTableID+4])
	if tableID == 0 {
		return errCorruptedHeaderPage
	}

	version := binary.LittleEndian.Uint32(page[headerPageBodyOffsetTableStorageVersion : headerPageBodyOffsetTableStorageVersion+4])
	if !SupportedTableStorageFormatVersion(version) {
		return errCorruptedHeaderPage
	}

	firstSpaceMapPageID := binary.LittleEndian.Uint32(page[headerPageBodyOffsetFirstSpaceMapPageID : headerPageBodyOffsetFirstSpaceMapPageID+4])
	ownedDataPageCount := binary.LittleEndian.Uint32(page[headerPageBodyOffsetOwnedDataPageCount : headerPageBodyOffsetOwnedDataPageCount+4])
	ownedSpaceMapPageCount := binary.LittleEndian.Uint32(page[headerPageBodyOffsetOwnedSpaceMapPageCount : headerPageBodyOffsetOwnedSpaceMapPageCount+4])

	if firstSpaceMapPageID == 0 && ownedSpaceMapPageCount != 0 {
		return errCorruptedHeaderPage
	}
	if ownedDataPageCount != 0 && firstSpaceMapPageID == 0 {
		return errCorruptedHeaderPage
	}

	return nil
}

func TableHeaderTableID(page []byte) (uint32, error) {
	if err := ValidateTableHeaderPage(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(page[headerPageBodyOffsetTableID : headerPageBodyOffsetTableID+4]), nil
}

func SetTableHeaderTableID(page []byte, tableID uint32) error {
	if err := requireTableHeaderPage(page); err != nil {
		return err
	}
	if tableID == 0 {
		return errCorruptedHeaderPage
	}
	binary.LittleEndian.PutUint32(page[headerPageBodyOffsetTableID:headerPageBodyOffsetTableID+4], tableID)
	return FinalizePageImage(page)
}

func TableHeaderStorageFormatVersion(page []byte) (uint32, error) {
	if err := ValidateTableHeaderPage(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(page[headerPageBodyOffsetTableStorageVersion : headerPageBodyOffsetTableStorageVersion+4]), nil
}

func SetTableHeaderStorageFormatVersion(page []byte, version uint32) error {
	if err := requireTableHeaderPage(page); err != nil {
		return err
	}
	if !SupportedTableStorageFormatVersion(version) {
		return errCorruptedHeaderPage
	}
	binary.LittleEndian.PutUint32(page[headerPageBodyOffsetTableStorageVersion:headerPageBodyOffsetTableStorageVersion+4], version)
	return FinalizePageImage(page)
}

func TableHeaderFirstSpaceMapPageID(page []byte) (uint32, error) {
	if err := ValidateTableHeaderPage(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(page[headerPageBodyOffsetFirstSpaceMapPageID : headerPageBodyOffsetFirstSpaceMapPageID+4]), nil
}

func SetTableHeaderFirstSpaceMapPageID(page []byte, firstPageID uint32) error {
	if err := requireTableHeaderPage(page); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(page[headerPageBodyOffsetFirstSpaceMapPageID:headerPageBodyOffsetFirstSpaceMapPageID+4], firstPageID)
	return FinalizePageImage(page)
}

func TableHeaderOwnedDataPageCount(page []byte) (uint32, error) {
	if err := ValidateTableHeaderPage(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(page[headerPageBodyOffsetOwnedDataPageCount : headerPageBodyOffsetOwnedDataPageCount+4]), nil
}

func SetTableHeaderOwnedDataPageCount(page []byte, count uint32) error {
	if err := requireTableHeaderPage(page); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(page[headerPageBodyOffsetOwnedDataPageCount:headerPageBodyOffsetOwnedDataPageCount+4], count)
	return FinalizePageImage(page)
}

func TableHeaderOwnedSpaceMapPageCount(page []byte) (uint32, error) {
	if err := ValidateTableHeaderPage(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(page[headerPageBodyOffsetOwnedSpaceMapPageCount : headerPageBodyOffsetOwnedSpaceMapPageCount+4]), nil
}

func SetTableHeaderOwnedSpaceMapPageCount(page []byte, count uint32) error {
	if err := requireTableHeaderPage(page); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(page[headerPageBodyOffsetOwnedSpaceMapPageCount:headerPageBodyOffsetOwnedSpaceMapPageCount+4], count)
	return FinalizePageImage(page)
}

func validateHeaderPage(page []byte) error {
	if err := validateChecksumPageHeader(page); err != nil {
		return errCorruptedHeaderPage
	}
	if PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2])) != PageTypeHeader {
		return errCorruptedHeaderPage
	}
	role := HeaderPageRole(binary.LittleEndian.Uint16(page[headerPageBodyOffsetRole : headerPageBodyOffsetRole+2]))
	if !IsValidHeaderPageRole(role) {
		return errCorruptedHeaderPage
	}
	return nil
}

func requireTableHeaderPage(page []byte) error {
	if err := validateHeaderPage(page); err != nil {
		return err
	}
	if HeaderPageRole(binary.LittleEndian.Uint16(page[headerPageBodyOffsetRole:headerPageBodyOffsetRole+2])) != HeaderPageRoleTable {
		return errCorruptedHeaderPage
	}
	return nil
}
