package storage

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestInitTableHeaderPageRoundTrip(t *testing.T) {
	page := InitTableHeaderPage(21, 7)

	if err := ValidateTableHeaderPage(page); err != nil {
		t.Fatalf("ValidateTableHeaderPage() error = %v", err)
	}
	role, err := HeaderPageRoleValue(page)
	if err != nil {
		t.Fatalf("HeaderPageRoleValue() error = %v", err)
	}
	if role != HeaderPageRoleTable {
		t.Fatalf("HeaderPageRoleValue() = %d, want %d", role, HeaderPageRoleTable)
	}
	if got := PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType : pageHeaderOffsetPageType+2])); got != PageTypeHeader {
		t.Fatalf("pageType = %d, want %d", got, PageTypeHeader)
	}

	tableID, err := TableHeaderTableID(page)
	if err != nil {
		t.Fatalf("TableHeaderTableID() error = %v", err)
	}
	if tableID != 7 {
		t.Fatalf("TableHeaderTableID() = %d, want 7", tableID)
	}

	version, err := TableHeaderStorageFormatVersion(page)
	if err != nil {
		t.Fatalf("TableHeaderStorageFormatVersion() error = %v", err)
	}
	if version != CurrentTableStorageFormatVersion {
		t.Fatalf("TableHeaderStorageFormatVersion() = %d, want %d", version, CurrentTableStorageFormatVersion)
	}
}

func TestTableHeaderFieldSettersRoundTrip(t *testing.T) {
	page := InitTableHeaderPage(22, 8)

	if err := SetTableHeaderFirstSpaceMapPageID(page, 40); err != nil {
		t.Fatalf("SetTableHeaderFirstSpaceMapPageID() error = %v", err)
	}
	if err := SetTableHeaderOwnedSpaceMapPageCount(page, 2); err != nil {
		t.Fatalf("SetTableHeaderOwnedSpaceMapPageCount() error = %v", err)
	}
	if err := SetTableHeaderOwnedDataPageCount(page, 9); err != nil {
		t.Fatalf("SetTableHeaderOwnedDataPageCount() error = %v", err)
	}

	firstSpaceMapPageID, _ := TableHeaderFirstSpaceMapPageID(page)
	ownedSpaceMapPageCount, _ := TableHeaderOwnedSpaceMapPageCount(page)
	ownedDataPageCount, _ := TableHeaderOwnedDataPageCount(page)
	if firstSpaceMapPageID != 40 || ownedSpaceMapPageCount != 2 || ownedDataPageCount != 9 {
		t.Fatalf("table header fields = (%d, %d, %d), want (40, 2, 9)", firstSpaceMapPageID, ownedSpaceMapPageCount, ownedDataPageCount)
	}
}

func TestValidateTableHeaderPageRejectsWrongType(t *testing.T) {
	page := InitializeTablePage(23)

	if err := ValidateTableHeaderPage(page); !errors.Is(err, errCorruptedHeaderPage) {
		t.Fatalf("ValidateTableHeaderPage() error = %v, want %v", err, errCorruptedHeaderPage)
	}
}

func TestValidateTableHeaderPageRejectsWrongRole(t *testing.T) {
	page := InitHeaderPage(24, HeaderPageRoleDatabase)

	if err := ValidateTableHeaderPage(page); !errors.Is(err, errCorruptedHeaderPage) {
		t.Fatalf("ValidateTableHeaderPage() error = %v, want %v", err, errCorruptedHeaderPage)
	}
}

func TestValidateHeaderPageAllowsDatabaseRoleAtPageZero(t *testing.T) {
	page := InitHeaderPage(0, HeaderPageRoleDatabase)

	if err := ValidateHeaderPage(page); err != nil {
		t.Fatalf("ValidateHeaderPage() error = %v", err)
	}
}

func TestValidateHeaderPageRejectsDatabaseRoleAwayFromPageZero(t *testing.T) {
	page := InitHeaderPage(25, HeaderPageRoleDatabase)

	if err := ValidateHeaderPage(page); !errors.Is(err, errCorruptedHeaderPage) {
		t.Fatalf("ValidateHeaderPage() error = %v, want %v", err, errCorruptedHeaderPage)
	}
}

func TestValidateTableHeaderPageRejectsUnsupportedStorageVersion(t *testing.T) {
	page := InitTableHeaderPage(26, 9)
	binary.LittleEndian.PutUint32(page[headerPageBodyOffsetTableStorageVersion:headerPageBodyOffsetTableStorageVersion+4], CurrentTableStorageFormatVersion+1)

	if err := ValidateTableHeaderPage(page); !errors.Is(err, errCorruptedHeaderPage) {
		t.Fatalf("ValidateTableHeaderPage() error = %v, want %v", err, errCorruptedHeaderPage)
	}
}

func TestValidateTableHeaderPageRejectsOwnedCountsWithoutSpaceMapRoot(t *testing.T) {
	page := InitTableHeaderPage(27, 10)
	binary.LittleEndian.PutUint32(page[headerPageBodyOffsetOwnedDataPageCount:headerPageBodyOffsetOwnedDataPageCount+4], 1)

	if err := ValidateTableHeaderPage(page); !errors.Is(err, errCorruptedHeaderPage) {
		t.Fatalf("ValidateTableHeaderPage() error = %v, want %v", err, errCorruptedHeaderPage)
	}
}

func TestValidateTableHeaderPageRejectsSpaceMapCountWithoutFirstPage(t *testing.T) {
	page := InitTableHeaderPage(28, 11)
	binary.LittleEndian.PutUint32(page[headerPageBodyOffsetOwnedSpaceMapPageCount:headerPageBodyOffsetOwnedSpaceMapPageCount+4], 1)

	if err := ValidateTableHeaderPage(page); !errors.Is(err, errCorruptedHeaderPage) {
		t.Fatalf("ValidateTableHeaderPage() error = %v, want %v", err, errCorruptedHeaderPage)
	}
}
