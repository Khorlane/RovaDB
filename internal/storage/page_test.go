package storage

import (
	"encoding/binary"
	"testing"
)

func TestNewPage(t *testing.T) {
	page := NewPage(7)
	if page == nil {
		t.Fatal("NewPage() = nil")
	}
	if got := page.ID(); got != 7 {
		t.Fatalf("page.ID() = %d, want 7", got)
	}
	if got := len(page.Data()); got != PageSize {
		t.Fatalf("len(page.Data()) = %d, want %d", got, PageSize)
	}
	if page.Dirty() {
		t.Fatal("page.Dirty() = true, want false")
	}
}

func TestPageMarkDirty(t *testing.T) {
	page := NewPage(1)
	page.MarkDirty()
	if !page.Dirty() {
		t.Fatal("page.Dirty() = false, want true")
	}
}

func TestPageOffset(t *testing.T) {
	tests := []struct {
		id   PageID
		want int64
	}{
		{id: 0, want: HeaderSize},
		{id: 1, want: HeaderSize + PageSize},
		{id: 2, want: HeaderSize + 2*PageSize},
	}

	for _, tt := range tests {
		if got := pageOffset(tt.id); got != tt.want {
			t.Fatalf("pageOffset(%d) = %d, want %d", tt.id, got, tt.want)
		}
	}
}

func TestIsValidPageType(t *testing.T) {
	valid := []PageType{PageTypeTable, PageTypeIndexLeaf, PageTypeIndexInternal, PageTypeFreePage, PageTypeDirectory, PageTypeCatalogOverflow}
	for _, pageType := range valid {
		if !IsValidPageType(pageType) {
			t.Fatalf("IsValidPageType(%d) = false, want true", pageType)
		}
	}
	if IsValidPageType(PageType(99)) {
		t.Fatal("IsValidPageType(99) = true, want false")
	}
}

func TestFreePageIsNotIndexPageType(t *testing.T) {
	if IsIndexPageType(PageTypeFreePage) {
		t.Fatal("IsIndexPageType(PageTypeFreePage) = true, want false")
	}
	if IsIndexPageType(PageTypeDirectory) {
		t.Fatal("IsIndexPageType(PageTypeDirectory) = true, want false")
	}
}

func TestCatalogOverflowPageHeaderRoundTrip(t *testing.T) {
	page := InitCatalogOverflowPage(9)

	if got := PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType : pageHeaderOffsetPageType+2])); got != PageTypeCatalogOverflow {
		t.Fatalf("pageType = %d, want %d", got, PageTypeCatalogOverflow)
	}
	if err := SetCatalogOverflowNextPageID(page, 11); err != nil {
		t.Fatalf("SetCatalogOverflowNextPageID() error = %v", err)
	}
	if err := WriteCatalogOverflowPayload(page, []byte("catdir")); err != nil {
		t.Fatalf("WriteCatalogOverflowPayload() error = %v", err)
	}
	if err := FinalizePageImage(page); err != nil {
		t.Fatalf("FinalizePageImage() error = %v", err)
	}

	nextPageID, err := CatalogOverflowNextPageID(page)
	if err != nil {
		t.Fatalf("CatalogOverflowNextPageID() error = %v", err)
	}
	if nextPageID != 11 {
		t.Fatalf("CatalogOverflowNextPageID() = %d, want 11", nextPageID)
	}
	usedBytes, err := CatalogOverflowPayloadUsedBytes(page)
	if err != nil {
		t.Fatalf("CatalogOverflowPayloadUsedBytes() error = %v", err)
	}
	if usedBytes != 6 {
		t.Fatalf("CatalogOverflowPayloadUsedBytes() = %d, want 6", usedBytes)
	}
	payload, err := CatalogOverflowPayload(page)
	if err != nil {
		t.Fatalf("CatalogOverflowPayload() error = %v", err)
	}
	if string(payload) != "catdir" {
		t.Fatalf("CatalogOverflowPayload() = %q, want %q", string(payload), "catdir")
	}
}

func TestStampedPageHelpersSupportFreePage(t *testing.T) {
	page := InitFreePage(9, 12)

	if err := SetPageLSN(page, 42); err != nil {
		t.Fatalf("SetPageLSN() error = %v", err)
	}
	if err := RecomputePageChecksum(page); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	lsn, err := PageLSN(page)
	if err != nil {
		t.Fatalf("PageLSN() error = %v", err)
	}
	if lsn != 42 {
		t.Fatalf("PageLSN() = %d, want 42", lsn)
	}
	if got := PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType : pageHeaderOffsetPageType+2])); got != PageTypeFreePage {
		t.Fatalf("pageType = %d, want %d", got, PageTypeFreePage)
	}
}

func TestIndexPageTypeHelpers(t *testing.T) {
	if !IsIndexPageType(PageTypeIndexLeaf) {
		t.Fatal("IsIndexPageType(PageTypeIndexLeaf) = false, want true")
	}
	if !IsIndexPageType(PageTypeIndexInternal) {
		t.Fatal("IsIndexPageType(PageTypeIndexInternal) = false, want true")
	}
	if IsIndexPageType(PageTypeTable) {
		t.Fatal("IsIndexPageType(PageTypeTable) = true, want false")
	}
	if !IsLeafIndexPageType(PageTypeIndexLeaf) {
		t.Fatal("IsLeafIndexPageType(PageTypeIndexLeaf) = false, want true")
	}
	if IsLeafIndexPageType(PageTypeIndexInternal) {
		t.Fatal("IsLeafIndexPageType(PageTypeIndexInternal) = true, want false")
	}
	if !IsInternalIndexPageType(PageTypeIndexInternal) {
		t.Fatal("IsInternalIndexPageType(PageTypeIndexInternal) = false, want true")
	}
	if IsInternalIndexPageType(PageTypeIndexLeaf) {
		t.Fatal("IsInternalIndexPageType(PageTypeIndexLeaf) = true, want false")
	}
}

func TestPageLSNAndChecksumHelpers(t *testing.T) {
	page := InitializeTablePage(7)

	if err := SetPageLSN(page, 42); err != nil {
		t.Fatalf("SetPageLSN() error = %v", err)
	}
	if err := RecomputePageChecksum(page); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}

	lsn, err := PageLSN(page)
	if err != nil {
		t.Fatalf("PageLSN() error = %v", err)
	}
	if lsn != 42 {
		t.Fatalf("PageLSN() = %d, want 42", lsn)
	}

	checksum, err := PageChecksum(page)
	if err != nil {
		t.Fatalf("PageChecksum() error = %v", err)
	}
	if checksum == 0 {
		t.Fatal("PageChecksum() = 0, want non-zero")
	}
}

func TestStampedPageHelpersRejectInvalidHeader(t *testing.T) {
	page := make([]byte, PageSize)

	if _, err := PageLSN(page); err != errCorruptedPageHeader {
		t.Fatalf("PageLSN() error = %v, want %v", err, errCorruptedPageHeader)
	}
	if err := SetPageLSN(page, 1); err != errCorruptedPageHeader {
		t.Fatalf("SetPageLSN() error = %v, want %v", err, errCorruptedPageHeader)
	}
	if _, err := PageChecksum(page); err != errCorruptedPageHeader {
		t.Fatalf("PageChecksum() error = %v, want %v", err, errCorruptedPageHeader)
	}
	if err := RecomputePageChecksum(page); err != errCorruptedPageHeader {
		t.Fatalf("RecomputePageChecksum() error = %v, want %v", err, errCorruptedPageHeader)
	}
}

func TestFinalizePageImageStampsValidTableChecksum(t *testing.T) {
	page := InitializeTablePage(7)
	if err := ValidatePageImage(page); err != nil {
		t.Fatalf("ValidatePageImage() error = %v", err)
	}
}

func TestFinalizePageImageStampsValidIndexFreeAndDirectoryPages(t *testing.T) {
	indexLeaf := InitIndexLeafPage(3)
	if err := ValidatePageImage(indexLeaf); err != nil {
		t.Fatalf("ValidatePageImage(indexLeaf) error = %v", err)
	}

	freePage := InitFreePage(4, 0)
	if err := ValidatePageImage(freePage); err != nil {
		t.Fatalf("ValidatePageImage(freePage) error = %v", err)
	}

	directoryPage, err := BuildCatalogPageData(&CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    1,
				RootPageID: 1,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
			},
		},
	})
	if err != nil {
		t.Fatalf("BuildCatalogPageData() error = %v", err)
	}
	if err := ValidatePageImage(directoryPage); err != nil {
		t.Fatalf("ValidatePageImage(directory) error = %v", err)
	}

	overflowPage := InitCatalogOverflowPage(5)
	if err := WriteCatalogOverflowPayload(overflowPage, []byte("payload")); err != nil {
		t.Fatalf("WriteCatalogOverflowPayload() error = %v", err)
	}
	if err := FinalizePageImage(overflowPage); err != nil {
		t.Fatalf("FinalizePageImage(overflow) error = %v", err)
	}
	if err := ValidatePageImage(overflowPage); err != nil {
		t.Fatalf("ValidatePageImage(overflow) error = %v", err)
	}
}

func TestValidatePageImageRejectsMutatedStampedPageByte(t *testing.T) {
	page := InitializeTablePage(7)
	page[tablePageBodyOffsetFreeEnd] ^= 0x01

	if err := ValidatePageImage(page); err != errCorruptedTablePage {
		t.Fatalf("ValidatePageImage() error = %v, want %v", err, errCorruptedTablePage)
	}
}

func TestValidatePageImageRejectsPageLSNChangeWithoutRestamp(t *testing.T) {
	page := InitializeTablePage(7)
	if err := SetPageLSN(page, 42); err != nil {
		t.Fatalf("SetPageLSN() error = %v", err)
	}

	if err := ValidatePageImage(page); err != errCorruptedTablePage {
		t.Fatalf("ValidatePageImage() error = %v, want %v", err, errCorruptedTablePage)
	}
}

func TestValidatePageImageRejectsPageIDChangeWithoutRestamp(t *testing.T) {
	page := InitializeTablePage(7)
	binary.LittleEndian.PutUint32(page[pageHeaderOffsetPageID:pageHeaderOffsetPageID+4], 8)

	if err := ValidatePageImage(page); err != errCorruptedTablePage {
		t.Fatalf("ValidatePageImage() error = %v, want %v", err, errCorruptedTablePage)
	}
}

func TestFinalizePageImageRestoresValidityAfterPageLSNChange(t *testing.T) {
	page := InitializeTablePage(7)
	if err := SetPageLSN(page, 42); err != nil {
		t.Fatalf("SetPageLSN() error = %v", err)
	}
	if err := FinalizePageImage(page); err != nil {
		t.Fatalf("FinalizePageImage() error = %v", err)
	}

	if err := ValidatePageImage(page); err != nil {
		t.Fatalf("ValidatePageImage() error = %v", err)
	}
}

func TestValidatePageImageRejectsInvalidPageType(t *testing.T) {
	page := InitializeTablePage(7)
	binary.LittleEndian.PutUint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2], 99)

	if err := ValidatePageImage(page); err != errCorruptedPageHeader {
		t.Fatalf("ValidatePageImage() error = %v, want %v", err, errCorruptedPageHeader)
	}
}
