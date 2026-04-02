package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

func TestCatalogRoundTripOmitsPhysicalRootsInLatestVersion(t *testing.T) {
	dbFile, err := OpenOrCreate(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	pager, err := NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() error = %v", err)
	}

	want := &CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    7,
				RootPageID: 1,
				RowCount:   0,
				Columns: []CatalogColumn{
					{Name: "id", Type: CatalogColumnTypeInt},
					{Name: "name", Type: CatalogColumnTypeText},
				},
				Indexes: []CatalogIndex{
					{
						Name:       "idx_users_id_name",
						Unique:     true,
						IndexID:    11,
						RootPageID: 9,
						Columns: []CatalogIndexColumn{
							{Name: "id"},
							{Name: "name", Desc: true},
						},
					},
				},
			},
		},
	}
	if err := SaveCatalog(pager, want); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	pager, err = NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() reload error = %v", err)
	}
	got, err := LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}

	if len(got.Tables) != 1 {
		t.Fatalf("len(got.Tables) = %d, want 1", len(got.Tables))
	}
	if got.Version != catalogVersion {
		t.Fatalf("catalog version = %d, want %d", got.Version, catalogVersion)
	}
	table := got.Tables[0]
	if table.RootPageID != 0 {
		t.Fatalf("table.RootPageID = %d, want 0 in latest logical payload", table.RootPageID)
	}
	if table.TableID != 7 {
		t.Fatalf("table.TableID = %d, want 7", table.TableID)
	}
	if table.RowCount != 0 {
		t.Fatalf("table.RowCount = %d, want 0", table.RowCount)
	}
	if len(table.Indexes) != 1 {
		t.Fatalf("len(table.Indexes) = %d, want 1", len(table.Indexes))
	}
	if table.Indexes[0].Name != "idx_users_id_name" || !table.Indexes[0].Unique {
		t.Fatalf("table.Indexes[0] = %#v, want named unique index", table.Indexes[0])
	}
	if table.Indexes[0].RootPageID != 0 {
		t.Fatalf("table.Indexes[0].RootPageID = %d, want 0 in latest logical payload", table.Indexes[0].RootPageID)
	}
	if table.Indexes[0].IndexID != 11 {
		t.Fatalf("table.Indexes[0].IndexID = %d, want 11", table.Indexes[0].IndexID)
	}
	if len(table.Indexes[0].Columns) != 2 || table.Indexes[0].Columns[0].Name != "id" || table.Indexes[0].Columns[1].Name != "name" || !table.Indexes[0].Columns[1].Desc {
		t.Fatalf("table.Indexes[0].Columns = %#v, want [id name DESC]", table.Indexes[0].Columns)
	}
}

func TestCatalogRoundTripIncludesRealType(t *testing.T) {
	dbFile, err := OpenOrCreate(filepath.Join(t.TempDir(), "catalog_real.db"))
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	pager, err := NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() error = %v", err)
	}

	want := &CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "measurements",
				TableID:    1,
				RootPageID: 1,
				RowCount:   0,
				Columns: []CatalogColumn{
					{Name: "x", Type: CatalogColumnTypeReal},
				},
			},
		},
	}
	if err := SaveCatalog(pager, want); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	pager, err = NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() reload error = %v", err)
	}
	got, err := LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}

	if len(got.Tables) != 1 || len(got.Tables[0].Columns) != 1 || got.Tables[0].Columns[0].Type != CatalogColumnTypeReal {
		t.Fatalf("got catalog = %#v, want REAL column metadata preserved", got)
	}
}

func TestLoadCatalogRejectsLegacyPayloadVersions(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{
			name: "v1",
			payload: buildLegacyCatalogPageDataForTest(1, &CatalogData{
				Tables: []CatalogTable{
					{
						Name:       "users",
						RootPageID: 1,
						RowCount:   2,
						Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
					},
				},
			}),
		},
		{
			name: "v2",
			payload: buildLegacyCatalogPageDataForTest(2, &CatalogData{
				Tables: []CatalogTable{
					{
						Name:       "users",
						RootPageID: 1,
						Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
						Indexes:    []CatalogIndex{{Name: "ignored", Columns: []CatalogIndexColumn{{Name: "id"}}}},
					},
				},
			}),
		},
		{
			name: "v3",
			payload: buildLegacyCatalogPageDataForTest(3, &CatalogData{
				Tables: []CatalogTable{
					{
						Name:       "users",
						RootPageID: 1,
						Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
						Indexes:    []CatalogIndex{{Name: "idx_users_id", Columns: []CatalogIndexColumn{{Name: "id"}}}},
					},
				},
			}),
		},
		{
			name: "v4",
			payload: buildLegacyCatalogPageDataForTest(4, &CatalogData{
				Tables: []CatalogTable{
					{
						Name:       "users",
						RootPageID: 1,
						Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
						Indexes:    []CatalogIndex{{Name: "idx_users_id", RootPageID: 9, Columns: []CatalogIndexColumn{{Name: "id"}}}},
					},
				},
			}),
		},
		{
			name: "v5",
			payload: buildLegacyCatalogPageDataForTest(5, &CatalogData{
				Tables: []CatalogTable{
					{
						Name:       "users",
						TableID:    7,
						RootPageID: 1,
						Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
						Indexes:    []CatalogIndex{{Name: "idx_users_id", IndexID: 9, RootPageID: 11, Columns: []CatalogIndexColumn{{Name: "id"}}}},
					},
				},
			}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadCatalogPageData(tc.payload)
			if !errors.Is(err, errUnsupportedCatalogPage) {
				t.Fatalf("LoadCatalogPageData() error = %v, want %v", err, errUnsupportedCatalogPage)
			}
		})
	}
}

func TestLoadCatalogRejectsLegacyRootPagePayloadLayout(t *testing.T) {
	payload := buildLegacyCatalogPageDataForTest(5, &CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    7,
				RootPageID: 1,
				RowCount:   0,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
				Indexes: []CatalogIndex{
					{Name: "idx_users_id", Unique: false, IndexID: 9, RootPageID: 11, Columns: []CatalogIndexColumn{{Name: "id"}}},
				},
			},
		},
	})

	_, err := LoadCatalogPageData(payload)
	if !errors.Is(err, errUnsupportedCatalogPage) {
		t.Fatalf("LoadCatalogPageData() error = %v, want %v", err, errUnsupportedCatalogPage)
	}
}

func buildLegacyCatalogPageDataForTest(version uint32, cat *CatalogData) []byte {
	buf := make([]byte, 0, PageSize)
	buf = appendUint32(buf, version)
	buf = appendUint32(buf, uint32(len(cat.Tables)))
	for _, table := range cat.Tables {
		buf = appendString(buf, table.Name)
		if version >= 5 {
			buf = appendUint32(buf, table.TableID)
		}
		if version < catalogVersion {
			buf = appendUint32(buf, table.RootPageID)
		}
		buf = appendUint32(buf, table.RowCount)
		buf = appendUint16(buf, uint16(len(table.Columns)))
		for _, column := range table.Columns {
			buf = appendString(buf, column.Name)
			buf = append(buf, column.Type)
		}
		if version >= 2 {
			buf = appendUint16(buf, uint16(len(table.Indexes)))
			for _, index := range table.Indexes {
				if version == 2 {
					columnName := ""
					if len(index.Columns) > 0 {
						columnName = index.Columns[0].Name
					}
					buf = appendString(buf, columnName)
					continue
				}
				buf = appendString(buf, index.Name)
				if index.Unique {
					buf = append(buf, 1)
				} else {
					buf = append(buf, 0)
				}
				if version >= 5 {
					buf = appendUint32(buf, index.IndexID)
				}
				if version >= 4 && version < catalogVersion {
					buf = appendUint32(buf, index.RootPageID)
				}
				buf = appendUint16(buf, uint16(len(index.Columns)))
				for _, column := range index.Columns {
					buf = appendString(buf, column.Name)
					if column.Desc {
						buf = append(buf, 1)
					} else {
						buf = append(buf, 0)
					}
				}
			}
		}
	}
	page := make([]byte, PageSize)
	copy(page, buf)
	return page
}

func TestSaveCatalogTooLarge(t *testing.T) {
	dbFile, err := OpenOrCreate(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	pager, err := NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() error = %v", err)
	}

	cat := &CatalogData{}
	for i := 0; i < 200; i++ {
		cat.Tables = append(cat.Tables, CatalogTable{
			Name:       fmt.Sprintf("table_%03d", i),
			RootPageID: uint32(i + 1),
			RowCount:   0,
			Columns: []CatalogColumn{
				{Name: "id", Type: CatalogColumnTypeInt},
				{Name: "name", Type: CatalogColumnTypeText},
			},
		})
	}

	err = SaveCatalog(pager, cat)
	if !errors.Is(err, errCatalogTooLarge) {
		t.Fatalf("SaveCatalog() error = %v, want %v", err, errCatalogTooLarge)
	}
}

func TestMutationPathsMarkPagesDirty(t *testing.T) {
	dbFile, err := OpenOrCreate(filepath.Join(t.TempDir(), "dirty.db"))
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	pager, err := NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() error = %v", err)
	}

	cat := &CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    1,
				RootPageID: 1,
				RowCount:   0,
				Columns: []CatalogColumn{
					{Name: "id", Type: CatalogColumnTypeInt},
				},
			},
		},
	}
	if err := SaveCatalog(pager, cat); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	catalogPage, err := pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get(0) error = %v", err)
	}
	if !pager.IsDirty(catalogPage) {
		t.Fatal("catalog page is clean after SaveCatalog(), want dirty")
	}
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if pager.IsDirty(catalogPage) {
		t.Fatal("catalog page still dirty after FlushDirty()")
	}

	tablePage := pager.NewPage()
	pager.ClearDirty(tablePage)
	InitTableRootPage(tablePage)
	if !pager.IsDirty(tablePage) {
		t.Fatal("table page is clean after InitTableRootPage(), want dirty")
	}
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if pager.IsDirty(tablePage) {
		t.Fatal("table page still dirty after FlushDirty()")
	}
}

func TestBuildCatalogOverflowPageChainSinglePage(t *testing.T) {
	payload := []byte("catalog-overflow")

	pages, err := BuildCatalogOverflowPageChain(payload, []PageID{7})
	if err != nil {
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v", err)
	}
	if len(pages) != 1 {
		t.Fatalf("len(BuildCatalogOverflowPageChain()) = %d, want 1", len(pages))
	}
	nextPageID, err := CatalogOverflowNextPageID(pages[0].Data)
	if err != nil {
		t.Fatalf("CatalogOverflowNextPageID() error = %v", err)
	}
	if nextPageID != 0 {
		t.Fatalf("CatalogOverflowNextPageID() = %d, want 0", nextPageID)
	}
	gotPayload, err := CatalogOverflowPayload(pages[0].Data)
	if err != nil {
		t.Fatalf("CatalogOverflowPayload() error = %v", err)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("CatalogOverflowPayload() = %q, want %q", string(gotPayload), string(payload))
	}
}

func TestBuildCatalogOverflowPageChainMultiPage(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), CatalogOverflowPayloadCapacity+37)

	pages, err := BuildCatalogOverflowPageChain(payload, []PageID{7, 8})
	if err != nil {
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v", err)
	}
	if len(pages) != 2 {
		t.Fatalf("len(BuildCatalogOverflowPageChain()) = %d, want 2", len(pages))
	}
	nextPageID, err := CatalogOverflowNextPageID(pages[0].Data)
	if err != nil {
		t.Fatalf("CatalogOverflowNextPageID(first) error = %v", err)
	}
	if nextPageID != 8 {
		t.Fatalf("CatalogOverflowNextPageID(first) = %d, want 8", nextPageID)
	}
	lastNextPageID, err := CatalogOverflowNextPageID(pages[1].Data)
	if err != nil {
		t.Fatalf("CatalogOverflowNextPageID(last) error = %v", err)
	}
	if lastNextPageID != 0 {
		t.Fatalf("CatalogOverflowNextPageID(last) = %d, want 0", lastNextPageID)
	}
}

func TestBuildCatalogOverflowPageChainRejectsInsufficientPageIDs(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), CatalogOverflowPayloadCapacity+1)

	_, err := BuildCatalogOverflowPageChain(payload, []PageID{7})
	if !errors.Is(err, errCatalogTooLarge) {
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v, want %v", err, errCatalogTooLarge)
	}
}

func TestBuildCatalogOverflowPageChainRejectsZeroAndDuplicatePageIDs(t *testing.T) {
	payload := []byte("payload")

	if _, err := BuildCatalogOverflowPageChain(payload, []PageID{0}); !errors.Is(err, errCorruptedCatalogOverflow) {
		t.Fatalf("BuildCatalogOverflowPageChain(zero id) error = %v, want %v", err, errCorruptedCatalogOverflow)
	}
	if _, err := BuildCatalogOverflowPageChain(payload, []PageID{7, 7}); !errors.Is(err, errCorruptedCatalogOverflow) {
		t.Fatalf("BuildCatalogOverflowPageChain(duplicate id) error = %v, want %v", err, errCorruptedCatalogOverflow)
	}
}

func TestReadCatalogOverflowPayloadSinglePage(t *testing.T) {
	payload := []byte("catalog")
	pages, err := BuildCatalogOverflowPageChain(payload, []PageID{7})
	if err != nil {
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v", err)
	}

	got, err := ReadCatalogOverflowPayload(testPageReaderForOverflow(pages), 7, 1, uint32(len(payload)))
	if err != nil {
		t.Fatalf("ReadCatalogOverflowPayload() error = %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("ReadCatalogOverflowPayload() = %q, want %q", string(got), string(payload))
	}
}

func TestReadCatalogOverflowPayloadMultiPage(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), CatalogOverflowPayloadCapacity+19)
	pages, err := BuildCatalogOverflowPageChain(payload, []PageID{7, 8})
	if err != nil {
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v", err)
	}

	got, err := ReadCatalogOverflowPayload(testPageReaderForOverflow(pages), 7, 2, uint32(len(payload)))
	if err != nil {
		t.Fatalf("ReadCatalogOverflowPayload() error = %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("ReadCatalogOverflowPayload() mismatch")
	}
}

func TestReadCatalogOverflowPayloadRejectsWrongPageType(t *testing.T) {
	tablePage := NewPage(7)
	InitTableRootPage(tablePage)
	page := tablePage.Data()
	reader := PageReaderFunc(func(pageID PageID) ([]byte, error) { return page, nil })

	_, err := ReadCatalogOverflowPayload(reader, 7, 1, 1)
	if !errors.Is(err, errCorruptedPageHeader) {
		t.Fatalf("ReadCatalogOverflowPayload() error = %v, want %v", err, errCorruptedPageHeader)
	}
}

func TestReadCatalogOverflowPayloadRejectsShortAndLongChains(t *testing.T) {
	payload := bytes.Repeat([]byte("b"), CatalogOverflowPayloadCapacity+3)
	pages, err := BuildCatalogOverflowPageChain(payload, []PageID{7, 8})
	if err != nil {
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v", err)
	}

	if _, err := ReadCatalogOverflowPayload(testPageReaderForOverflow(pages), 7, 3, uint32(len(payload))); !errors.Is(err, errCorruptedCatalogOverflow) {
		t.Fatalf("ReadCatalogOverflowPayload(short chain) error = %v, want %v", err, errCorruptedCatalogOverflow)
	}
	if _, err := ReadCatalogOverflowPayload(testPageReaderForOverflow(pages), 7, 2, uint32(len(payload)-1)); !errors.Is(err, errCorruptedCatalogOverflow) {
		t.Fatalf("ReadCatalogOverflowPayload(length mismatch) error = %v, want %v", err, errCorruptedCatalogOverflow)
	}
}

func TestReadCatalogOverflowPayloadRejectsRepeatedPageIDsAndMalformedTermination(t *testing.T) {
	payload := bytes.Repeat([]byte("c"), CatalogOverflowPayloadCapacity+5)
	pages, err := BuildCatalogOverflowPageChain(payload, []PageID{7, 8})
	if err != nil {
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v", err)
	}

	binary.LittleEndian.PutUint32(pages[0].Data[catalogOverflowOffsetNextPageID:catalogOverflowOffsetNextPageID+4], 7)
	if err := RecomputePageChecksum(pages[0].Data); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	if _, err := ReadCatalogOverflowPayload(testPageReaderForOverflow(pages), 7, 2, uint32(len(payload))); !errors.Is(err, errCorruptedCatalogOverflow) {
		t.Fatalf("ReadCatalogOverflowPayload(self-loop) error = %v, want %v", err, errCorruptedCatalogOverflow)
	}

	pages, err = BuildCatalogOverflowPageChain(payload, []PageID{7, 8})
	if err != nil {
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v", err)
	}
	binary.LittleEndian.PutUint32(pages[1].Data[catalogOverflowOffsetNextPageID:catalogOverflowOffsetNextPageID+4], 9)
	if err := RecomputePageChecksum(pages[1].Data); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	if _, err := ReadCatalogOverflowPayload(testPageReaderForOverflow(pages), 7, 2, uint32(len(payload))); !errors.Is(err, errCorruptedCatalogOverflow) {
		t.Fatalf("ReadCatalogOverflowPayload(last next) error = %v, want %v", err, errCorruptedCatalogOverflow)
	}

	pages, err = BuildCatalogOverflowPageChain(payload, []PageID{7, 8})
	if err != nil {
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v", err)
	}
	binary.LittleEndian.PutUint32(pages[0].Data[catalogOverflowOffsetNextPageID:catalogOverflowOffsetNextPageID+4], 0)
	if err := RecomputePageChecksum(pages[0].Data); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	if _, err := ReadCatalogOverflowPayload(testPageReaderForOverflow(pages), 7, 2, uint32(len(payload))); !errors.Is(err, errCorruptedCatalogOverflow) {
		t.Fatalf("ReadCatalogOverflowPayload(intermediate termination) error = %v, want %v", err, errCorruptedCatalogOverflow)
	}
}

func testPageReaderForOverflow(pages []CatalogOverflowPageImage) PageReader {
	pageMap := make(map[PageID][]byte, len(pages))
	for _, page := range pages {
		pageMap[page.PageID] = page.Data
	}
	return PageReaderFunc(func(pageID PageID) ([]byte, error) {
		page, ok := pageMap[pageID]
		if !ok {
			return nil, errCorruptedCatalogOverflow
		}
		return page, nil
	})
}
