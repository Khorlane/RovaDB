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
			name: "v6",
			payload: buildLegacyCatalogPageDataForTest(6, &CatalogData{
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
			_, err := loadCatalogPageData(tc.payload)
			if !errors.Is(err, errUnsupportedCatalogPage) {
				t.Fatalf("loadCatalogPageData() error = %v, want %v", err, errUnsupportedCatalogPage)
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

	_, err := loadCatalogPageData(payload)
	if !errors.Is(err, errUnsupportedCatalogPage) {
		t.Fatalf("loadCatalogPageData() error = %v, want %v", err, errUnsupportedCatalogPage)
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

func TestSaveCatalogPromotesToOverflowWhenEmbeddedCapacityExceeded(t *testing.T) {
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
			TableID:    uint32(i + 1),
			RootPageID: uint32(i + 1),
			RowCount:   0,
			Columns: []CatalogColumn{
				{Name: "id", Type: CatalogColumnTypeInt},
				{Name: "name", Type: CatalogColumnTypeText},
			},
		})
	}

	if err := SaveCatalog(pager, cat); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	pageData, err := pager.ReadPage(DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.ReadPage(directory) error = %v", err)
	}
	mode, err := DirectoryCATDIRStorageMode(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIRStorageMode() error = %v", err)
	}
	if mode != DirectoryCATDIRStorageModeOverflow {
		t.Fatalf("DirectoryCATDIRStorageMode() = %d, want %d", mode, DirectoryCATDIRStorageModeOverflow)
	}
	overflowHead, err := DirectoryCATDIROverflowHeadPageID(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID() error = %v", err)
	}
	if overflowHead == 0 {
		t.Fatal("DirectoryCATDIROverflowHeadPageID() = 0, want nonzero")
	}
	overflowCount, err := DirectoryCATDIROverflowPageCount(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowPageCount() error = %v", err)
	}
	if overflowCount == 0 {
		t.Fatal("DirectoryCATDIROverflowPageCount() = 0, want > 0")
	}
	payloadLength, err := DirectoryCATDIRPayloadByteLength(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIRPayloadByteLength() error = %v", err)
	}
	payload, err := encodeCatalogPayload(cat)
	if err != nil {
		t.Fatalf("encodeCatalogPayload() error = %v", err)
	}
	if payloadLength != uint32(len(payload)) {
		t.Fatalf("DirectoryCATDIRPayloadByteLength() = %d, want %d", payloadLength, len(payload))
	}

	reopenPager, err := NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() reopen error = %v", err)
	}
	got, err := LoadCatalog(reopenPager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	if len(got.Tables) != len(cat.Tables) {
		t.Fatalf("len(LoadCatalog().Tables) = %d, want %d", len(got.Tables), len(cat.Tables))
	}
	if got.Tables[0].TableID != cat.Tables[0].TableID || got.Tables[len(got.Tables)-1].TableID != cat.Tables[len(cat.Tables)-1].TableID {
		t.Fatalf("LoadCatalog() table IDs = (%d,%d), want (%d,%d)", got.Tables[0].TableID, got.Tables[len(got.Tables)-1].TableID, cat.Tables[0].TableID, cat.Tables[len(cat.Tables)-1].TableID)
	}
}

func TestSaveCatalogDemotesOverflowBackToEmbeddedWhenPayloadFits(t *testing.T) {
	dbFile, err := OpenOrCreate(filepath.Join(t.TempDir(), "catalog-demote.db"))
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	pager, err := NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() error = %v", err)
	}

	oversize := &CatalogData{}
	for i := 0; i < 200; i++ {
		oversize.Tables = append(oversize.Tables, CatalogTable{
			Name:       fmt.Sprintf("table_%03d", i),
			TableID:    uint32(i + 1),
			RootPageID: uint32(i + 1),
			Columns: []CatalogColumn{
				{Name: "id", Type: CatalogColumnTypeInt},
				{Name: "name", Type: CatalogColumnTypeText},
			},
		})
	}
	if err := SaveCatalog(pager, oversize); err != nil {
		t.Fatalf("SaveCatalog(oversize) error = %v", err)
	}
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush(oversize) error = %v", err)
	}

	pageData, err := pager.ReadPage(DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.ReadPage(directory oversize) error = %v", err)
	}
	mode, err := DirectoryCATDIRStorageMode(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIRStorageMode(oversize) error = %v", err)
	}
	if mode != DirectoryCATDIRStorageModeOverflow {
		t.Fatalf("DirectoryCATDIRStorageMode(oversize) = %d, want %d", mode, DirectoryCATDIRStorageModeOverflow)
	}
	previousOverflowHead, err := DirectoryCATDIROverflowHeadPageID(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID(oversize) error = %v", err)
	}
	if previousOverflowHead == 0 {
		t.Fatal("DirectoryCATDIROverflowHeadPageID(oversize) = 0, want nonzero")
	}

	smaller := &CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    1,
				RootPageID: 3,
				Columns: []CatalogColumn{
					{Name: "id", Type: CatalogColumnTypeInt},
					{Name: "name", Type: CatalogColumnTypeText},
				},
			},
		},
	}
	if err := SaveCatalog(pager, smaller); err != nil {
		t.Fatalf("SaveCatalog(smaller) error = %v", err)
	}
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush(smaller) error = %v", err)
	}

	pageData, err = pager.ReadPage(DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.ReadPage(directory smaller) error = %v", err)
	}
	mode, err = DirectoryCATDIRStorageMode(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIRStorageMode(smaller) error = %v", err)
	}
	if mode != DirectoryCATDIRStorageModeEmbedded {
		t.Fatalf("DirectoryCATDIRStorageMode(smaller) = %d, want %d", mode, DirectoryCATDIRStorageModeEmbedded)
	}
	overflowHead, err := DirectoryCATDIROverflowHeadPageID(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID(smaller) error = %v", err)
	}
	if overflowHead != 0 {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID(smaller) = %d, want 0", overflowHead)
	}
	overflowCount, err := DirectoryCATDIROverflowPageCount(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowPageCount(smaller) error = %v", err)
	}
	if overflowCount != 0 {
		t.Fatalf("DirectoryCATDIROverflowPageCount(smaller) = %d, want 0", overflowCount)
	}
	payloadLength, err := DirectoryCATDIRPayloadByteLength(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIRPayloadByteLength(smaller) error = %v", err)
	}
	smallerPayload, err := encodeCatalogPayload(smaller)
	if err != nil {
		t.Fatalf("encodeCatalogPayload(smaller) error = %v", err)
	}
	if payloadLength != uint32(len(smallerPayload)) {
		t.Fatalf("DirectoryCATDIRPayloadByteLength(smaller) = %d, want %d", payloadLength, len(smallerPayload))
	}

	reopenPager, err := NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() reopen error = %v", err)
	}
	got, err := LoadCatalog(reopenPager)
	if err != nil {
		t.Fatalf("LoadCatalog() after demotion error = %v", err)
	}
	if len(got.Tables) != 1 || got.Tables[0].Name != "users" || got.Tables[0].TableID != 1 {
		t.Fatalf("LoadCatalog() after demotion = %#v, want smaller embedded catalog", got)
	}
}

func TestSaveCatalogDemotionReclaimsSupersededOverflowChain(t *testing.T) {
	dbFile, err := OpenOrCreate(filepath.Join(t.TempDir(), "catalog-reclaim-demote.db"))
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	pager, err := NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() error = %v", err)
	}

	oversize := &CatalogData{}
	for i := 0; i < 200; i++ {
		oversize.Tables = append(oversize.Tables, CatalogTable{
			Name:       fmt.Sprintf("table_%03d", i),
			TableID:    uint32(i + 1),
			RootPageID: uint32(i + 1),
			Columns: []CatalogColumn{
				{Name: "id", Type: CatalogColumnTypeInt},
				{Name: "name", Type: CatalogColumnTypeText},
			},
		})
	}
	if err := SaveCatalog(pager, oversize); err != nil {
		t.Fatalf("SaveCatalog(oversize) error = %v", err)
	}
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush(oversize) error = %v", err)
	}

	pageData, err := pager.ReadPage(DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.ReadPage(directory oversize) error = %v", err)
	}
	oldHead, err := DirectoryCATDIROverflowHeadPageID(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID(oversize) error = %v", err)
	}
	oldCount, err := DirectoryCATDIROverflowPageCount(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowPageCount(oversize) error = %v", err)
	}
	oldIDs, err := ReadCatalogOverflowChainPageIDs(pager, PageID(oldHead), oldCount)
	if err != nil {
		t.Fatalf("ReadCatalogOverflowChainPageIDs() error = %v", err)
	}

	smaller := &CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    1,
				RootPageID: 3,
				Columns: []CatalogColumn{
					{Name: "id", Type: CatalogColumnTypeInt},
				},
			},
		},
	}
	if err := SaveCatalog(pager, smaller); err != nil {
		t.Fatalf("SaveCatalog(smaller) error = %v", err)
	}
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush(smaller) error = %v", err)
	}

	pageData, err = pager.ReadPage(DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.ReadPage(directory smaller) error = %v", err)
	}
	freeListHead, err := DirectoryFreeListHead(pageData)
	if err != nil {
		t.Fatalf("DirectoryFreeListHead() error = %v", err)
	}
	reclaimedIDs := allocateFreePagesForTest(t, pager, freeListHead, len(oldIDs))
	assertSamePageSet(t, reclaimedIDs, oldIDs)
}

func TestSaveCatalogOverflowRewriteReclaimsPriorOverflowChain(t *testing.T) {
	dbFile, err := OpenOrCreate(filepath.Join(t.TempDir(), "catalog-reclaim-overflow.db"))
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	pager, err := NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() error = %v", err)
	}

	oversizeA := &CatalogData{}
	for i := 0; i < 200; i++ {
		oversizeA.Tables = append(oversizeA.Tables, CatalogTable{
			Name:       fmt.Sprintf("table_a_%03d", i),
			TableID:    uint32(i + 1),
			RootPageID: uint32(i + 1),
			Columns: []CatalogColumn{
				{Name: "id", Type: CatalogColumnTypeInt},
				{Name: "name", Type: CatalogColumnTypeText},
			},
		})
	}
	if err := SaveCatalog(pager, oversizeA); err != nil {
		t.Fatalf("SaveCatalog(oversizeA) error = %v", err)
	}
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush(oversizeA) error = %v", err)
	}

	pageData, err := pager.ReadPage(DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.ReadPage(directory oversizeA) error = %v", err)
	}
	oldHead, err := DirectoryCATDIROverflowHeadPageID(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID(oversizeA) error = %v", err)
	}
	oldCount, err := DirectoryCATDIROverflowPageCount(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowPageCount(oversizeA) error = %v", err)
	}
	oldIDs, err := ReadCatalogOverflowChainPageIDs(pager, PageID(oldHead), oldCount)
	if err != nil {
		t.Fatalf("ReadCatalogOverflowChainPageIDs(oversizeA) error = %v", err)
	}

	oversizeB := &CatalogData{}
	for i := 0; i < 220; i++ {
		oversizeB.Tables = append(oversizeB.Tables, CatalogTable{
			Name:       fmt.Sprintf("table_b_%03d_%s", i, "expanded"),
			TableID:    uint32(i + 1),
			RootPageID: uint32(i + 1000),
			Columns: []CatalogColumn{
				{Name: "id", Type: CatalogColumnTypeInt},
				{Name: "name", Type: CatalogColumnTypeText},
				{Name: "city", Type: CatalogColumnTypeText},
			},
		})
	}
	if err := SaveCatalog(pager, oversizeB); err != nil {
		t.Fatalf("SaveCatalog(oversizeB) error = %v", err)
	}
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush(oversizeB) error = %v", err)
	}

	pageData, err = pager.ReadPage(DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.ReadPage(directory oversizeB) error = %v", err)
	}
	mode, err := DirectoryCATDIRStorageMode(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIRStorageMode(oversizeB) error = %v", err)
	}
	if mode != DirectoryCATDIRStorageModeOverflow {
		t.Fatalf("DirectoryCATDIRStorageMode(oversizeB) = %d, want %d", mode, DirectoryCATDIRStorageModeOverflow)
	}
	newHead, err := DirectoryCATDIROverflowHeadPageID(pageData)
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID(oversizeB) error = %v", err)
	}
	if containsPageID(oldIDs, PageID(newHead)) {
		t.Fatalf("new overflow head %d unexpectedly belongs to reclaimed old chain %#v", newHead, oldIDs)
	}
	freeListHead, err := DirectoryFreeListHead(pageData)
	if err != nil {
		t.Fatalf("DirectoryFreeListHead(oversizeB) error = %v", err)
	}
	reclaimedIDs := allocateFreePagesForTest(t, pager, freeListHead, len(oldIDs))
	assertSamePageSet(t, reclaimedIDs, oldIDs)

	got, err := LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog(oversizeB) error = %v", err)
	}
	if len(got.Tables) != len(oversizeB.Tables) {
		t.Fatalf("len(LoadCatalog().Tables) = %d, want %d", len(got.Tables), len(oversizeB.Tables))
	}
}

func allocateFreePagesForTest(t *testing.T, pager *Pager, freeListHead uint32, count int) []PageID {
	t.Helper()
	allocator := PageAllocator{
		NextPageID: uint32(pager.NextPageID()),
		FreePage: FreePageState{
			HeadPageID: freeListHead,
		},
		ReadFreeNext: func(pageID uint32) (uint32, error) {
			pageData, err := pager.ReadPage(PageID(pageID))
			if err != nil {
				return 0, err
			}
			return FreePageNext(pageData)
		},
	}
	pageIDs := make([]PageID, 0, count)
	for i := 0; i < count; i++ {
		pageID, reused, err := allocator.Allocate()
		if err != nil {
			t.Fatalf("allocator.Allocate() error = %v", err)
		}
		if !reused {
			t.Fatalf("allocator.Allocate() reused = false, want reclaimed free page")
		}
		pageIDs = append(pageIDs, PageID(pageID))
	}
	return pageIDs
}

func assertSamePageSet(t *testing.T, got []PageID, want []PageID) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(pageIDs) = %d, want %d", len(got), len(want))
	}
	wantSet := make(map[PageID]int, len(want))
	for _, pageID := range want {
		wantSet[pageID]++
	}
	for _, pageID := range got {
		wantSet[pageID]--
	}
	for pageID, remaining := range wantSet {
		if remaining != 0 {
			t.Fatalf("page set mismatch for page %d, remaining count %d, got=%#v want=%#v", pageID, remaining, got, want)
		}
	}
}

func containsPageID(pageIDs []PageID, target PageID) bool {
	for _, pageID := range pageIDs {
		if pageID == target {
			return true
		}
	}
	return false
}

func TestSaveCatalogOverflowAllocationFailureLeavesCommittedMetadataIntact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog-intact.db")
	dbFile, err := OpenOrCreate(path)
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	pager, err := NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() error = %v", err)
	}
	original := &CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    1,
				RootPageID: 3,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
			},
		},
	}
	if err := SaveCatalog(pager, original); err != nil {
		t.Fatalf("SaveCatalog(original) error = %v", err)
	}
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	oversize := &CatalogData{}
	for i := 0; i < 200; i++ {
		oversize.Tables = append(oversize.Tables, CatalogTable{
			Name:       fmt.Sprintf("table_%03d", i),
			TableID:    uint32(i + 1),
			RootPageID: uint32(i + 10),
			Columns: []CatalogColumn{
				{Name: "id", Type: CatalogColumnTypeInt},
				{Name: "name", Type: CatalogColumnTypeText},
			},
		})
	}

	restoreAllocator := newCatalogOverflowAllocator
	defer func() {
		newCatalogOverflowAllocator = restoreAllocator
	}()
	newCatalogOverflowAllocator = func(pager *Pager, freeListHead *uint32) *catalogOverflowPagerAllocator {
		return &catalogOverflowPagerAllocator{
			pager:        nil,
			nextFreshID:  0,
			freeListHead: freeListHead,
		}
	}
	if err := SaveCatalog(pager, oversize); !errors.Is(err, errCorruptedDirectoryPage) {
		t.Fatalf("SaveCatalog(oversize) error = %v, want %v", err, errCorruptedDirectoryPage)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	dbFile, err = OpenOrCreate(path)
	if err != nil {
		t.Fatalf("reopen OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()
	reopenPager, err := NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager(reopen) error = %v", err)
	}
	got, err := LoadCatalog(reopenPager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	if len(got.Tables) != 1 || got.Tables[0].Name != "users" || got.Tables[0].TableID != 1 {
		t.Fatalf("reopened catalog = %#v, want original committed metadata", got)
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

func TestLoadCatalogReadsOverflowBackedDirectoryPayload(t *testing.T) {
	embeddedPage, err := BuildCatalogPageData(&CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    7,
				RootPageID: 11,
				Columns: []CatalogColumn{
					{Name: "id", Type: CatalogColumnTypeInt},
					{Name: "name", Type: CatalogColumnTypeText},
				},
				Indexes: []CatalogIndex{
					{
						Name:       "idx_users_name",
						Unique:     false,
						IndexID:    13,
						RootPageID: 17,
						Columns:    []CatalogIndexColumn{{Name: "name"}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("BuildCatalogPageData() error = %v", err)
	}
	payload, err := directoryCatalogPayload(embeddedPage)
	if err != nil {
		t.Fatalf("directoryCatalogPayload() error = %v", err)
	}
	overflowPages, err := BuildCatalogOverflowPageChain(payload, []PageID{7})
	if err != nil {
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v", err)
	}
	directoryPage := InitDirectoryPage(uint32(DirectoryControlPageID), CurrentDBFormatVersion)
	binary.LittleEndian.PutUint32(directoryPage[directoryBodyOffsetCATDIRStorageMode:directoryBodyOffsetCATDIRStorageMode+4], DirectoryCATDIRStorageModeOverflow)
	binary.LittleEndian.PutUint32(directoryPage[directoryBodyOffsetCATDIROverflowHead:directoryBodyOffsetCATDIROverflowHead+4], uint32(overflowPages[0].PageID))
	binary.LittleEndian.PutUint32(directoryPage[directoryBodyOffsetCATDIROverflowCount:directoryBodyOffsetCATDIROverflowCount+4], uint32(len(overflowPages)))
	binary.LittleEndian.PutUint32(directoryPage[directoryBodyOffsetCATDIRPayloadByteSize:directoryBodyOffsetCATDIRPayloadByteSize+4], uint32(len(payload)))
	if err := FinalizePageImage(directoryPage); err != nil {
		t.Fatalf("FinalizePageImage(directory) error = %v", err)
	}

	reader := PageReaderFunc(func(pageID PageID) ([]byte, error) {
		if pageID == DirectoryControlPageID {
			return directoryPage, nil
		}
		for _, page := range overflowPages {
			if page.PageID == pageID {
				return page.Data, nil
			}
		}
		return nil, errCorruptedCatalogOverflow
	})
	got, err := LoadCatalog(reader)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	if got.Version != catalogVersion {
		t.Fatalf("got.Version = %d, want %d", got.Version, catalogVersion)
	}
	if len(got.Tables) != 1 || got.Tables[0].Name != "users" {
		t.Fatalf("got.Tables = %#v, want users table", got.Tables)
	}
	if got.Tables[0].RootPageID != 0 {
		t.Fatalf("got.Tables[0].RootPageID = %d, want 0 from logical payload", got.Tables[0].RootPageID)
	}
	if len(got.Tables[0].Indexes) != 1 || got.Tables[0].Indexes[0].Name != "idx_users_name" {
		t.Fatalf("got.Tables[0].Indexes = %#v, want idx_users_name", got.Tables[0].Indexes)
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
	if !errors.Is(err, errMalformedCATDIROverflow) {
		t.Fatalf("ReadCatalogOverflowPayload() error = %v, want %v", err, errMalformedCATDIROverflow)
	}
}

func TestReadCatalogOverflowPayloadRejectsShortAndLongChains(t *testing.T) {
	payload := bytes.Repeat([]byte("b"), CatalogOverflowPayloadCapacity+3)
	pages, err := BuildCatalogOverflowPageChain(payload, []PageID{7, 8})
	if err != nil {
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v", err)
	}

	if _, err := ReadCatalogOverflowPayload(testPageReaderForOverflow(pages), 7, 3, uint32(len(payload))); !errors.Is(err, errMalformedCATDIROverflow) {
		t.Fatalf("ReadCatalogOverflowPayload(short chain) error = %v, want %v", err, errMalformedCATDIROverflow)
	}
	if _, err := ReadCatalogOverflowPayload(testPageReaderForOverflow(pages), 7, 2, uint32(len(payload)-1)); !errors.Is(err, errMalformedCATDIROverflow) {
		t.Fatalf("ReadCatalogOverflowPayload(length mismatch) error = %v, want %v", err, errMalformedCATDIROverflow)
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
	if _, err := ReadCatalogOverflowPayload(testPageReaderForOverflow(pages), 7, 2, uint32(len(payload))); !errors.Is(err, errMalformedCATDIROverflow) {
		t.Fatalf("ReadCatalogOverflowPayload(self-loop) error = %v, want %v", err, errMalformedCATDIROverflow)
	}

	pages, err = BuildCatalogOverflowPageChain(payload, []PageID{7, 8})
	if err != nil {
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v", err)
	}
	binary.LittleEndian.PutUint32(pages[1].Data[catalogOverflowOffsetNextPageID:catalogOverflowOffsetNextPageID+4], 9)
	if err := RecomputePageChecksum(pages[1].Data); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	if _, err := ReadCatalogOverflowPayload(testPageReaderForOverflow(pages), 7, 2, uint32(len(payload))); !errors.Is(err, errMalformedCATDIROverflow) {
		t.Fatalf("ReadCatalogOverflowPayload(last next) error = %v, want %v", err, errMalformedCATDIROverflow)
	}

	pages, err = BuildCatalogOverflowPageChain(payload, []PageID{7, 8})
	if err != nil {
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v", err)
	}
	binary.LittleEndian.PutUint32(pages[0].Data[catalogOverflowOffsetNextPageID:catalogOverflowOffsetNextPageID+4], 0)
	if err := RecomputePageChecksum(pages[0].Data); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	if _, err := ReadCatalogOverflowPayload(testPageReaderForOverflow(pages), 7, 2, uint32(len(payload))); !errors.Is(err, errMalformedCATDIROverflow) {
		t.Fatalf("ReadCatalogOverflowPayload(intermediate termination) error = %v, want %v", err, errMalformedCATDIROverflow)
	}
}

func TestReadCatalogOverflowPayloadRejectsZeroPayloadUsedAndReclaimMatches(t *testing.T) {
	payload := bytes.Repeat([]byte("d"), CatalogOverflowPayloadCapacity+5)
	pages, err := BuildCatalogOverflowPageChain(payload, []PageID{7, 8})
	if err != nil {
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v", err)
	}

	binary.LittleEndian.PutUint32(pages[0].Data[catalogOverflowOffsetPayloadUsed:catalogOverflowOffsetPayloadUsed+4], 0)
	if err := RecomputePageChecksum(pages[0].Data); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	if _, err := ReadCatalogOverflowPayload(testPageReaderForOverflow(pages), 7, 2, uint32(len(payload))); !errors.Is(err, errMalformedCATDIROverflow) {
		t.Fatalf("ReadCatalogOverflowPayload(zero used bytes) error = %v, want %v", err, errMalformedCATDIROverflow)
	}
	if _, _, err := buildCatalogOverflowReclaimPages(testPageReaderForOverflow(pages), 7, 2, 0); !errors.Is(err, errMalformedCATDIROverflow) {
		t.Fatalf("buildCatalogOverflowReclaimPages(zero used bytes) error = %v, want %v", err, errMalformedCATDIROverflow)
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
