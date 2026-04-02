package storage

import (
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
