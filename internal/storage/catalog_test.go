package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

func TestCatalogRoundTripIncludesStorageMetadata(t *testing.T) {
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
				RootPageID: 1,
				RowCount:   0,
				Columns: []CatalogColumn{
					{Name: "id", Type: CatalogColumnTypeInt},
					{Name: "name", Type: CatalogColumnTypeText},
				},
				Indexes: []CatalogIndex{
					{
						Name:   "idx_users_id_name",
						Unique: true,
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
	table := got.Tables[0]
	if table.RootPageID != 1 {
		t.Fatalf("table.RootPageID = %d, want 1", table.RootPageID)
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

func TestLoadCatalogV1WithoutIndexes(t *testing.T) {
	dbFile, err := OpenOrCreate(filepath.Join(t.TempDir(), "catalog_v1.db"))
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	pager, err := NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() error = %v", err)
	}

	page, err := pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get(0) error = %v", err)
	}
	v1 := make([]byte, PageSize)
	copy(v1, buildCatalogPageDataV1(&CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				RootPageID: 1,
				RowCount:   2,
				Columns: []CatalogColumn{
					{Name: "id", Type: CatalogColumnTypeInt},
				},
			},
		},
	}))
	clear(page.data)
	copy(page.data, v1)
	pager.MarkDirtyWithOriginal(page)
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}

	got, err := LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	if len(got.Tables) != 1 {
		t.Fatalf("len(got.Tables) = %d, want 1", len(got.Tables))
	}
	if len(got.Tables[0].Indexes) != 0 {
		t.Fatalf("got.Tables[0].Indexes = %#v, want empty", got.Tables[0].Indexes)
	}
}

func TestLoadCatalogV2SingleColumnIndexesRemainCompatible(t *testing.T) {
	dbFile, err := OpenOrCreate(filepath.Join(t.TempDir(), "catalog_v2.db"))
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	pager, err := NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() error = %v", err)
	}

	page, err := pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get(0) error = %v", err)
	}
	v2 := make([]byte, PageSize)
	copy(v2, buildCatalogPageDataV2(&CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				RootPageID: 1,
				RowCount:   0,
				Columns: []CatalogColumn{
					{Name: "id", Type: CatalogColumnTypeInt},
				},
				Indexes: []CatalogIndex{
					{Name: "ignored-in-v2", Columns: []CatalogIndexColumn{{Name: "id"}}},
				},
			},
		},
	}))
	clear(page.data)
	copy(page.data, v2)
	pager.MarkDirtyWithOriginal(page)
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}

	got, err := LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	if len(got.Tables) != 1 || len(got.Tables[0].Indexes) != 1 {
		t.Fatalf("got = %#v, want one table with one index", got)
	}
	index := got.Tables[0].Indexes[0]
	if index.Name != "id" || index.Unique || len(index.Columns) != 1 || index.Columns[0].Name != "id" || index.Columns[0].Desc {
		t.Fatalf("index = %#v, want v2-compatible single-column ASC non-unique index", index)
	}
}

func buildCatalogPageDataV1(cat *CatalogData) []byte {
	buf := make([]byte, 0, PageSize)
	buf = appendUint32(buf, catalogVersionV1)
	buf = appendUint32(buf, uint32(len(cat.Tables)))
	for _, table := range cat.Tables {
		buf = appendString(buf, table.Name)
		buf = appendUint32(buf, table.RootPageID)
		buf = appendUint32(buf, table.RowCount)
		buf = appendUint16(buf, uint16(len(table.Columns)))
		for _, column := range table.Columns {
			buf = appendString(buf, column.Name)
			buf = append(buf, column.Type)
		}
	}
	page := make([]byte, PageSize)
	copy(page, buf)
	return page
}

func buildCatalogPageDataV2(cat *CatalogData) []byte {
	buf := make([]byte, 0, PageSize)
	buf = appendUint32(buf, catalogVersionV2)
	buf = appendUint32(buf, uint32(len(cat.Tables)))
	for _, table := range cat.Tables {
		buf = appendString(buf, table.Name)
		buf = appendUint32(buf, table.RootPageID)
		buf = appendUint32(buf, table.RowCount)
		buf = appendUint16(buf, uint16(len(table.Columns)))
		for _, column := range table.Columns {
			buf = appendString(buf, column.Name)
			buf = append(buf, column.Type)
		}
		buf = appendUint16(buf, uint16(len(table.Indexes)))
		for _, index := range table.Indexes {
			columnName := ""
			if len(index.Columns) > 0 {
				columnName = index.Columns[0].Name
			}
			buf = appendString(buf, columnName)
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
