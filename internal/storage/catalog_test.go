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
