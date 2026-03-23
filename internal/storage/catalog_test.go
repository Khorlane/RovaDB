package storage

import (
	"fmt"
	"path/filepath"
	"testing"
)

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
			Name: fmt.Sprintf("table_%03d", i),
			Columns: []CatalogColumn{
				{Name: "id", Type: CatalogColumnTypeInt},
				{Name: "name", Type: CatalogColumnTypeText},
			},
		})
	}

	if err := SaveCatalog(pager, cat); !errorsIs(err, errCatalogTooLarge) {
		t.Fatalf("SaveCatalog() error = %v, want %v", err, errCatalogTooLarge)
	}
}

func errorsIs(err, target error) bool {
	return err == target
}
