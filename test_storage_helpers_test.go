package rovadb

import (
	"fmt"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func openRawStorage(t testFataler, path string) (*storage.DBFile, *storage.Pager) {
	t.Helper()

	dbFile, err := storage.OpenOrCreate(path)
	if err != nil {
		t.Fatalf("storage.OpenOrCreate() error = %v", err)
	}
	pager, err := storage.NewPager(dbFile.File())
	if err != nil {
		_ = dbFile.Close()
		t.Fatalf("storage.NewPager() error = %v", err)
	}
	return dbFile, pager
}

func writeMalformedCatalogPage(t testFataler, pager *storage.Pager, data []byte) {
	t.Helper()

	page, err := pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get(0) error = %v", err)
	}
	clear(page.Data())
	copy(page.Data(), data)
	page.MarkDirty()
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
}

type testFataler interface {
	Helper()
	Fatalf(format string, args ...any)
}

func errInvalidArgumentForTest(msg string) error {
	return fmt.Errorf("test assertion failed: %s", msg)
}
