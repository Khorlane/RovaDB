package storage

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestPagerNewPage(t *testing.T) {
	dbFile, pager, _ := openTestPager(t)
	defer dbFile.Close()

	page := pager.NewPage()
	if got := page.ID(); got != 1 {
		t.Fatalf("page.ID() = %d, want 1", got)
	}
	if !page.Dirty() {
		t.Fatal("page.Dirty() = false, want true")
	}
}

func TestPagerGetEmptyPage(t *testing.T) {
	dbFile, pager, _ := openTestPager(t)
	defer dbFile.Close()

	page, err := pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get() error = %v", err)
	}
	if got := len(page.Data()); got != PageSize {
		t.Fatalf("len(page.Data()) = %d, want %d", got, PageSize)
	}
	if !bytes.Equal(page.Data(), make([]byte, PageSize)) {
		t.Fatal("page.Data() is not zeroed")
	}
}

func TestPagerFlushAndReload(t *testing.T) {
	dbFile, pager, path := openTestPager(t)
	page := pager.NewPage()
	copy(page.Data(), []byte("hello"))

	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	var err error
	dbFile, err = OpenOrCreate(path)
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	pager, err = NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() error = %v", err)
	}

	reloaded, err := pager.Get(page.ID())
	if err != nil {
		t.Fatalf("pager.Get() error = %v", err)
	}
	if got := string(reloaded.Data()[:5]); got != "hello" {
		t.Fatalf("reloaded data = %q, want %q", got, "hello")
	}
}

func openTestPager(t *testing.T) (*DBFile, *Pager, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "pager.db")
	dbFile, err := OpenOrCreate(path)
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}

	pager, err := NewPager(dbFile.file)
	if err != nil {
		_ = dbFile.Close()
		t.Fatalf("NewPager() error = %v", err)
	}

	return dbFile, pager, path
}
