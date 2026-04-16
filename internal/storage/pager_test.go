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
	pager.MarkDirty(page)

	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	var err error
	dbFile, err = Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
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

func TestMarkDirtyAndClearDirty(t *testing.T) {
	dbFile, pager, _ := openTestPager(t)
	defer dbFile.Close()

	page, err := pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get() error = %v", err)
	}
	pager.MarkDirty(page)
	if !pager.IsDirty(page) {
		t.Fatal("pager.IsDirty() = false, want true")
	}

	pager.ClearDirty(page)
	if pager.IsDirty(page) {
		t.Fatal("pager.IsDirty() = true, want false")
	}
}

func TestDirtyPagesStableOrder(t *testing.T) {
	dbFile, pager, _ := openTestPager(t)
	defer dbFile.Close()

	page3 := pager.NewPage()
	page1 := pager.NewPage()
	page2 := pager.NewPage()
	pager.ClearDirty(page1)
	pager.ClearDirty(page2)
	pager.ClearDirty(page3)

	pager.MarkDirty(page2)
	pager.MarkDirty(page3)
	pager.MarkDirty(page1)

	dirty := pager.DirtyPages()
	if len(dirty) != 3 {
		t.Fatalf("len(pager.DirtyPages()) = %d, want 3", len(dirty))
	}
	if dirty[0].ID() != 1 || dirty[1].ID() != 2 || dirty[2].ID() != 3 {
		t.Fatalf("dirty page ids = [%d %d %d], want [1 2 3]", dirty[0].ID(), dirty[1].ID(), dirty[2].ID())
	}
}

func TestFlushDirtyWritesOnlyDirtyPages(t *testing.T) {
	dbFile, pager, path := openTestPager(t)

	dirtyPage := pager.NewPage()
	cleanPage := pager.NewPage()

	copy(dirtyPage.Data(), []byte("dirty"))
	pager.MarkDirty(dirtyPage)
	copy(cleanPage.Data(), []byte("clean"))
	pager.ClearDirty(cleanPage)

	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if pager.IsDirty(dirtyPage) {
		t.Fatal("dirtyPage still dirty after FlushDirty()")
	}
	if pager.IsDirty(cleanPage) {
		t.Fatal("cleanPage became dirty after FlushDirty()")
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	dbFile, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer dbFile.Close()

	pager, err = NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() error = %v", err)
	}

	reloadedDirty, err := pager.Get(dirtyPage.ID())
	if err != nil {
		t.Fatalf("pager.Get(dirty) error = %v", err)
	}
	if got := string(reloadedDirty.Data()[:5]); got != "dirty" {
		t.Fatalf("dirty page data = %q, want %q", got, "dirty")
	}

	reloadedClean, err := pager.Get(cleanPage.ID())
	if err != nil {
		t.Fatalf("pager.Get(clean) error = %v", err)
	}
	if !bytes.Equal(reloadedClean.Data(), make([]byte, PageSize)) {
		t.Fatal("clean page was flushed unexpectedly")
	}
}

func TestRestoreDirtyPages(t *testing.T) {
	dbFile, pager, _ := openTestPager(t)
	defer dbFile.Close()

	page := pager.NewPage()
	copy(page.Data(), []byte("before"))
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	pager.ClearDirtyTracking()

	pager.MarkDirtyWithOriginal(page)
	copy(page.Data(), []byte("after!"))
	if !pager.IsDirty(page) {
		t.Fatal("pager.IsDirty() = false, want true")
	}
	if !pager.HasOriginal(page) {
		t.Fatal("pager.HasOriginal() = false, want true")
	}

	pager.RestoreDirtyPages()
	if pager.IsDirty(page) {
		t.Fatal("pager.IsDirty() = true, want false after restore")
	}
	if pager.HasOriginal(page) {
		t.Fatal("pager.HasOriginal() = true, want false after restore")
	}
	if got := string(page.Data()[:6]); got != "before" {
		t.Fatalf("restored page data = %q, want %q", got, "before")
	}
}

func openTestPager(t *testing.T) (*DBFile, *Pager, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "pager.db")
	dbFile, err := Create(path)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	pager, err := NewPager(dbFile.file)
	if err != nil {
		_ = dbFile.Close()
		t.Fatalf("NewPager() error = %v", err)
	}

	return dbFile, pager, path
}
