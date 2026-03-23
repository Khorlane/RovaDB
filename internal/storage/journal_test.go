package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestJournalPath(t *testing.T) {
	got := JournalPath(filepath.Join("tmp", "test.db"))
	want := filepath.Join("tmp", "test.db") + ".journal"
	if got != want {
		t.Fatalf("JournalPath() = %q, want %q", got, want)
	}
}

func TestJournalHeaderRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	want := JournalHeader{
		Magic:      journalMagic,
		Version:    journalVersion,
		PageSize:   PageSize,
		EntryCount: 3,
	}

	if err := WriteJournalHeader(&buf, want); err != nil {
		t.Fatalf("WriteJournalHeader() error = %v", err)
	}
	got, err := ReadJournalHeader(&buf)
	if err != nil {
		t.Fatalf("ReadJournalHeader() error = %v", err)
	}

	if got.Magic != want.Magic || got.Version != want.Version || got.PageSize != want.PageSize || got.EntryCount != want.EntryCount {
		t.Fatalf("round-trip header = %#v, want %#v", got, want)
	}
}

func TestJournalEntryRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	wantData := bytes.Repeat([]byte{0xAB}, PageSize)

	if err := WriteJournalEntry(&buf, 7, wantData); err != nil {
		t.Fatalf("WriteJournalEntry() error = %v", err)
	}
	got, err := ReadJournalEntry(&buf, PageSize)
	if err != nil {
		t.Fatalf("ReadJournalEntry() error = %v", err)
	}

	if got.PageID != 7 {
		t.Fatalf("entry.PageID = %d, want 7", got.PageID)
	}
	if !bytes.Equal(got.Data, wantData) {
		t.Fatal("entry.Data mismatch after round trip")
	}
}

func TestDirtyPagesWithOriginalsStableOrder(t *testing.T) {
	dbFile, pager, _ := openTestPager(t)
	defer dbFile.Close()

	page1 := pager.NewPage()
	page3 := pager.NewPage()
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	pager.ClearDirtyTracking()
	page2 := pager.NewPage()

	pager.MarkDirtyWithOriginal(page3)
	copy(page3.Data(), []byte("three"))
	pager.MarkDirtyWithOriginal(page1)
	copy(page1.Data(), []byte("one"))
	pager.MarkDirty(page2)

	pages := pager.DirtyPagesWithOriginals()
	if len(pages) != 2 {
		t.Fatalf("len(pager.DirtyPagesWithOriginals()) = %d, want 2", len(pages))
	}
	if pages[0].ID() != 1 || pages[1].ID() != 2 {
		t.Fatalf("page ids = [%d %d], want [1 2]", pages[0].ID(), pages[1].ID())
	}
}

func TestReadJournalHeaderRejectsBadMagic(t *testing.T) {
	var buf bytes.Buffer
	header := JournalHeader{
		Magic:      [8]byte{'B', 'A', 'D', 'J', 'N', 'L', '0', '0'},
		Version:    journalVersion,
		PageSize:   PageSize,
		EntryCount: 1,
	}
	if err := WriteJournalHeader(&buf, header); err != nil {
		t.Fatalf("WriteJournalHeader() error = %v", err)
	}

	_, err := ReadJournalHeader(&buf)
	if !errors.Is(err, errInvalidJournal) {
		t.Fatalf("ReadJournalHeader() error = %v, want %v", err, errInvalidJournal)
	}
}

func TestRecoverFromRollbackJournalRestoresMultipleEntries(t *testing.T) {
	dbFile, pager, path := openTestPager(t)

	page1 := pager.NewPage()
	copy(page1.Data(), []byte("page-one"))
	page2 := pager.NewPage()
	copy(page2.Data(), []byte("page-two"))
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := pager.Sync(); err != nil {
		t.Fatalf("pager.Sync() error = %v", err)
	}
	pager.ClearDirtyTracking()

	pager.MarkDirtyWithOriginal(page2)
	clear(page2.Data())
	copy(page2.Data(), []byte("mut-two"))
	pager.MarkDirtyWithOriginal(page1)
	clear(page1.Data())
	copy(page1.Data(), []byte("mut-one"))

	if err := WriteRollbackJournal(JournalPath(path), PageSize, pager.DirtyPagesWithOriginals()); err != nil {
		t.Fatalf("WriteRollbackJournal() error = %v", err)
	}
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := pager.Sync(); err != nil {
		t.Fatalf("pager.Sync() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	if err := RecoverFromRollbackJournal(path, PageSize); err != nil {
		t.Fatalf("RecoverFromRollbackJournal() error = %v", err)
	}
	if _, err := os.Stat(JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists", err)
	}

	dbFile, err := OpenOrCreate(path)
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	pager, err = NewPager(dbFile.file)
	if err != nil {
		t.Fatalf("NewPager() error = %v", err)
	}

	reloaded1, err := pager.Get(page1.ID())
	if err != nil {
		t.Fatalf("pager.Get(page1) error = %v", err)
	}
	reloaded2, err := pager.Get(page2.ID())
	if err != nil {
		t.Fatalf("pager.Get(page2) error = %v", err)
	}
	if got := string(reloaded1.Data()[:8]); got != "page-one" {
		t.Fatalf("page1 data = %q, want %q", got, "page-one")
	}
	if got := string(reloaded2.Data()[:8]); got != "page-two" {
		t.Fatalf("page2 data = %q, want %q", got, "page-two")
	}
}
