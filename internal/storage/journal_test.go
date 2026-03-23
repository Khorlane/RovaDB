package storage

import (
	"bytes"
	"errors"
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
