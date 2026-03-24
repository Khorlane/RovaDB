package rovadb

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestOpenCreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	_ = os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("os.Stat(%q) error = %v", path, err)
	}
}

func TestOpenExistingValidFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db.Close()
}

func TestOpenInvalidHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.db")
	if err := os.WriteFile(path, []byte("not-a-rovadb-file"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	db, err := Open(path)
	if err == nil {
		db.Close()
		t.Fatal("Open() error = nil, want non-nil")
	}
}

func TestRecoveryOnOpenRestoresLastCommittedState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	db.afterDatabaseSyncHook = func() error {
		return errors.New("boom after db sync")
	}
	if _, err := db.Exec("UPDATE t SET id = 2 WHERE id = 1"); err == nil {
		t.Fatal("Exec(update) error = nil, want failure")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open(recover) error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
	if _, err := os.Stat(storage.JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists", err)
	}
}

func TestOpenWithoutJournalSkipsRecovery(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestOpenFailsOnMalformedJournal(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := os.WriteFile(storage.JournalPath(path), []byte("bad-journal"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		db.Close()
		t.Fatal("Open() error = nil, want malformed journal error")
	}
}

func TestOpenFailsOnJournalPageSizeMismatch(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	journalFile, err := storage.CreateJournalFile(storage.JournalPath(path), 1234, 0)
	if err != nil {
		t.Fatalf("CreateJournalFile() error = %v", err)
	}
	if err := journalFile.Close(); err != nil {
		t.Fatalf("journalFile.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		db.Close()
		t.Fatal("Open() error = nil, want journal page size mismatch")
	}
}

func TestRecoveryRunsBeforeCatalogLoad(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	catalogPage, err := pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get(0) error = %v", err)
	}
	originalCatalog := append([]byte(nil), catalogPage.Data()...)
	clear(catalogPage.Data())
	copy(catalogPage.Data(), []byte("corrupt-catalog"))
	pager.MarkDirty(catalogPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	journalFile, err := storage.CreateJournalFile(storage.JournalPath(path), storage.PageSize, 1)
	if err != nil {
		t.Fatalf("CreateJournalFile() error = %v", err)
	}
	if err := storage.WriteJournalEntry(journalFile, 0, originalCatalog); err != nil {
		t.Fatalf("WriteJournalEntry() error = %v", err)
	}
	if err := journalFile.Sync(); err != nil {
		t.Fatalf("journalFile.Sync() error = %v", err)
	}
	if err := journalFile.Close(); err != nil {
		t.Fatalf("journalFile.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open(recover) error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}
