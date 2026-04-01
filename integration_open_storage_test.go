package rovadb

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
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
	if _, err := os.Stat(storage.WALPath(path)); err != nil {
		t.Fatalf("os.Stat(%q) error = %v", storage.WALPath(path), err)
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

func TestOpenExistingValidFileWithValidWAL(t *testing.T) {
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

	assertSelectIntRows(t, db, "SELECT * FROM t", 2)
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

func TestOpenFailsOnMalformedWAL(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := os.WriteFile(storage.WALPath(path), []byte("bad-wal"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		db.Close()
		t.Fatal("Open() error = nil, want malformed wal error")
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

func TestOpenReplaysCommittedWALState(t *testing.T) {
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

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(1)
	if err != nil {
		t.Fatalf("pager.Get(1) error = %v", err)
	}
	corrupted := storage.InitializeTablePage(1)
	if err := storage.RecomputePageChecksum(corrupted); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	clear(rootPage.Data())
	copy(rootPage.Data(), corrupted)
	pager.MarkDirty(rootPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestOpenIgnoresTrailingUncommittedWALFrames(t *testing.T) {
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

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(1)
	if err != nil {
		t.Fatalf("pager.Get(1) error = %v", err)
	}
	uncommitted := append([]byte(nil), rootPage.Data()...)
	row, err := storage.EncodeSlottedRow([]parser.Value{parser.Int64Value(2)})
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}
	slotCount, err := storage.TablePageSlotCount(uncommitted)
	if err != nil {
		t.Fatalf("TablePageSlotCount() error = %v", err)
	}
	if slotCount != 1 {
		t.Fatalf("TablePageSlotCount() = %d, want 1", slotCount)
	}
	if _, err := storage.InsertRowIntoTablePage(uncommitted, row); err != nil {
		t.Fatalf("InsertRowIntoTablePage() error = %v", err)
	}
	if err := storage.SetPageLSN(uncommitted, 999); err != nil {
		t.Fatalf("SetPageLSN() error = %v", err)
	}
	if err := storage.RecomputePageChecksum(uncommitted); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	var frame storage.WALFrame
	frame.FrameLSN = 999
	frame.PageID = 1
	frame.PageLSN = 999
	copy(frame.PageData[:], uncommitted)
	if err := storage.AppendWALFrame(path, frame); err != nil {
		t.Fatalf("AppendWALFrame() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestOpenReplayIsIdempotentAcrossRepeatedOpens(t *testing.T) {
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

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(1)
	if err != nil {
		t.Fatalf("pager.Get(1) error = %v", err)
	}
	corrupted := storage.InitializeTablePage(1)
	if err := storage.RecomputePageChecksum(corrupted); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	clear(rootPage.Data())
	copy(rootPage.Data(), corrupted)
	pager.MarkDirty(rootPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("first replay Open() error = %v", err)
	}
	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
	if err := db.Close(); err != nil {
		t.Fatalf("first replay Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second replay Open() error = %v", err)
	}
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestOpenReplaysMultipleCommittedWALTransactions(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(first insert) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (2)"); err != nil {
		t.Fatalf("Exec(second insert) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(1)
	if err != nil {
		t.Fatalf("pager.Get(1) error = %v", err)
	}
	older := storage.InitializeTablePage(1)
	row, err := storage.EncodeSlottedRow([]parser.Value{parser.Int64Value(1)})
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}
	if _, err := storage.InsertRowIntoTablePage(older, row); err != nil {
		t.Fatalf("InsertRowIntoTablePage() error = %v", err)
	}
	if err := storage.SetPageLSN(older, 5); err != nil {
		t.Fatalf("SetPageLSN() error = %v", err)
	}
	if err := storage.RecomputePageChecksum(older); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	clear(rootPage.Data())
	copy(rootPage.Data(), older)
	pager.MarkDirty(rootPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)
}

func TestOpenFailsOnTruncatedWALFrameDuringReplay(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	file, err := os.OpenFile(storage.WALPath(path), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	raw := make([]byte, storage.WALFrameSize-1)
	binary.LittleEndian.PutUint32(raw[0:4], storage.WALRecordTypeFrame)
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		t.Fatalf("file.Write() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want truncated WAL replay failure")
	}
}
