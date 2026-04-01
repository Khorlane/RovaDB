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

	rawDB, pager := openRawStorage(t, path)
	defer rawDB.Close()

	page, err := pager.Get(storage.DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.Get(directory) error = %v", err)
	}
	if err := storage.ValidateDirectoryPage(page.Data()); err != nil {
		t.Fatalf("ValidateDirectoryPage() error = %v", err)
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

func TestOpenRevalidatesDirectoryPageOnReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	page, err := pager.Get(storage.DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.Get(directory) error = %v", err)
	}
	if err := storage.ValidateDirectoryPage(page.Data()); err != nil {
		t.Fatalf("ValidateDirectoryPage() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db.Close()
}

func TestOpenWithHeaderOnlyWALSucceeds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := storage.ResetWALFile(path, storage.DBFormatVersion()); err != nil {
		t.Fatalf("ResetWALFile() error = %v", err)
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

func TestOpenFailsOnMalformedDirectoryPage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	corrupted := make([]byte, storage.PageSize)
	copy(corrupted, []byte("bad-directory"))
	if _, err := file.WriteAt(corrupted, storage.HeaderSize); err != nil {
		_ = file.Close()
		t.Fatalf("file.WriteAt() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want malformed directory page error")
	}
}

func TestDirectoryRootMappingsPersistTableRootAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	usersRoot := uint32(db.tables["users"].RootPageID())
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	mappings, err := storage.ReadDirectoryRootMappings(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryRootMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	found := false
	for _, mapping := range mappings {
		if mapping.ObjectType == storage.DirectoryRootMappingObjectTable && mapping.TableName == "users" {
			found = true
			if mapping.RootPageID != usersRoot {
				t.Fatalf("users table root mapping = %d, want %d", mapping.RootPageID, usersRoot)
			}
		}
	}
	if !found {
		t.Fatal("users table root mapping not found")
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()
	if got := uint32(db.tables["users"].RootPageID()); got != usersRoot {
		t.Fatalf("reopened users.RootPageID() = %d, want %d", got, usersRoot)
	}
}

func TestDirectoryRootMappingsPersistIndexRootAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	indexRoot := db.tables["users"].IndexDefinition("idx_users_name").RootPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	mappings, err := storage.ReadDirectoryRootMappings(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryRootMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	found := false
	for _, mapping := range mappings {
		if mapping.ObjectType == storage.DirectoryRootMappingObjectIndex && mapping.TableName == "users" && mapping.IndexName == "idx_users_name" {
			found = true
			if mapping.RootPageID != indexRoot {
				t.Fatalf("users index root mapping = %d, want %d", mapping.RootPageID, indexRoot)
			}
		}
	}
	if !found {
		t.Fatal("users index root mapping not found")
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()
	if got := db.tables["users"].IndexDefinition("idx_users_name").RootPageID; got != indexRoot {
		t.Fatalf("reopened index RootPageID = %d, want %d", got, indexRoot)
	}
}

func TestOpenFailsOnDirectoryRootMappingMismatch(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	page := make([]byte, storage.PageSize)
	if _, err := file.ReadAt(page, storage.HeaderSize); err != nil {
		_ = file.Close()
		t.Fatalf("file.ReadAt() error = %v", err)
	}
	rootMapBytes := binary.LittleEndian.Uint32(page[44:48])
	catalogOffset := 48 + int(rootMapBytes)
	nameLen := int(binary.LittleEndian.Uint16(page[catalogOffset+8 : catalogOffset+10]))
	rootPageOffset := catalogOffset + 10 + nameLen
	binary.LittleEndian.PutUint32(page[rootPageOffset:rootPageOffset+4], 999)
	if _, err := file.WriteAt(page, storage.HeaderSize); err != nil {
		_ = file.Close()
		t.Fatalf("file.WriteAt() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want directory root mapping mismatch failure")
	}
}

func TestOpenFailsWhenFreeListHeadPointsToNonFreePage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), 1); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want invalid free-list head failure")
	}
}

func TestOpenFailsWhenTableRootMappingPointsToIndexPage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	indexRoot := db.tables["users"].IndexDefinition("idx_users_name").RootPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := storage.WriteDirectoryRootMappings(rawDB.File(), []storage.DirectoryRootMapping{{
		ObjectType: storage.DirectoryRootMappingObjectTable,
		TableName:  "users",
		RootPageID: indexRoot,
	}}); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryRootMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want invalid table root mapping failure")
	}
}

func TestOpenFailsWhenIndexRootMappingPointsToTablePage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	tableRoot := uint32(db.tables["users"].RootPageID())
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := storage.WriteDirectoryRootMappings(rawDB.File(), []storage.DirectoryRootMapping{
		{ObjectType: storage.DirectoryRootMappingObjectTable, TableName: "users", RootPageID: tableRoot},
		{ObjectType: storage.DirectoryRootMappingObjectIndex, TableName: "users", IndexName: "idx_users_name", RootPageID: tableRoot},
	}); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryRootMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want invalid index root mapping failure")
	}
}

func TestOpenLoadsDurableFreeListHead(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	seedFreePageForTest(t, pager, 2, 0)
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), 2); err != nil {
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	if db.freeListHead != 2 {
		t.Fatalf("db.freeListHead = %d, want 2", db.freeListHead)
	}
}

func TestCreateTableReusesDurableFreeListHeadAndAdvancesIt(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatalf("Exec(create t1) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	seedFreePageForTest(t, pager, 2, 3)
	seedFreePageForTest(t, pager, 3, 0)
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), 2); err != nil {
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t2 (id INT)"); err != nil {
		t.Fatalf("Exec(create t2) error = %v", err)
	}
	if got := db.tables["t2"].RootPageID(); got != 2 {
		t.Fatalf("t2 rootPageID = %d, want 2", got)
	}
	if db.freeListHead != 3 {
		t.Fatalf("db.freeListHead = %d, want 3", db.freeListHead)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ = openRawStorage(t, path)
	defer rawDB.Close()
	head, err := storage.ReadDirectoryFreeListHead(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	if head != 3 {
		t.Fatalf("ReadDirectoryFreeListHead() = %d, want 3", head)
	}
}

func TestCreateTableFallsBackToFreshAllocationWhenFreeListEmpty(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatalf("Exec(create t1) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t2 (id INT)"); err != nil {
		t.Fatalf("Exec(create t2) error = %v", err)
	}
	defer db.Close()

	if got := db.tables["t2"].RootPageID(); got != 2 {
		t.Fatalf("t2 rootPageID = %d, want 2", got)
	}
	if db.freeListHead != 0 {
		t.Fatalf("db.freeListHead = %d, want 0", db.freeListHead)
	}
}

func TestReopenPreservesFreeListHeadForSubsequentAllocation(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatalf("Exec(create t1) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	seedFreePageForTest(t, pager, 2, 3)
	seedFreePageForTest(t, pager, 3, 0)
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), 2); err != nil {
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t2 (id INT)"); err != nil {
		t.Fatalf("Exec(create t2) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second reopen Open() error = %v", err)
	}
	defer db.Close()
	if db.freeListHead != 3 {
		t.Fatalf("db.freeListHead = %d, want 3", db.freeListHead)
	}
	if _, err := db.Exec("CREATE TABLE t3 (id INT)"); err != nil {
		t.Fatalf("Exec(create t3) error = %v", err)
	}
	if got := db.tables["t3"].RootPageID(); got != 3 {
		t.Fatalf("t3 rootPageID = %d, want 3", got)
	}
}

func TestCreateTableFailsOnMalformedFreePageLinkage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatalf("Exec(create t1) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	page, err := pager.Get(2)
	if err != nil {
		t.Fatalf("pager.Get(2) error = %v", err)
	}
	clear(page.Data())
	copy(page.Data(), []byte("not-a-free-page"))
	pager.MarkDirty(page)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), 2); err != nil {
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t2 (id INT)"); err == nil {
		t.Fatal("Exec(create t2) error = nil, want malformed free page failure")
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
	db.afterDatabaseSyncHook = func() error {
		return errors.New("checkpoint failed after WAL durability")
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err == nil {
		t.Fatal("Exec(insert) error = nil, want checkpoint failure")
	}
	db.afterDatabaseSyncHook = nil
	if records, err := storage.ReadWALRecords(path); err != nil {
		t.Fatalf("ReadWALRecords() error = %v", err)
	} else if len(records) == 0 {
		t.Fatal("len(ReadWALRecords()) = 0, want committed WAL after checkpoint failure")
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
	db.afterDatabaseSyncHook = func() error {
		return errors.New("checkpoint failed after WAL durability")
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err == nil {
		t.Fatal("Exec(insert) error = nil, want checkpoint failure")
	}
	db.afterDatabaseSyncHook = nil
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
	db.afterDatabaseSyncHook = func() error {
		return errors.New("checkpoint failed after WAL durability")
	}
	if _, err := db.Exec("INSERT INTO t VALUES (2)"); err == nil {
		t.Fatal("Exec(second insert) error = nil, want checkpoint failure")
	}
	db.afterDatabaseSyncHook = nil
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

func seedFreePageForTest(t *testing.T, pager *storage.Pager, pageID storage.PageID, next storage.PageID) {
	t.Helper()

	page, err := pager.Get(pageID)
	if err != nil {
		t.Fatalf("pager.Get(%d) error = %v", pageID, err)
	}
	clear(page.Data())
	copy(page.Data(), storage.InitFreePage(uint32(pageID), uint32(next)))
	pager.MarkDirty(page)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
}
