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

func TestOpenFailsOnUnsupportedDBHeaderVersion(t *testing.T) {
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
	var header [storage.HeaderSize]byte
	if _, err := file.ReadAt(header[:], 0); err != nil {
		_ = file.Close()
		t.Fatalf("file.ReadAt() error = %v", err)
	}
	binary.LittleEndian.PutUint32(header[8:12], storage.CurrentDBFormatVersion+1)
	if _, err := file.WriteAt(header[:], 0); err != nil {
		_ = file.Close()
		t.Fatalf("file.WriteAt() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want unsupported DB header version failure")
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

func TestOpenFailsOnUnsupportedDirectoryFormatVersion(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	page, err := storage.ReadDirectoryPage(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryPage() error = %v", err)
	}
	binary.LittleEndian.PutUint32(page[32:36], storage.CurrentDBFormatVersion+1)
	if _, err := rawDB.File().WriteAt(page, storage.HeaderSize); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteAt() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want unsupported directory format failure")
	}
}

func TestOpenFailsOnWALDBFormatVersionMismatch(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	walFile, err := os.OpenFile(storage.WALPath(path), os.O_RDWR|os.O_TRUNC, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	if err := storage.WriteWALHeader(walFile, storage.WALHeader{
		Magic:           [8]byte{'R', 'O', 'V', 'A', 'W', 'A', 'L', '1'},
		WALVersion:      storage.CurrentWALVersion,
		DBFormatVersion: storage.CurrentDBFormatVersion + 1,
		PageSize:        storage.PageSize,
	}); err != nil {
		_ = walFile.Close()
		t.Fatalf("WriteWALHeader() error = %v", err)
	}
	if err := walFile.Close(); err != nil {
		t.Fatalf("walFile.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want WAL/DB format mismatch failure")
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

func TestOpenFallsBackToLegacyNameBasedTableRootMappings(t *testing.T) {
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
	if err := storage.WriteDirectoryRootMappings(rawDB.File(), []storage.DirectoryRootMapping{{
		ObjectType: storage.DirectoryRootMappingObjectTable,
		TableName:  "users",
		RootPageID: usersRoot,
	}}); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryRootMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
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

func TestOpenFallsBackToLegacyNameBasedIndexRootMappings(t *testing.T) {
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
		ObjectType: storage.DirectoryRootMappingObjectIndex,
		TableName:  "users",
		IndexName:  "idx_users_name",
		RootPageID: indexRoot,
	}}); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryRootMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
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

func TestDirectoryRootIDMappingsPersistAcrossReopen(t *testing.T) {
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
	tableID := db.tables["users"].TableID
	tableRoot := uint32(db.tables["users"].RootPageID())
	indexDef := db.tables["users"].IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatal("IndexDefinition(idx_users_name) = nil")
	}
	indexID := indexDef.IndexID
	indexRoot := indexDef.RootPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	nameMappings, err := storage.ReadDirectoryRootMappings(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryRootMappings() error = %v", err)
	}
	idMappings, err := storage.ReadDirectoryRootIDMappings(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}
	if len(nameMappings) != 0 {
		t.Fatalf("len(ReadDirectoryRootMappings()) = %d, want 0 on new writes", len(nameMappings))
	}

	foundTable := false
	foundIndex := false
	for _, mapping := range idMappings {
		switch {
		case mapping.ObjectType == storage.DirectoryRootMappingObjectTable && mapping.ObjectID == tableID:
			foundTable = true
			if mapping.RootPageID != tableRoot {
				t.Fatalf("table ID root mapping = %d, want %d", mapping.RootPageID, tableRoot)
			}
		case mapping.ObjectType == storage.DirectoryRootMappingObjectIndex && mapping.ObjectID == indexID:
			foundIndex = true
			if mapping.RootPageID != indexRoot {
				t.Fatalf("index ID root mapping = %d, want %d", mapping.RootPageID, indexRoot)
			}
		}
	}
	if !foundTable {
		t.Fatal("table ID root mapping not found")
	}
	if !foundIndex {
		t.Fatal("index ID root mapping not found")
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()
	if got := db.tables["users"].RootPageID(); uint32(got) != tableRoot {
		t.Fatalf("reopened users.RootPageID() = %d, want %d", got, tableRoot)
	}
	if got := db.tables["users"].IndexDefinition("idx_users_name").RootPageID; got != indexRoot {
		t.Fatalf("reopened index RootPageID = %d, want %d", got, indexRoot)
	}
}

func TestDirectoryWritePathWritesIDMappingsOnly(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	teamsTableID := db.tables["teams"].TableID
	teamsRoot := uint32(db.tables["teams"].RootPageID())
	usersTableID := db.tables["users"].TableID
	usersRoot := uint32(db.tables["users"].RootPageID())
	indexDef := db.tables["users"].IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatal("IndexDefinition(idx_users_name) = nil")
	}
	indexID := indexDef.IndexID
	indexRoot := indexDef.RootPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	nameMappings, err := storage.ReadDirectoryRootMappings(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryRootMappings() error = %v", err)
	}
	idMappings, err := storage.ReadDirectoryRootIDMappings(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	if len(nameMappings) != 0 {
		t.Fatalf("len(nameMappings) = %d, want 0 on new writes", len(nameMappings))
	}
	if catalog.Version != 6 {
		t.Fatalf("catalog.Version = %d, want 6", catalog.Version)
	}
	for _, table := range catalog.Tables {
		if table.RootPageID != 0 {
			t.Fatalf("catalog table %q RootPageID = %d, want 0 on new writes", table.Name, table.RootPageID)
		}
		for _, index := range table.Indexes {
			if index.RootPageID != 0 {
				t.Fatalf("catalog index %q RootPageID = %d, want 0 on new writes", index.Name, index.RootPageID)
			}
		}
	}
	wantIDMappings := map[storage.DirectoryRootIDMapping]struct{}{
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: teamsTableID, RootPageID: teamsRoot}: {},
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: usersTableID, RootPageID: usersRoot}: {},
		{ObjectType: storage.DirectoryRootMappingObjectIndex, ObjectID: indexID, RootPageID: indexRoot}:      {},
	}
	for want := range wantIDMappings {
		found := false
		for _, got := range idMappings {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("directory ID mapping %#v not found in %#v", want, idMappings)
		}
	}
}

func TestOpenFailsWhenNameAndIDRootMappingsDisagree(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	teamsRoot := uint32(db.tables["teams"].RootPageID())
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := storage.WriteDirectoryRootMappings(rawDB.File(), []storage.DirectoryRootMapping{
		{ObjectType: storage.DirectoryRootMappingObjectTable, TableName: "users", RootPageID: teamsRoot},
		{ObjectType: storage.DirectoryRootMappingObjectTable, TableName: "teams", RootPageID: teamsRoot},
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
		t.Fatal("Open() error = nil, want conflicting name/id mapping failure")
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

	rawDB, _ := openRawStorage(t, path)
	if err := storage.WriteDirectoryRootMappings(rawDB.File(), []storage.DirectoryRootMapping{{
		ObjectType: storage.DirectoryRootMappingObjectTable,
		TableName:  "users",
		RootPageID: 999,
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
		t.Fatal("Open() error = nil, want directory root mapping mismatch failure")
	}
}

func TestOpenFailsWhenNewCatalogPayloadHasNoDirectoryRoots(t *testing.T) {
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
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	if catalog.Version != 6 {
		t.Fatalf("catalog.Version = %d, want 6", catalog.Version)
	}
	if err := storage.WriteDirectoryRootIDMappings(rawDB.File(), nil); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryRootIDMappings() error = %v", err)
	}
	if err := storage.WriteDirectoryRootMappings(rawDB.File(), nil); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryRootMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want missing directory roots failure")
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
	freeHead := appendFreePageForTest(t, pager, 0)
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), uint32(freeHead)); err != nil {
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

	if db.freeListHead != uint32(freeHead) {
		t.Fatalf("db.freeListHead = %d, want %d", db.freeListHead, freeHead)
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
	secondFreePageID := appendFreePageForTest(t, pager, 0)
	firstFreePageID := appendFreePageForTest(t, pager, secondFreePageID)
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), uint32(firstFreePageID)); err != nil {
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
	if got := db.tables["t2"].RootPageID(); got != firstFreePageID {
		t.Fatalf("t2 rootPageID = %d, want %d", got, firstFreePageID)
	}
	if db.freeListHead != uint32(secondFreePageID) {
		t.Fatalf("db.freeListHead = %d, want %d", db.freeListHead, secondFreePageID)
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
	if head != uint32(secondFreePageID) {
		t.Fatalf("ReadDirectoryFreeListHead() = %d, want %d", head, secondFreePageID)
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
	t1Root := db.tables["t1"].RootPageID()
	if _, err := db.Exec("CREATE TABLE t2 (id INT)"); err != nil {
		t.Fatalf("Exec(create t2) error = %v", err)
	}
	defer db.Close()

	if got := db.tables["t2"].RootPageID(); got != t1Root+1 {
		t.Fatalf("t2 rootPageID = %d, want %d", got, t1Root+1)
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
	secondFreePageID := appendFreePageForTest(t, pager, 0)
	firstFreePageID := appendFreePageForTest(t, pager, secondFreePageID)
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), uint32(firstFreePageID)); err != nil {
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
	if db.freeListHead != uint32(secondFreePageID) {
		t.Fatalf("db.freeListHead = %d, want %d", db.freeListHead, secondFreePageID)
	}
	if _, err := db.Exec("CREATE TABLE t3 (id INT)"); err != nil {
		t.Fatalf("Exec(create t3) error = %v", err)
	}
	if got := db.tables["t3"].RootPageID(); got != secondFreePageID {
		t.Fatalf("t3 rootPageID = %d, want %d", got, secondFreePageID)
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
	badFreePageID := appendFreePageForTest(t, pager, 0)
	page, err := pager.Get(badFreePageID)
	if err != nil {
		t.Fatalf("pager.Get(%d) error = %v", badFreePageID, err)
	}
	clear(page.Data())
	copy(page.Data(), []byte("not-a-free-page"))
	pager.MarkDirty(page)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), uint32(badFreePageID)); err != nil {
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
	rootPageID := db.tables["t"].RootPageID()
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(rootPageID)
	if err != nil {
		t.Fatalf("pager.Get(root) error = %v", err)
	}
	corrupted := storage.InitializeTablePage(uint32(rootPageID))
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
	rootPageID := db.tables["t"].RootPageID()
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(rootPageID)
	if err != nil {
		t.Fatalf("pager.Get(root) error = %v", err)
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
	frame.PageID = uint32(rootPageID)
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
	rootPageID := db.tables["t"].RootPageID()
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(rootPageID)
	if err != nil {
		t.Fatalf("pager.Get(root) error = %v", err)
	}
	corrupted := storage.InitializeTablePage(uint32(rootPageID))
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
	rootPageID := db.tables["t"].RootPageID()
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(rootPageID)
	if err != nil {
		t.Fatalf("pager.Get(root) error = %v", err)
	}
	older := storage.InitializeTablePage(uint32(rootPageID))
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

func appendFreePageForTest(t *testing.T, pager *storage.Pager, next storage.PageID) storage.PageID {
	t.Helper()

	page := pager.NewPage()
	pageID := page.ID()
	clear(page.Data())
	copy(page.Data(), storage.InitFreePage(uint32(pageID), uint32(next)))
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	return pageID
}
