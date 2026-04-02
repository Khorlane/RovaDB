package rovadb

import (
	"errors"
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestEngineStatusOnFreshDB(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	status, err := db.EngineStatus()
	if err != nil {
		t.Fatalf("EngineStatus() error = %v", err)
	}
	if status.DBFormatVersion != storage.CurrentDBFormatVersion {
		t.Fatalf("EngineStatus().DBFormatVersion = %d, want %d", status.DBFormatVersion, storage.CurrentDBFormatVersion)
	}
	if status.WALVersion != storage.CurrentWALVersion {
		t.Fatalf("EngineStatus().WALVersion = %d, want %d", status.WALVersion, storage.CurrentWALVersion)
	}
	if status.TableCount != 0 {
		t.Fatalf("EngineStatus().TableCount = %d, want 0", status.TableCount)
	}
	if status.IndexCount != 0 {
		t.Fatalf("EngineStatus().IndexCount = %d, want 0", status.IndexCount)
	}
	if status.FreeListHead != 0 {
		t.Fatalf("EngineStatus().FreeListHead = %d, want 0", status.FreeListHead)
	}
}

func TestEngineStatusTracksUserTableAndIndexCounts(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE teams (id INT)",
		"CREATE INDEX idx_users_name ON users (name)",
		"CREATE UNIQUE INDEX idx_users_id ON users (id)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	status, err := db.EngineStatus()
	if err != nil {
		t.Fatalf("EngineStatus() error = %v", err)
	}
	if status.TableCount != 2 {
		t.Fatalf("EngineStatus().TableCount = %d, want 2", status.TableCount)
	}
	if status.IndexCount != 2 {
		t.Fatalf("EngineStatus().IndexCount = %d, want 2", status.IndexCount)
	}
}

func TestEngineStatusSurfacesCheckpointAndFreeListState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	droppedRootPageID := storage.PageID(db.tables["users"].IndexDefinition("idx_users_name").RootPageID)
	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}

	status, err := db.EngineStatus()
	if err != nil {
		t.Fatalf("EngineStatus() error = %v", err)
	}
	if status.LastCheckpointLSN == 0 {
		t.Fatal("EngineStatus().LastCheckpointLSN = 0, want nonzero")
	}
	if status.LastCheckpointPageCount == 0 {
		t.Fatal("EngineStatus().LastCheckpointPageCount = 0, want nonzero")
	}
	if status.FreeListHead != uint32(droppedRootPageID) {
		t.Fatalf("EngineStatus().FreeListHead = %d, want %d", status.FreeListHead, droppedRootPageID)
	}
	if status.TableCount != 1 {
		t.Fatalf("EngineStatus().TableCount = %d, want 1", status.TableCount)
	}
	if status.IndexCount != 0 {
		t.Fatalf("EngineStatus().IndexCount = %d, want 0", status.IndexCount)
	}
}

func TestEngineStatusOnClosedDBReturnsErrClosed(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = db.EngineStatus()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("EngineStatus() error = %v, want ErrClosed", err)
	}
}

func TestCheckEngineConsistencyOnFreshDB(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	result, err := db.CheckEngineConsistency()
	if err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}
	if !result.OK {
		t.Fatal("CheckEngineConsistency().OK = false, want true")
	}
	if result.CheckedTableRoots != 0 {
		t.Fatalf("CheckEngineConsistency().CheckedTableRoots = %d, want 0", result.CheckedTableRoots)
	}
	if result.CheckedIndexRoots != 0 {
		t.Fatalf("CheckEngineConsistency().CheckedIndexRoots = %d, want 0", result.CheckedIndexRoots)
	}
	if result.FreeListHead != 0 {
		t.Fatalf("CheckEngineConsistency().FreeListHead = %d, want 0", result.FreeListHead)
	}
}

func TestCheckEngineConsistencyTracksUserRootsOnly(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	result, err := db.CheckEngineConsistency()
	if err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}
	if !result.OK {
		t.Fatal("CheckEngineConsistency().OK = false, want true")
	}
	if result.CheckedTableRoots != 1 {
		t.Fatalf("CheckEngineConsistency().CheckedTableRoots = %d, want 1", result.CheckedTableRoots)
	}
	if result.CheckedIndexRoots != 1 {
		t.Fatalf("CheckEngineConsistency().CheckedIndexRoots = %d, want 1", result.CheckedIndexRoots)
	}
}

func TestCheckEngineConsistencySurfacesFreeListHead(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	droppedRootPageID := storage.PageID(db.tables["users"].IndexDefinition("idx_users_name").RootPageID)
	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}

	result, err := db.CheckEngineConsistency()
	if err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}
	if !result.OK {
		t.Fatal("CheckEngineConsistency().OK = false, want true")
	}
	if result.FreeListHead != uint32(droppedRootPageID) {
		t.Fatalf("CheckEngineConsistency().FreeListHead = %d, want %d", result.FreeListHead, droppedRootPageID)
	}
}

func TestCheckEngineConsistencyRejectsMalformedFreeListHead(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}

	db.freeListHead = uint32(db.tables["users"].RootPageID())
	if _, err := db.CheckEngineConsistency(); err == nil {
		t.Fatal("CheckEngineConsistency() error = nil, want malformed free-list-head failure")
	}
}

func TestCheckEngineConsistencyRejectsMalformedTableRootPageType(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	db.tables["users"].SetStorageMeta(storage.PageID(db.tables["users"].IndexDefinition("idx_users_name").RootPageID), db.tables["users"].PersistedRowCount())
	if _, err := db.CheckEngineConsistency(); err == nil {
		t.Fatal("CheckEngineConsistency() error = nil, want malformed table-root failure")
	}
}

func TestCheckEngineConsistencyRejectsMalformedIndexRootPageType(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	db.tables["users"].IndexDefinition("idx_users_name").RootPageID = uint32(db.tables["users"].RootPageID())
	if _, err := db.CheckEngineConsistency(); err == nil {
		t.Fatal("CheckEngineConsistency() error = nil, want malformed index-root failure")
	}
}

func TestCheckEngineConsistencyOnClosedDBReturnsErrClosed(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = db.CheckEngineConsistency()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("CheckEngineConsistency() error = %v, want ErrClosed", err)
	}
}

func TestPageUsageOnFreshDB(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	usage, err := db.PageUsage()
	if err != nil {
		t.Fatalf("PageUsage() error = %v", err)
	}
	if usage.TotalPages != 5 {
		t.Fatalf("PageUsage().TotalPages = %d, want 5", usage.TotalPages)
	}
	if usage.DirectoryPages != 1 {
		t.Fatalf("PageUsage().DirectoryPages = %d, want 1", usage.DirectoryPages)
	}
	if usage.TablePages != 4 {
		t.Fatalf("PageUsage().TablePages = %d, want 4 system table pages", usage.TablePages)
	}
	if usage.IndexLeafPages != 0 || usage.IndexInternalPages != 0 || usage.FreePages != 0 {
		t.Fatalf("PageUsage() = %#v, want only directory + system table pages", usage)
	}
}

func TestPageUsageTracksTableIndexAndFreePages(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	before, err := db.PageUsage()
	if err != nil {
		t.Fatalf("PageUsage(before) error = %v", err)
	}

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	afterTable, err := db.PageUsage()
	if err != nil {
		t.Fatalf("PageUsage(after table) error = %v", err)
	}
	if afterTable.TablePages != before.TablePages+1 {
		t.Fatalf("PageUsage().TablePages after table = %d, want %d", afterTable.TablePages, before.TablePages+1)
	}
	if afterTable.DirectoryPages != 1 {
		t.Fatalf("PageUsage().DirectoryPages after table = %d, want 1", afterTable.DirectoryPages)
	}

	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	afterIndex, err := db.PageUsage()
	if err != nil {
		t.Fatalf("PageUsage(after index) error = %v", err)
	}
	if afterIndex.IndexLeafPages < afterTable.IndexLeafPages+1 {
		t.Fatalf("PageUsage().IndexLeafPages after index = %d, want at least %d", afterIndex.IndexLeafPages, afterTable.IndexLeafPages+1)
	}

	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	afterDrop, err := db.PageUsage()
	if err != nil {
		t.Fatalf("PageUsage(after drop) error = %v", err)
	}
	if afterDrop.FreePages < afterIndex.FreePages+1 {
		t.Fatalf("PageUsage().FreePages after drop = %d, want at least %d", afterDrop.FreePages, afterIndex.FreePages+1)
	}
}

func TestPageUsageFailsOnMalformedPage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
		"CREATE UNIQUE INDEX idx_users_id ON users (id)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	tailFreePageID := storage.PageID(db.tables["users"].IndexDefinition("idx_users_id").RootPageID)
	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	page, err := pager.Get(tailFreePageID)
	if err != nil {
		t.Fatalf("pager.Get(%d) error = %v", tailFreePageID, err)
	}
	clear(page.Data())
	copy(page.Data(), []byte("not-a-valid-page"))
	pager.MarkDirty(page)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()
	if _, err := db.PageUsage(); err == nil {
		t.Fatal("PageUsage() error = nil, want malformed page failure")
	}
}

func TestPageUsageOnClosedDBReturnsErrClosed(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = db.PageUsage()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("PageUsage() error = %v, want ErrClosed", err)
	}
}
