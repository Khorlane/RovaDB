package rovadb

import (
	"errors"
	"strings"
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
	if usage.TotalPages != 9 {
		t.Fatalf("PageUsage().TotalPages = %d, want 9", usage.TotalPages)
	}
	if usage.DirectoryPages != 1 {
		t.Fatalf("PageUsage().DirectoryPages = %d, want 1", usage.DirectoryPages)
	}
	if usage.HeaderPages != 4 {
		t.Fatalf("PageUsage().HeaderPages = %d, want 4 system table header pages", usage.HeaderPages)
	}
	if usage.TablePages != 4 {
		t.Fatalf("PageUsage().TablePages = %d, want 4 system table pages", usage.TablePages)
	}
	if usage.SpaceMapPages != 0 || usage.IndexLeafPages != 0 || usage.IndexInternalPages != 0 || usage.FreePages != 0 {
		t.Fatalf("PageUsage() = %#v, want only directory + system table/header pages", usage)
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

func TestSchemaInventoryOnFreshDB(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	inventory, err := db.SchemaInventory()
	if err != nil {
		t.Fatalf("SchemaInventory() error = %v", err)
	}
	if len(inventory.Tables) != 0 {
		t.Fatalf("len(SchemaInventory().Tables) = %d, want 0", len(inventory.Tables))
	}
	if len(inventory.Indexes) != 0 {
		t.Fatalf("len(SchemaInventory().Indexes) = %d, want 0", len(inventory.Indexes))
	}
}

func TestSchemaInventoryIncludesUserTablesAndIndexes(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE UNIQUE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	users := db.tables["users"]
	indexDef := users.IndexDefinition("idx_users_name")
	if users == nil || indexDef == nil {
		t.Fatalf("schema setup failed: users=%v index=%v", users, indexDef)
	}

	inventory, err := db.SchemaInventory()
	if err != nil {
		t.Fatalf("SchemaInventory() error = %v", err)
	}
	if len(inventory.Tables) != 1 {
		t.Fatalf("len(SchemaInventory().Tables) = %d, want 1", len(inventory.Tables))
	}
	if len(inventory.Indexes) != 1 {
		t.Fatalf("len(SchemaInventory().Indexes) = %d, want 1", len(inventory.Indexes))
	}

	tableInfo := inventory.Tables[0]
	if tableInfo.TableID != users.TableID || tableInfo.TableName != "users" || tableInfo.RootPageID != uint32(users.RootPageID()) || tableInfo.IndexCount != 1 {
		t.Fatalf("SchemaInventory().Tables[0] = %#v, want users metadata", tableInfo)
	}

	indexInfo := inventory.Indexes[0]
	if indexInfo.IndexID != indexDef.IndexID || indexInfo.TableName != "users" || indexInfo.IndexName != "idx_users_name" || indexInfo.RootPageID != indexDef.RootPageID || !indexInfo.IsUnique {
		t.Fatalf("SchemaInventory().Indexes[0] = %#v, want idx_users_name metadata", indexInfo)
	}
}

func TestSchemaInventoryOrderingIsDeterministic(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE accounts (id INT, email TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
		"CREATE UNIQUE INDEX idx_accounts_email ON accounts (email)",
		"CREATE INDEX idx_accounts_id ON accounts (id)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	inventory, err := db.SchemaInventory()
	if err != nil {
		t.Fatalf("SchemaInventory() error = %v", err)
	}

	if got := []string{inventory.Tables[0].TableName, inventory.Tables[1].TableName}; got[0] != "accounts" || got[1] != "users" {
		t.Fatalf("SchemaInventory().Tables order = %#v, want [accounts users]", got)
	}
	if got := []string{
		inventory.Indexes[0].TableName + "." + inventory.Indexes[0].IndexName,
		inventory.Indexes[1].TableName + "." + inventory.Indexes[1].IndexName,
		inventory.Indexes[2].TableName + "." + inventory.Indexes[2].IndexName,
	}; got[0] != "accounts.idx_accounts_email" || got[1] != "accounts.idx_accounts_id" || got[2] != "users.idx_users_name" {
		t.Fatalf("SchemaInventory().Indexes order = %#v, want [accounts.idx_accounts_email accounts.idx_accounts_id users.idx_users_name]", got)
	}
}

func TestSchemaInventoryExcludesSystemTables(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	inventory, err := db.SchemaInventory()
	if err != nil {
		t.Fatalf("SchemaInventory() error = %v", err)
	}
	for _, tableInfo := range inventory.Tables {
		if isSystemCatalogTableName(tableInfo.TableName) {
			t.Fatalf("SchemaInventory() included system table %#v", tableInfo)
		}
	}
	for _, indexInfo := range inventory.Indexes {
		if isSystemCatalogTableName(indexInfo.TableName) {
			t.Fatalf("SchemaInventory() included system-table index %#v", indexInfo)
		}
	}
}

func TestSchemaInventoryOnClosedDBReturnsErrClosed(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = db.SchemaInventory()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("SchemaInventory() error = %v, want ErrClosed", err)
	}
}

func TestEngineSnapshotOnFreshDBMatchesHelpers(t *testing.T) {
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
	check, err := db.CheckEngineConsistency()
	if err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}
	pageUsage, err := db.PageUsage()
	if err != nil {
		t.Fatalf("PageUsage() error = %v", err)
	}
	inventory, err := db.SchemaInventory()
	if err != nil {
		t.Fatalf("SchemaInventory() error = %v", err)
	}

	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() error = %v", err)
	}
	if snapshot.Status != status {
		t.Fatalf("EngineSnapshot().Status = %#v, want %#v", snapshot.Status, status)
	}
	if snapshot.Check != check {
		t.Fatalf("EngineSnapshot().Check = %#v, want %#v", snapshot.Check, check)
	}
	if snapshot.PageUsage != pageUsage {
		t.Fatalf("EngineSnapshot().PageUsage = %#v, want %#v", snapshot.PageUsage, pageUsage)
	}
	if len(snapshot.Inventory.Tables) != len(inventory.Tables) || len(snapshot.Inventory.Indexes) != len(inventory.Indexes) {
		t.Fatalf("EngineSnapshot().Inventory = %#v, want %#v", snapshot.Inventory, inventory)
	}
	for i := range inventory.Tables {
		if snapshot.Inventory.Tables[i] != inventory.Tables[i] {
			t.Fatalf("EngineSnapshot().Inventory.Tables[%d] = %#v, want %#v", i, snapshot.Inventory.Tables[i], inventory.Tables[i])
		}
	}
	for i := range inventory.Indexes {
		if snapshot.Inventory.Indexes[i] != inventory.Indexes[i] {
			t.Fatalf("EngineSnapshot().Inventory.Indexes[%d] = %#v, want %#v", i, snapshot.Inventory.Indexes[i], inventory.Indexes[i])
		}
	}
}

func TestEngineSnapshotReflectsSchemaAndDropChanges(t *testing.T) {
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

	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() after create error = %v", err)
	}
	if snapshot.Status.TableCount != 1 || snapshot.Status.IndexCount != 1 {
		t.Fatalf("EngineSnapshot().Status after create = %#v, want 1 table and 1 index", snapshot.Status)
	}
	if snapshot.Check.CheckedTableRoots != 1 || snapshot.Check.CheckedIndexRoots != 1 {
		t.Fatalf("EngineSnapshot().Check after create = %#v, want 1 checked table root and 1 checked index root", snapshot.Check)
	}
	if len(snapshot.Inventory.Tables) != 1 || len(snapshot.Inventory.Indexes) != 1 {
		t.Fatalf("EngineSnapshot().Inventory after create = %#v, want one table and one index", snapshot.Inventory)
	}

	indexRootPageID := db.tables["users"].IndexDefinition("idx_users_name").RootPageID
	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}

	snapshot, err = db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() after drop error = %v", err)
	}
	if snapshot.Status.TableCount != 0 || snapshot.Status.IndexCount != 0 {
		t.Fatalf("EngineSnapshot().Status after drop = %#v, want empty user schema", snapshot.Status)
	}
	if snapshot.Check.CheckedTableRoots != 0 || snapshot.Check.CheckedIndexRoots != 0 {
		t.Fatalf("EngineSnapshot().Check after drop = %#v, want zero checked user roots", snapshot.Check)
	}
	if len(snapshot.Inventory.Tables) != 0 || len(snapshot.Inventory.Indexes) != 0 {
		t.Fatalf("EngineSnapshot().Inventory after drop = %#v, want empty inventory", snapshot.Inventory)
	}
	if snapshot.PageUsage.FreePages == 0 {
		t.Fatalf("EngineSnapshot().PageUsage = %#v, want freed pages after drop", snapshot.PageUsage)
	}
	if snapshot.Check.FreeListHead != indexRootPageID && snapshot.Check.FreeListHead == 0 {
		t.Fatalf("EngineSnapshot().Check.FreeListHead = %d, want nonzero free-list head after drop", snapshot.Check.FreeListHead)
	}
}

func TestEngineSnapshotOnClosedDBReturnsErrClosed(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = db.EngineSnapshot()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("EngineSnapshot() error = %v, want ErrClosed", err)
	}
}

func TestEngineSnapshotStringOnFreshDBIsStable(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() error = %v", err)
	}

	got := snapshot.String()
	want := "Engine Status\n" +
		"DB format: 1\n" +
		"WAL version: 1\n" +
		"Checkpoint: LSN=0 pages=0\n" +
		"Free list head: 0\n" +
		"Tables: 0\n" +
		"Indexes: 0\n\n" +
		"Consistency\n" +
		"OK: true\n" +
		"Checked table roots: 0\n" +
		"Checked index roots: 0\n\n" +
		"Page Usage\n" +
		"Total: 9\n" +
		"Header: 4\n" +
		"Space map: 0\n" +
		"Table: 4\n" +
		"Index leaf: 0\n" +
		"Index internal: 0\n" +
		"Free: 0\n" +
		"Directory: 1\n\n" +
		"Schema Inventory\n" +
		"Tables:\n" +
		"Indexes:\n"
	if got != want {
		t.Fatalf("EngineSnapshot().String() = %q, want %q", got, want)
	}
}

func TestEngineSnapshotStringIncludesSchemaDetails(t *testing.T) {
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

	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() error = %v", err)
	}
	formatted := snapshot.String()

	for _, want := range []string{
		"Engine Status\n",
		"\nConsistency\n",
		"\nPage Usage\n",
		"\nSchema Inventory\n",
		"Tables:\n- users (id=",
		"Indexes:\n- users.idx_users_name (id=",
	} {
		if !strings.Contains(formatted, want) {
			t.Fatalf("EngineSnapshot().String() missing %q in %q", want, formatted)
		}
	}
}

func TestEngineSnapshotStringOrderingIsDeterministic(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE accounts (id INT, email TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
		"CREATE UNIQUE INDEX idx_accounts_email ON accounts (email)",
		"CREATE INDEX idx_accounts_id ON accounts (id)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() error = %v", err)
	}
	formatted := snapshot.String()

	accountsTablePos := strings.Index(formatted, "- accounts (id=")
	usersTablePos := strings.Index(formatted, "- users (id=")
	if accountsTablePos == -1 || usersTablePos == -1 || accountsTablePos >= usersTablePos {
		t.Fatalf("EngineSnapshot().String() table order incorrect: %q", formatted)
	}

	accountsEmailPos := strings.Index(formatted, "- accounts.idx_accounts_email (id=")
	accountsIDPos := strings.Index(formatted, "- accounts.idx_accounts_id (id=")
	usersIndexPos := strings.Index(formatted, "- users.idx_users_name (id=")
	if accountsEmailPos == -1 || accountsIDPos == -1 || usersIndexPos == -1 {
		t.Fatalf("EngineSnapshot().String() missing expected index lines: %q", formatted)
	}
	if !(accountsEmailPos < accountsIDPos && accountsIDPos < usersIndexPos) {
		t.Fatalf("EngineSnapshot().String() index order incorrect: %q", formatted)
	}
}

func TestEngineReportMatchesSnapshotStringOnFreshDB(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() error = %v", err)
	}
	report, err := db.EngineReport()
	if err != nil {
		t.Fatalf("EngineReport() error = %v", err)
	}
	if report != snapshot.String() {
		t.Fatalf("EngineReport() = %q, want %q", report, snapshot.String())
	}
}

func TestEngineReportReflectsUpdatedState(t *testing.T) {
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
	report, err := db.EngineReport()
	if err != nil {
		t.Fatalf("EngineReport() after create error = %v", err)
	}
	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() after create error = %v", err)
	}
	if report != snapshot.String() {
		t.Fatalf("EngineReport() after create = %q, want %q", report, snapshot.String())
	}

	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}
	report, err = db.EngineReport()
	if err != nil {
		t.Fatalf("EngineReport() after drop error = %v", err)
	}
	snapshot, err = db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() after drop error = %v", err)
	}
	if report != snapshot.String() {
		t.Fatalf("EngineReport() after drop = %q, want %q", report, snapshot.String())
	}
}

func TestEngineReportOnClosedDBReturnsErrClosed(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = db.EngineReport()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("EngineReport() error = %v, want ErrClosed", err)
	}
}
