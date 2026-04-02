package rovadb

import (
	"sort"
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestCreateTablePersistsAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if name != "steve" {
		t.Fatalf("Scan() got %q, want %q", name, "steve")
	}
}

func TestOpenEmptyDBHasEmptyCatalog(t *testing.T) {
	path := testDBPath(t)

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

func TestCreateTableAllocatesPersistentRootPage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[\"users\"] = nil")
	}
	if table.TableID == 0 {
		t.Fatal("table.TableID = 0, want nonzero")
	}
	if table.RootPageID() < 1 {
		t.Fatalf("table.RootPageID() = %d, want >= 1", table.RootPageID())
	}
	if table.PersistedRowCount() != 0 {
		t.Fatalf("table.PersistedRowCount() = %d, want 0", table.PersistedRowCount())
	}
}

func TestCreateMultipleTablesGetDistinctRootPages(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}

	usersRoot := db.tables["users"].RootPageID()
	teamsRoot := db.tables["teams"].RootPageID()
	if usersRoot == teamsRoot {
		t.Fatalf("root page ids are equal: users=%d teams=%d", usersRoot, teamsRoot)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	users := db.tables["users"]
	teams := db.tables["teams"]
	if users == nil || teams == nil {
		t.Fatalf("reopened tables missing: users=%v teams=%v", users, teams)
	}
	if users.RootPageID() == teams.RootPageID() {
		t.Fatalf("reopened root page ids are equal: users=%d teams=%d", users.RootPageID(), teams.RootPageID())
	}
	if users.RootPageID() != usersRoot {
		t.Fatalf("users.RootPageID() = %d, want %d", users.RootPageID(), usersRoot)
	}
	if teams.RootPageID() != teamsRoot {
		t.Fatalf("teams.RootPageID() = %d, want %d", teams.RootPageID(), teamsRoot)
	}
	if users.PersistedRowCount() != 0 || teams.PersistedRowCount() != 0 {
		t.Fatalf("persisted row counts = (%d,%d), want (0,0)", users.PersistedRowCount(), teams.PersistedRowCount())
	}
	if users.TableID == 0 || teams.TableID == 0 {
		t.Fatalf("table IDs = (%d,%d), want both nonzero", users.TableID, teams.TableID)
	}
	if users.TableID == teams.TableID {
		t.Fatalf("table IDs = (%d,%d), want distinct values", users.TableID, teams.TableID)
	}
}

func TestOpenBackfillsMissingTableAndIndexIDs(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
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
	var users *storage.CatalogTable
	for i := range catalog.Tables {
		if catalog.Tables[i].Name == "users" {
			users = &catalog.Tables[i]
			break
		}
	}
	if users == nil {
		t.Fatal("catalog missing users table")
	}
	users.TableID = 0
	if len(users.Indexes) == 0 {
		t.Fatal("users catalog entry missing indexes")
	}
	users.Indexes[0].IndexID = 0
	if err := storage.SaveCatalog(pager, catalog); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	table := db.tables["users"]
	if table == nil {
		_ = db.Close()
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := table.IndexDefinition("idx_users_name")
	if table.TableID == 0 {
		_ = db.Close()
		t.Fatal("table.TableID = 0 after backfill, want nonzero")
	}
	if indexDef == nil || indexDef.IndexID == 0 {
		_ = db.Close()
		t.Fatalf("indexDef = %#v, want nonzero IndexID after backfill", indexDef)
	}
	backfilledTableID := table.TableID
	backfilledIndexID := indexDef.IndexID
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second reopen Open() error = %v", err)
	}
	defer db.Close()

	table = db.tables["users"]
	indexDef = table.IndexDefinition("idx_users_name")
	if table.TableID != backfilledTableID {
		t.Fatalf("table.TableID = %d, want %d", table.TableID, backfilledTableID)
	}
	if indexDef == nil || indexDef.IndexID != backfilledIndexID {
		t.Fatalf("indexDef.IndexID = %v, want %d", indexDef, backfilledIndexID)
	}
}

func TestOpenBootstrapsInternalSystemCatalogTables(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	wantSchemas := map[string][]string{
		systemTableTables:       {"table_id", "table_name"},
		systemTableColumns:      {"table_id", "column_name", "column_type", "ordinal_position"},
		systemTableIndexes:      {"index_id", "index_name", "table_id", "is_unique"},
		systemTableIndexColumns: {"index_id", "column_name", "ordinal_position"},
	}

	for name, wantColumns := range wantSchemas {
		table := db.tables[name]
		if table == nil {
			t.Fatalf("db.tables[%q] = nil", name)
		}
		if !table.IsSystem {
			t.Fatalf("db.tables[%q].IsSystem = false, want true", name)
		}
		if table.TableID == 0 {
			t.Fatalf("db.tables[%q].TableID = 0, want nonzero", name)
		}
		if table.RootPageID() == 0 {
			t.Fatalf("db.tables[%q].RootPageID() = 0, want nonzero", name)
		}
		if len(table.Columns) != len(wantColumns) {
			t.Fatalf("len(db.tables[%q].Columns) = %d, want %d", name, len(table.Columns), len(wantColumns))
		}
		for i, wantColumn := range wantColumns {
			if table.Columns[i].Name != wantColumn {
				t.Fatalf("db.tables[%q].Columns[%d].Name = %q, want %q", name, i, table.Columns[i].Name, wantColumn)
			}
		}
	}

	tables, err := db.ListTables()
	if err != nil {
		t.Fatalf("ListTables() error = %v", err)
	}
	if len(tables) != 0 {
		t.Fatalf("len(ListTables()) = %d, want 0 for empty user catalog", len(tables))
	}
}

func TestOpenPreservesBootstrappedInternalSystemCatalogTables(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	snapshots := make(map[string]struct {
		tableID uint32
		rootID  storage.PageID
	}, 4)
	for _, name := range []string{
		systemTableTables,
		systemTableColumns,
		systemTableIndexes,
		systemTableIndexColumns,
	} {
		table := db.tables[name]
		if table == nil {
			t.Fatalf("db.tables[%q] = nil", name)
		}
		snapshots[name] = struct {
			tableID uint32
			rootID  storage.PageID
		}{
			tableID: table.TableID,
			rootID:  table.RootPageID(),
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	for name, snapshot := range snapshots {
		table := db.tables[name]
		if table == nil {
			t.Fatalf("reopened db.tables[%q] = nil", name)
		}
		if table.TableID != snapshot.tableID {
			t.Fatalf("reopened db.tables[%q].TableID = %d, want %d", name, table.TableID, snapshot.tableID)
		}
		if table.RootPageID() != snapshot.rootID {
			t.Fatalf("reopened db.tables[%q].RootPageID() = %d, want %d", name, table.RootPageID(), snapshot.rootID)
		}
	}
}

func TestOpenBootstrapsMissingInternalSystemCatalogTablesForOlderCatalog(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	filtered := make([]storage.CatalogTable, 0, len(catalog.Tables))
	for _, table := range catalog.Tables {
		if isSystemCatalogTableName(table.Name) {
			continue
		}
		filtered = append(filtered, table)
	}
	catalog.Tables = filtered
	if err := storage.SaveCatalog(pager, catalog); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("upgrade Open() error = %v", err)
	}
	defer db.Close()

	gotNames := make([]string, 0, 4)
	for _, name := range []string{
		systemTableTables,
		systemTableColumns,
		systemTableIndexes,
		systemTableIndexColumns,
	} {
		table := db.tables[name]
		if table == nil {
			t.Fatalf("upgraded db.tables[%q] = nil", name)
		}
		if !table.IsSystem {
			t.Fatalf("upgraded db.tables[%q].IsSystem = false, want true", name)
		}
		if table.TableID == 0 || table.RootPageID() == 0 {
			t.Fatalf("upgraded db.tables[%q] has zero durable identifiers: tableID=%d rootPageID=%d", name, table.TableID, table.RootPageID())
		}
		gotNames = append(gotNames, name)
	}
	sort.Strings(gotNames)
	if len(gotNames) != 4 {
		t.Fatalf("bootstrapped system table count = %d, want 4", len(gotNames))
	}
}
