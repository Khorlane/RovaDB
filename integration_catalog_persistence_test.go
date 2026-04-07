package rovadb

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
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

func TestOpenFailsWhenCurrentCatalogIDsAreMissing(t *testing.T) {
	path := testDBPath(t)

	rawDB, pager := openRawStorage(t, path)
	writeMalformedCatalogPage(t, pager, currentCatalogBytesForTest([]currentCatalogTableForTest{
		{
			name:    "users",
			tableID: 0,
			columns: []currentCatalogColumnForTest{
				{name: "id", typ: storage.CatalogColumnTypeInt},
				{name: "name", typ: storage.CatalogColumnTypeText},
			},
			indexes: []currentCatalogIndexForTest{
				{name: "idx_users_name", indexID: 9, columns: []currentCatalogIndexColumnForTest{{name: "name"}}},
			},
		},
	}))
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err := Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("reopen Open() error = nil, want zero durable table ID rejection")
	}
}

func TestOpenRejectsZeroDurableIndexID(t *testing.T) {
	path := testDBPath(t)

	rawDB, pager := openRawStorage(t, path)
	writeMalformedCatalogPage(t, pager, currentCatalogBytesForTest([]currentCatalogTableForTest{
		{
			name:    "users",
			tableID: 7,
			columns: []currentCatalogColumnForTest{
				{name: "id", typ: storage.CatalogColumnTypeInt},
				{name: "name", typ: storage.CatalogColumnTypeText},
			},
			indexes: []currentCatalogIndexForTest{
				{name: "idx_users_name", indexID: 0, columns: []currentCatalogIndexColumnForTest{{name: "name"}}},
			},
		},
	}))
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err := Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("reopen Open() error = nil, want zero durable index ID rejection")
	}
}

func TestOpenBootstrapsInternalSystemCatalogTablesOnCurrentFormatDB(t *testing.T) {
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

func TestOpenBootstrapsMissingInternalSystemCatalogTablesOnCurrentFormatDB(t *testing.T) {
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
	catalog = catalogWithDirectoryRootsForSave(t, rawDB.File(), catalog)
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
	rewriteDirectoryRootMappingsForCatalogTables(t, rawDB.File(), catalog)
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("reopen Open() error = nil, want corrupted header page")
	}
	if !strings.Contains(err.Error(), "storage: corrupted header page:") || !strings.Contains(err.Error(), "orphan table-header page") {
		t.Fatalf("reopen Open() error = %v, want orphan table-header detail", err)
	}
}

func TestSystemCatalogRowsTrackSchemaMetadata(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	assertSystemCatalogRows(t, db,
		[][]any{{int64(db.tables["users"].TableID), "users"}},
		[][]any{
			{int64(db.tables["users"].TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(db.tables["users"].TableID), "name", parser.ColumnTypeText, int64(2)},
		},
		nil,
		nil,
	)

	if _, err := db.Exec("ALTER TABLE users ADD COLUMN active INT"); err != nil {
		t.Fatalf("Exec(alter table) error = %v", err)
	}
	assertSystemCatalogRows(t, db,
		[][]any{{int64(db.tables["users"].TableID), "users"}},
		[][]any{
			{int64(db.tables["users"].TableID), "active", parser.ColumnTypeInt, int64(3)},
			{int64(db.tables["users"].TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(db.tables["users"].TableID), "name", parser.ColumnTypeText, int64(2)},
		},
		nil,
		nil,
	)

	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create unique index) error = %v", err)
	}
	indexDef := db.tables["users"].IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatal("IndexDefinition(idx_users_name) = nil")
	}
	assertSystemCatalogRows(t, db,
		[][]any{{int64(db.tables["users"].TableID), "users"}},
		[][]any{
			{int64(db.tables["users"].TableID), "active", parser.ColumnTypeInt, int64(3)},
			{int64(db.tables["users"].TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(db.tables["users"].TableID), "name", parser.ColumnTypeText, int64(2)},
		},
		[][]any{{int64(indexDef.IndexID), "idx_users_name", int64(db.tables["users"].TableID), true}},
		[][]any{{int64(indexDef.IndexID), "name", int64(1)}},
	)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	indexDef = db.tables["users"].IndexDefinition("idx_users_name")
	assertSystemCatalogRows(t, db,
		[][]any{{int64(db.tables["users"].TableID), "users"}},
		[][]any{
			{int64(db.tables["users"].TableID), "active", parser.ColumnTypeInt, int64(3)},
			{int64(db.tables["users"].TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(db.tables["users"].TableID), "name", parser.ColumnTypeText, int64(2)},
		},
		[][]any{{int64(indexDef.IndexID), "idx_users_name", int64(db.tables["users"].TableID), true}},
		[][]any{{int64(indexDef.IndexID), "name", int64(1)}},
	)
}

func TestSystemCatalogRowsRebuildAcrossDropOperations(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	usersTable := db.tables["users"]
	teamsTable := db.tables["teams"]
	indexDef := usersTable.IndexDefinition("idx_users_name")
	if usersTable == nil || teamsTable == nil || indexDef == nil {
		t.Fatalf("schema setup failed: users=%v teams=%v index=%v", usersTable, teamsTable, indexDef)
	}

	assertSystemCatalogRows(t, db,
		[][]any{
			{int64(teamsTable.TableID), "teams"},
			{int64(usersTable.TableID), "users"},
		},
		[][]any{
			{int64(teamsTable.TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(usersTable.TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(usersTable.TableID), "name", parser.ColumnTypeText, int64(2)},
		},
		[][]any{{int64(indexDef.IndexID), "idx_users_name", int64(usersTable.TableID), false}},
		[][]any{{int64(indexDef.IndexID), "name", int64(1)}},
	)

	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	assertSystemCatalogRows(t, db,
		[][]any{
			{int64(teamsTable.TableID), "teams"},
			{int64(usersTable.TableID), "users"},
		},
		[][]any{
			{int64(teamsTable.TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(usersTable.TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(usersTable.TableID), "name", parser.ColumnTypeText, int64(2)},
		},
		nil,
		nil,
	)

	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}
	assertSystemCatalogRows(t, db,
		[][]any{{int64(teamsTable.TableID), "teams"}},
		[][]any{{int64(teamsTable.TableID), "id", parser.ColumnTypeInt, int64(1)}},
		nil,
		nil,
	)
}

func TestOpenRebuildsSystemCatalogRowsForCurrentFormatDBMissingSystemTables(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
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
	catalog = catalogWithDirectoryRootsForSave(t, rawDB.File(), catalog)
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
	rewriteDirectoryRootMappingsForCatalogTables(t, rawDB.File(), catalog)
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("reopen Open() error = nil, want corrupted header page")
	}
	if !strings.Contains(err.Error(), "storage: corrupted header page:") || !strings.Contains(err.Error(), "orphan table-header page") {
		t.Fatalf("reopen Open() error = %v, want orphan table-header detail", err)
	}
}

func assertSystemCatalogRows(t *testing.T, db *DB, wantTables, wantColumns, wantIndexes, wantIndexColumns [][]any) {
	t.Helper()
	assertSystemTableRows(t, db, systemTableTables, wantTables)
	assertSystemTableRows(t, db, systemTableColumns, wantColumns)
	assertSystemTableRows(t, db, systemTableIndexes, wantIndexes)
	assertSystemTableRows(t, db, systemTableIndexColumns, wantIndexColumns)
}

func assertSystemTableRows(t *testing.T, db *DB, tableName string, want [][]any) {
	t.Helper()

	table := db.tables[tableName]
	if table == nil {
		t.Fatalf("db.tables[%q] = nil", tableName)
	}
	rows, err := db.scanTableRows(table)
	if err != nil {
		t.Fatalf("scanTableRows(%q) error = %v", tableName, err)
	}
	got := make([][]any, 0, len(rows))
	for _, row := range rows {
		got = append(got, materializeSystemCatalogRow(row))
	}
	sortSystemCatalogRows(got)
	sortSystemCatalogRows(want)
	if len(got) != len(want) {
		t.Fatalf("%s row count = %d, want %d; got=%#v", tableName, len(got), len(want), got)
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("%s row %d width = %d, want %d; row=%#v", tableName, i, len(got[i]), len(want[i]), got[i])
		}
		for j := range want[i] {
			if got[i][j] != want[i][j] {
				t.Fatalf("%s row %d col %d = %#v, want %#v; got=%#v", tableName, i, j, got[i][j], want[i][j], got)
			}
		}
	}
}

func materializeSystemCatalogRow(row []parser.Value) []any {
	out := make([]any, 0, len(row))
	for _, value := range row {
		switch value.Kind {
		case parser.ValueKindInt64:
			out = append(out, value.I64)
		case parser.ValueKindString:
			out = append(out, value.Str)
		case parser.ValueKindBool:
			out = append(out, value.Bool)
		case parser.ValueKindNull:
			out = append(out, nil)
		default:
			out = append(out, value.Any())
		}
	}
	return out
}

func sortSystemCatalogRows(rows [][]any) {
	sort.Slice(rows, func(i, j int) bool {
		left := rows[i]
		right := rows[j]
		limit := len(left)
		if len(right) < limit {
			limit = len(right)
		}
		for k := 0; k < limit; k++ {
			ls := systemCatalogCellKey(left[k])
			rs := systemCatalogCellKey(right[k])
			if ls == rs {
				continue
			}
			return ls < rs
		}
		return len(left) < len(right)
	})
}

func systemCatalogCellKey(value any) string {
	switch v := value.(type) {
	case nil:
		return "0:"
	case bool:
		if v {
			return "1:1"
		}
		return "1:0"
	case int64:
		return fmt.Sprintf("2:%020d", v)
	case string:
		return "3:" + v
	default:
		return "4:"
	}
}

func catalogWithDirectoryRootsForSave(t *testing.T, file *os.File, catalog *storage.CatalogData) *storage.CatalogData {
	t.Helper()
	if catalog == nil {
		return nil
	}
	idMappings, err := storage.ReadDirectoryRootIDMappings(file)
	if err != nil {
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	applied, err := storage.ApplyDirectoryRootIDMappings(catalog, idMappings)
	if err != nil {
		t.Fatalf("ApplyDirectoryRootIDMappings() error = %v", err)
	}
	return applied
}

func rewriteDirectoryRootMappingsForCatalogTables(t *testing.T, file *os.File, catalog *storage.CatalogData) {
	t.Helper()
	if file == nil || catalog == nil {
		t.Fatal("rewriteDirectoryRootMappingsForCatalogTables() requires file and catalog")
	}
	tableIDs := make(map[uint32]struct{}, len(catalog.Tables))
	indexIDs := make(map[uint32]struct{})
	for _, table := range catalog.Tables {
		if table.TableID != 0 {
			tableIDs[table.TableID] = struct{}{}
		}
		for _, index := range table.Indexes {
			if index.IndexID != 0 {
				indexIDs[index.IndexID] = struct{}{}
			}
		}
	}

	idMappings, err := storage.ReadDirectoryRootIDMappings(file)
	if err != nil {
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	filtered := make([]storage.DirectoryRootIDMapping, 0, len(idMappings))
	for _, mapping := range idMappings {
		switch mapping.ObjectType {
		case storage.DirectoryRootMappingObjectTable, storage.DirectoryRootMappingObjectTableHeader:
			if _, ok := tableIDs[mapping.ObjectID]; ok {
				filtered = append(filtered, mapping)
			}
		case storage.DirectoryRootMappingObjectIndex:
			if _, ok := indexIDs[mapping.ObjectID]; ok {
				filtered = append(filtered, mapping)
			}
		}
	}
	if err := storage.WriteDirectoryRootIDMappings(file, filtered); err != nil {
		t.Fatalf("WriteDirectoryRootIDMappings() error = %v", err)
	}
}

type currentCatalogTableForTest struct {
	name     string
	tableID  uint32
	rowCount uint32
	columns  []currentCatalogColumnForTest
	indexes  []currentCatalogIndexForTest
}

type currentCatalogColumnForTest struct {
	name string
	typ  uint8
}

type currentCatalogIndexForTest struct {
	name    string
	indexID uint32
	unique  bool
	columns []currentCatalogIndexColumnForTest
}

type currentCatalogIndexColumnForTest struct {
	name string
	desc bool
}

func currentCatalogBytesForTest(tables []currentCatalogTableForTest) []byte {
	buf := make([]byte, 0, storage.PageSize)
	buf = appendUint32LE(buf, 6)
	buf = appendUint32LE(buf, uint32(len(tables)))
	for _, table := range tables {
		buf = appendStringLE(buf, table.name)
		buf = appendUint32LE(buf, table.tableID)
		buf = appendUint32LE(buf, table.rowCount)
		buf = appendUint16LE(buf, uint16(len(table.columns)))
		for _, column := range table.columns {
			buf = appendStringLE(buf, column.name)
			buf = append(buf, column.typ)
		}
		buf = appendUint16LE(buf, uint16(len(table.indexes)))
		for _, index := range table.indexes {
			buf = appendStringLE(buf, index.name)
			if index.unique {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
			buf = appendUint32LE(buf, index.indexID)
			buf = appendUint16LE(buf, uint16(len(index.columns)))
			for _, column := range index.columns {
				buf = appendStringLE(buf, column.name)
				if column.desc {
					buf = append(buf, 1)
				} else {
					buf = append(buf, 0)
				}
			}
		}
	}
	return buf
}
