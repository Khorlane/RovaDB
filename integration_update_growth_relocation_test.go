package rovadb

import (
	"strings"
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestUpdateThatFitsKeepsLocatorStable(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'amy')",
		"INSERT INTO users VALUES (2, 'bob')",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	before := committedLocatorsByIDForTest(t, db, "users")
	if _, err := db.Exec("UPDATE users SET name = 'ann' WHERE id = 1"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}

	after := committedLocatorsByIDForTest(t, db, "users")
	if after[1] != before[1] {
		t.Fatalf("locator after fit update = %#v, want %#v", after[1], before[1])
	}

	rows, err := db.Query("SELECT id FROM users WHERE name = 'ann'")
	if err != nil {
		t.Fatalf("Query(new indexed value) error = %v", err)
	}
	assertRowsIntSequence(t, rows, 1)
	rows.Close()

	rows, err = db.Query("SELECT id FROM users WHERE name = 'amy'")
	if err != nil {
		t.Fatalf("Query(old indexed value) error = %v", err)
	}
	assertRowsIntSequence(t, rows)
	rows.Close()
}

func TestUpdateGrowthRelocatesRowAndPreservesIndexReadsAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, note TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for id := 1; id <= 18; id++ {
		name := "filler"
		if id == 1 {
			name = "alice"
		}
		note := strings.Repeat("x", 120)
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", id, name, note); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	before := committedLocatorsByIDForTest(t, db, "users")
	oldLocator := before[1]
	bigNote := strings.Repeat("grown-row-", 220)
	if _, err := db.Exec("UPDATE users SET note = ? WHERE id = 1", bigNote); err != nil {
		t.Fatalf("Exec(growth update) error = %v", err)
	}

	after := committedLocatorsByIDForTest(t, db, "users")
	newLocator := after[1]
	if newLocator == oldLocator {
		t.Fatalf("relocated locator = %#v, want different from old locator %#v", newLocator, oldLocator)
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	if _, err := db.fetchRowByLocator(table, oldLocator); err == nil {
		t.Fatal("fetchRowByLocator(old locator) error = nil, want explicit failure")
	}
	row, err := db.fetchRowByLocator(table, newLocator)
	if err != nil {
		t.Fatalf("fetchRowByLocator(new locator) error = %v", err)
	}
	if got := row[2]; got != parser.StringValue(bigNote) {
		t.Fatalf("updated note = %#v, want %#v", got, parser.StringValue(bigNote))
	}

	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query(indexed read after relocation) error = %v", err)
	}
	assertRowsIntSequence(t, rows, 1)
	rows.Close()

	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err = db.Query("SELECT note FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query(reopen indexed read) error = %v", err)
	}
	assertRowsStringSequence(t, rows, bigNote)
	rows.Close()

	reopenedLocators := committedLocatorsByIDForTest(t, db, "users")
	if reopenedLocators[1] != newLocator {
		t.Fatalf("reopened locator = %#v, want %#v", reopenedLocators[1], newLocator)
	}
}

func TestDeleteRewriteReclaimsSupersededPhysicalPages(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, note TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	for id := 1; id <= 30; id++ {
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?)", id, strings.Repeat("payload-", 120)); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	oldSpaceMapPageIDs, oldDataPageIDs, err := committedTablePhysicalStorageInventory(db.pool, table)
	if err != nil {
		t.Fatalf("committedTablePhysicalStorageInventory(before) error = %v", err)
	}
	if len(oldDataPageIDs) < 2 {
		t.Fatalf("len(oldDataPageIDs) = %d, want at least 2", len(oldDataPageIDs))
	}

	if _, err := db.Exec("DELETE FROM users WHERE id >= 20"); err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	table = db.tables["users"]
	newSpaceMapPageIDs, newDataPageIDs, err := committedTablePhysicalStorageInventory(db.pool, table)
	if err != nil {
		t.Fatalf("committedTablePhysicalStorageInventory(after) error = %v", err)
	}
	for _, pageID := range append(append([]storage.PageID(nil), oldSpaceMapPageIDs...), oldDataPageIDs...) {
		if containsPageID(newSpaceMapPageIDs, pageID) || containsPageID(newDataPageIDs, pageID) {
			t.Fatalf("superseded page %d still present in authoritative inventory", pageID)
		}
	}
	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	rawDB, pager := openRawStorage(t, path)
	defer rawDB.Close()
	head, err := storage.ReadDirectoryFreeListHead(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	chain := freeListChainForTest(t, pager, storage.PageID(head))
	for _, pageID := range append(append([]storage.PageID(nil), oldSpaceMapPageIDs...), oldDataPageIDs...) {
		if !containsPageID(chain, pageID) {
			t.Fatalf("free list chain = %#v, want reclaimed superseded page %d present", chain, pageID)
		}
	}
}

func committedLocatorsByIDForTest(t *testing.T, db *DB, tableName string) map[int]storage.RowLocator {
	t.Helper()
	if db == nil || db.pool == nil {
		t.Fatal("committedLocatorsByIDForTest() requires open db with pool")
	}
	table := db.tables[tableName]
	if table == nil {
		t.Fatalf("db.tables[%q] = nil", tableName)
	}
	locators, rows, err := loadCommittedTableRowsAndLocators(db.pool, table)
	if err != nil {
		t.Fatalf("loadCommittedTableRowsAndLocators() error = %v", err)
	}
	if len(locators) != len(rows) {
		t.Fatalf("len(locators) = %d, len(rows) = %d", len(locators), len(rows))
	}
	byID := make(map[int]storage.RowLocator, len(rows))
	for i, row := range rows {
		if row[0].Kind != parser.ValueKindInt64 {
			t.Fatalf("row[0] = %#v, want int value", row[0])
		}
		byID[int(row[0].I64)] = locators[i]
	}
	return byID
}

func verifyPhysicalTableInventoryMatchesMetadata(t *testing.T, db *DB, tableName string) {
	t.Helper()
	table := db.tables[tableName]
	if table == nil {
		t.Fatalf("db.tables[%q] = nil", tableName)
	}
	spaceMapPageIDs, dataPageIDs, err := committedTablePhysicalStorageInventory(db.pool, table)
	if err != nil {
		t.Fatalf("committedTablePhysicalStorageInventory() error = %v", err)
	}
	if uint32(len(spaceMapPageIDs)) != table.OwnedSpaceMapPageCount() {
		t.Fatalf("len(spaceMapPageIDs) = %d, want %d", len(spaceMapPageIDs), table.OwnedSpaceMapPageCount())
	}
	if uint32(len(dataPageIDs)) != table.OwnedDataPageCount() {
		t.Fatalf("len(dataPageIDs) = %d, want %d", len(dataPageIDs), table.OwnedDataPageCount())
	}
}
