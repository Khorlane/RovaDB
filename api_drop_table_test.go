package rovadb

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestExecAPIDropTableRemovesTableAndDependentIndexes(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
		"CREATE UNIQUE INDEX idx_users_id ON users (id)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	result, err := db.Exec("DROP TABLE users")
	if err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}
	if result.RowsAffected() != 0 {
		t.Fatalf("RowsAffected() = %d, want 0", result.RowsAffected())
	}
	if _, ok := db.tables["users"]; ok {
		t.Fatalf("db.tables[users] still present: %#v", db.tables["users"])
	}

	rows, err := db.Query("SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query(dropped table) direct error = %v, want deferred row error", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("rows.Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: table not found: users" {
		t.Fatalf("rows.Err() = %v, want %q", rows.Err(), "execution: table not found: users")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	if _, ok := db.tables["users"]; ok {
		t.Fatalf("reopened db.tables[users] still present: %#v", db.tables["users"])
	}
	rows, err = db.Query("SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query(dropped table after reopen) direct error = %v, want deferred row error", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("rows.Next() after reopen = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: table not found: users" {
		t.Fatalf("rows.Err() after reopen = %v, want %q", rows.Err(), "execution: table not found: users")
	}
}

func TestExecAPIDropTableMissingFails(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("DROP TABLE users"); err == nil || err.Error() != "execution: table not found: users" {
		t.Fatalf("Exec(drop missing table) error = %v, want %q", err, "execution: table not found: users")
	}
}

func TestExecAPIDropTableLeavesUnrelatedTablesIntact(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE teams (id INT, name TEXT)",
		"INSERT INTO teams VALUES (1, 'ops')",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM teams")
	if err != nil {
		t.Fatalf("Query(teams) error = %v", err)
	}
	defer rows.Close()
	var id int
	var name string
	if !rows.Next() {
		t.Fatal("rows.Next() = false, want true")
	}
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if id != 1 || name != "ops" {
		t.Fatalf("teams row = (%d,%q), want (1,\"ops\")", id, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v", err)
	}
}

func TestExecAPIDropTableFreesTableAndIndexRootsIntoFreeList(t *testing.T) {
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
	table := db.tables["users"]
	tableRootPageID := table.RootPageID()
	indexNames := []string{"idx_users_id", "idx_users_name"}
	indexRootPageIDs := make([]storage.PageID, 0, len(indexNames))
	for _, indexName := range indexNames {
		indexRootPageIDs = append(indexRootPageIDs, storage.PageID(table.IndexDefinition(indexName).RootPageID))
	}

	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}
	if db.freeListHead != uint32(tableRootPageID) {
		t.Fatalf("db.freeListHead = %d, want %d", db.freeListHead, tableRootPageID)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	defer rawDB.Close()

	head, err := storage.ReadDirectoryFreeListHead(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	if head != uint32(tableRootPageID) {
		t.Fatalf("ReadDirectoryFreeListHead() = %d, want %d", head, tableRootPageID)
	}

	expectedNext := []storage.PageID{indexRootPageIDs[1], indexRootPageIDs[0], table.TableHeaderPageID(), 0}
	pageIDs := []storage.PageID{tableRootPageID, indexRootPageIDs[1], indexRootPageIDs[0], table.TableHeaderPageID()}
	for i, pageID := range pageIDs {
		page, err := pager.Get(pageID)
		if err != nil {
			t.Fatalf("pager.Get(%d) error = %v", pageID, err)
		}
		nextFreePageID, err := storage.FreePageNext(page.Data())
		if err != nil {
			t.Fatalf("FreePageNext(%d) error = %v", pageID, err)
		}
		if nextFreePageID != uint32(expectedNext[i]) {
			t.Fatalf("FreePageNext(%d) = %d, want %d", pageID, nextFreePageID, expectedNext[i])
		}
	}
}

func TestExecAPIDropTableFreedRootIsReusableAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	droppedRootPageID := db.tables["users"].RootPageID()
	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop users) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if got := db.tables["teams"].RootPageID(); got != droppedRootPageID {
		t.Fatalf("teams.RootPageID() = %d, want %d", got, droppedRootPageID)
	}
}
