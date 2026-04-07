package rovadb

import (
	"encoding/binary"
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
	spaceMapPageIDs, dataPageIDs, err := committedTablePhysicalStorageInventory(db.pool, table)
	if err != nil {
		t.Fatalf("committedTablePhysicalStorageInventory() error = %v", err)
	}
	indexNames := []string{"idx_users_id", "idx_users_name"}
	indexRootPageIDs := make([]storage.PageID, 0, len(indexNames))
	for _, indexName := range indexNames {
		indexRootPageIDs = append(indexRootPageIDs, storage.PageID(table.IndexDefinition(indexName).RootPageID))
	}

	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}
	if db.freeListHead == 0 {
		t.Fatal("db.freeListHead = 0, want nonzero after drop")
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
	if head == 0 {
		t.Fatal("ReadDirectoryFreeListHead() = 0, want nonzero after drop")
	}
	chain := freeListChainForTest(t, pager, storage.PageID(head))
	wantPages := []storage.PageID{tableRootPageID, indexRootPageIDs[1], indexRootPageIDs[0], table.TableHeaderPageID()}
	wantPages = append(wantPages, spaceMapPageIDs...)
	wantPages = append(wantPages, dataPageIDs...)
	for _, pageID := range wantPages {
		if !containsPageID(chain, pageID) {
			t.Fatalf("free list chain = %#v, want page %d present", chain, pageID)
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
	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop users) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	headBeforeCreate := db.freeListHead
	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if got := db.tables["teams"].RootPageID(); got != storage.PageID(headBeforeCreate) {
		t.Fatalf("teams.RootPageID() = %d, want free-list head %d", got, headBeforeCreate)
	}
}

func TestExecAPIDropTableReopenLeavesNoGhostPhysicalOwnership(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	droppedTableID := db.tables["users"].TableID
	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop users) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	if _, ok := db.tables["users"]; ok {
		t.Fatalf("reopened db.tables[users] still present: %#v", db.tables["users"])
	}
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}
	if ghost := findPhysicalOwnershipPageForTableID(t, db, droppedTableID); ghost != 0 {
		t.Fatalf("found ghost physical ownership for dropped table id %d on page %d", droppedTableID, ghost)
	}
}

func freeListChainForTest(t *testing.T, pager *storage.Pager, head storage.PageID) []storage.PageID {
	t.Helper()
	chain := make([]storage.PageID, 0)
	seen := make(map[storage.PageID]struct{})
	for head != 0 {
		if _, exists := seen[head]; exists {
			t.Fatalf("free list cycle at %d", head)
		}
		seen[head] = struct{}{}
		chain = append(chain, head)
		page, err := pager.Get(head)
		if err != nil {
			t.Fatalf("pager.Get(%d) error = %v", head, err)
		}
		next, err := storage.FreePageNext(page.Data())
		if err != nil {
			t.Fatalf("FreePageNext(%d) error = %v", head, err)
		}
		head = storage.PageID(next)
	}
	return chain
}

func containsPageID(ids []storage.PageID, want storage.PageID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func findPhysicalOwnershipPageForTableID(t *testing.T, db *DB, tableID uint32) storage.PageID {
	t.Helper()
	if db == nil || db.pool == nil || db.pager == nil {
		t.Fatal("findPhysicalOwnershipPageForTableID() requires open db")
	}
	for pageID := storage.PageID(0); pageID < db.pager.NextPageID(); pageID++ {
		pageData, err := readCommittedPageData(db.pool, pageID)
		if err != nil {
			t.Fatalf("readCommittedPageData(%d) error = %v", pageID, err)
		}
		if err := storage.ValidatePageImage(pageData); err != nil {
			t.Fatalf("ValidatePageImage(%d) error = %v", pageID, err)
		}
		switch storage.PageType(binary.LittleEndian.Uint16(pageData[4:6])) {
		case storage.PageTypeHeader:
			role, err := storage.HeaderPageRoleValue(pageData)
			if err != nil {
				t.Fatalf("HeaderPageRoleValue(%d) error = %v", pageID, err)
			}
			if role != storage.HeaderPageRoleTable {
				continue
			}
			owner, err := storage.TableHeaderTableID(pageData)
			if err != nil {
				t.Fatalf("TableHeaderTableID(%d) error = %v", pageID, err)
			}
			if owner == tableID {
				return pageID
			}
		case storage.PageTypeSpaceMap:
			owner, err := storage.SpaceMapOwningTableID(pageData)
			if err != nil {
				t.Fatalf("SpaceMapOwningTableID(%d) error = %v", pageID, err)
			}
			if owner == tableID {
				return pageID
			}
		case storage.PageTypeTable:
			owner, err := storage.TablePageOwningTableID(pageData)
			if err != nil {
				t.Fatalf("TablePageOwningTableID(%d) error = %v", pageID, err)
			}
			if owner == tableID {
				return pageID
			}
		}
	}
	return 0
}
