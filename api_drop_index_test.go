package rovadb

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestExecAPIDropIndexRemovesDefinitionAndPlannerUse(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	stmt, ok := parser.ParseSelectExpr("SELECT id FROM users WHERE name = 'alice'")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	plan, err := planner.PlanSelect(stmt, plannerTableMetadata(db.tables))
	if err != nil {
		t.Fatalf("PlanSelect(before drop) error = %v", err)
	}
	if plan.ScanType != planner.ScanTypeIndex || plan.IndexScan == nil {
		t.Fatalf("plan before drop = %#v, want index scan", plan)
	}

	result, err := db.Exec("DROP INDEX idx_users_name")
	if err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	if result.RowsAffected() != 0 {
		t.Fatalf("RowsAffected() = %d, want 0", result.RowsAffected())
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	if table.IndexDefinition("idx_users_name") != nil {
		t.Fatalf("IndexDefinition(idx_users_name) = %#v, want nil", table.IndexDefinition("idx_users_name"))
	}
	plan, err = planner.PlanSelect(stmt, plannerTableMetadata(db.tables))
	if err != nil {
		t.Fatalf("PlanSelect(after drop) error = %v", err)
	}
	if plan.ScanType != planner.ScanTypeTable || plan.IndexScan != nil {
		t.Fatalf("plan after drop = %#v, want table scan fallback", plan)
	}
}

func TestExecAPIDropIndexMissingFails(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}

	if _, err := db.Exec("DROP INDEX idx_users_name"); err == nil || err.Error() != "execution: index not found" {
		t.Fatalf("Exec(drop missing index) error = %v, want %q", err, "execution: index not found")
	}
}

func TestExecAPIDropIndexLeavesOtherIndexesIntactAndPersistsAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT, score INT)",
		"CREATE INDEX idx_users_name ON users (name)",
		"CREATE INDEX idx_users_name_score ON users (name, score DESC)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	if table.IndexDefinition("idx_users_name") != nil {
		t.Fatalf("IndexDefinition(idx_users_name) = %#v, want nil", table.IndexDefinition("idx_users_name"))
	}
	if table.IndexDefinition("idx_users_name_score") == nil {
		t.Fatalf("IndexDefinition(idx_users_name_score) = nil, want non-nil (defs=%#v)", table.IndexDefs)
	}
}

func TestExecAPIDropIndexFreesRootPageAndReusesIt(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
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
	if db.freeListHead == 0 {
		t.Fatal("db.freeListHead = 0, want nonzero after drop")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	freePage, err := pager.Get(droppedRootPageID)
	if err != nil {
		t.Fatalf("pager.Get(%d) error = %v", droppedRootPageID, err)
	}
	if _, err := storage.FreePageNext(freePage.Data()); err != nil {
		t.Fatalf("FreePageNext() error = %v", err)
	}
	head, err := storage.ReadDirectoryFreeListHead(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	if head != db.freeListHead {
		t.Fatalf("ReadDirectoryFreeListHead() = %d, want %d", head, db.freeListHead)
	}
	chain := freeListChainForTest(t, pager, storage.PageID(head))
	if !containsPageID(chain, droppedRootPageID) {
		t.Fatalf("free list chain = %#v, want dropped index root %d present", chain, droppedRootPageID)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	headBeforeCreate := db.freeListHead
	if _, err := db.Exec("CREATE INDEX idx_users_name_again ON users (name)"); err != nil {
		t.Fatalf("Exec(create replacement index) error = %v", err)
	}
	reusedRootPageID := db.tables["users"].IndexDefinition("idx_users_name_again").RootPageID
	if reusedRootPageID != headBeforeCreate {
		t.Fatalf("replacement index RootPageID = %d, want free-list head %d", reusedRootPageID, headBeforeCreate)
	}
}
