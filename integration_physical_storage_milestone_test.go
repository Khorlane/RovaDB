package rovadb

import (
	"strings"
	"testing"
)

func TestPhysicalStorageLayerMilestoneLifecycle(t *testing.T) {
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
	for id := 1; id <= 40; id++ {
		name := "user"
		if id == 7 {
			name = "target"
		}
		note := strings.Repeat("seed-", 35)
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", id, name, note); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	dataPageIDs, err := committedTableDataPageIDs(db.pool, table)
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	if len(dataPageIDs) < 2 {
		t.Fatalf("len(committedTableDataPageIDs()) = %d, want at least 2", len(dataPageIDs))
	}
	before := committedLocatorsByIDForTest(t, db, "users")[7]

	relocatedNote := strings.Repeat("relocated-", 260)
	if _, err := db.Exec("UPDATE users SET note = ? WHERE id = 7", relocatedNote); err != nil {
		t.Fatalf("Exec(relocating update) error = %v", err)
	}

	after := committedLocatorsByIDForTest(t, db, "users")[7]
	if after == before {
		t.Fatalf("relocated locator = %#v, want different from %#v", after, before)
	}
	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query("SELECT id FROM users WHERE name = 'target'")
	if err != nil {
		t.Fatalf("Query(indexed lookup) error = %v", err)
	}
	assertRowsIntSequence(t, rows, 7)
	rows.Close()

	rows, err = db.Query("SELECT note FROM users WHERE id = 7")
	if err != nil {
		t.Fatalf("Query(relocated row) error = %v", err)
	}
	assertRowsStringSequence(t, rows, relocatedNote)
	rows.Close()

	rows, err = db.Query("SELECT id FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query(full scan) error = %v", err)
	}
	wantIDs := make([]int, 0, 40)
	for id := 1; id <= 40; id++ {
		wantIDs = append(wantIDs, id)
	}
	assertRowsIntSequence(t, rows, wantIDs...)
	rows.Close()

	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() after reopen error = %v", err)
	}
}
