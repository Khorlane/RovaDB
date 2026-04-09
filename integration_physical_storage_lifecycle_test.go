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

func TestPhysicalStoragePolishMilestoneLifecycleAndDiagnostics(t *testing.T) {
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
	for id := 1; id <= 36; id++ {
		name := "bulk"
		switch id % 6 {
		case 0:
			name = "blue"
		case 1:
			name = "green"
		case 2:
			name = "red"
		}
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", id, name, strings.Repeat("payload-", 110)); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	if _, err := db.Exec("UPDATE users SET note = ? WHERE id = 1", strings.Repeat("grow-a-", 220)); err != nil {
		t.Fatalf("Exec(update relocate 1) error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET name = 'amber', note = ? WHERE id = 7", strings.Repeat("grow-b-", 210)); err != nil {
		t.Fatalf("Exec(update relocate 7) error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET name = 'violet' WHERE id = 12"); err != nil {
		t.Fatalf("Exec(update indexed value 12) error = %v", err)
	}
	if _, err := db.Exec("DELETE FROM users WHERE id = 6 OR id = 18 OR id = 24 OR id = 30 OR id = 36"); err != nil {
		t.Fatalf("Exec(delete group) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (101, 'blue', ?)", strings.Repeat("new-", 90)); err != nil {
		t.Fatalf("Exec(insert 101) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (102, 'amber', ?)", strings.Repeat("newer-", 95)); err != nil {
		t.Fatalf("Exec(insert 102) error = %v", err)
	}

	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	check, err := db.CheckEngineConsistency()
	if err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}
	if !check.OK {
		t.Fatal("CheckEngineConsistency().OK = false, want true")
	}
	if check.CheckedTableHeaders != 1 {
		t.Fatalf("CheckEngineConsistency().CheckedTableHeaders = %d, want 1", check.CheckedTableHeaders)
	}
	if check.CheckedSpaceMapPages == 0 {
		t.Fatal("CheckEngineConsistency().CheckedSpaceMapPages = 0, want > 0")
	}
	if check.CheckedDataPages < 2 {
		t.Fatalf("CheckEngineConsistency().CheckedDataPages = %d, want at least 2", check.CheckedDataPages)
	}

	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() error = %v", err)
	}
	if len(snapshot.Inventory.Tables) != 1 {
		t.Fatalf("len(EngineSnapshot().Inventory.Tables) = %d, want 1", len(snapshot.Inventory.Tables))
	}
	tableInfo := snapshot.Inventory.Tables[0]
	if !tableInfo.PhysicalMetaPresent || !tableInfo.PhysicalMetaValid || !tableInfo.PhysicalInventoryMatch {
		t.Fatalf("EngineSnapshot().Inventory.Tables[0] = %#v, want physical metadata and inventory marked valid", tableInfo)
	}
	if tableInfo.EnumeratedDataPages < 2 {
		t.Fatalf("EngineSnapshot().Inventory.Tables[0].EnumeratedDataPages = %d, want at least 2", tableInfo.EnumeratedDataPages)
	}
	report := snapshot.String()
	for _, want := range []string{"physical=ok", "space_maps=", "data_pages="} {
		if !strings.Contains(report, want) {
			t.Fatalf("EngineSnapshot().String() missing %q in %q", want, report)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	db = reopenDB(t, path)
	defer db.Close()

	checkIDs := func(label, sql string, want ...int) {
		t.Helper()
		rows, err := db.Query(sql)
		if err != nil {
			t.Fatalf("Query(%s) error = %v", label, err)
		}
		assertRowsIntSequence(t, rows, want...)
		rows.Close()
	}

	checkIDs("amber", "SELECT id FROM users WHERE name = 'amber' ORDER BY id", 7, 102)
	checkIDs("violet", "SELECT id FROM users WHERE name = 'violet' ORDER BY id", 12)
	checkIDs("green", "SELECT id FROM users WHERE name = 'green' ORDER BY id", 1, 13, 19, 25, 31)
	checkIDs("blue", "SELECT id FROM users WHERE name = 'blue' ORDER BY id", 101)

	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() after reopen error = %v", err)
	}
}
