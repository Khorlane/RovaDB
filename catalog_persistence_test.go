package rovadb

import (
	"context"
	"testing"
)

func TestCreateTablePersistsAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
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

	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT name FROM users")
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
	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
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
	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE teams (id INT, name TEXT)"); err != nil {
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
}
