package rovadb

import "testing"

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
	if rows.Err() == nil || rows.Err().Error() != "execution: table not found" {
		t.Fatalf("rows.Err() = %v, want %q", rows.Err(), "execution: table not found")
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
	if rows.Err() == nil || rows.Err().Error() != "execution: table not found" {
		t.Fatalf("rows.Err() after reopen = %v, want %q", rows.Err(), "execution: table not found")
	}
}

func TestExecAPIDropTableMissingFails(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("DROP TABLE users"); err == nil || err.Error() != "execution: table not found" {
		t.Fatalf("Exec(drop missing table) error = %v, want %q", err, "execution: table not found")
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
