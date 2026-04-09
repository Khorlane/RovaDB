package rovadb

import (
	"strings"
	"testing"
)

func TestExecMutationPathsPreserveIndexedVisibilityAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT, note TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	for id := 1; id <= 18; id++ {
		name := "filler"
		if id == 1 {
			name = "alice"
		}
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", id, name, strings.Repeat("seed-", 90)); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	if _, err := db.Exec("UPDATE users SET note = ? WHERE id = 1", strings.Repeat("relocate-", 220)); err != nil {
		t.Fatalf("Exec(relocating update) error = %v", err)
	}

	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query(indexed read after update) error = %v", err)
	}
	assertRowsIntSequence(t, rows, 1)
	rows.Close()

	if _, err := db.Exec("DELETE FROM users WHERE id = 1"); err != nil {
		t.Fatalf("Exec(delete relocated row) error = %v", err)
	}

	rows, err = db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query(indexed read after delete) error = %v", err)
	}
	assertRowsIntSequence(t, rows)
	rows.Close()

	if _, err := db.Exec("INSERT INTO users VALUES (101, 'alice', ?)", strings.Repeat("fresh-", 80)); err != nil {
		t.Fatalf("Exec(reinsert reused key) error = %v", err)
	}

	rows, err = db.Query("SELECT id FROM users WHERE name = 'alice' ORDER BY id")
	if err != nil {
		t.Fatalf("Query(indexed read after key reuse) error = %v", err)
	}
	assertRowsIntSequence(t, rows, 101)
	rows.Close()

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err = db.Query("SELECT id FROM users WHERE name = 'alice' ORDER BY id")
	if err != nil {
		t.Fatalf("Query(reopen reused key) error = %v", err)
	}
	assertRowsIntSequence(t, rows, 101)
	rows.Close()
}
