package rovadb

import (
	"strings"
	"testing"
)

func TestExecAPIIndexedTextLimitRejectsInsertIntoIndexedTextColumn(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT, bio TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	tooLarge := strings.Repeat("a", 513)
	if _, err := db.Exec("INSERT INTO users VALUES (1, ?, ?)", tooLarge, "ok"); err == nil || err.Error() != "execution: indexed TEXT column value exceeds 512-byte limit" {
		t.Fatalf("Exec(insert oversized indexed text) error = %v, want %q", err, "execution: indexed TEXT column value exceeds 512-byte limit")
	}
}

func TestExecAPIIndexedTextLimitRejectsUpdateIntoIndexedTextColumn(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	tooLarge := strings.Repeat("b", 513)
	if _, err := db.Exec("UPDATE users SET name = ? WHERE id = 1", tooLarge); err == nil || err.Error() != "execution: indexed TEXT column value exceeds 512-byte limit" {
		t.Fatalf("Exec(update oversized indexed text) error = %v, want %q", err, "execution: indexed TEXT column value exceeds 512-byte limit")
	}
}

func TestExecAPIIndexedTextLimitAllowsOversizedNonIndexedText(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT, bio TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	tooLarge := strings.Repeat("c", 513)
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice', ?)", tooLarge); err != nil {
		t.Fatalf("Exec(insert oversized non-indexed text) error = %v", err)
	}

	rows, err := db.Query("SELECT bio FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	var got string
	if !rows.Next() {
		t.Fatal("rows.Next() = false, want true")
	}
	if err := rows.Scan(&got); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if got != tooLarge {
		t.Fatalf("bio length = %d, want %d", len(got), len(tooLarge))
	}
}
