package rovadb

import (
	"testing"
)

func TestMinimalUsabilityContractExampleFlow(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	assertSelectRowsWithNames(t, db, "SELECT id, name FROM users", [][2]any{
		{int64(1), "alice"},
		{int64(2), "bob"},
	})

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertSelectRowsWithNames(t, db, "SELECT id, name FROM users", [][2]any{
		{int64(1), "alice"},
		{int64(2), "bob"},
	})
}
