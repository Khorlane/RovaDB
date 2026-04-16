package rovadb_test

import "path/filepath"
import "testing"

func testDBPath(t *testing.T) string {
	t.Helper()
	path := freshDBPath(t)
	db, err := Create(path)
	if err != nil {
		t.Fatalf("Create(%q) error = %v", path, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close(%q) error = %v", path, err)
	}
	return path
}

func freshDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.db")
}

func reopenDB(t *testing.T, path string) *DB {
	t.Helper()

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() after reopen error = %v", err)
	}
	return db
}
