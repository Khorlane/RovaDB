package rovadb

import (
	"errors"
	"os"
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestInterruptedDropIndexRecoversLastCommittedIndexState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	db.afterDatabaseSyncHook = func() error {
		return errors.New("boom after db sync")
	}
	if _, err := db.Exec("DROP INDEX idx_users_name"); err == nil {
		t.Fatal("Exec(drop index) error = nil, want interrupted commit failure")
	}
	if _, err := os.Stat(storage.JournalPath(path)); err != nil {
		t.Fatalf("journal stat error = %v, want surviving journal", err)
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
		t.Fatalf("IndexDefinition(idx_users_name) = %#v, want nil after WAL recovery", table.IndexDefinition("idx_users_name"))
	}
	if len(table.Indexes) != 0 {
		t.Fatalf("table.Indexes = %#v, want no active index after WAL recovery", table.Indexes)
	}
	if _, err := os.Stat(storage.JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists after recovery", err)
	}
}

func TestDropIndexStateStaysGoneAcrossRepeatedReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
		"DROP INDEX idx_users_name",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	if table := db.tables["users"]; table == nil || table.IndexDefinition("idx_users_name") != nil || len(table.Indexes) != 0 {
		t.Fatalf("after first reopen table = %#v, want no surviving index", table)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	if table := db.tables["users"]; table == nil || table.IndexDefinition("idx_users_name") != nil || len(table.Indexes) != 0 {
		t.Fatalf("after second reopen table = %#v, want no surviving index", table)
	}
}
