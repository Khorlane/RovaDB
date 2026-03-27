package rovadb

import (
	"errors"
	"os"
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestInterruptedDropTableRecoversLastCommittedTableState(t *testing.T) {
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
	if _, err := db.Exec("DROP TABLE users"); err == nil {
		t.Fatal("Exec(drop table) error = nil, want interrupted commit failure")
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
		t.Fatal("db.tables[users] = nil, want restored table after recovery")
	}
	if table.IndexDefinition("idx_users_name") == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil, want restored dependent index (defs=%#v)", table.IndexDefs)
	}
	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if got := collectIntRowsFromRows(t, rows); len(got) != 1 || got[0] != 1 {
		t.Fatalf("query rows = %#v, want []int{1}", got)
	}
	if _, err := os.Stat(storage.JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists after recovery", err)
	}
}

func TestDropTableStateStaysGoneAcrossRepeatedReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"DROP TABLE users",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	if _, ok := db.tables["users"]; ok {
		t.Fatalf("after first reopen db.tables[users] = %#v, want absent", db.tables["users"])
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	if _, ok := db.tables["users"]; ok {
		t.Fatalf("after second reopen db.tables[users] = %#v, want absent", db.tables["users"])
	}
}
