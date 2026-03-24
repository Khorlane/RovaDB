package rovadb

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestInsertPersistsRowsToTableRootPage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert 1) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (2, 'bob')"); err != nil {
		t.Fatalf("Exec(insert 2) error = %v", err)
	}

	rootPageID := db.tables["users"].RootPageID()
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[\"users\"] = nil")
	}
	if table.PersistedRowCount() == 0 {
		t.Fatal("table.PersistedRowCount() = 0, want > 0")
	}
	if table.RootPageID() != rootPageID {
		t.Fatalf("table.RootPageID() = %d, want %d", table.RootPageID(), rootPageID)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	dbFile, err := storage.OpenOrCreate(path)
	if err != nil {
		t.Fatalf("storage.OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	pager, err := storage.NewPager(dbFile.File())
	if err != nil {
		t.Fatalf("storage.NewPager() error = %v", err)
	}
	page, err := pager.Get(rootPageID)
	if err != nil {
		t.Fatalf("pager.Get() error = %v", err)
	}
	if got := storage.TablePageRowCount(page); got != 2 {
		t.Fatalf("storage.TablePageRowCount() = %d, want 2", got)
	}
}
