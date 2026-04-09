package rovadb

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestInsertPersistsRowsToOwnedDataPage(t *testing.T) {
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
	dataPageIDs, err := committedTableDataPageIDs(db.pool, table)
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	if len(dataPageIDs) != 1 {
		t.Fatalf("len(committedTableDataPageIDs()) = %d, want 1", len(dataPageIDs))
	}
	dataPageID := dataPageIDs[0]
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
	page, err := pager.Get(dataPageID)
	if err != nil {
		t.Fatalf("pager.Get() error = %v", err)
	}
	if err := storage.ValidateOwnedDataPage(page.Data(), table.TableID); err != nil {
		t.Fatalf("storage.ValidateOwnedDataPage() error = %v", err)
	}
	if got := storage.TablePageRowCount(page); got != 2 {
		t.Fatalf("storage.TablePageRowCount() = %d, want 2", got)
	}
	storageRows, err := storage.ReadSlottedRowsFromTablePageData(page.Data(), []uint8{
		storage.CatalogColumnTypeInt,
		storage.CatalogColumnTypeText,
	})
	if err != nil {
		t.Fatalf("storage.ReadSlottedRowsFromTablePageData() error = %v", err)
	}
	rows := parserRowsFromStorage(storageRows)
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(rows))
	}
	if rows[0][0].I64 != 1 || rows[0][1].Str != "steve" {
		t.Fatalf("rows[0] = %#v, want id=1 name=steve", rows[0])
	}
	if rows[1][0].I64 != 2 || rows[1][1].Str != "bob" {
		t.Fatalf("rows[1] = %#v, want id=2 name=bob", rows[1])
	}
}
