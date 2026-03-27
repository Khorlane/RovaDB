package rovadb

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestOpenRejectsExactStorageRowCountMismatch(t *testing.T) {
	path := testDBPath(t)

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	rootPage := pager.NewPage()
	storage.InitTableRootPage(rootPage)
	row, err := storage.EncodeRow([]parser.Value{parser.Int64Value(1)})
	if err != nil {
		t.Fatalf("EncodeRow() error = %v", err)
	}
	if err := storage.AppendRowToTablePage(rootPage, row); err != nil {
		t.Fatalf("AppendRowToTablePage() error = %v", err)
	}
	if err := storage.SaveCatalog(pager, &storage.CatalogData{
		Tables: []storage.CatalogTable{
			{
				Name:       "users",
				RootPageID: uint32(rootPage.ID()),
				RowCount:   2,
				Columns: []storage.CatalogColumn{
					{Name: "id", Type: storage.CatalogColumnTypeInt},
				},
			},
		},
	}); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want non-nil")
	}
	if err.Error() != "storage: row count mismatch" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: row count mismatch")
	}
}

func TestQueryRejectsIndexTableMismatch(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.defineBasicIndex("users", "name"); err != nil {
		t.Fatalf("defineBasicIndex() error = %v", err)
	}

	db.tables["users"].Indexes["name"].Entries[planner.IndexKey("string:alice")] = []int{99}

	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: index/table mismatch" {
		t.Fatalf("Rows.Err() = %v, want %q", rows.Err(), "execution: index/table mismatch")
	}
}

func TestQueryRejectsLegacyIndexWithoutMatchingDefinition(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.defineBasicIndex("users", "name"); err != nil {
		t.Fatalf("defineBasicIndex() error = %v", err)
	}

	db.tables["users"].IndexDefs = nil

	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: index/table mismatch" {
		t.Fatalf("Rows.Err() = %v, want %q", rows.Err(), "execution: index/table mismatch")
	}
}

func TestQueryRejectsInvalidTransactionState(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	page := db.pager.NewPage()
	db.pager.MarkDirty(page)

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: invalid transaction state" {
		t.Fatalf("Rows.Err() = %v, want %q", rows.Err(), "execution: invalid transaction state")
	}
}
