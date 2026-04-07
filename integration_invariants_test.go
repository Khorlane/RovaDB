package rovadb

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
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
				TableID:    1,
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
	persistStrictPhysicalMetaForTests(t, dbFile.File(), pager, []strictTablePhysicalMetaForTest{{
		tableID:       1,
		rowRootPageID: rootPage.ID(),
	}})

	_, err = Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want non-nil")
	}
	if err.Error() != "storage: row count mismatch" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: row count mismatch")
	}
}

func TestQueryRejectsIndexScanWhenRootPageIsNotAnIndexPage(t *testing.T) {
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

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := table.IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatalf("index setup failed: indexDef=%v", indexDef)
	}
	indexDef.RootPageID = uint32(table.RootPageID())

	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "storage: corrupted index page" {
		t.Fatalf("Rows.Err() = %v, want %q", rows.Err(), "storage: corrupted index page")
	}
}

func TestQueryRejectsIndexScanWhenLogicalIndexMetadataIsIncomplete(t *testing.T) {
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

	indexDef := db.tables["users"].IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatal("IndexDefinition(idx_users_name) = nil")
	}
	indexDef.IndexID = 0

	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: invalid select plan" {
		t.Fatalf("Rows.Err() = %v, want %q", rows.Err(), "execution: invalid select plan")
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
