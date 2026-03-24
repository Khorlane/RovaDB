package rovadb

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestIndexMetadataPersistsAcrossReopen(t *testing.T) {
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
		"INSERT INTO users VALUES (3, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.defineBasicIndex("users", "name"); err != nil {
		t.Fatalf("defineBasicIndex() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[\"users\"] = nil")
	}
	index := table.Indexes["name"]
	if index == nil {
		t.Fatal("table.Indexes[\"name\"] = nil")
	}
	got := index.LookupEqual(parser.StringValue("alice"))
	if len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("LookupEqual(alice) = %#v, want []int{0, 2}", got)
	}
}

func TestCatalogRoundTripPreservesIndexMetadataForOpen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.defineBasicIndex("users", "id"); err != nil {
		t.Fatalf("defineBasicIndex() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	if len(catalog.Tables) != 1 || len(catalog.Tables[0].Indexes) != 1 || catalog.Tables[0].Indexes[0].ColumnName != "id" {
		t.Fatalf("catalog.Tables = %#v, want users index on id", catalog.Tables)
	}
}
