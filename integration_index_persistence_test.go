package rovadb

import (
	"encoding/binary"
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
	if err := db.defineLegacyBasicIndex("users", "name"); err != nil {
		t.Fatalf("defineLegacyBasicIndex() error = %v", err)
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
	if err := db.defineLegacyBasicIndex("users", "id"); err != nil {
		t.Fatalf("defineLegacyBasicIndex() error = %v", err)
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
	if len(catalog.Tables) != 1 || len(catalog.Tables[0].Indexes) != 1 {
		t.Fatalf("catalog.Tables = %#v, want one persisted users index", catalog.Tables)
	}
	index := catalog.Tables[0].Indexes[0]
	if index.Name != "id" || index.Unique || len(index.Columns) != 1 || index.Columns[0].Name != "id" || index.Columns[0].Desc {
		t.Fatalf("catalog.Tables[0].Indexes[0] = %#v, want named single-column ASC non-unique id index", index)
	}
	if index.RootPageID == 0 {
		t.Fatalf("catalog.Tables[0].Indexes[0].RootPageID = 0, want nonzero")
	}
	rootPage, err := pager.Get(storage.PageID(index.RootPageID))
	if err != nil {
		t.Fatalf("pager.Get(index root) error = %v", err)
	}
	if got := storage.PageType(binary.LittleEndian.Uint16(rootPage.Data()[4:6])); got != storage.PageTypeIndexLeaf {
		t.Fatalf("index root page type = %d, want %d", got, storage.PageTypeIndexLeaf)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := table.IndexDefinition("id")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(id) = nil, defs=%#v", table.IndexDefs)
	}
	if indexDef.RootPageID != index.RootPageID {
		t.Fatalf("IndexDefinition(id).RootPageID = %d, want %d", indexDef.RootPageID, index.RootPageID)
	}
	if basic := table.Indexes["id"]; basic == nil {
		t.Fatal("table.Indexes[id] = nil")
	} else if basic.RootPageID != index.RootPageID {
		t.Fatalf("table.Indexes[id].RootPageID = %d, want %d", basic.RootPageID, index.RootPageID)
	}
}

func TestOpenRetainsUnsupportedIndexDefinitionsWithoutActivatingBasicIndex(t *testing.T) {
	path := testDBPath(t)

	dbFile, pager := openRawStorage(t, path)
	rootPage := pager.NewPage()
	storage.InitTableRootPage(rootPage)
	if err := storage.SaveCatalog(pager, &storage.CatalogData{
		Tables: []storage.CatalogTable{
			{
				Name:       "users",
				RootPageID: uint32(rootPage.ID()),
				RowCount:   0,
				Columns: []storage.CatalogColumn{
					{Name: "id", Type: storage.CatalogColumnTypeInt},
					{Name: "name", Type: storage.CatalogColumnTypeText},
				},
				Indexes: []storage.CatalogIndex{
					{
						Name:   "idx_users_id_name",
						Unique: true,
						Columns: []storage.CatalogIndexColumn{
							{Name: "id"},
							{Name: "name", Desc: true},
						},
					},
				},
			},
		},
	}); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	if len(table.IndexDefs) != 1 || table.IndexDefs[0].Name != "idx_users_id_name" {
		t.Fatalf("table.IndexDefs = %#v, want retained rich index definition", table.IndexDefs)
	}
	if len(table.Indexes) != 0 {
		t.Fatalf("table.Indexes = %#v, want no active BasicIndex for unsupported definition", table.Indexes)
	}
}
