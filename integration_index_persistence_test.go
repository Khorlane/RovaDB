package rovadb

import (
	"bytes"
	"encoding/binary"
	"os"
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
	usersTable := findCatalogTableByName(catalog, "users")
	if usersTable == nil || len(usersTable.Indexes) != 1 {
		t.Fatalf("catalog.Tables = %#v, want one persisted users index", catalog.Tables)
	}
	index := usersTable.Indexes[0]
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

func TestInsertMaintainsPersistedIndexLeafEntries(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'bob')",
		"INSERT INTO users VALUES (2, 'alice')",
		"INSERT INTO users VALUES (3, 'alice')",
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
		t.Fatalf("IndexDefinition(idx_users_name) = nil, defs=%#v", table.IndexDefs)
	}
	rootPageID := indexDef.RootPageID
	tableRootPageID := uint32(table.RootPageID())
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	indexPage, err := pager.Get(storage.PageID(rootPageID))
	if err != nil {
		t.Fatalf("pager.Get(index root) error = %v", err)
	}
	records, err := storage.ReadIndexLeafRecords(indexPage.Data())
	if err != nil {
		t.Fatalf("ReadIndexLeafRecords() error = %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("len(records) = %d, want 3", len(records))
	}
	aliceKey, err := storage.EncodeIndexKey([]parser.Value{parser.StringValue("alice")})
	if err != nil {
		t.Fatalf("EncodeIndexKey(alice) error = %v", err)
	}
	bobKey, err := storage.EncodeIndexKey([]parser.Value{parser.StringValue("bob")})
	if err != nil {
		t.Fatalf("EncodeIndexKey(bob) error = %v", err)
	}
	if !bytes.Equal(records[0].Key, aliceKey) || records[0].Locator != (storage.RowLocator{PageID: tableRootPageID, SlotID: 1}) {
		t.Fatalf("records[0] = %#v, want alice -> (%d,1)", records[0], tableRootPageID)
	}
	if !bytes.Equal(records[1].Key, aliceKey) || records[1].Locator != (storage.RowLocator{PageID: tableRootPageID, SlotID: 2}) {
		t.Fatalf("records[1] = %#v, want alice -> (%d,2)", records[1], tableRootPageID)
	}
	if !bytes.Equal(records[2].Key, bobKey) || records[2].Locator != (storage.RowLocator{PageID: tableRootPageID, SlotID: 0}) {
		t.Fatalf("records[2] = %#v, want bob -> (%d,0)", records[2], tableRootPageID)
	}

	pageReader := func(pageID uint32) ([]byte, error) {
		return pager.ReadPage(storage.PageID(pageID))
	}
	locators, err := storage.LookupIndexExact(pageReader, rootPageID, aliceKey)
	if err != nil {
		t.Fatalf("LookupIndexExact(alice) error = %v", err)
	}
	if len(locators) != 2 || locators[0] != (storage.RowLocator{PageID: tableRootPageID, SlotID: 1}) || locators[1] != (storage.RowLocator{PageID: tableRootPageID, SlotID: 2}) {
		t.Fatalf("LookupIndexExact(alice) = %#v, want [(%d,1), (%d,2)]", locators, tableRootPageID, tableRootPageID)
	}
}

func TestFetchRowByLocatorReturnsPersistedBaseRow(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'bob')",
		"INSERT INTO users VALUES (2, 'alice')",
		"INSERT INTO users VALUES (3, 'cara')",
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
		t.Fatalf("IndexDefinition(idx_users_name) = nil, defs=%#v", table.IndexDefs)
	}
	rootPageData, err := readCommittedPageData(db.pool, storage.PageID(indexDef.RootPageID))
	if err != nil {
		t.Fatalf("readCommittedPageData(index root) error = %v", err)
	}
	records, err := storage.ReadIndexLeafRecords(rootPageData)
	if err != nil {
		t.Fatalf("ReadIndexLeafRecords() error = %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("len(records) = %d, want 3", len(records))
	}

	row, err := db.fetchRowByLocator(table, records[0].Locator)
	if err != nil {
		t.Fatalf("fetchRowByLocator() error = %v", err)
	}
	want := []parser.Value{parser.Int64Value(2), parser.StringValue("alice")}
	for i := range want {
		if row[i] != want[i] {
			t.Fatalf("row[%d] = %#v, want %#v", i, row[i], want[i])
		}
	}
}

func TestFetchRowByLocatorFromIndexLeafSurvivesReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'bob')",
		"INSERT INTO users VALUES (2, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
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
	indexDef := table.IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil, defs=%#v", table.IndexDefs)
	}
	rootPageData, err := readCommittedPageData(db.pool, storage.PageID(indexDef.RootPageID))
	if err != nil {
		t.Fatalf("readCommittedPageData(index root) error = %v", err)
	}
	records, err := storage.ReadIndexLeafRecords(rootPageData)
	if err != nil {
		t.Fatalf("ReadIndexLeafRecords() error = %v", err)
	}
	aliceKey, err := storage.EncodeIndexKey([]parser.Value{parser.StringValue("alice")})
	if err != nil {
		t.Fatalf("EncodeIndexKey(alice) error = %v", err)
	}
	var locator storage.RowLocator
	found := false
	for _, record := range records {
		if bytes.Equal(record.Key, aliceKey) {
			locator = record.Locator
			found = true
			break
		}
	}
	if !found {
		t.Fatal("alice locator not found in index leaf records")
	}

	row, err := db.fetchRowByLocator(table, locator)
	if err != nil {
		t.Fatalf("fetchRowByLocator() error = %v", err)
	}
	want := []parser.Value{parser.Int64Value(2), parser.StringValue("alice")}
	for i := range want {
		if row[i] != want[i] {
			t.Fatalf("row[%d] = %#v, want %#v", i, row[i], want[i])
		}
	}
}

func TestIndexedCountStarLookupSurvivesReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'alice')",
		"INSERT INTO users VALUES (3, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	db.tables["users"].Indexes["name"].Entries = nil

	rows, err := db.Query("SELECT COUNT(*) FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 2)
}

func TestDeleteRebuildsPersistedIndexEntriesAndSurvivesReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, age INT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice', 10)",
		"INSERT INTO users VALUES (2, 'bob', 20)",
		"INSERT INTO users VALUES (3, 'cara', 30)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("DELETE FROM users WHERE id = 2"); err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}

	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("bob")}, nil)
	rows := assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("alice")}, [][]parser.Value{
		{parser.Int64Value(1), parser.StringValue("alice"), parser.Int64Value(10)},
	})
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1", len(rows))
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("bob")}, nil)
	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("cara")}, [][]parser.Value{
		{parser.Int64Value(3), parser.StringValue("cara"), parser.Int64Value(30)},
	})
}

func TestUpdateIndexedColumnRebuildsPersistedIndexEntriesAndSurvivesReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, age INT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice', 10)",
		"INSERT INTO users VALUES (2, 'bob', 20)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("UPDATE users SET name = 'zoe' WHERE id = 2"); err != nil {
		t.Fatalf("Exec(update indexed column) error = %v", err)
	}

	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("bob")}, nil)
	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("zoe")}, [][]parser.Value{
		{parser.Int64Value(2), parser.StringValue("zoe"), parser.Int64Value(20)},
	})

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("bob")}, nil)
	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("zoe")}, [][]parser.Value{
		{parser.Int64Value(2), parser.StringValue("zoe"), parser.Int64Value(20)},
	})
}

func TestUpdateNonIndexedColumnPreservesIndexMembership(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, active BOOL)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice', false)",
		"INSERT INTO users VALUES (2, 'bob', true)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("UPDATE users SET active = true WHERE id = 1"); err != nil {
		t.Fatalf("Exec(update non-indexed column) error = %v", err)
	}

	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("alice")}, [][]parser.Value{
		{parser.Int64Value(1), parser.StringValue("alice"), parser.BoolValue(true)},
	})
	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("bob")}, [][]parser.Value{
		{parser.Int64Value(2), parser.StringValue("bob"), parser.BoolValue(true)},
	})
}

func TestInsertMaintainsIndexAcrossRootSplitAndReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := table.IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil, defs=%#v", table.IndexDefs)
	}
	initialRootPageID := indexDef.RootPageID

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	usersTable := findCatalogTableByName(catalog, "users")
	if usersTable == nil || len(usersTable.Indexes) != 1 {
		t.Fatalf("catalog = %#v, want one table with one index", catalog)
	}

	leafPageIDs := make([]uint32, 0, 7)
	for i := 0; i < 7; i++ {
		page := pager.NewPage()
		leafPageIDs = append(leafPageIDs, uint32(page.ID()))
	}

	insertedValue := string(bytes.Repeat([]byte("z"), 512))
	insertedKey, err := storage.EncodeIndexKey([]parser.Value{parser.StringValue(insertedValue)})
	if err != nil {
		t.Fatalf("EncodeIndexKey(insertedValue) error = %v", err)
	}

	separatorKeys := make([][]byte, 0, 6)
	for i := 0; i < 6; i++ {
		value := string(bytes.Repeat([]byte{byte('b' + i)}, 512))
		encodedKey, err := storage.EncodeIndexKey([]parser.Value{parser.StringValue(value)})
		if err != nil {
			t.Fatalf("EncodeIndexKey(separator %d) error = %v", i, err)
		}
		separatorKeys = append(separatorKeys, encodedKey)
	}

	for i, pageID := range leafPageIDs {
		records := make([]storage.IndexLeafRecord, 0)
		if i == len(leafPageIDs)-1 {
			for j := 0; j < 7; j++ {
				records = append(records, storage.IndexLeafRecord{
					Key:     append([]byte(nil), insertedKey...),
					Locator: storage.RowLocator{PageID: uint32(table.RootPageID()), SlotID: uint16(j)},
				})
			}
		} else {
			records = append(records, storage.IndexLeafRecord{
				Key:     append([]byte(nil), separatorKeys[i]...),
				Locator: storage.RowLocator{PageID: uint32(table.RootPageID()), SlotID: uint16(i)},
			})
		}
		var rightSibling uint32
		if i+1 < len(leafPageIDs) {
			rightSibling = leafPageIDs[i+1]
		}
		pageData, err := storage.BuildIndexLeafPageData(pageID, records, rightSibling)
		if err != nil {
			t.Fatalf("BuildIndexLeafPageData(%d) error = %v", pageID, err)
		}
		page, err := pager.Get(storage.PageID(pageID))
		if err != nil {
			t.Fatalf("pager.Get(leaf %d) error = %v", pageID, err)
		}
		pager.MarkDirtyWithOriginal(page)
		clear(page.Data())
		copy(page.Data(), pageData)
	}

	rootPageData, err := storage.BuildIndexInternalPageData(indexDef.RootPageID, []storage.IndexInternalRecord{
		{Key: append([]byte(nil), separatorKeys[0]...), ChildPageID: leafPageIDs[0]},
		{Key: append([]byte(nil), separatorKeys[1]...), ChildPageID: leafPageIDs[1]},
		{Key: append([]byte(nil), separatorKeys[2]...), ChildPageID: leafPageIDs[2]},
		{Key: append([]byte(nil), separatorKeys[3]...), ChildPageID: leafPageIDs[3]},
		{Key: append([]byte(nil), separatorKeys[4]...), ChildPageID: leafPageIDs[4]},
		{Key: append([]byte(nil), separatorKeys[5]...), ChildPageID: leafPageIDs[5]},
		{Key: append([]byte(nil), separatorKeys[5]...), ChildPageID: leafPageIDs[6]},
	})
	if err != nil {
		t.Fatalf("BuildIndexInternalPageData(root) error = %v", err)
	}
	rootPage, err := pager.Get(storage.PageID(indexDef.RootPageID))
	if err != nil {
		t.Fatalf("pager.Get(root) error = %v", err)
	}
	pager.MarkDirtyWithOriginal(rootPage)
	clear(rootPage.Data())
	copy(rootPage.Data(), rootPageData)
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	walFrames := make([]storage.WALFrame, 0, len(leafPageIDs)+1)
	for i, pageID := range leafPageIDs {
		var pageData []byte
		var rightSibling uint32
		if i+1 < len(leafPageIDs) {
			rightSibling = leafPageIDs[i+1]
		}
		if i == len(leafPageIDs)-1 {
			records := make([]storage.IndexLeafRecord, 0, 7)
			for j := 0; j < 7; j++ {
				records = append(records, storage.IndexLeafRecord{
					Key:     append([]byte(nil), insertedKey...),
					Locator: storage.RowLocator{PageID: uint32(table.RootPageID()), SlotID: uint16(j)},
				})
			}
			pageData, err = storage.BuildIndexLeafPageData(pageID, records, rightSibling)
		} else {
			pageData, err = storage.BuildIndexLeafPageData(pageID, []storage.IndexLeafRecord{
				{Key: append([]byte(nil), separatorKeys[i]...), Locator: storage.RowLocator{PageID: uint32(table.RootPageID()), SlotID: uint16(i)}},
			}, rightSibling)
		}
		if err != nil {
			t.Fatalf("BuildIndexLeafPageData(%d for wal) error = %v", pageID, err)
		}
		walFrames = append(walFrames, stagedWALFrame(storage.PageID(pageID), pageData, uint64(1100+i)))
	}
	walFrames = append(walFrames, stagedWALFrame(storage.PageID(indexDef.RootPageID), rootPageData, 1200))
	if err := appendCommittedWALFramesForTest(path, walFrames...); err != nil {
		t.Fatalf("appendCommittedWALFramesForTest() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if _, err := db.Exec("INSERT INTO users VALUES (?)", insertedValue); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	table = db.tables["users"]
	indexDef = table.IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil after reopen insert, defs=%#v", table.IndexDefs)
	}
	if indexDef.RootPageID == initialRootPageID {
		t.Fatalf("RootPageID = %d, want changed after root split from %d", indexDef.RootPageID, initialRootPageID)
	}
	rootPageData, err = readCommittedPageData(db.pool, storage.PageID(indexDef.RootPageID))
	if err != nil {
		t.Fatalf("readCommittedPageData(root) error = %v", err)
	}
	if got := storage.PageType(binary.LittleEndian.Uint16(rootPageData[4:6])); got != storage.PageTypeIndexInternal {
		t.Fatalf("root page type = %d, want %d", got, storage.PageTypeIndexInternal)
	}
	rawDB, _ := openRawStorage(t, path)
	mappings, err := storage.ReadDirectoryRootMappings(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryRootMappings() error = %v", err)
	}
	idMappings, err := storage.ReadDirectoryRootIDMappings(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}
	if len(mappings) != 0 {
		t.Fatalf("len(ReadDirectoryRootMappings()) = %d, want 0 on new writes", len(mappings))
	}
	foundIDRootMapping := false
	for _, mapping := range idMappings {
		if mapping.ObjectType == storage.DirectoryRootMappingObjectIndex && mapping.ObjectID == indexDef.IndexID {
			foundIDRootMapping = true
			if mapping.RootPageID != indexDef.RootPageID {
				t.Fatalf("directory index ID root mapping = %d, want %d", mapping.RootPageID, indexDef.RootPageID)
			}
		}
	}
	if !foundIDRootMapping {
		t.Fatal("directory index ID root mapping not found after root split")
	}

	db.tables["users"].Indexes["name"].Entries = nil
	rows, err := db.Query("SELECT name FROM users WHERE name = ?", insertedValue)
	if err != nil {
		t.Fatalf("Query(index lookup after split) error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, insertedValue)

	pageReader := func(pageID uint32) ([]byte, error) {
		return readCommittedPageData(db.pool, storage.PageID(pageID))
	}
	locators, err := storage.LookupIndexExact(pageReader, indexDef.RootPageID, insertedKey)
	if err != nil {
		t.Fatalf("LookupIndexExact() error = %v", err)
	}
	if len(locators) != 4 {
		t.Fatalf("len(locators) = %d, want 4 from the located rightmost duplicate leaf", len(locators))
	}
	foundInsertedRow := false
	for _, locator := range locators {
		row, err := db.fetchRowByLocator(table, locator)
		if err != nil {
			continue
		}
		if len(row) == 1 && row[0] == parser.StringValue(insertedValue) {
			foundInsertedRow = true
			break
		}
	}
	if !foundInsertedRow {
		t.Fatalf("locators = %#v, want one locator resolving to inserted row %q", locators, insertedValue)
	}
}

func TestSplitIndexLookupSurvivesReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, value := range []string{"alice", "zoe"} {
		if _, err := db.Exec("INSERT INTO users VALUES (?)", value); err != nil {
			t.Fatalf("Exec(insert %q) error = %v", value, err)
		}
	}

	indexDef := db.tables["users"].IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil, defs=%#v", db.tables["users"].IndexDefs)
	}
	tableRootPageID := uint32(db.tables["users"].RootPageID())

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	leftLeafPage := pager.NewPage()
	rightLeafPage := pager.NewPage()

	aliceKey, err := storage.EncodeIndexKey([]parser.Value{parser.StringValue("alice")})
	if err != nil {
		t.Fatalf("EncodeIndexKey(alice) error = %v", err)
	}
	zoeKey, err := storage.EncodeIndexKey([]parser.Value{parser.StringValue("zoe")})
	if err != nil {
		t.Fatalf("EncodeIndexKey(zoe) error = %v", err)
	}
	leftLeafData, err := storage.BuildIndexLeafPageData(uint32(leftLeafPage.ID()), []storage.IndexLeafRecord{
		{Key: aliceKey, Locator: storage.RowLocator{PageID: tableRootPageID, SlotID: 0}},
	}, uint32(rightLeafPage.ID()))
	if err != nil {
		t.Fatalf("BuildIndexLeafPageData(left) error = %v", err)
	}
	rightLeafData, err := storage.BuildIndexLeafPageData(uint32(rightLeafPage.ID()), []storage.IndexLeafRecord{
		{Key: zoeKey, Locator: storage.RowLocator{PageID: tableRootPageID, SlotID: 1}},
	}, 0)
	if err != nil {
		t.Fatalf("BuildIndexLeafPageData(right) error = %v", err)
	}
	rootPageData, err := storage.BuildIndexInternalPageData(indexDef.RootPageID, []storage.IndexInternalRecord{
		{Key: zoeKey, ChildPageID: uint32(leftLeafPage.ID())},
		{Key: zoeKey, ChildPageID: uint32(rightLeafPage.ID())},
	})
	if err != nil {
		t.Fatalf("BuildIndexInternalPageData(root) error = %v", err)
	}
	for _, staged := range []struct {
		id   storage.PageID
		data []byte
	}{
		{id: leftLeafPage.ID(), data: leftLeafData},
		{id: rightLeafPage.ID(), data: rightLeafData},
		{id: storage.PageID(indexDef.RootPageID), data: rootPageData},
	} {
		page, err := pager.Get(staged.id)
		if err != nil {
			t.Fatalf("pager.Get(%d) error = %v", staged.id, err)
		}
		pager.MarkDirtyWithOriginal(page)
		clear(page.Data())
		copy(page.Data(), staged.data)
	}
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := appendCommittedWALFramesForTest(path,
		stagedWALFrame(leftLeafPage.ID(), leftLeafData, 1000),
		stagedWALFrame(rightLeafPage.ID(), rightLeafData, 1001),
		stagedWALFrame(storage.PageID(indexDef.RootPageID), rootPageData, 1002),
	); err != nil {
		t.Fatalf("appendCommittedWALFramesForTest() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	indexDef = table.IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil after reopen, defs=%#v", table.IndexDefs)
	}
	rootPageData, err = readCommittedPageData(db.pool, storage.PageID(indexDef.RootPageID))
	if err != nil {
		t.Fatalf("readCommittedPageData(root after reopen) error = %v", err)
	}
	if got := storage.PageType(binary.LittleEndian.Uint16(rootPageData[4:6])); got != storage.PageTypeIndexInternal {
		t.Fatalf("root page type after reopen = %d, want %d", got, storage.PageTypeIndexInternal)
	}

	pageReader := func(pageID uint32) ([]byte, error) {
		return readCommittedPageData(db.pool, storage.PageID(pageID))
	}
	for _, searchValue := range []string{"alice", "zoe"} {
		searchKey, err := storage.EncodeIndexKey([]parser.Value{parser.StringValue(searchValue)})
		if err != nil {
			t.Fatalf("EncodeIndexKey(%q) error = %v", searchValue, err)
		}
		locators, err := storage.LookupIndexExact(pageReader, indexDef.RootPageID, searchKey)
		if err != nil {
			t.Fatalf("LookupIndexExact(%q) error = %v", searchValue, err)
		}
		if len(locators) != 1 {
			t.Fatalf("len(locators) for %q = %d, want 1", searchValue, len(locators))
		}
		row, err := db.fetchRowByLocator(table, locators[0])
		if err != nil {
			t.Fatalf("fetchRowByLocator(%q) error = %v", searchValue, err)
		}
		if len(row) != 1 || row[0] != parser.StringValue(searchValue) {
			t.Fatalf("row for %q = %#v, want [%#v]", searchValue, row, parser.StringValue(searchValue))
		}
	}
}

func TestOpenFailsWhenPersistedIndexRootHasWrongPageType(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := os.Remove(storage.WALPath(path)); err != nil {
		t.Fatalf("Remove(WALPath) error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	usersTable := findCatalogTableByName(catalog, "users")
	if usersTable == nil || len(usersTable.Indexes) == 0 {
		t.Fatalf("catalog = %#v, want users table with persisted index", catalog)
	}
	rootPageID := storage.PageID(usersTable.Indexes[0].RootPageID)
	rootPage, err := pager.Get(rootPageID)
	if err != nil {
		t.Fatalf("pager.Get(index root) error = %v", err)
	}
	pager.MarkDirtyWithOriginal(rootPage)
	wrongPage := storage.InitializeTablePage(uint32(rootPageID))
	clear(rootPage.Data())
	copy(rootPage.Data(), wrongPage)
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		db.Close()
		t.Fatal("Open() error = nil, want corrupted index page")
	}
	if err.Error() != "storage: corrupted index page" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: corrupted index page")
	}
}

func findCatalogTableByName(catalog *storage.CatalogData, name string) *storage.CatalogTable {
	if catalog == nil {
		return nil
	}
	for i := range catalog.Tables {
		if catalog.Tables[i].Name == name {
			return &catalog.Tables[i]
		}
	}
	return nil
}

func appendCommittedWALFramesForTest(path string, frames ...storage.WALFrame) error {
	if len(frames) == 0 {
		return nil
	}
	var maxLSN uint64
	for _, frame := range frames {
		if err := storage.AppendWALFrame(path, frame); err != nil {
			return err
		}
		if frame.FrameLSN > maxLSN {
			maxLSN = frame.FrameLSN
		}
	}
	return storage.AppendWALCommitRecord(path, storage.WALCommitRecord{CommitLSN: maxLSN + 1})
}

func stagedWALFrame(pageID storage.PageID, pageData []byte, lsn uint64) storage.WALFrame {
	cloned := append([]byte(nil), pageData...)
	if pageID != 0 {
		if err := storage.SetPageLSN(cloned, lsn); err != nil {
			panic(err)
		}
		if err := storage.RecomputePageChecksum(cloned); err != nil {
			panic(err)
		}
	}
	var frame storage.WALFrame
	frame.FrameLSN = lsn
	frame.PageID = uint32(pageID)
	frame.PageLSN = lsn
	copy(frame.PageData[:], cloned)
	return frame
}

func assertIndexedRowLookup(t *testing.T, db *DB, tableName, indexName string, keyValues []parser.Value, wantRows [][]parser.Value) [][]parser.Value {
	t.Helper()

	table := db.tables[tableName]
	if table == nil {
		t.Fatalf("db.tables[%q] = nil", tableName)
	}
	indexDef := table.IndexDefinition(indexName)
	if indexDef == nil {
		t.Fatalf("IndexDefinition(%s) = nil, defs=%#v", indexName, table.IndexDefs)
	}
	searchKey, err := storage.EncodeIndexKey(keyValues)
	if err != nil {
		t.Fatalf("EncodeIndexKey(%#v) error = %v", keyValues, err)
	}
	pageReader := func(pageID uint32) ([]byte, error) {
		return readCommittedPageData(db.pool, storage.PageID(pageID))
	}
	locators, err := storage.LookupIndexExact(pageReader, indexDef.RootPageID, searchKey)
	if err != nil {
		t.Fatalf("LookupIndexExact(%s) error = %v", indexName, err)
	}
	rows := make([][]parser.Value, 0, len(locators))
	for _, locator := range locators {
		row, err := db.fetchRowByLocator(table, locator)
		if err != nil {
			t.Fatalf("fetchRowByLocator(%#v) error = %v", locator, err)
		}
		rows = append(rows, row)
	}
	if len(rows) != len(wantRows) {
		t.Fatalf("len(rows) = %d, want %d (rows=%#v)", len(rows), len(wantRows), rows)
	}
	for i := range wantRows {
		if len(rows[i]) != len(wantRows[i]) {
			t.Fatalf("len(rows[%d]) = %d, want %d", i, len(rows[i]), len(wantRows[i]))
		}
		for j := range wantRows[i] {
			if rows[i][j] != wantRows[i][j] {
				t.Fatalf("rows[%d][%d] = %#v, want %#v", i, j, rows[i][j], wantRows[i][j])
			}
		}
	}
	return rows
}
