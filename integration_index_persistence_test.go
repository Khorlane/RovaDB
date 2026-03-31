package rovadb

import (
	"bytes"
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
	if len(catalog.Tables) != 1 || len(catalog.Tables[0].Indexes) != 1 {
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
