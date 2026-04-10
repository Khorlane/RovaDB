package rovadb

import (
	"encoding/binary"
	"os"
	"strings"
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestCorruptedDatabaseHeaderDetected(t *testing.T) {
	path := testDBPath(t)
	if err := os.WriteFile(path, []byte("bad-header"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err := Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want non-nil")
	}
	if err.Error() != "storage: corrupted database header" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: corrupted database header")
	}
}

func TestCorruptedPageHeaderDetected(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	if _, err := f.Write([]byte{0xff}); err != nil {
		_ = f.Close()
		t.Fatalf("Write() error = %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want non-nil")
	}
	if err.Error() != "storage: corrupted page header" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: corrupted page header")
	}
}

func TestCorruptedTablePageDetected(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	dataPageIDs, err := committedTableDataPageIDs(db.pool, db.tables["users"])
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	if len(dataPageIDs) != 1 {
		t.Fatalf("len(committedTableDataPageIDs()) = %d, want 1", len(dataPageIDs))
	}
	dataPageID := dataPageIDs[0]
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := os.Remove(storage.WALPath(path)); err != nil {
		t.Fatalf("Remove(WALPath) error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	page, err := pager.Get(dataPageID)
	if err != nil {
		t.Fatalf("pager.Get(data) error = %v", err)
	}
	binary.LittleEndian.PutUint32(page.Data()[4:8], 4)
	pager.MarkDirty(page)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want non-nil")
	}
	assertErrorContainsAll(t, err, "storage: corrupted table page:", `table "users"`, "data page", "wrong owning table id")
}

func TestCorruptedRowDataDetected(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	dataPageIDs, err := committedTableDataPageIDs(db.pool, db.tables["users"])
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	if len(dataPageIDs) != 1 {
		t.Fatalf("len(committedTableDataPageIDs()) = %d, want 1", len(dataPageIDs))
	}
	dataPageID := dataPageIDs[0]
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := os.Remove(storage.WALPath(path)); err != nil {
		t.Fatalf("Remove(WALPath) error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	page, err := pager.Get(dataPageID)
	if err != nil {
		t.Fatalf("pager.Get(data) error = %v", err)
	}
	offset, _, err := storage.TablePageSlot(page.Data(), 0)
	if err != nil {
		t.Fatalf("storage.TablePageSlot() error = %v", err)
	}
	binary.LittleEndian.PutUint16(page.Data()[offset:offset+2], 2)
	pager.MarkDirty(page)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want non-nil")
	}
	if err.Error() != "storage: corrupted table page" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: corrupted table page")
	}
}

func TestCorruptedIndexMetadataDetected(t *testing.T) {
	path := testDBPath(t)

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	rootPage := pager.NewPage()
	storage.InitTableRootPage(rootPage)
	indexPage := pager.NewPage()
	copy(indexPage.Data(), storage.InitIndexLeafPage(uint32(indexPage.ID())))
	writeMalformedCatalogPageWithIDMappings(t, pager, corruptedIndexCatalogBytes(uint32(rootPage.ID())), []storage.DirectoryRootIDMapping{
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: 7, RootPageID: uint32(rootPage.ID())},
		{ObjectType: storage.DirectoryRootMappingObjectIndex, ObjectID: 9, RootPageID: uint32(indexPage.ID())},
	})

	_, err := Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want non-nil")
	}
	if err.Error() != "storage: corrupted index metadata" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: corrupted index metadata")
	}
}

func corruptedIndexCatalogBytes(_ uint32) []byte {
	buf := make([]byte, 0, storage.PageSize)
	buf = appendUint32LE(buf, 7)
	buf = appendUint32LE(buf, 1)
	buf = appendStringLE(buf, "users")
	buf = appendUint32LE(buf, 7)
	buf = appendUint32LE(buf, 0)
	buf = appendUint16LE(buf, 1)
	buf = appendStringLE(buf, "id")
	buf = append(buf, storage.CatalogColumnTypeInt)
	buf = appendUint16LE(buf, 1)
	buf = appendStringLE(buf, "idx_users_missing")
	buf = append(buf, 0)
	buf = appendUint32LE(buf, 9)
	buf = appendUint16LE(buf, 1)
	buf = appendStringLE(buf, "missing")
	buf = append(buf, 0)
	buf = append(buf, 0)
	buf = appendUint16LE(buf, 0)
	return buf
}

func TestOpenRejectsWrongOwnedDataPageCountInTableHeader(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	headerPageID := db.tables["users"].TableHeaderPageID()
	rawDB, pager := closeAndOpenRawWithoutWAL(t, path, db)
	defer rawDB.Close()

	headerPage, err := pager.Get(headerPageID)
	if err != nil {
		t.Fatalf("pager.Get(header) error = %v", err)
	}
	if err := storage.SetTableHeaderOwnedDataPageCount(headerPage.Data(), 2); err != nil {
		t.Fatalf("SetTableHeaderOwnedDataPageCount() error = %v", err)
	}
	pager.MarkDirty(headerPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err = Open(path)
	assertErrorContainsAll(t, err, "storage: corrupted header page:", `table "users"`, "owned data page count mismatch")
}

func TestOpenRejectsWrongOwnedSpaceMapPageCountInTableHeader(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	headerPageID := db.tables["users"].TableHeaderPageID()
	rawDB, pager := closeAndOpenRawWithoutWAL(t, path, db)
	defer rawDB.Close()

	headerPage, err := pager.Get(headerPageID)
	if err != nil {
		t.Fatalf("pager.Get(header) error = %v", err)
	}
	if err := storage.SetTableHeaderOwnedSpaceMapPageCount(headerPage.Data(), 2); err != nil {
		t.Fatalf("SetTableHeaderOwnedSpaceMapPageCount() error = %v", err)
	}
	pager.MarkDirty(headerPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err = Open(path)
	assertErrorContainsAll(t, err, "storage: corrupted header page:", `table "users"`, "owned space-map page count mismatch")
}

func TestOpenRejectsDuplicateDataPageIDsInSpaceMapInventory(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	table := db.tables["users"]
	spaceMapPageID := table.FirstSpaceMapPageID()
	dataPageIDs, err := committedTableDataPageIDs(db.pool, table)
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	headerPageID := table.TableHeaderPageID()
	rawDB, pager := closeAndOpenRawWithoutWAL(t, path, db)
	defer rawDB.Close()

	headerPage, err := pager.Get(headerPageID)
	if err != nil {
		t.Fatalf("pager.Get(header) error = %v", err)
	}
	if err := storage.SetTableHeaderOwnedDataPageCount(headerPage.Data(), 2); err != nil {
		t.Fatalf("SetTableHeaderOwnedDataPageCount() error = %v", err)
	}
	pager.MarkDirty(headerPage)

	spaceMapPage, err := pager.Get(spaceMapPageID)
	if err != nil {
		t.Fatalf("pager.Get(space map) error = %v", err)
	}
	if _, err := storage.AppendSpaceMapEntry(spaceMapPage.Data(), storage.SpaceMapEntry{
		DataPageID:      dataPageIDs[0],
		FreeSpaceBucket: storage.SpaceMapBucketHigh,
	}); err != nil {
		t.Fatalf("AppendSpaceMapEntry() error = %v", err)
	}
	pager.MarkDirty(spaceMapPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err = Open(path)
	assertErrorContainsAll(t, err, "storage: corrupted space map page:", `table "users"`, "duplicate data page")
}

func TestOpenRejectsSpaceMapEntryPointingAtWrongPageType(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	table := db.tables["users"]
	rawDB, pager := closeAndOpenRawWithoutWAL(t, path, db)
	defer rawDB.Close()

	spaceMapPage, err := pager.Get(table.FirstSpaceMapPageID())
	if err != nil {
		t.Fatalf("pager.Get(space map) error = %v", err)
	}
	entry, err := storage.SpaceMapPageEntry(spaceMapPage.Data(), 0)
	if err != nil {
		t.Fatalf("SpaceMapPageEntry() error = %v", err)
	}
	entry.DataPageID = table.TableHeaderPageID()
	if err := storage.UpdateSpaceMapEntry(spaceMapPage.Data(), 0, entry); err != nil {
		t.Fatalf("UpdateSpaceMapEntry() error = %v", err)
	}
	pager.MarkDirty(spaceMapPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err = Open(path)
	assertErrorContainsAll(t, err, "storage: corrupted table page:", `table "users"`, "data page", "wrong owning table id")
}

func TestOpenRejectsReferencedDataPageWithWrongOwningTableID(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	table := db.tables["users"]
	dataPageIDs, err := committedTableDataPageIDs(db.pool, table)
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	rawDB, pager := closeAndOpenRawWithoutWAL(t, path, db)
	defer rawDB.Close()

	dataPage, err := pager.Get(dataPageIDs[0])
	if err != nil {
		t.Fatalf("pager.Get(data) error = %v", err)
	}
	clear(dataPage.Data())
	copy(dataPage.Data(), storage.InitOwnedDataPage(uint32(dataPageIDs[0]), table.TableID+99))
	pager.MarkDirty(dataPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err = Open(path)
	assertErrorContainsAll(t, err, "storage: corrupted table page:", `table "users"`, "data page", "wrong owning table id")
}

func TestOpenRejectsBrokenSpaceMapChain(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	table := db.tables["users"]
	rawDB, pager := closeAndOpenRawWithoutWAL(t, path, db)
	defer rawDB.Close()

	secondSpaceMap := pager.NewPage()
	clear(secondSpaceMap.Data())
	copy(secondSpaceMap.Data(), storage.InitSpaceMapPage(uint32(secondSpaceMap.ID()), table.TableID))

	firstPage, err := pager.Get(table.FirstSpaceMapPageID())
	if err != nil {
		t.Fatalf("pager.Get(first space map) error = %v", err)
	}
	if err := storage.SetSpaceMapNextPageID(firstPage.Data(), uint32(secondSpaceMap.ID())); err != nil {
		t.Fatalf("SetSpaceMapNextPageID(first) error = %v", err)
	}
	if err := storage.SetSpaceMapNextPageID(secondSpaceMap.Data(), uint32(firstPage.ID())); err != nil {
		t.Fatalf("SetSpaceMapNextPageID(second) error = %v", err)
	}
	headerPage, err := pager.Get(table.TableHeaderPageID())
	if err != nil {
		t.Fatalf("pager.Get(header) error = %v", err)
	}
	if err := storage.SetTableHeaderOwnedSpaceMapPageCount(headerPage.Data(), 2); err != nil {
		t.Fatalf("SetTableHeaderOwnedSpaceMapPageCount() error = %v", err)
	}
	pager.MarkDirty(firstPage)
	pager.MarkDirty(secondSpaceMap)
	pager.MarkDirty(headerPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err = Open(path)
	assertErrorContainsAll(t, err, "storage: corrupted space map page:", `table "users"`, "space-map chain revisits page")
}

func TestOpenRejectsOrphanOwnedDataPage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	table := db.tables["users"]
	rawDB, pager := closeAndOpenRawWithoutWAL(t, path, db)
	defer rawDB.Close()

	orphan := pager.NewPage()
	clear(orphan.Data())
	copy(orphan.Data(), storage.InitOwnedDataPage(uint32(orphan.ID()), table.TableID))
	pager.MarkDirty(orphan)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err = Open(path)
	assertErrorContainsAll(t, err, "storage: corrupted table page:", "orphan owned data page", "claims table id")
}

func closeAndOpenRawWithoutWAL(t *testing.T, path string, db *DB) (*storage.DBFile, *storage.Pager) {
	t.Helper()
	if db == nil {
		t.Fatal("closeAndOpenRawWithoutWAL() requires open db")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := os.Remove(storage.WALPath(path)); err != nil {
		t.Fatalf("Remove(WALPath) error = %v", err)
	}
	return openRawStorage(t, path)
}

func assertErrorContainsAll(t *testing.T, err error, want ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("error = nil, want non-nil")
	}
	for _, part := range want {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("error %q missing %q", err.Error(), part)
		}
	}
}
