package rovadb

import (
	"encoding/binary"
	"os"
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
	if err.Error() != "storage: corrupted table page" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: corrupted table page")
	}
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
	if err.Error() != "storage: corrupted row data" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: corrupted row data")
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
	buf = appendUint32LE(buf, 6)
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
	return buf
}
