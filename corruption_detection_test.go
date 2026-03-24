package rovadb

import (
	"context"
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
	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("pager.Get(1) error = %v", err)
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
	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("pager.Get(1) error = %v", err)
	}
	page.Data()[14] = 99
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
	writeMalformedCatalogPage(t, pager, corruptedIndexCatalogBytes(uint32(rootPage.ID())))

	_, err := Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want non-nil")
	}
	if err.Error() != "storage: corrupted index metadata" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: corrupted index metadata")
	}
}

func corruptedIndexCatalogBytes(rootPageID uint32) []byte {
	buf := make([]byte, 0, storage.PageSize)
	buf = appendUint32LE(buf, 2)
	buf = appendUint32LE(buf, 1)
	buf = appendStringLE(buf, "users")
	buf = appendUint32LE(buf, rootPageID)
	buf = appendUint32LE(buf, 0)
	buf = appendUint16LE(buf, 1)
	buf = appendStringLE(buf, "id")
	buf = append(buf, storage.CatalogColumnTypeInt)
	buf = appendUint16LE(buf, 1)
	buf = appendStringLE(buf, "missing")
	return buf
}
