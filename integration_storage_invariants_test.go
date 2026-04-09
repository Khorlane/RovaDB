package rovadb

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestOpenRejectsInvalidPagerSizedFile(t *testing.T) {
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
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	if _, err := f.Write([]byte{0xff}); err != nil {
		_ = f.Close()
		t.Fatalf("f.Write() error = %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("f.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want non-nil")
	}
}

func TestOpenRejectsDuplicateRootPageIDs(t *testing.T) {
	path := testDBPath(t)

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	writeMalformedCatalogPageWithIDMappings(t, pager, currentCatalogBytesForTest([]currentCatalogTableForTest{
		{
			name:    "users",
			tableID: 1,
			columns: []currentCatalogColumnForTest{{name: "id", typ: storage.CatalogColumnTypeInt}},
		},
		{
			name:    "teams",
			tableID: 2,
			columns: []currentCatalogColumnForTest{{name: "id", typ: storage.CatalogColumnTypeInt}},
		},
	}), []storage.DirectoryRootIDMapping{
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: 1, RootPageID: 1},
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: 2, RootPageID: 1},
	})

	db, err := Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want non-nil")
	}
}

func TestOpenRejectsInvalidRootPageIDZero(t *testing.T) {
	path := testDBPath(t)

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	writeMalformedCatalogPage(t, pager, malformedCatalogBytes([]malformedCatalogTable{
		{
			name:       "users",
			rootPageID: 0,
			rowCount:   0,
			columns: []malformedCatalogColumn{
				{name: "id", typ: storage.CatalogColumnTypeInt},
			},
		},
	}))

	db, err := Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want non-nil")
	}
}

func TestOpenRejectsPersistedRowCountMismatch(t *testing.T) {
	path := testDBPath(t)

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	rootPage := pager.NewPage()
	storage.InitTableRootPage(rootPage)
	row, err := storage.EncodeRow(storageValuesFromParser([]parser.Value{parser.Int64Value(1), parser.StringValue("steve")}))
	if err != nil {
		t.Fatalf("EncodeRow() error = %v", err)
	}
	if err := storage.AppendRowToTablePage(rootPage, row); err != nil {
		t.Fatalf("AppendRowToTablePage() error = %v", err)
	}
	writeMalformedCatalogPageWithIDMappings(t, pager, currentCatalogBytesForTest([]currentCatalogTableForTest{
		{
			name:     "users",
			tableID:  1,
			rowCount: 2,
			columns: []currentCatalogColumnForTest{
				{name: "id", typ: storage.CatalogColumnTypeInt},
				{name: "name", typ: storage.CatalogColumnTypeText},
			},
		},
	}), []storage.DirectoryRootIDMapping{
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: 1, RootPageID: uint32(rootPage.ID())},
	})

	db, err := Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want non-nil")
	}
}

func TestCloseReopenRoundTripFinal(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert 1) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (2, 'bob')"); err != nil {
		t.Fatalf("Exec(insert 2) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (3, 'carol')"); err != nil {
		t.Fatalf("Exec(insert 3) error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET name = 'bobby' WHERE id = 2"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if _, err := db.Exec("DELETE FROM users WHERE id = 1"); err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	if err := assertFinalUsersState(db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second reopen Open() error = %v", err)
	}
	defer db.Close()
	if err := assertFinalUsersState(db); err != nil {
		t.Fatal(err)
	}
}

func assertFinalUsersState(db *DB) error {
	rows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		return errInvalidArgumentForTest("missing first row")
	}
	var id1 int
	var name1 string
	if err := rows.Scan(&id1, &name1); err != nil {
		return err
	}
	if id1 != 2 || name1 != "bobby" {
		return errInvalidArgumentForTest("first row mismatch")
	}

	if !rows.Next() {
		return errInvalidArgumentForTest("missing second row")
	}
	var id2 int
	var name2 string
	if err := rows.Scan(&id2, &name2); err != nil {
		return err
	}
	if id2 != 3 || name2 != "carol" {
		return errInvalidArgumentForTest("second row mismatch")
	}
	if rows.Next() {
		return errInvalidArgumentForTest("unexpected extra row")
	}
	return nil
}

type malformedCatalogTable struct {
	name       string
	rootPageID uint32
	rowCount   uint32
	columns    []malformedCatalogColumn
}

type malformedCatalogColumn struct {
	name string
	typ  uint8
}

func malformedCatalogBytes(tables []malformedCatalogTable) []byte {
	buf := make([]byte, 0, storage.PageSize)
	buf = appendUint32LE(buf, 1)
	buf = appendUint32LE(buf, uint32(len(tables)))
	for _, table := range tables {
		buf = appendStringLE(buf, table.name)
		buf = appendUint32LE(buf, table.rootPageID)
		buf = appendUint32LE(buf, table.rowCount)
		buf = appendUint16LE(buf, uint16(len(table.columns)))
		for _, column := range table.columns {
			buf = appendStringLE(buf, column.name)
			buf = append(buf, column.typ)
		}
	}
	return buf
}

func appendUint32LE(buf []byte, v uint32) []byte {
	var raw [4]byte
	binary.LittleEndian.PutUint32(raw[:], v)
	return append(buf, raw[:]...)
}

func appendUint16LE(buf []byte, v uint16) []byte {
	var raw [2]byte
	binary.LittleEndian.PutUint16(raw[:], v)
	return append(buf, raw[:]...)
}

func appendStringLE(buf []byte, s string) []byte {
	buf = appendUint16LE(buf, uint16(len(s)))
	return append(buf, s...)
}
