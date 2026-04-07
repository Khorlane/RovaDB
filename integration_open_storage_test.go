package rovadb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestOpenCreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	_ = os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("os.Stat(%q) error = %v", path, err)
	}
	if _, err := os.Stat(storage.WALPath(path)); err != nil {
		t.Fatalf("os.Stat(%q) error = %v", storage.WALPath(path), err)
	}

	rawDB, pager := openRawStorage(t, path)
	defer rawDB.Close()

	page, err := pager.Get(storage.DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.Get(directory) error = %v", err)
	}
	if err := storage.ValidateDirectoryPage(page.Data()); err != nil {
		t.Fatalf("ValidateDirectoryPage() error = %v", err)
	}
	mode, err := storage.DirectoryCATDIRStorageMode(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIRStorageMode() error = %v", err)
	}
	if mode != storage.DirectoryCATDIRStorageModeEmbedded {
		t.Fatalf("DirectoryCATDIRStorageMode() = %d, want %d", mode, storage.DirectoryCATDIRStorageModeEmbedded)
	}
	overflowHead, err := storage.DirectoryCATDIROverflowHeadPageID(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID() error = %v", err)
	}
	if overflowHead != 0 {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID() = %d, want 0", overflowHead)
	}
	overflowCount, err := storage.DirectoryCATDIROverflowPageCount(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowPageCount() error = %v", err)
	}
	if overflowCount != 0 {
		t.Fatalf("DirectoryCATDIROverflowPageCount() = %d, want 0", overflowCount)
	}
}

func TestOpenExistingValidFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db.Close()
}

func TestOpenExistingValidFileWithValidWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db.Close()
}

func TestOpenRevalidatesDirectoryPageOnReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	page, err := pager.Get(storage.DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.Get(directory) error = %v", err)
	}
	if err := storage.ValidateDirectoryPage(page.Data()); err != nil {
		t.Fatalf("ValidateDirectoryPage() error = %v", err)
	}
	payloadBytes, err := storage.DirectoryCATDIRPayloadByteLength(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIRPayloadByteLength() error = %v", err)
	}
	payload, err := storage.LoadCatalog(storage.PageReaderFunc(func(pageID storage.PageID) ([]byte, error) {
		return page.Data(), nil
	}))
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	if payload.Version != 0 && payloadBytes == 0 {
		t.Fatalf("DirectoryCATDIRPayloadByteLength() = %d, want nonzero for non-empty wrapped payload", payloadBytes)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db.Close()
}

func TestOpenLoadsCatalogFromCATDIROverflowMode(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'ada')"); err != nil {
		t.Fatalf("Exec(insert users) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	page, err := storage.ReadDirectoryPage(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryPage() error = %v", err)
	}
	payloadBytes, err := storage.DirectoryCATDIRPayloadByteLength(page)
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("DirectoryCATDIRPayloadByteLength() error = %v", err)
	}
	payload := append([]byte(nil), page[testDirectoryCatalogOffset:testDirectoryCatalogOffset+int(payloadBytes)]...)
	mappings, err := storage.ReadDirectoryRootIDMappings(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	freeListHead, err := storage.ReadDirectoryFreeListHead(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	overflowSlot := pager.NewPage()
	overflowPages, err := storage.BuildCatalogOverflowPageChain(payload, []storage.PageID{overflowSlot.ID()})
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v", err)
	}
	clear(overflowSlot.Data())
	copy(overflowSlot.Data(), overflowPages[0].Data)
	pager.MarkDirty(overflowSlot)
	writeOverflowCatalogPageWithIDMappings(t, pager, payloadBytes, overflowPages[0].PageID, uint32(len(overflowPages)), freeListHead, mappings)
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("rows.Next() = false, want one row")
	}
	var id int
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if id != 1 || name != "ada" {
		t.Fatalf("row = (%d, %q), want (1, %q)", id, name, "ada")
	}
	if rows.Next() {
		t.Fatal("rows.Next() = true after first row, want one row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() error = %v", err)
	}
}

func TestPersistCatalogStatePromotesCATDIRToOverflowWhenNeeded(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	stagedTables := cloneTables(db.tables)
	stagedPages := make([]stagedPage, 0, 64)
	nextTableID := uint32(1000)
	nextRootPageID := db.pager.NextPageID()
	for i := 0; i < 48; i++ {
		rootPageID := nextRootPageID
		nextRootPageID++
		tableName := fmt.Sprintf("table_%03d_%s", i, strings.Repeat("x", 48))
		columns := []parser.ColumnDef{
			{Name: fmt.Sprintf("id_%02d_%s", i, strings.Repeat("a", 20)), Type: parser.ColumnTypeInt},
			{Name: fmt.Sprintf("name_%02d_%s", i, strings.Repeat("b", 20)), Type: parser.ColumnTypeText},
			{Name: fmt.Sprintf("city_%02d_%s", i, strings.Repeat("c", 20)), Type: parser.ColumnTypeText},
			{Name: fmt.Sprintf("state_%02d_%s", i, strings.Repeat("d", 20)), Type: parser.ColumnTypeText},
			{Name: fmt.Sprintf("zip_%02d_%s", i, strings.Repeat("e", 20)), Type: parser.ColumnTypeText},
		}
		table := &executor.Table{
			Name:    tableName,
			TableID: nextTableID,
			Columns: columns,
		}
		table.SetStorageMeta(rootPageID, 0)
		stagedTables[tableName] = table
		stagedPages = append(stagedPages, stagedPage{
			id:    rootPageID,
			data:  storage.InitializeTablePage(uint32(rootPageID)),
			isNew: true,
		})
		nextTableID++
	}
	if err := db.persistCatalogState(stagedTables, stagedPages); err != nil {
		t.Fatalf("persistCatalogState() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	defer rawDB.Close()
	page, err := pager.Get(storage.DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.Get(directory) error = %v", err)
	}
	mode, err := storage.DirectoryCATDIRStorageMode(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIRStorageMode() error = %v", err)
	}
	if mode != storage.DirectoryCATDIRStorageModeOverflow {
		t.Fatalf("DirectoryCATDIRStorageMode() = %d, want %d", mode, storage.DirectoryCATDIRStorageModeOverflow)
	}
	head, err := storage.DirectoryCATDIROverflowHeadPageID(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID() error = %v", err)
	}
	if head == 0 {
		t.Fatal("DirectoryCATDIROverflowHeadPageID() = 0, want nonzero")
	}
	count, err := storage.DirectoryCATDIROverflowPageCount(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowPageCount() error = %v", err)
	}
	if count == 0 {
		t.Fatal("DirectoryCATDIROverflowPageCount() = 0, want > 0")
	}

	got, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	if len(got.Tables) != len(stagedTables) {
		t.Fatalf("len(LoadCatalog().Tables) = %d, want %d", len(got.Tables), len(stagedTables))
	}
}

func TestPersistCatalogStateDemotesCATDIRBackToEmbeddedWhenPayloadFits(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	largeTables := cloneTables(db.tables)
	largePages := make([]stagedPage, 0, 64)
	nextTableID := uint32(1000)
	nextRootPageID := db.pager.NextPageID()
	for i := 0; i < 48; i++ {
		rootPageID := nextRootPageID
		nextRootPageID++
		tableName := fmt.Sprintf("table_%03d_%s", i, strings.Repeat("x", 48))
		table := &executor.Table{
			Name:    tableName,
			TableID: nextTableID,
			Columns: []parser.ColumnDef{
				{Name: fmt.Sprintf("id_%02d_%s", i, strings.Repeat("a", 20)), Type: parser.ColumnTypeInt},
				{Name: fmt.Sprintf("name_%02d_%s", i, strings.Repeat("b", 20)), Type: parser.ColumnTypeText},
				{Name: fmt.Sprintf("city_%02d_%s", i, strings.Repeat("c", 20)), Type: parser.ColumnTypeText},
				{Name: fmt.Sprintf("state_%02d_%s", i, strings.Repeat("d", 20)), Type: parser.ColumnTypeText},
				{Name: fmt.Sprintf("zip_%02d_%s", i, strings.Repeat("e", 20)), Type: parser.ColumnTypeText},
			},
		}
		table.SetStorageMeta(rootPageID, 0)
		largeTables[tableName] = table
		largePages = append(largePages, stagedPage{
			id:    rootPageID,
			data:  storage.InitializeTablePage(uint32(rootPageID)),
			isNew: true,
		})
		nextTableID++
	}
	if err := db.persistCatalogState(largeTables, largePages); err != nil {
		t.Fatalf("persistCatalogState(large) error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	page, err := pager.Get(storage.DirectoryControlPageID)
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("pager.Get(directory large) error = %v", err)
	}
	mode, err := storage.DirectoryCATDIRStorageMode(page.Data())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("DirectoryCATDIRStorageMode(large) error = %v", err)
	}
	if mode != storage.DirectoryCATDIRStorageModeOverflow {
		_ = rawDB.Close()
		t.Fatalf("DirectoryCATDIRStorageMode(large) = %d, want %d", mode, storage.DirectoryCATDIRStorageModeOverflow)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	smallTables := cloneTables(db.tables)
	if err := db.persistCatalogState(smallTables, nil); err != nil {
		t.Fatalf("persistCatalogState(small) error = %v", err)
	}

	rawDB, pager = openRawStorage(t, path)
	defer rawDB.Close()
	page, err = pager.Get(storage.DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.Get(directory small) error = %v", err)
	}
	mode, err = storage.DirectoryCATDIRStorageMode(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIRStorageMode(small) error = %v", err)
	}
	if mode != storage.DirectoryCATDIRStorageModeEmbedded {
		t.Fatalf("DirectoryCATDIRStorageMode(small) = %d, want %d", mode, storage.DirectoryCATDIRStorageModeEmbedded)
	}
	head, err := storage.DirectoryCATDIROverflowHeadPageID(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID(small) error = %v", err)
	}
	if head != 0 {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID(small) = %d, want 0", head)
	}
	count, err := storage.DirectoryCATDIROverflowPageCount(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowPageCount(small) error = %v", err)
	}
	if count != 0 {
		t.Fatalf("DirectoryCATDIROverflowPageCount(small) = %d, want 0", count)
	}
	freeListHead, err := storage.DirectoryFreeListHead(page.Data())
	if err != nil {
		t.Fatalf("DirectoryFreeListHead(small) error = %v", err)
	}
	if freeListHead == 0 {
		t.Fatal("DirectoryFreeListHead(small) = 0, want reclaimed overflow pages on free list")
	}

	got, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() after demotion error = %v", err)
	}
	if len(got.Tables) != len(smallTables) {
		t.Fatalf("len(LoadCatalog().Tables) after demotion = %d, want %d", len(got.Tables), len(smallTables))
	}
}

func TestOpenRejectsMalformedCATDIROverflowModeWithZeroHead(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	page, err := storage.ReadDirectoryPage(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryPage() error = %v", err)
	}
	binary.LittleEndian.PutUint32(page[testDirectoryCATDIRModeOffset:testDirectoryCATDIRModeOffset+4], storage.DirectoryCATDIRStorageModeOverflow)
	binary.LittleEndian.PutUint32(page[testDirectoryCATDIROverflowHeadOff:testDirectoryCATDIROverflowHeadOff+4], 0)
	binary.LittleEndian.PutUint32(page[testDirectoryCATDIROverflowCountOff:testDirectoryCATDIROverflowCountOff+4], 1)
	binary.LittleEndian.PutUint32(page[testDirectoryCATDIRPayloadBytesOff:testDirectoryCATDIRPayloadBytesOff+4], 9)
	if err := storage.RecomputePageChecksum(page); err != nil {
		_ = rawDB.Close()
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	if _, err := rawDB.File().WriteAt(page, int64(storage.HeaderSize)); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteAt() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want malformed CAT/DIR overflow state failure")
	}
}

func TestOpenRejectsMalformedCATDIROverflowChainPageType(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	page, err := storage.ReadDirectoryPage(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryPage() error = %v", err)
	}
	payloadBytes, err := storage.DirectoryCATDIRPayloadByteLength(page)
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("DirectoryCATDIRPayloadByteLength() error = %v", err)
	}
	mappings, err := storage.ReadDirectoryRootIDMappings(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	freeListHead, err := storage.ReadDirectoryFreeListHead(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	overflowPage := pager.NewPage()
	clear(overflowPage.Data())
	copy(overflowPage.Data(), storage.InitializeTablePage(uint32(overflowPage.ID())))
	pager.MarkDirty(overflowPage)
	writeOverflowCatalogPageWithIDMappings(t, pager, payloadBytes, overflowPage.ID(), 1, freeListHead, mappings)
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want malformed CAT/DIR overflow chain failure")
	}
}

func TestOpenWithHeaderOnlyWALSucceeds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := storage.ResetWALFile(path, storage.DBFormatVersion()); err != nil {
		t.Fatalf("ResetWALFile() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db.Close()
}

func TestOpenInvalidHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.db")
	if err := os.WriteFile(path, []byte("not-a-rovadb-file"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	db, err := Open(path)
	if err == nil {
		db.Close()
		t.Fatal("Open() error = nil, want non-nil")
	}
}

func TestOpenFailsOnUnsupportedDBHeaderVersion(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	var header [storage.HeaderSize]byte
	if _, err := file.ReadAt(header[:], 0); err != nil {
		_ = file.Close()
		t.Fatalf("file.ReadAt() error = %v", err)
	}
	binary.LittleEndian.PutUint32(header[8:12], storage.CurrentDBFormatVersion+1)
	if _, err := file.WriteAt(header[:], 0); err != nil {
		_ = file.Close()
		t.Fatalf("file.WriteAt() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want unsupported DB header version failure")
	}
}

func TestRecoveryOnOpenRestoresLastCommittedState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	db.afterDatabaseSyncHook = func() error {
		return errors.New("boom after db sync")
	}
	if _, err := db.Exec("UPDATE t SET id = 2 WHERE id = 1"); err == nil {
		t.Fatal("Exec(update) error = nil, want failure")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open(recover) error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 2)
	if _, err := os.Stat(storage.JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists", err)
	}
}

func TestOpenWithoutJournalSkipsRecovery(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestOpenFailsOnMalformedJournal(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := os.WriteFile(storage.JournalPath(path), []byte("bad-journal"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		db.Close()
		t.Fatal("Open() error = nil, want malformed journal error")
	}
}

func TestOpenFailsOnMalformedWAL(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := os.WriteFile(storage.WALPath(path), []byte("bad-wal"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		db.Close()
		t.Fatal("Open() error = nil, want malformed wal error")
	}
}

func TestOpenFailsOnUnsupportedDirectoryFormatVersion(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	page, err := storage.ReadDirectoryPage(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryPage() error = %v", err)
	}
	binary.LittleEndian.PutUint32(page[32:36], storage.CurrentDBFormatVersion+1)
	if _, err := rawDB.File().WriteAt(page, storage.HeaderSize); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteAt() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want unsupported directory format failure")
	}
}

func TestOpenFailsOnWALDBFormatVersionMismatch(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	walFile, err := os.OpenFile(storage.WALPath(path), os.O_RDWR|os.O_TRUNC, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	if err := storage.WriteWALHeader(walFile, storage.WALHeader{
		Magic:           [8]byte{'R', 'O', 'V', 'A', 'W', 'A', 'L', '1'},
		WALVersion:      storage.CurrentWALVersion,
		DBFormatVersion: storage.CurrentDBFormatVersion + 1,
		PageSize:        storage.PageSize,
	}); err != nil {
		_ = walFile.Close()
		t.Fatalf("WriteWALHeader() error = %v", err)
	}
	if err := walFile.Close(); err != nil {
		t.Fatalf("walFile.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want WAL/DB format mismatch failure")
	}
}

func TestOpenFailsOnMalformedDirectoryPage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	corrupted := make([]byte, storage.PageSize)
	copy(corrupted, []byte("bad-directory"))
	if _, err := file.WriteAt(corrupted, storage.HeaderSize); err != nil {
		_ = file.Close()
		t.Fatalf("file.WriteAt() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want malformed directory page error")
	}
}

func TestOpenFailsOnLegacyNameBasedDirectoryMappings(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := injectLegacyNameMappingsForOpenTest(rawDB.File(), []byte{storage.DirectoryRootMappingObjectTable, 5, 0, 'u', 's', 'e', 'r', 's', 0, 0, 2, 0, 0, 0}); err != nil {
		_ = rawDB.Close()
		t.Fatalf("injectLegacyNameMappingsForOpenTest() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("reopen Open() error = nil, want legacy directory mapping rejection")
	}
}

func TestDirectoryRootIDMappingsPersistAcrossReopen(t *testing.T) {
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
	tableID := db.tables["users"].TableID
	tableRoot := uint32(db.tables["users"].RootPageID())
	indexDef := db.tables["users"].IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatal("IndexDefinition(idx_users_name) = nil")
	}
	indexID := indexDef.IndexID
	indexRoot := indexDef.RootPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	idMappings, err := storage.ReadDirectoryRootIDMappings(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}
	foundTable := false
	foundIndex := false
	for _, mapping := range idMappings {
		switch {
		case mapping.ObjectType == storage.DirectoryRootMappingObjectTable && mapping.ObjectID == tableID:
			foundTable = true
			if mapping.RootPageID != tableRoot {
				t.Fatalf("table ID root mapping = %d, want %d", mapping.RootPageID, tableRoot)
			}
		case mapping.ObjectType == storage.DirectoryRootMappingObjectIndex && mapping.ObjectID == indexID:
			foundIndex = true
			if mapping.RootPageID != indexRoot {
				t.Fatalf("index ID root mapping = %d, want %d", mapping.RootPageID, indexRoot)
			}
		}
	}
	if !foundTable {
		t.Fatal("table ID root mapping not found")
	}
	if !foundIndex {
		t.Fatal("index ID root mapping not found")
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()
	if got := db.tables["users"].RootPageID(); uint32(got) != tableRoot {
		t.Fatalf("reopened users.RootPageID() = %d, want %d", got, tableRoot)
	}
	if got := db.tables["users"].IndexDefinition("idx_users_name").RootPageID; got != indexRoot {
		t.Fatalf("reopened index RootPageID = %d, want %d", got, indexRoot)
	}
}

func TestDirectoryWritePathWritesIDMappingsOnly(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	teamsTableID := db.tables["teams"].TableID
	teamsRoot := uint32(db.tables["teams"].RootPageID())
	usersTableID := db.tables["users"].TableID
	usersRoot := uint32(db.tables["users"].RootPageID())
	indexDef := db.tables["users"].IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatal("IndexDefinition(idx_users_name) = nil")
	}
	indexID := indexDef.IndexID
	indexRoot := indexDef.RootPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	idMappings, err := storage.ReadDirectoryRootIDMappings(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	if catalog.Version != 6 {
		t.Fatalf("catalog.Version = %d, want 6", catalog.Version)
	}
	for _, table := range catalog.Tables {
		if table.RootPageID != 0 {
			t.Fatalf("catalog table %q RootPageID = %d, want 0 on new writes", table.Name, table.RootPageID)
		}
		for _, index := range table.Indexes {
			if index.RootPageID != 0 {
				t.Fatalf("catalog index %q RootPageID = %d, want 0 on new writes", index.Name, index.RootPageID)
			}
		}
	}
	wantIDMappings := map[storage.DirectoryRootIDMapping]struct{}{
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: teamsTableID, RootPageID: teamsRoot}: {},
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: usersTableID, RootPageID: usersRoot}: {},
		{ObjectType: storage.DirectoryRootMappingObjectIndex, ObjectID: indexID, RootPageID: indexRoot}:      {},
	}
	for want := range wantIDMappings {
		found := false
		for _, got := range idMappings {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("directory ID mapping %#v not found in %#v", want, idMappings)
		}
	}
}

func TestOpenFailsWhenLegacyNameMappingsPresentAlongsideIDMappings(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := injectLegacyNameMappingsForOpenTest(rawDB.File(), []byte{
		storage.DirectoryRootMappingObjectTable, 5, 0, 'u', 's', 'e', 'r', 's', 0, 0, 2, 0, 0, 0,
	}); err != nil {
		_ = rawDB.Close()
		t.Fatalf("injectLegacyNameMappingsForOpenTest() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want legacy directory mapping rejection")
	}
}

func TestOpenFailsWhenNewCatalogPayloadHasNoDirectoryRoots(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := storage.WriteDirectoryRootIDMappings(rawDB.File(), nil); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want missing directory roots failure")
	}
}

func TestOpenFailsWithoutDirectoryRootsEvenIfCurrentCatalogSnapshotCarriesRoots(t *testing.T) {
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
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	catalog = catalogWithDirectoryRootsForSave(t, rawDB.File(), catalog)
	if err := storage.SaveCatalog(pager, catalog); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := storage.WriteDirectoryRootIDMappings(rawDB.File(), nil); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want missing directory roots failure without catalog fallback")
	}
}

func TestOpenRejectsLegacyCatalogPayloadWithoutDirectoryMappings(t *testing.T) {
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
	users := db.tables["users"]
	if users == nil {
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := users.IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatal("IndexDefinition(idx_users_name) = nil")
	}
	tableRoot := uint32(users.RootPageID())
	indexRoot := indexDef.RootPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	legacyPage := buildLegacyV5DirectoryPageForOpenTest(&storage.CatalogData{
		Tables: []storage.CatalogTable{
			{
				Name:       "users",
				TableID:    users.TableID,
				RootPageID: tableRoot,
				RowCount:   0,
				Columns: []storage.CatalogColumn{
					{Name: "name", Type: storage.CatalogColumnTypeText},
				},
				Indexes: []storage.CatalogIndex{
					{
						Name:       "idx_users_name",
						Unique:     false,
						IndexID:    indexDef.IndexID,
						RootPageID: indexRoot,
						Columns:    []storage.CatalogIndexColumn{{Name: "name"}},
					},
				},
			},
		},
	})
	if _, err := rawDB.File().WriteAt(legacyPage, int64(storage.HeaderSize)); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteAt() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("reopen Open() error = nil, want legacy catalog payload rejection")
	}
}

func TestOpenFailsWhenFreeListHeadPointsToNonFreePage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), 1); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want invalid free-list head failure")
	}
}

func TestOpenFailsWhenTableRootMappingPointsToIndexPage(t *testing.T) {
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
	tableID := db.tables["users"].TableID
	indexRoot := db.tables["users"].IndexDefinition("idx_users_name").RootPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := storage.WriteDirectoryRootIDMappings(rawDB.File(), []storage.DirectoryRootIDMapping{{
		ObjectType: storage.DirectoryRootMappingObjectTable,
		ObjectID:   tableID,
		RootPageID: indexRoot,
	}}); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want invalid table root mapping failure")
	}
}

func TestOpenFailsWhenIndexRootMappingPointsToTablePage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	tableRoot := uint32(db.tables["users"].RootPageID())
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	tableID := db.tables["users"].TableID
	indexDef := db.tables["users"].IndexDefinition("idx_users_name")
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := storage.WriteDirectoryRootIDMappings(rawDB.File(), []storage.DirectoryRootIDMapping{
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: tableID, RootPageID: tableRoot},
		{ObjectType: storage.DirectoryRootMappingObjectIndex, ObjectID: indexDef.IndexID, RootPageID: tableRoot},
	}); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want invalid index root mapping failure")
	}
}

func TestOpenLoadsDurableFreeListHead(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	freeHead := appendFreePageForTest(t, pager, 0)
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), uint32(freeHead)); err != nil {
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	if db.freeListHead != uint32(freeHead) {
		t.Fatalf("db.freeListHead = %d, want %d", db.freeListHead, freeHead)
	}
}

func TestCreateTableReusesDurableFreeListHeadAndAdvancesIt(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatalf("Exec(create t1) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	secondFreePageID := appendFreePageForTest(t, pager, 0)
	firstFreePageID := appendFreePageForTest(t, pager, secondFreePageID)
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), uint32(firstFreePageID)); err != nil {
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t2 (id INT)"); err != nil {
		t.Fatalf("Exec(create t2) error = %v", err)
	}
	if got := db.tables["t2"].RootPageID(); got != firstFreePageID {
		t.Fatalf("t2 rootPageID = %d, want %d", got, firstFreePageID)
	}
	if db.freeListHead != uint32(secondFreePageID) {
		t.Fatalf("db.freeListHead = %d, want %d", db.freeListHead, secondFreePageID)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ = openRawStorage(t, path)
	defer rawDB.Close()
	head, err := storage.ReadDirectoryFreeListHead(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	if head != uint32(secondFreePageID) {
		t.Fatalf("ReadDirectoryFreeListHead() = %d, want %d", head, secondFreePageID)
	}
}

func TestCreateTableFallsBackToFreshAllocationWhenFreeListEmpty(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatalf("Exec(create t1) error = %v", err)
	}
	t1Root := db.tables["t1"].RootPageID()
	if _, err := db.Exec("CREATE TABLE t2 (id INT)"); err != nil {
		t.Fatalf("Exec(create t2) error = %v", err)
	}
	defer db.Close()

	if got := db.tables["t2"].RootPageID(); got != t1Root+2 {
		t.Fatalf("t2 rootPageID = %d, want %d", got, t1Root+2)
	}
	if db.freeListHead != 0 {
		t.Fatalf("db.freeListHead = %d, want 0", db.freeListHead)
	}
}

func TestReopenPreservesFreeListHeadForSubsequentAllocation(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatalf("Exec(create t1) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	secondFreePageID := appendFreePageForTest(t, pager, 0)
	firstFreePageID := appendFreePageForTest(t, pager, secondFreePageID)
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), uint32(firstFreePageID)); err != nil {
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t2 (id INT)"); err != nil {
		t.Fatalf("Exec(create t2) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second reopen Open() error = %v", err)
	}
	defer db.Close()
	if db.freeListHead != uint32(secondFreePageID) {
		t.Fatalf("db.freeListHead = %d, want %d", db.freeListHead, secondFreePageID)
	}
	if _, err := db.Exec("CREATE TABLE t3 (id INT)"); err != nil {
		t.Fatalf("Exec(create t3) error = %v", err)
	}
	if got := db.tables["t3"].RootPageID(); got != secondFreePageID {
		t.Fatalf("t3 rootPageID = %d, want %d", got, secondFreePageID)
	}
}

func TestCreateTableFailsOnMalformedFreePageLinkage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatalf("Exec(create t1) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	badFreePageID := appendFreePageForTest(t, pager, 0)
	page, err := pager.Get(badFreePageID)
	if err != nil {
		t.Fatalf("pager.Get(%d) error = %v", badFreePageID, err)
	}
	clear(page.Data())
	copy(page.Data(), []byte("not-a-free-page"))
	pager.MarkDirty(page)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), uint32(badFreePageID)); err != nil {
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t2 (id INT)"); err == nil {
		t.Fatal("Exec(create t2) error = nil, want malformed free page failure")
	}
}

func TestOpenFailsOnJournalPageSizeMismatch(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	journalFile, err := storage.CreateJournalFile(storage.JournalPath(path), 1234, 0)
	if err != nil {
		t.Fatalf("CreateJournalFile() error = %v", err)
	}
	if err := journalFile.Close(); err != nil {
		t.Fatalf("journalFile.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		db.Close()
		t.Fatal("Open() error = nil, want journal page size mismatch")
	}
}

func TestRecoveryRunsBeforeCatalogLoad(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	catalogPage, err := pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get(0) error = %v", err)
	}
	originalCatalog := append([]byte(nil), catalogPage.Data()...)
	clear(catalogPage.Data())
	copy(catalogPage.Data(), []byte("corrupt-catalog"))
	pager.MarkDirty(catalogPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	journalFile, err := storage.CreateJournalFile(storage.JournalPath(path), storage.PageSize, 1)
	if err != nil {
		t.Fatalf("CreateJournalFile() error = %v", err)
	}
	if err := storage.WriteJournalEntry(journalFile, 0, originalCatalog); err != nil {
		t.Fatalf("WriteJournalEntry() error = %v", err)
	}
	if err := journalFile.Sync(); err != nil {
		t.Fatalf("journalFile.Sync() error = %v", err)
	}
	if err := journalFile.Close(); err != nil {
		t.Fatalf("journalFile.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open(recover) error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestOpenReplaysCommittedWALState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	db.afterDatabaseSyncHook = func() error {
		return errors.New("checkpoint failed after WAL durability")
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err == nil {
		t.Fatal("Exec(insert) error = nil, want checkpoint failure")
	}
	db.afterDatabaseSyncHook = nil
	if records, err := storage.ReadWALRecords(path); err != nil {
		t.Fatalf("ReadWALRecords() error = %v", err)
	} else if len(records) == 0 {
		t.Fatal("len(ReadWALRecords()) = 0, want committed WAL after checkpoint failure")
	}
	rootPageID := db.tables["t"].RootPageID()
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(rootPageID)
	if err != nil {
		t.Fatalf("pager.Get(root) error = %v", err)
	}
	corrupted := storage.InitializeTablePage(uint32(rootPageID))
	if err := storage.RecomputePageChecksum(corrupted); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	clear(rootPage.Data())
	copy(rootPage.Data(), corrupted)
	pager.MarkDirty(rootPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestOpenIgnoresTrailingUncommittedWALFrames(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	rootPageID := db.tables["t"].RootPageID()
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(rootPageID)
	if err != nil {
		t.Fatalf("pager.Get(root) error = %v", err)
	}
	uncommitted := append([]byte(nil), rootPage.Data()...)
	row, err := storage.EncodeSlottedRow([]parser.Value{parser.Int64Value(2)})
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}
	slotCount, err := storage.TablePageSlotCount(uncommitted)
	if err != nil {
		t.Fatalf("TablePageSlotCount() error = %v", err)
	}
	if slotCount != 1 {
		t.Fatalf("TablePageSlotCount() = %d, want 1", slotCount)
	}
	if _, err := storage.InsertRowIntoTablePage(uncommitted, row); err != nil {
		t.Fatalf("InsertRowIntoTablePage() error = %v", err)
	}
	if err := storage.SetPageLSN(uncommitted, 999); err != nil {
		t.Fatalf("SetPageLSN() error = %v", err)
	}
	if err := storage.RecomputePageChecksum(uncommitted); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	var frame storage.WALFrame
	frame.FrameLSN = 999
	frame.PageID = uint32(rootPageID)
	frame.PageLSN = 999
	copy(frame.PageData[:], uncommitted)
	if err := storage.AppendWALFrame(path, frame); err != nil {
		t.Fatalf("AppendWALFrame() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestOpenReplayIsIdempotentAcrossRepeatedOpens(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	db.afterDatabaseSyncHook = func() error {
		return errors.New("checkpoint failed after WAL durability")
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err == nil {
		t.Fatal("Exec(insert) error = nil, want checkpoint failure")
	}
	db.afterDatabaseSyncHook = nil
	rootPageID := db.tables["t"].RootPageID()
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(rootPageID)
	if err != nil {
		t.Fatalf("pager.Get(root) error = %v", err)
	}
	corrupted := storage.InitializeTablePage(uint32(rootPageID))
	if err := storage.RecomputePageChecksum(corrupted); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	clear(rootPage.Data())
	copy(rootPage.Data(), corrupted)
	pager.MarkDirty(rootPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("first replay Open() error = %v", err)
	}
	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
	if err := db.Close(); err != nil {
		t.Fatalf("first replay Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second replay Open() error = %v", err)
	}
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestOpenReplaysMultipleCommittedWALTransactions(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(first insert) error = %v", err)
	}
	db.afterDatabaseSyncHook = func() error {
		return errors.New("checkpoint failed after WAL durability")
	}
	if _, err := db.Exec("INSERT INTO t VALUES (2)"); err == nil {
		t.Fatal("Exec(second insert) error = nil, want checkpoint failure")
	}
	db.afterDatabaseSyncHook = nil
	rootPageID := db.tables["t"].RootPageID()
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(rootPageID)
	if err != nil {
		t.Fatalf("pager.Get(root) error = %v", err)
	}
	older := storage.InitializeTablePage(uint32(rootPageID))
	row, err := storage.EncodeSlottedRow([]parser.Value{parser.Int64Value(1)})
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}
	if _, err := storage.InsertRowIntoTablePage(older, row); err != nil {
		t.Fatalf("InsertRowIntoTablePage() error = %v", err)
	}
	if err := storage.SetPageLSN(older, 5); err != nil {
		t.Fatalf("SetPageLSN() error = %v", err)
	}
	if err := storage.RecomputePageChecksum(older); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	clear(rootPage.Data())
	copy(rootPage.Data(), older)
	pager.MarkDirty(rootPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)
}

func TestOpenFailsOnTruncatedWALFrameDuringReplay(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	file, err := os.OpenFile(storage.WALPath(path), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	raw := make([]byte, storage.WALFrameSize-1)
	binary.LittleEndian.PutUint32(raw[0:4], storage.WALRecordTypeFrame)
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		t.Fatalf("file.Write() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want truncated WAL replay failure")
	}
}

func seedFreePageForTest(t *testing.T, pager *storage.Pager, pageID storage.PageID, next storage.PageID) {
	t.Helper()

	page, err := pager.Get(pageID)
	if err != nil {
		t.Fatalf("pager.Get(%d) error = %v", pageID, err)
	}
	clear(page.Data())
	copy(page.Data(), storage.InitFreePage(uint32(pageID), uint32(next)))
	pager.MarkDirty(page)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
}

func appendFreePageForTest(t *testing.T, pager *storage.Pager, next storage.PageID) storage.PageID {
	t.Helper()

	page := pager.NewPage()
	pageID := page.ID()
	clear(page.Data())
	copy(page.Data(), storage.InitFreePage(uint32(pageID), uint32(next)))
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	return pageID
}

func buildLegacyV5DirectoryPageForOpenTest(cat *storage.CatalogData) []byte {
	page := storage.InitDirectoryPage(uint32(storage.DirectoryControlPageID), storage.CurrentDBFormatVersion)
	payload := make([]byte, 0, storage.PageSize)
	payload = appendUint32ForOpenTest(payload, 5)
	payload = appendUint32ForOpenTest(payload, uint32(len(cat.Tables)))
	for _, table := range cat.Tables {
		payload = appendStringForOpenTest(payload, table.Name)
		payload = appendUint32ForOpenTest(payload, table.TableID)
		payload = appendUint32ForOpenTest(payload, table.RootPageID)
		payload = appendUint32ForOpenTest(payload, table.RowCount)
		payload = appendUint16ForOpenTest(payload, uint16(len(table.Columns)))
		for _, column := range table.Columns {
			payload = appendStringForOpenTest(payload, column.Name)
			payload = append(payload, column.Type)
		}
		payload = appendUint16ForOpenTest(payload, uint16(len(table.Indexes)))
		for _, index := range table.Indexes {
			payload = appendStringForOpenTest(payload, index.Name)
			if index.Unique {
				payload = append(payload, 1)
			} else {
				payload = append(payload, 0)
			}
			payload = appendUint32ForOpenTest(payload, index.IndexID)
			payload = appendUint32ForOpenTest(payload, index.RootPageID)
			payload = appendUint16ForOpenTest(payload, uint16(len(index.Columns)))
			for _, column := range index.Columns {
				payload = appendStringForOpenTest(payload, column.Name)
				if column.Desc {
					payload = append(payload, 1)
				} else {
					payload = append(payload, 0)
				}
			}
		}
	}
	copy(page[48:], payload)
	_ = storage.RecomputePageChecksum(page)
	return page
}

func appendUint32ForOpenTest(buf []byte, value uint32) []byte {
	var raw [4]byte
	binary.LittleEndian.PutUint32(raw[:], value)
	return append(buf, raw[:]...)
}

func appendUint16ForOpenTest(buf []byte, value uint16) []byte {
	var raw [2]byte
	binary.LittleEndian.PutUint16(raw[:], value)
	return append(buf, raw[:]...)
}

func appendStringForOpenTest(buf []byte, value string) []byte {
	buf = appendUint16ForOpenTest(buf, uint16(len(value)))
	return append(buf, value...)
}

func injectLegacyNameMappingsForOpenTest(file *os.File, payload []byte) error {
	page := make([]byte, storage.PageSize)
	if _, err := file.ReadAt(page, storage.HeaderSize); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(page[40:44], 1)
	binary.LittleEndian.PutUint32(page[44:48], uint32(len(payload)))
	copy(page[48:], payload)
	if _, err := file.WriteAt(page, storage.HeaderSize); err != nil {
		return err
	}
	return file.Sync()
}
