package rovadb

import (
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
	"github.com/Khorlane/RovaDB/internal/temporal"
)

func TestOpenRejectsOrphanWALWhenDBFileIsMissing(t *testing.T) {
	path := freshDBPath(t)
	walPath := storage.WALPath(path)

	if err := storage.EnsureWALFile(path, storage.DBFormatVersion()); err != nil {
		t.Fatalf("EnsureWALFile() error = %v", err)
	}

	db, err := Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want orphan WAL failure")
	}
	if err.Error() != "open: database file does not exist but WAL sidecar exists: "+walPath {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "open: database file does not exist but WAL sidecar exists: "+walPath)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("os.Stat(db path) error = %v, want %v", statErr, os.ErrNotExist)
	}
}

func TestVersionReturnsCurrentProductVersion(t *testing.T) {
	if got := Version(); got != "v0.48.3" {
		t.Fatalf("Version() = %q, want %q", got, "v0.48.3")
	}
}

func TestPublicOpenAPIMethodShapesCompile(t *testing.T) {
	var open func(string) (*DB, error) = Open
	var create func(string) (*DB, error) = Create
	var openWithOptions func(string, OpenOptions) (*DB, error) = OpenWithOptions
	var createWithOptions func(string, OpenOptions) (*DB, error) = CreateWithOptions
	opts := OpenOptions{DefaultTimezone: "UTC"}

	_, _, _, _, _ = open, create, openWithOptions, createWithOptions, opts
}

func TestPublicTransactionAPIMethodShapesCompile(t *testing.T) {
	var begin func(*DB) (*Tx, error) = (*DB).Begin
	var commit func(*Tx) error = (*Tx).Commit
	var rollback func(*Tx) error = (*Tx).Rollback
	var exec func(*Tx, string, ...any) (Result, error) = (*Tx).Exec
	var query func(*Tx, string, ...any) (*Rows, error) = (*Tx).Query
	var queryRow func(*Tx, string, ...any) *Row = (*Tx).QueryRow

	// Keep explicit references so method-shape compatibility is checked at compile time.
	_, _, _, _, _, _ = begin, commit, rollback, exec, query, queryRow
}

func TestCreateWithOptionsAcceptsValidDefaultTimezone(t *testing.T) {
	db, err := CreateWithOptions(freshDBPath(t), OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("CreateWithOptions() error = %v", err)
	}
	defer db.Close()

	if db.defaultTimezone != "America/New_York" {
		t.Fatalf("db.defaultTimezone = %q, want %q", db.defaultTimezone, "America/New_York")
	}
	if db.defaultLocation == nil {
		t.Fatal("db.defaultLocation = nil, want non-nil")
	}
	if db.defaultLocation.String() != "America/New_York" {
		t.Fatalf("db.defaultLocation.String() = %q, want %q", db.defaultLocation.String(), "America/New_York")
	}
	if db.catalogDefaultTimezone != "America/New_York" {
		t.Fatalf("db.catalogDefaultTimezone = %q, want %q", db.catalogDefaultTimezone, "America/New_York")
	}
	if db.catalogTimezoneBasisVersion != temporal.CurrentTimezoneBasisVersion {
		t.Fatalf("db.catalogTimezoneBasisVersion = %q, want %q", db.catalogTimezoneBasisVersion, temporal.CurrentTimezoneBasisVersion)
	}
	if len(db.catalogTimezoneDictionary) != 1 || db.catalogTimezoneDictionary[0] != "America/New_York" {
		t.Fatalf("db.catalogTimezoneDictionary = %v, want [%q]", db.catalogTimezoneDictionary, "America/New_York")
	}
}

func TestCreateWithOptionsRejectsInvalidDefaultTimezone(t *testing.T) {
	db, err := CreateWithOptions(freshDBPath(t), OpenOptions{DefaultTimezone: "Mars/Olympus"})
	if err == nil {
		_ = db.Close()
		t.Fatal("CreateWithOptions() error = nil, want non-nil")
	}
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("CreateWithOptions() error = %v, want %v", err, ErrInvalidArgument)
	}
	if !strings.Contains(err.Error(), `invalid default timezone "Mars/Olympus"`) {
		t.Fatalf("CreateWithOptions() error = %q, want invalid timezone detail", err.Error())
	}
}

func TestCreateUsesDefaultOptionsWhenNoExplicitOptionsAreProvided(t *testing.T) {
	db, err := Create(freshDBPath(t))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer db.Close()

	if db.defaultTimezone != "" {
		t.Fatalf("db.defaultTimezone = %q, want empty", db.defaultTimezone)
	}
	if db.defaultLocation != nil {
		t.Fatalf("db.defaultLocation = %#v, want nil", db.defaultLocation)
	}
	if db.catalogDefaultTimezone != "" {
		t.Fatalf("db.catalogDefaultTimezone = %q, want empty", db.catalogDefaultTimezone)
	}
	if db.catalogTimezoneBasisVersion != "" {
		t.Fatalf("db.catalogTimezoneBasisVersion = %q, want empty", db.catalogTimezoneBasisVersion)
	}
	if len(db.catalogTimezoneDictionary) != 0 {
		t.Fatalf("len(db.catalogTimezoneDictionary) = %d, want 0", len(db.catalogTimezoneDictionary))
	}
}

func TestOpenWithOptionsPersistsTemporalTimezoneMetadataInCatalog(t *testing.T) {
	path := freshDBPath(t)
	db, err := CreateWithOptions(path, OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("CreateWithOptions() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() error = %v", err)
	}

	dbFile, err := storage.Open(path)
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer dbFile.Close()

	pager, err := storage.NewPager(dbFile.File())
	if err != nil {
		t.Fatalf("storage.NewPager() error = %v", err)
	}
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("storage.LoadCatalog() error = %v", err)
	}

	if catalog.DefaultTimezone != "America/New_York" {
		t.Fatalf("catalog.DefaultTimezone = %q, want %q", catalog.DefaultTimezone, "America/New_York")
	}
	if catalog.TimezoneBasisVersion != temporal.CurrentTimezoneBasisVersion {
		t.Fatalf("catalog.TimezoneBasisVersion = %q, want %q", catalog.TimezoneBasisVersion, temporal.CurrentTimezoneBasisVersion)
	}
	if len(catalog.TimezoneDictionary) != 1 || catalog.TimezoneDictionary[0] != "America/New_York" {
		t.Fatalf("catalog.TimezoneDictionary = %v, want [%q]", catalog.TimezoneDictionary, "America/New_York")
	}
}

func TestOpenReloadsPersistedTemporalTimezoneMetadata(t *testing.T) {
	path := freshDBPath(t)
	db, err := CreateWithOptions(path, OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("CreateWithOptions() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() error = %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("Open() reopen error = %v", err)
	}
	defer reopened.Close()

	if reopened.defaultTimezone != "America/New_York" {
		t.Fatalf("reopened.defaultTimezone = %q, want %q", reopened.defaultTimezone, "America/New_York")
	}
	if reopened.defaultLocation == nil || reopened.defaultLocation.String() != "America/New_York" {
		t.Fatalf("reopened.defaultLocation = %#v, want America/New_York", reopened.defaultLocation)
	}
	if reopened.catalogDefaultTimezone != "America/New_York" {
		t.Fatalf("reopened.catalogDefaultTimezone = %q, want %q", reopened.catalogDefaultTimezone, "America/New_York")
	}
	if reopened.catalogTimezoneBasisVersion != temporal.CurrentTimezoneBasisVersion {
		t.Fatalf("reopened.catalogTimezoneBasisVersion = %q, want %q", reopened.catalogTimezoneBasisVersion, temporal.CurrentTimezoneBasisVersion)
	}
	if len(reopened.catalogTimezoneDictionary) != 1 || reopened.catalogTimezoneDictionary[0] != "America/New_York" {
		t.Fatalf("reopened.catalogTimezoneDictionary = %v, want [%q]", reopened.catalogTimezoneDictionary, "America/New_York")
	}
}

func TestOpenRejectsPersistedMismatchedTimezoneBasisVersion(t *testing.T) {
	path := freshDBPath(t)

	db, err := CreateWithOptions(path, OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("CreateWithOptions() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rewriteCatalogTemporalMetadataForTest(t, path, "America/New_York", "embedded-tzdata-v999", []string{"America/New_York"})

	reopened, err := Open(path)
	if err == nil {
		_ = reopened.Close()
		t.Fatal("Open() error = nil, want mismatched persisted timezone basis failure")
	}
	assertErrorContainsAll(t, err, "open", `persisted timezone basis version "embedded-tzdata-v999"`, temporal.CurrentTimezoneBasisVersion, "corrupted catalog page")
}

func TestOpenReloadsPersistedTemporalTimezoneMetadataWithMatchingBasisVersion(t *testing.T) {
	path := freshDBPath(t)

	db, err := CreateWithOptions(path, OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("CreateWithOptions() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rewriteCatalogTemporalMetadataForTest(t, path, "America/New_York", temporal.CurrentTimezoneBasisVersion, []string{"America/New_York", "UTC"})

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("Open() reopen error = %v", err)
	}
	defer reopened.Close()

	if reopened.defaultTimezone != "America/New_York" {
		t.Fatalf("reopened.defaultTimezone = %q, want %q", reopened.defaultTimezone, "America/New_York")
	}
	if reopened.defaultLocation == nil || reopened.defaultLocation.String() != "America/New_York" {
		t.Fatalf("reopened.defaultLocation = %#v, want America/New_York", reopened.defaultLocation)
	}
	if reopened.catalogTimezoneBasisVersion != temporal.CurrentTimezoneBasisVersion {
		t.Fatalf("reopened.catalogTimezoneBasisVersion = %q, want %q", reopened.catalogTimezoneBasisVersion, temporal.CurrentTimezoneBasisVersion)
	}
	if got := strings.Join(reopened.catalogTimezoneDictionary, ","); got != "America/New_York,UTC" {
		t.Fatalf("reopened.catalogTimezoneDictionary = %v, want [America/New_York UTC]", reopened.catalogTimezoneDictionary)
	}
}

func TestOpenRejectsPersistedDefaultTimezoneMissingFromDictionary(t *testing.T) {
	path := freshDBPath(t)

	db, err := CreateWithOptions(path, OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("CreateWithOptions() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rewriteMalformedCatalogTemporalMetadataForTest(t, path, "America/New_York", temporal.CurrentTimezoneBasisVersion, []string{"UTC"})

	reopened, err := Open(path)
	if err == nil {
		_ = reopened.Close()
		t.Fatal("Open() error = nil, want missing persisted default timezone failure")
	}
	assertErrorContainsAll(t, err, `default timezone "America/New_York"`, "missing from dictionary ordering", "corrupted catalog page")
}

func TestOpenRejectsPersistedEmptyTimezoneDictionaryEntry(t *testing.T) {
	path := freshDBPath(t)

	db, err := CreateWithOptions(path, OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("CreateWithOptions() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rewriteMalformedCatalogTemporalMetadataForTest(t, path, "America/New_York", temporal.CurrentTimezoneBasisVersion, []string{"America/New_York", ""})

	reopened, err := Open(path)
	if err == nil {
		_ = reopened.Close()
		t.Fatal("Open() error = nil, want empty persisted timezone dictionary entry failure")
	}
	assertErrorContainsAll(t, err, "zone_id 1 is empty", "corrupted catalog page")
}

func TestOpenRejectsPersistedDuplicateTimezoneDictionaryEntry(t *testing.T) {
	path := freshDBPath(t)

	db, err := CreateWithOptions(path, OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("CreateWithOptions() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rewriteMalformedCatalogTemporalMetadataForTest(t, path, "America/New_York", temporal.CurrentTimezoneBasisVersion, []string{"America/New_York", "America/New_York"})

	reopened, err := Open(path)
	if err == nil {
		_ = reopened.Close()
		t.Fatal("Open() error = nil, want duplicate persisted timezone dictionary entry failure")
	}
	assertErrorContainsAll(t, err, `zone_id 1 ("America/New_York")`, "duplicated", "corrupted catalog page")
}

func TestTxExecAndQueryNormalizeTimestampsThroughDatabaseTimezoneContext(t *testing.T) {
	db, err := CreateWithOptions(freshDBPath(t), OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("CreateWithOptions() error = %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := tx.Exec("CREATE TABLE events (id INT, recorded_at TIMESTAMP)"); err != nil {
		t.Fatalf("Tx.Exec(create) error = %v", err)
	}
	if _, err := tx.Exec("INSERT INTO events VALUES (?, ?)", int32(1), "2026-04-10 13:45:21"); err != nil {
		t.Fatalf("Tx.Exec(insert) error = %v", err)
	}

	location, err := temporal.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation() error = %v", err)
	}
	wantTimestamp := time.Date(2026, time.April, 10, 13, 45, 21, 0, location)
	wantValue := parser.TimestampValue(wantTimestamp.UnixMilli(), 0)

	if got := tx.tables["events"].Rows[0][1]; got != wantValue {
		t.Fatalf("tx.tables[events].Rows[0][1] = %#v, want %#v", got, wantValue)
	}

	var got time.Time
	if err := tx.QueryRow("SELECT recorded_at FROM events WHERE recorded_at = '2026-04-10 13:45:21'").Scan(&got); err != nil {
		t.Fatalf("Tx.QueryRow().Scan() error = %v", err)
	}
	if !got.Equal(wantTimestamp) {
		t.Fatalf("Tx.QueryRow().Scan() = %v, want %v", got, wantTimestamp)
	}
	if got.Location().String() != "America/New_York" {
		t.Fatalf("Tx.QueryRow().Scan() location = %q, want %q", got.Location().String(), "America/New_York")
	}
}

func TestBeginOnOpenDBReturnsActiveTx(t *testing.T) {
	db, err := Create(freshDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v, want nil", err)
	}
	if tx == nil {
		t.Fatal("Begin() tx = nil, want value")
	}
	if tx.db != db {
		t.Fatalf("tx.db = %#v, want originating db %#v", tx.db, db)
	}
	if tx.finished {
		t.Fatal("tx.finished = true, want false for active transaction")
	}
	if db.tx != tx {
		t.Fatalf("db.tx = %#v, want returned tx %#v", db.tx, tx)
	}
}

func rewriteCatalogTemporalMetadataForTest(t *testing.T, path string, defaultTimezone string, timezoneBasisVersion string, timezoneDictionary []string) {
	t.Helper()

	rawDB, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("storage.LoadCatalog() error = %v", err)
	}
	catalog = catalogWithDirectoryRootsForSave(t, rawDB.File(), catalog)
	catalog.DefaultTimezone = defaultTimezone
	catalog.TimezoneBasisVersion = timezoneBasisVersion
	catalog.TimezoneDictionary = append([]string(nil), timezoneDictionary...)
	if err := storage.SaveCatalog(pager, catalog); err != nil {
		t.Fatalf("storage.SaveCatalog() error = %v", err)
	}
	rewriteDirectoryRootMappingsForCatalogTables(t, rawDB.File(), catalog)
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("raw Close() error = %v", err)
	}
}

func rewriteMalformedCatalogTemporalMetadataForTest(t *testing.T, path string, defaultTimezone string, timezoneBasisVersion string, timezoneDictionary []string) {
	t.Helper()

	rawDB, pager := openRawStorage(t, path)
	page, err := pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get(0) error = %v", err)
	}
	mode, err := storage.DirectoryCATDIRStorageMode(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIRStorageMode() error = %v", err)
	}
	if mode != storage.DirectoryCATDIRStorageModeEmbedded {
		t.Fatalf("rewriteMalformedCatalogTemporalMetadataForTest() requires embedded catalog storage, got mode %d", mode)
	}
	payloadBytes, err := storage.DirectoryCATDIRPayloadByteLength(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIRPayloadByteLength() error = %v", err)
	}
	payloadEnd := testDirectoryCatalogOffset + int(payloadBytes)
	if payloadEnd > len(page.Data()) {
		t.Fatalf("catalog payload end = %d, page size = %d", payloadEnd, len(page.Data()))
	}
	currentPayload := append([]byte(nil), page.Data()[testDirectoryCatalogOffset:payloadEnd]...)
	rewrittenPayload := rewriteCatalogTemporalMetadataInPayloadForTest(t, currentPayload, defaultTimezone, timezoneBasisVersion, timezoneDictionary)

	mappings, err := storage.ReadDirectoryRootIDMappings(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	writeMalformedCatalogPageWithIDMappings(t, pager, rewrittenPayload, mappings)
	if err := rawDB.Close(); err != nil {
		t.Fatalf("raw Close() error = %v", err)
	}
}

func rewriteCatalogTemporalMetadataInPayloadForTest(t *testing.T, currentPayload []byte, defaultTimezone string, timezoneBasisVersion string, timezoneDictionary []string) []byte {
	t.Helper()

	offset := 0
	version, ok := readUint32ForCatalogPayload(currentPayload, &offset)
	if !ok {
		t.Fatal("readUint32ForCatalogPayload(version) = false")
	}
	if _, ok := readStringForCatalogPayload(currentPayload, &offset); !ok {
		t.Fatal("readStringForCatalogPayload(default timezone) = false")
	}
	if _, ok := readStringForCatalogPayload(currentPayload, &offset); !ok {
		t.Fatal("readStringForCatalogPayload(timezone basis version) = false")
	}
	dictionaryCount, ok := readUint32ForCatalogPayload(currentPayload, &offset)
	if !ok {
		t.Fatal("readUint32ForCatalogPayload(dictionary count) = false")
	}
	for i := uint32(0); i < dictionaryCount; i++ {
		if _, ok := readStringForCatalogPayload(currentPayload, &offset); !ok {
			t.Fatalf("readStringForCatalogPayload(dictionary[%d]) = false", i)
		}
	}

	payload := make([]byte, 0, len(currentPayload)+64)
	payload = appendUint32ForCatalogPayload(payload, version)
	payload = appendStringForCatalogPayload(payload, defaultTimezone)
	payload = appendStringForCatalogPayload(payload, timezoneBasisVersion)
	payload = appendUint32ForCatalogPayload(payload, uint32(len(timezoneDictionary)))
	for _, zone := range timezoneDictionary {
		payload = appendStringForCatalogPayload(payload, zone)
	}
	payload = append(payload, currentPayload[offset:]...)
	return payload
}

func appendUint32ForCatalogPayload(buf []byte, value uint32) []byte {
	var raw [4]byte
	binary.LittleEndian.PutUint32(raw[:], value)
	return append(buf, raw[:]...)
}

func appendStringForCatalogPayload(buf []byte, value string) []byte {
	var raw [2]byte
	binary.LittleEndian.PutUint16(raw[:], uint16(len(value)))
	buf = append(buf, raw[:]...)
	return append(buf, value...)
}

func readUint32ForCatalogPayload(data []byte, offset *int) (uint32, bool) {
	if *offset+4 > len(data) {
		return 0, false
	}
	value := binary.LittleEndian.Uint32(data[*offset : *offset+4])
	*offset += 4
	return value, true
}

func readStringForCatalogPayload(data []byte, offset *int) (string, bool) {
	if *offset+2 > len(data) {
		return "", false
	}
	length := int(binary.LittleEndian.Uint16(data[*offset : *offset+2]))
	*offset += 2
	if *offset+length > len(data) {
		return "", false
	}
	value := string(data[*offset : *offset+length])
	*offset += length
	return value, true
}

func TestBeginOnNilDBReturnsInvalidArgument(t *testing.T) {
	var db *DB

	tx, err := db.Begin()
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Begin() error = %v, want %v", err, ErrInvalidArgument)
	}
	if tx != nil {
		t.Fatalf("Begin() tx = %#v, want nil on nil DB", tx)
	}
}

func TestBeginOnClosedDBReturnsClosed(t *testing.T) {
	db, err := Create(freshDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	tx, err := db.Begin()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Begin() error = %v, want %v", err, ErrClosed)
	}
	if tx != nil {
		t.Fatalf("Begin() tx = %#v, want nil on closed DB", tx)
	}
}

func TestNilTxCommitAndRollbackReturnWithoutActiveErrors(t *testing.T) {
	var tx *Tx

	if err := tx.Commit(); !errors.Is(err, ErrTxnCommitWithoutActive) {
		t.Fatalf("Commit() error = %v, want %v", err, ErrTxnCommitWithoutActive)
	}
	if err := tx.Rollback(); !errors.Is(err, ErrTxnRollbackWithoutActive) {
		t.Fatalf("Rollback() error = %v, want %v", err, ErrTxnRollbackWithoutActive)
	}
}

func TestNilTxExecQueryAndQueryRowUsePublicMisuseErrors(t *testing.T) {
	var tx *Tx

	if _, err := tx.Exec("CREATE TABLE users (id INT)"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Exec() error = %v, want %v", err, ErrInvalidArgument)
	}
	if _, err := tx.Query("SELECT 1"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Query() error = %v, want %v", err, ErrInvalidArgument)
	}
	if err := tx.QueryRow("SELECT 1").Scan(new(any)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("QueryRow().Scan() error = %v, want %v", err, ErrInvalidArgument)
	}
}

func TestSecondBeginWhileTxActiveIsRejected(t *testing.T) {
	db, err := Create(freshDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	first, err := db.Begin()
	if err != nil {
		t.Fatalf("first Begin() error = %v", err)
	}

	second, err := db.Begin()
	if !errors.Is(err, ErrTxnAlreadyActive) {
		t.Fatalf("second Begin() error = %v, want %v", err, ErrTxnAlreadyActive)
	}
	if second != nil {
		t.Fatalf("second Begin() tx = %#v, want nil", second)
	}
	if db.tx != first {
		t.Fatalf("db.tx = %#v, want first tx %#v", db.tx, first)
	}
}

func TestCommitAppliesTransactionLocalStateToLaterDBReads(t *testing.T) {
	db, err := Create(freshDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := tx.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Tx.Exec(create) error = %v", err)
	}
	if _, err := tx.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Tx.Exec(insert) error = %v", err)
	}

	var txName string
	if err := tx.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&txName); err != nil {
		t.Fatalf("Tx.QueryRow().Scan() before commit error = %v", err)
	}
	if txName != "alice" {
		t.Fatalf("tx-scoped name = %q, want %q", txName, "alice")
	}

	dbRows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("DB.Query() before commit error = %v", err)
	}
	if dbRows.Next() {
		t.Fatal("DB.Query() before commit unexpectedly saw uncommitted rows")
	}
	if dbRows.Err() == nil || dbRows.Err().Error() != "execution: table not found: users" {
		t.Fatalf("DB.Query() before commit Err() = %v, want %q", dbRows.Err(), "execution: table not found: users")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v, want nil", err)
	}
	if !tx.finished {
		t.Fatal("tx.finished = false, want true after Commit()")
	}
	if db.tx != nil {
		t.Fatalf("db.tx = %#v, want nil after Commit()", db.tx)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxnCommitWithoutActive) {
		t.Fatalf("second Commit() error = %v, want %v", err, ErrTxnCommitWithoutActive)
	}

	var dbName string
	if err := db.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&dbName); err != nil {
		t.Fatalf("DB.QueryRow().Scan() after commit error = %v", err)
	}
	if dbName != "alice" {
		t.Fatalf("committed name = %q, want %q", dbName, "alice")
	}
	if _, err := tx.Exec("CREATE TABLE later (id INT)"); !errors.Is(err, ErrTxNotActive) {
		t.Fatalf("Exec() after Commit() error = %v, want %v", err, ErrTxNotActive)
	}
	if _, err := tx.Query("SELECT 1"); !errors.Is(err, ErrTxNotActive) {
		t.Fatalf("Query() after Commit() error = %v, want %v", err, ErrTxNotActive)
	}
	if err := tx.QueryRow("SELECT 1").Scan(new(any)); !errors.Is(err, ErrTxNotActive) {
		t.Fatalf("QueryRow().Scan() after Commit() error = %v, want %v", err, ErrTxNotActive)
	}

	next, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() after Commit() error = %v", err)
	}
	if next == nil {
		t.Fatal("Begin() after Commit() tx = nil, want value")
	}
}

func TestRollbackDiscardsTransactionLocalStateFromLaterDBReads(t *testing.T) {
	db, err := Create(freshDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := tx.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Tx.Exec(create) error = %v", err)
	}
	if _, err := tx.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Tx.Exec(insert) error = %v", err)
	}

	var txName string
	if err := tx.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&txName); err != nil {
		t.Fatalf("Tx.QueryRow().Scan() before rollback error = %v", err)
	}
	if txName != "alice" {
		t.Fatalf("tx-scoped name = %q, want %q", txName, "alice")
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v, want nil", err)
	}
	if !tx.finished {
		t.Fatal("tx.finished = false, want true after Rollback()")
	}
	if db.tx != nil {
		t.Fatalf("db.tx = %#v, want nil after Rollback()", db.tx)
	}
	if err := tx.Rollback(); !errors.Is(err, ErrTxnRollbackWithoutActive) {
		t.Fatalf("second Rollback() error = %v, want %v", err, ErrTxnRollbackWithoutActive)
	}

	dbRows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("DB.Query() after rollback error = %v", err)
	}
	if dbRows.Next() {
		t.Fatal("DB.Query() after rollback unexpectedly saw discarded rows")
	}
	if dbRows.Err() == nil || dbRows.Err().Error() != "execution: table not found: users" {
		t.Fatalf("DB.Query() after rollback Err() = %v, want %q", dbRows.Err(), "execution: table not found: users")
	}
	if _, err := tx.Exec("CREATE TABLE later (id INT)"); !errors.Is(err, ErrTxNotActive) {
		t.Fatalf("Exec() after Rollback() error = %v, want %v", err, ErrTxNotActive)
	}
	if _, err := tx.Query("SELECT 1"); !errors.Is(err, ErrTxNotActive) {
		t.Fatalf("Query() after Rollback() error = %v, want %v", err, ErrTxNotActive)
	}
	if err := tx.QueryRow("SELECT 1").Scan(new(any)); !errors.Is(err, ErrTxNotActive) {
		t.Fatalf("QueryRow().Scan() after Rollback() error = %v, want %v", err, ErrTxNotActive)
	}

	next, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() after Rollback() error = %v", err)
	}
	if next == nil {
		t.Fatal("Begin() after Rollback() tx = nil, want value")
	}
}

func TestActiveTxRejectedAfterDBClose(t *testing.T) {
	db, err := Create(freshDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := tx.Exec("CREATE TABLE users (id INT)"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Exec() after DB close error = %v, want %v", err, ErrClosed)
	}
	if _, err := tx.Query("SELECT 1"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Query() after DB close error = %v, want %v", err, ErrClosed)
	}
	if err := tx.QueryRow("SELECT 1").Scan(new(any)); !errors.Is(err, ErrClosed) {
		t.Fatalf("QueryRow().Scan() after DB close error = %v, want %v", err, ErrClosed)
	}
}

func TestTxOwnershipMismatchIsRejectedAsInactive(t *testing.T) {
	db, err := Create(freshDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	db.tx = nil

	if _, err := tx.Exec("CREATE TABLE users (id INT)"); !errors.Is(err, ErrTxNotActive) {
		t.Fatalf("Exec() with ownership mismatch error = %v, want %v", err, ErrTxNotActive)
	}
	if _, err := tx.Query("SELECT 1"); !errors.Is(err, ErrTxNotActive) {
		t.Fatalf("Query() with ownership mismatch error = %v, want %v", err, ErrTxNotActive)
	}
	if err := tx.QueryRow("SELECT 1").Scan(new(any)); !errors.Is(err, ErrTxNotActive) {
		t.Fatalf("QueryRow().Scan() with ownership mismatch error = %v, want %v", err, ErrTxNotActive)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxnCommitWithoutActive) {
		t.Fatalf("Commit() with ownership mismatch error = %v, want %v", err, ErrTxnCommitWithoutActive)
	}
	if err := tx.Rollback(); !errors.Is(err, ErrTxnRollbackWithoutActive) {
		t.Fatalf("Rollback() with ownership mismatch error = %v, want %v", err, ErrTxnRollbackWithoutActive)
	}
}

func TestTxExecAndQueryOperateWithinExplicitTransactionSnapshot(t *testing.T) {
	db, err := Create(freshDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := tx.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Tx.Exec(create) error = %v", err)
	}
	if _, err := tx.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Tx.Exec(insert) error = %v", err)
	}

	rows, err := tx.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Tx.Query() error = %v", err)
	}
	if got := rows.Columns(); len(got) != 2 || got[0] != "id" || got[1] != "name" {
		t.Fatalf("rows.Columns() = %#v, want [id name]", got)
	}
	if !rows.Next() {
		t.Fatal("rows.Next() = false, want one row")
	}
	var id int32
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if id != 1 || name != "alice" {
		t.Fatalf("scanned row = (%d, %q), want (1, %q)", id, name, "alice")
	}
	if rows.Next() {
		t.Fatal("rows.Next() = true, want one row only")
	}

	dbRows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("DB.Query() error = %v", err)
	}
	if dbRows.Next() {
		t.Fatal("DB.Query() unexpectedly saw Tx-only rows")
	}
	if dbRows.Err() == nil || dbRows.Err().Error() != "execution: table not found: users" {
		t.Fatalf("DB.Query() Err() = %v, want %q", dbRows.Err(), "execution: table not found: users")
	}
}

func TestTxQueryRowOperatesWithinExplicitTransactionSnapshot(t *testing.T) {
	db, err := Create(freshDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := tx.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Tx.Exec(create) error = %v", err)
	}
	if _, err := tx.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Tx.Exec(insert) error = %v", err)
	}

	var name string
	if err := tx.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("Tx.QueryRow().Scan() error = %v", err)
	}
	if name != "alice" {
		t.Fatalf("name = %q, want %q", name, "alice")
	}
}

func TestDBAutocommitBehaviorRemainsUnchangedWithoutBegin(t *testing.T) {
	db, err := Create(freshDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("DB.Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("DB.Exec(insert) error = %v", err)
	}

	var name string
	if err := db.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("DB.QueryRow().Scan() error = %v", err)
	}
	if name != "alice" {
		t.Fatalf("name = %q, want %q", name, "alice")
	}
}

func TestTxCommitPersistsLifecycleStateAcrossCloseAndReopen(t *testing.T) {
	path := freshDBPath(t)

	db, err := Create(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("DB.Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("DB.Exec(%q) error = %v", sql, err)
		}
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (4, 'dana')",
		"UPDATE users SET name = 'alice cooper' WHERE id = 1",
		"DELETE FROM users WHERE id = 2",
	} {
		if _, err := tx.Exec(sql); err != nil {
			t.Fatalf("Tx.Exec(%q) error = %v", sql, err)
		}
	}

	assertUserRows(t, tx, "SELECT id, name FROM users ORDER BY id",
		userRow{id: 1, name: "alice cooper"},
		userRow{id: 3, name: "cara"},
		userRow{id: 4, name: "dana"},
	)
	assertUserRows(t, db, "SELECT id, name FROM users ORDER BY id",
		userRow{id: 1, name: "alice"},
		userRow{id: 2, name: "bob"},
		userRow{id: 3, name: "cara"},
	)

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertUserRows(t, db, "SELECT id, name FROM users ORDER BY id",
		userRow{id: 1, name: "alice cooper"},
		userRow{id: 3, name: "cara"},
		userRow{id: 4, name: "dana"},
	)

	next, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() after reopen error = %v", err)
	}
	if next == nil {
		t.Fatal("Begin() after reopen tx = nil, want value")
	}
}

func TestTxRollbackDiscardsLifecycleStateAcrossCloseAndReopen(t *testing.T) {
	path := freshDBPath(t)

	db, err := Create(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("DB.Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("DB.Exec(%q) error = %v", sql, err)
		}
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (4, 'dana')",
		"UPDATE users SET name = 'alice cooper' WHERE id = 1",
		"DELETE FROM users WHERE id = 2",
	} {
		if _, err := tx.Exec(sql); err != nil {
			t.Fatalf("Tx.Exec(%q) error = %v", sql, err)
		}
	}

	assertUserRows(t, tx, "SELECT id, name FROM users ORDER BY id",
		userRow{id: 1, name: "alice cooper"},
		userRow{id: 3, name: "cara"},
		userRow{id: 4, name: "dana"},
	)
	assertUserRows(t, db, "SELECT id, name FROM users ORDER BY id",
		userRow{id: 1, name: "alice"},
		userRow{id: 2, name: "bob"},
		userRow{id: 3, name: "cara"},
	)

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertUserRows(t, db, "SELECT id, name FROM users ORDER BY id",
		userRow{id: 1, name: "alice"},
		userRow{id: 2, name: "bob"},
		userRow{id: 3, name: "cara"},
	)

	next, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() after reopen error = %v", err)
	}
	if next == nil {
		t.Fatal("Begin() after reopen tx = nil, want value")
	}
}

func TestTxCommitPersistsMultiPagePhysicalStorageAcrossReopen(t *testing.T) {
	path := freshDBPath(t)

	db, err := Create(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, note TEXT)"); err != nil {
		t.Fatalf("DB.Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("DB.Exec(create index) error = %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	for id := 1; id <= 24; id++ {
		name := "bulk"
		if id == 7 {
			name = "alice"
		}
		if _, err := tx.Exec("INSERT INTO users VALUES (?, ?, ?)", int32(id), name, strings.Repeat("payload-", 110)); err != nil {
			t.Fatalf("Tx.Exec(insert %d) error = %v", id, err)
		}
	}
	if _, err := tx.Exec("UPDATE users SET note = ? WHERE id = 7", strings.Repeat("grown-", 220)); err != nil {
		t.Fatalf("Tx.Exec(update relocate) error = %v", err)
	}
	if _, err := tx.Exec("DELETE FROM users WHERE id = 6"); err != nil {
		t.Fatalf("Tx.Exec(delete) error = %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertRowsIntSequenceFromDB(t, db, "SELECT id FROM users WHERE name = 'alice' ORDER BY id", 7)
	assertRowsIntSequenceFromDB(t, db, "SELECT id FROM users WHERE name = 'bulk' ORDER BY id",
		1, 2, 3, 4, 5, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24)
	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() after reopen error = %v", err)
	}
}

type userRow struct {
	id   int32
	name string
}

type queryer interface {
	Query(string, ...any) (*Rows, error)
}

func assertUserRows(t *testing.T, q queryer, sql string, want ...userRow) {
	t.Helper()

	rows, err := q.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q) error = %v", sql, err)
	}
	defer rows.Close()

	got := make([]userRow, 0, len(want))
	for rows.Next() {
		var row userRow
		if err := rows.Scan(&row.id, &row.name); err != nil {
			t.Fatalf("rows.Scan() error = %v", err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d (rows = %#v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d = %#v, want %#v (rows = %#v)", i, got[i], want[i], got)
		}
	}
}

func assertRowsIntSequenceFromDB(t *testing.T, db *DB, sql string, want ...int) {
	t.Helper()

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q) error = %v", sql, err)
	}
	defer rows.Close()
	assertRowsIntSequence(t, rows, want...)
}
