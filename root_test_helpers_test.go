package rovadb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func testDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.db")
}

func reopenDB(t *testing.T, path string) *DB {
	t.Helper()

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() after reopen error = %v", err)
	}
	return db
}

func assertSelectBoolRows(t *testing.T, db *DB, sql string, want [][3]any) {
	t.Helper()

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q) error = %v", sql, err)
	}
	defer rows.Close()

	got := make([][3]any, 0, len(want))
	for rows.Next() {
		var id any
		var name string
		var active any
		if err := rows.Scan(&id, &name, &active); err != nil {
			t.Fatalf("Scan(%q) error = %v", sql, err)
		}
		got = append(got, [3]any{numericValueToInt(t, id), name, active})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err(%q) = %v", sql, err)
	}
	if len(got) != len(want) {
		t.Fatalf("rows(%q) len = %d, want %d; got = %#v", sql, len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rows(%q)[%d] = %#v, want %#v", sql, i, got[i], want[i])
		}
	}
}

func assertSelectRealCommitRows(t *testing.T, db *DB, sql string, want [][3]any) {
	t.Helper()

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q) error = %v", sql, err)
	}
	defer rows.Close()

	got := make([][3]any, 0, len(want))
	for rows.Next() {
		var id any
		var label string
		var x any
		if err := rows.Scan(&id, &label, &x); err != nil {
			t.Fatalf("Scan(%q) error = %v", sql, err)
		}
		got = append(got, [3]any{numericValueToInt(t, id), label, x})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err(%q) = %v", sql, err)
	}
	if len(got) != len(want) {
		t.Fatalf("rows(%q) len = %d, want %d; got = %#v", sql, len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rows(%q)[%d] = %#v, want %#v", sql, i, got[i], want[i])
		}
	}
}

func assertSelectTextRows(t *testing.T, db *DB, sql string, want ...string) {
	t.Helper()

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q) error = %v", sql, err)
	}
	defer rows.Close()

	got := make([]string, 0, len(want))
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("Scan(%q) error = %v", sql, err)
		}
		got = append(got, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err(%q) = %v", sql, err)
	}
	if len(got) != len(want) {
		t.Fatalf("rows(%q) len = %d, want %d; got = %#v", sql, len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rows(%q)[%d] = %q, want %q", sql, i, got[i], want[i])
		}
	}
}

func singleCommittedDataPageIDForTest(t *testing.T, db *DB, tableName string) storage.PageID {
	t.Helper()
	if db == nil || db.tables == nil {
		t.Fatal("singleCommittedDataPageIDForTest() requires db tables")
	}
	table := db.tables[tableName]
	if table == nil {
		t.Fatalf("db.tables[%q] = nil", tableName)
	}
	dataPageIDs, err := committedTableDataPageIDs(db.pool, table)
	if err != nil {
		t.Fatalf("committedTableDataPageIDs(%q) error = %v", tableName, err)
	}
	if len(dataPageIDs) != 1 {
		t.Fatalf("len(committedTableDataPageIDs(%q)) = %d, want 1", tableName, len(dataPageIDs))
	}
	return dataPageIDs[0]
}

func privateOwnedDataPageIDForTest(t *testing.T, db *DB, table *executor.Table) storage.PageID {
	t.Helper()
	if db == nil || table == nil {
		t.Fatal("privateOwnedDataPageIDForTest() requires db and table")
	}
	for _, staged := range db.pendingPages {
		if err := storage.ValidateOwnedDataPage(staged.data, table.TableID); err == nil {
			return staged.id
		}
	}
	t.Fatalf("no private owned data page found for table %q", table.Name)
	return 0
}

func assertSystemCatalogRows(t *testing.T, db *DB, wantTables, wantColumns, wantIndexes, wantIndexColumns [][]any) {
	t.Helper()
	assertSystemTableRows(t, db, systemTableTables, wantTables)
	assertSystemTableRows(t, db, systemTableColumns, wantColumns)
	assertSystemTableRows(t, db, systemTableIndexes, wantIndexes)
	assertSystemTableRows(t, db, systemTableIndexColumns, wantIndexColumns)
}

func assertSystemTableRows(t *testing.T, db *DB, tableName string, want [][]any) {
	t.Helper()

	table := db.tables[tableName]
	if table == nil {
		t.Fatalf("db.tables[%q] = nil", tableName)
	}
	rows, err := db.scanTableRows(table)
	if err != nil {
		t.Fatalf("scanTableRows(%q) error = %v", tableName, err)
	}
	got := make([][]any, 0, len(rows))
	for _, row := range rows {
		got = append(got, materializeSystemCatalogRow(row))
	}
	sortSystemCatalogRows(got)
	sortSystemCatalogRows(want)
	if len(got) != len(want) {
		t.Fatalf("%s row count = %d, want %d; got=%#v", tableName, len(got), len(want), got)
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("%s row %d width = %d, want %d; row=%#v", tableName, i, len(got[i]), len(want[i]), got[i])
		}
		for j := range want[i] {
			if got[i][j] != want[i][j] {
				t.Fatalf("%s row %d col %d = %#v, want %#v; got=%#v", tableName, i, j, got[i][j], want[i][j], got)
			}
		}
	}
}

func materializeSystemCatalogRow(row []parser.Value) []any {
	out := make([]any, 0, len(row))
	for _, value := range row {
		switch value.Kind {
		case parser.ValueKindIntegerLiteral, parser.ValueKindSmallInt, parser.ValueKindInt, parser.ValueKindBigInt:
			out = append(out, value.IntegerValue())
		case parser.ValueKindString:
			out = append(out, value.Str)
		case parser.ValueKindBool:
			out = append(out, value.Bool)
		case parser.ValueKindNull:
			out = append(out, nil)
		default:
			out = append(out, value.Any())
		}
	}
	return out
}

func sortSystemCatalogRows(rows [][]any) {
	sort.Slice(rows, func(i, j int) bool {
		left := rows[i]
		right := rows[j]
		limit := len(left)
		if len(right) < limit {
			limit = len(right)
		}
		for k := 0; k < limit; k++ {
			ls := systemCatalogCellKey(left[k])
			rs := systemCatalogCellKey(right[k])
			if ls == rs {
				continue
			}
			return ls < rs
		}
		return len(left) < len(right)
	})
}

func systemCatalogCellKey(value any) string {
	switch v := value.(type) {
	case nil:
		return "0:"
	case bool:
		if v {
			return "1:1"
		}
		return "1:0"
	case int64:
		return fmt.Sprintf("2:%020d", v)
	case string:
		return "3:" + v
	default:
		return "4:"
	}
}

func catalogWithDirectoryRootsForSave(t *testing.T, file *os.File, catalog *storage.CatalogData) *storage.CatalogData {
	t.Helper()
	if catalog == nil {
		return nil
	}
	idMappings, err := storage.ReadDirectoryRootIDMappings(file)
	if err != nil {
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	applied, err := storage.ApplyDirectoryRootIDMappings(catalog, idMappings)
	if err != nil {
		t.Fatalf("ApplyDirectoryRootIDMappings() error = %v", err)
	}
	return applied
}

func rewriteDirectoryRootMappingsForCatalogTables(t *testing.T, file *os.File, catalog *storage.CatalogData) {
	t.Helper()
	if file == nil || catalog == nil {
		t.Fatal("rewriteDirectoryRootMappingsForCatalogTables() requires file and catalog")
	}
	tableIDs := make(map[uint32]struct{}, len(catalog.Tables))
	indexIDs := make(map[uint32]struct{})
	for _, table := range catalog.Tables {
		if table.TableID != 0 {
			tableIDs[table.TableID] = struct{}{}
		}
		for _, index := range table.Indexes {
			if index.IndexID != 0 {
				indexIDs[index.IndexID] = struct{}{}
			}
		}
	}

	idMappings, err := storage.ReadDirectoryRootIDMappings(file)
	if err != nil {
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	filtered := make([]storage.DirectoryRootIDMapping, 0, len(idMappings))
	for _, mapping := range idMappings {
		switch mapping.ObjectType {
		case storage.DirectoryRootMappingObjectTable, storage.DirectoryRootMappingObjectTableHeader:
			if _, ok := tableIDs[mapping.ObjectID]; ok {
				filtered = append(filtered, mapping)
			}
		case storage.DirectoryRootMappingObjectIndex:
			if _, ok := indexIDs[mapping.ObjectID]; ok {
				filtered = append(filtered, mapping)
			}
		}
	}
	if err := storage.WriteDirectoryRootIDMappings(file, filtered); err != nil {
		t.Fatalf("WriteDirectoryRootIDMappings() error = %v", err)
	}
}

type currentCatalogTableForTest struct {
	name     string
	tableID  uint32
	rowCount uint32
	columns  []currentCatalogColumnForTest
	indexes  []currentCatalogIndexForTest
}

type currentCatalogColumnForTest struct {
	name string
	typ  uint8
}

type currentCatalogIndexForTest struct {
	name    string
	indexID uint32
	unique  bool
	columns []currentCatalogIndexColumnForTest
}

type currentCatalogIndexColumnForTest struct {
	name string
	desc bool
}

func currentCatalogBytesForTest(tables []currentCatalogTableForTest) []byte {
	buf := make([]byte, 0, storage.PageSize)
	buf = appendUint32LE(buf, 7)
	buf = appendUint32LE(buf, uint32(len(tables)))
	for _, table := range tables {
		buf = appendStringLE(buf, table.name)
		buf = appendUint32LE(buf, table.tableID)
		buf = appendUint32LE(buf, table.rowCount)
		buf = appendUint16LE(buf, uint16(len(table.columns)))
		for _, column := range table.columns {
			buf = appendStringLE(buf, column.name)
			buf = append(buf, column.typ)
		}
		buf = appendUint16LE(buf, uint16(len(table.indexes)))
		for _, index := range table.indexes {
			buf = appendStringLE(buf, index.name)
			if index.unique {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
			buf = appendUint32LE(buf, index.indexID)
			buf = appendUint16LE(buf, uint16(len(index.columns)))
			for _, column := range index.columns {
				buf = appendStringLE(buf, column.name)
				if column.desc {
					buf = append(buf, 1)
				} else {
					buf = append(buf, 0)
				}
			}
		}
		buf = append(buf, 0)
		buf = appendUint16LE(buf, 0)
	}
	return buf
}

func buildSyntheticCATDIRTablesForTest(db *DB, baselineTables map[string]*executor.Table, tableCount int, prefix string) (map[string]*executor.Table, []stagedPage) {
	if db == nil {
		return nil, nil
	}
	stagedTables := cloneTables(baselineTables)
	stagedPages := make([]stagedPage, 0, tableCount)
	nextTableID := nextSyntheticTableIDForTest(stagedTables)
	nextIndexID := nextSyntheticIndexIDForTest(stagedTables)
	nextRootPageID := db.pager.NextPageID()

	for i := 0; i < tableCount; i++ {
		rootPageID := nextRootPageID
		nextRootPageID++
		tableHeaderPageID := nextRootPageID
		nextRootPageID++
		tableName := fmt.Sprintf("%s_table_%03d_%s", prefix, i, strings.Repeat("x", 32))
		columns := make([]parser.ColumnDef, 0, 41)
		columns = append(columns, parser.ColumnDef{
			Name: fmt.Sprintf("id_%02d_%s", i, strings.Repeat("a", 24)),
			Type: parser.ColumnTypeInt,
		})
		for col := 0; col < 40; col++ {
			columns = append(columns, parser.ColumnDef{
				Name: fmt.Sprintf("col_%02d_%02d_%s", i, col, strings.Repeat(string(rune('b'+(col%5))), 40)),
				Type: parser.ColumnTypeText,
			})
		}
		stagedPages = append(stagedPages, stagedPage{
			id:    rootPageID,
			data:  storage.InitializeTablePage(uint32(rootPageID)),
			isNew: true,
		})
		stagedPages = append(stagedPages, stagedPage{
			id:    tableHeaderPageID,
			data:  storage.InitTableHeaderPage(uint32(tableHeaderPageID), nextTableID),
			isNew: true,
		})
		indexDefs := make([]storage.CatalogIndex, 0, len(columns)-1)
		for col := 1; col < len(columns); col++ {
			indexRootPageID := nextRootPageID
			nextRootPageID++
			indexDefs = append(indexDefs, storage.CatalogIndex{
				Name:       fmt.Sprintf("%s_idx_%02d_%02d_%s", prefix, i, col, strings.Repeat("n", 40)),
				IndexID:    nextIndexID,
				RootPageID: uint32(indexRootPageID),
				Columns:    []storage.CatalogIndexColumn{{Name: columns[col].Name}},
			})
			stagedPages = append(stagedPages, stagedPage{
				id:    indexRootPageID,
				data:  storage.InitIndexLeafPage(uint32(indexRootPageID)),
				isNew: true,
			})
			nextIndexID++
		}
		table := &executor.Table{
			Name:      tableName,
			TableID:   nextTableID,
			Columns:   columns,
			IndexDefs: indexDefs,
		}
		table.SetStorageMeta(rootPageID, 0)
		table.SetPhysicalTableRootMeta(tableHeaderPageID, storage.CurrentTableStorageFormatVersion, 0, 0, 0)
		stagedTables[tableName] = table
		nextTableID++
	}
	return stagedTables, stagedPages
}

func nextSyntheticTableIDForTest(tables map[string]*executor.Table) uint32 {
	var maxID uint32
	for _, table := range tables {
		if table != nil && table.TableID > maxID {
			maxID = table.TableID
		}
	}
	if maxID == 0 {
		return 1
	}
	return maxID + 1
}

func nextSyntheticIndexIDForTest(tables map[string]*executor.Table) uint32 {
	var maxID uint32
	for _, table := range tables {
		if table == nil {
			continue
		}
		for _, indexDef := range table.IndexDefs {
			if indexDef.IndexID > maxID {
				maxID = indexDef.IndexID
			}
		}
	}
	if maxID == 0 {
		return 1
	}
	return maxID + 1
}

func rewriteSyntheticCATDIRTablesForTest(current map[string]*executor.Table, prefix string) map[string]*executor.Table {
	rewritten := cloneTables(current)
	for name, table := range current {
		if table == nil || table.IsSystem || name == "users" {
			continue
		}
		delete(rewritten, name)
		columns := make([]parser.ColumnDef, len(table.Columns))
		for i, column := range table.Columns {
			columns[i] = parser.ColumnDef{
				Name: fmt.Sprintf("%s_col_%02d_%s", prefix, i, strings.Repeat("q", 24)),
				Type: column.Type,
			}
		}
		indexDefs := make([]storage.CatalogIndex, len(table.IndexDefs))
		for i, indexDef := range table.IndexDefs {
			columnName := columns[(i%(len(columns)-1))+1].Name
			indexDefs[i] = storage.CatalogIndex{
				Name:       fmt.Sprintf("%s_idx_%02d_%s", prefix, i, strings.Repeat("z", 28)),
				Unique:     indexDef.Unique,
				IndexID:    indexDef.IndexID,
				RootPageID: indexDef.RootPageID,
				Columns:    []storage.CatalogIndexColumn{{Name: columnName}},
			}
		}
		updated := &executor.Table{
			Name:      fmt.Sprintf("%s_table_%s", prefix, strings.Repeat("y", 20)),
			TableID:   table.TableID,
			IsSystem:  false,
			Columns:   columns,
			Rows:      append([][]parser.Value(nil), table.Rows...),
			IndexDefs: indexDefs,
		}
		updated.SetStorageMeta(table.RootPageID(), table.PersistedRowCount())
		updated.SetPhysicalTableRootMeta(table.TableHeaderPageID(), table.TableStorageFormatVersion(), table.FirstSpaceMapPageID(), table.OwnedDataPageCount(), table.OwnedSpaceMapPageCount())
		rewritten[updated.Name] = updated
	}
	return rewritten
}

func assertCATDIRModeForPath(t *testing.T, path string, want uint32) {
	t.Helper()
	mode, _, _, _ := readCATDIRStateForPath(t, path)
	if mode != want {
		t.Fatalf("CAT/DIR mode = %d, want %d", mode, want)
	}
}

func readCATDIRStateForPath(t *testing.T, path string) (mode uint32, head uint32, count uint32, freeListHead uint32) {
	t.Helper()
	rawDB, pager := openRawStorage(t, path)
	defer rawDB.Close()

	page, err := pager.Get(storage.DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.Get(directory) error = %v", err)
	}
	mode, err = storage.DirectoryCATDIRStorageMode(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIRStorageMode() error = %v", err)
	}
	head, err = storage.DirectoryCATDIROverflowHeadPageID(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID() error = %v", err)
	}
	count, err = storage.DirectoryCATDIROverflowPageCount(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowPageCount() error = %v", err)
	}
	freeListHead, err = storage.DirectoryFreeListHead(page.Data())
	if err != nil {
		t.Fatalf("DirectoryFreeListHead() error = %v", err)
	}
	return mode, head, count, freeListHead
}

func stagedPageIDsForTest(pages []stagedPage) []storage.PageID {
	ids := make([]storage.PageID, 0, len(pages))
	for _, page := range pages {
		ids = append(ids, page.id)
	}
	return ids
}

func readCATDIROverflowChainIDsForPath(t *testing.T, path string, head uint32, count uint32) []storage.PageID {
	t.Helper()
	rawDB, pager := openRawStorage(t, path)
	defer rawDB.Close()

	ids, err := storage.ReadCatalogOverflowChainPageIDs(pager, storage.PageID(head), count)
	if err != nil {
		t.Fatalf("ReadCatalogOverflowChainPageIDs() error = %v", err)
	}
	return ids
}

func allocateFreePagesFromHeadForTest(t *testing.T, path string, freeListHead uint32, count int) []storage.PageID {
	t.Helper()
	rawDB, pager := openRawStorage(t, path)
	defer rawDB.Close()

	allocator := storage.PageAllocator{
		NextPageID: uint32(pager.NextPageID()),
		FreePage: storage.FreePageState{
			HeadPageID: freeListHead,
		},
		ReadFreeNext: func(pageID uint32) (uint32, error) {
			pageData, err := pager.ReadPage(storage.PageID(pageID))
			if err != nil {
				return 0, err
			}
			return storage.FreePageNext(pageData)
		},
	}

	pageIDs := make([]storage.PageID, 0, count)
	for i := 0; i < count; i++ {
		pageID, reused, err := allocator.Allocate()
		if err != nil {
			t.Fatalf("allocator.Allocate() error = %v", err)
		}
		if !reused {
			t.Fatalf("allocator.Allocate() reused = false, want reclaimed free page")
		}
		pageIDs = append(pageIDs, storage.PageID(pageID))
	}
	return pageIDs
}

func assertAutocommitClean(t *testing.T, db *DB, path string) {
	t.Helper()

	if db.txn != nil {
		t.Fatalf("db.txn = %#v, want nil", db.txn)
	}
	if len(db.pager.DirtyPages()) != 0 {
		t.Fatalf("len(db.pager.DirtyPages()) = %d, want 0", len(db.pager.DirtyPages()))
	}
	if len(db.pager.DirtyPagesWithOriginals()) != 0 {
		t.Fatalf("len(db.pager.DirtyPagesWithOriginals()) = %d, want 0", len(db.pager.DirtyPagesWithOriginals()))
	}
	if _, err := os.Stat(storage.JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists", err)
	}
}

func corruptedIndexCatalogBytes(_ uint32) []byte {
	buf := make([]byte, 0, storage.PageSize)
	buf = appendUint32LE(buf, 8)
	buf = appendUint32LE(buf, 1)
	buf = appendStringLE(buf, "users")
	buf = appendUint32LE(buf, 7)
	buf = appendUint32LE(buf, 0)
	buf = appendUint16LE(buf, 1)
	buf = appendStringLE(buf, "id")
	buf = append(buf, storage.CatalogColumnTypeInt)
	buf = append(buf, 0)
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

func assertIndexConsistency(t *testing.T, table *executor.Table) {
	t.Helper()

	if table == nil || len(table.IndexDefs) == 0 {
		return
	}

	for _, indexDef := range table.IndexDefs {
		if indexDef.IndexID == 0 {
			t.Fatalf("index %q IndexID = 0, want nonzero", indexDef.Name)
		}
		if indexDef.RootPageID == 0 {
			t.Fatalf("index %q RootPageID = 0, want nonzero", indexDef.Name)
		}
		if len(indexDef.Columns) == 0 {
			t.Fatalf("index %q Columns = %#v, want non-empty", indexDef.Name, indexDef.Columns)
		}
	}
}

func assertQueryIntRows(t *testing.T, db *DB, sql string, want ...int) {
	t.Helper()
	got := collectIntRows(t, db, sql)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("collectIntRows(%q) = %#v, want %#v", sql, got, want)
	}
}

func collectIntRows(t *testing.T, db *DB, sql string) []int {
	t.Helper()

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q) error = %v", sql, err)
	}
	defer rows.Close()

	got := []int{}
	for rows.Next() {
		var v any
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan(%q) error = %v", sql, err)
		}
		got = append(got, numericValueToInt(t, v))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err(%q) = %v", sql, err)
	}
	return got
}

func itoa(v int) string {
	return strconv.Itoa(v)
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
	searchKey, err := storage.EncodeIndexKey(storageValuesFromParser(keyValues))
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

func encodedOutOfRangeIntRow(t *testing.T, value int64) []byte {
	t.Helper()

	data := make([]byte, 0, 11)
	data = append(data, 1, 0, 1)
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], uint64(value))
	data = append(data, raw[:]...)
	return data
}

func assertSelectRowsWithNames(t *testing.T, db *DB, sql string, want [][2]any) {
	t.Helper()

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q) error = %v", sql, err)
	}
	defer rows.Close()

	got := make([][2]any, 0, len(want))
	for rows.Next() {
		var id any
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan(%q) error = %v", sql, err)
		}
		got = append(got, [2]any{numericValueToInt(t, id), name})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err(%q) = %v", sql, err)
	}
	if len(got) != len(want) {
		t.Fatalf("rows(%q) len = %d, want %d; got = %#v", sql, len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rows(%q)[%d] = %#v, want %#v", sql, i, got[i], want[i])
		}
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

func userTableCount(tables map[string]*executor.Table) int {
	count := 0
	for _, table := range tables {
		if table == nil || table.IsSystem {
			continue
		}
		count++
	}
	return count
}

func assertIntRows(t *testing.T, rows *Rows, want ...int) {
	t.Helper()

	got := make([]int, 0, len(want))
	for rows.Next() {
		var value any
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		got = append(got, numericValueToInt(t, value))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err() = %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d (rows = %v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d = %d, want %d (rows = %v)", i, got[i], want[i], got)
		}
	}
}

func assertSelectIntRows(t *testing.T, db *DB, sql string, want ...int) {
	t.Helper()

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q) error = %v", sql, err)
	}
	defer rows.Close()

	assertIntRows(t, rows, want...)
}

func numericValueToInt(t *testing.T, value any) int {
	t.Helper()

	switch v := value.(type) {
	case int:
		return v
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	default:
		t.Fatalf("numeric value = %#v (%T), want integer-compatible type", value, value)
		return 0
	}
}

type strictIndexRootMappingForTest struct {
	indexID    uint32
	rootPageID storage.PageID
}

type strictTablePhysicalMetaForTest struct {
	tableID           uint32
	rowRootPageID     storage.PageID
	tableHeaderPageID storage.PageID
	spaceMapPageID    storage.PageID
	dataPageIDs       []storage.PageID
	indexRoots        []strictIndexRootMappingForTest
}

func persistStrictPhysicalMetaForTests(t *testing.T, file *os.File, pager *storage.Pager, tables []strictTablePhysicalMetaForTest) []strictTablePhysicalMetaForTest {
	t.Helper()
	if file == nil || pager == nil {
		t.Fatal("persistStrictPhysicalMetaForTests() requires file and pager")
	}

	mappings := make([]storage.DirectoryRootIDMapping, 0, len(tables)*3)
	updated := make([]strictTablePhysicalMetaForTest, 0, len(tables))
	for _, table := range tables {
		if table.tableID == 0 || table.rowRootPageID == 0 {
			t.Fatal("persistStrictPhysicalMetaForTests() requires non-zero table ID and row root")
		}
		rowRootPage, err := pager.Get(table.rowRootPageID)
		if err != nil {
			t.Fatalf("pager.Get(row root %d) error = %v", table.rowRootPageID, err)
		}
		payloads, err := storage.ReadRowsFromTablePage(rowRootPage)
		if err != nil {
			t.Fatalf("ReadRowsFromTablePage(%d) error = %v", table.rowRootPageID, err)
		}
		if table.tableHeaderPageID == 0 {
			headerPage := pager.NewPage()
			clear(headerPage.Data())
			copy(headerPage.Data(), storage.InitTableHeaderPage(uint32(headerPage.ID()), table.tableID))
			table.tableHeaderPageID = headerPage.ID()
		}
		if len(payloads) != 0 {
			if table.spaceMapPageID == 0 {
				spaceMapPage := pager.NewPage()
				clear(spaceMapPage.Data())
				copy(spaceMapPage.Data(), storage.InitSpaceMapPage(uint32(spaceMapPage.ID()), table.tableID))
				table.spaceMapPageID = spaceMapPage.ID()
			}
			if len(table.dataPageIDs) == 0 {
				dataPage := pager.NewPage()
				clear(dataPage.Data())
				copy(dataPage.Data(), storage.InitOwnedDataPage(uint32(dataPage.ID()), table.tableID))
				for _, payload := range payloads {
					if _, err := storage.InsertRowIntoTablePage(dataPage.Data(), payload); err != nil {
						t.Fatalf("InsertRowIntoTablePage(%d) error = %v", dataPage.ID(), err)
					}
				}
				table.dataPageIDs = append(table.dataPageIDs, dataPage.ID())
			}

			headerPage, err := pager.Get(table.tableHeaderPageID)
			if err != nil {
				t.Fatalf("pager.Get(header %d) error = %v", table.tableHeaderPageID, err)
			}
			pager.MarkDirty(headerPage)
			clear(headerPage.Data())
			copy(headerPage.Data(), storage.InitTableHeaderPage(uint32(table.tableHeaderPageID), table.tableID))
			if err := storage.SetTableHeaderFirstSpaceMapPageID(headerPage.Data(), uint32(table.spaceMapPageID)); err != nil {
				t.Fatalf("SetTableHeaderFirstSpaceMapPageID() error = %v", err)
			}
			if err := storage.SetTableHeaderOwnedDataPageCount(headerPage.Data(), uint32(len(table.dataPageIDs))); err != nil {
				t.Fatalf("SetTableHeaderOwnedDataPageCount() error = %v", err)
			}
			if err := storage.SetTableHeaderOwnedSpaceMapPageCount(headerPage.Data(), 1); err != nil {
				t.Fatalf("SetTableHeaderOwnedSpaceMapPageCount() error = %v", err)
			}

			spaceMapPage, err := pager.Get(table.spaceMapPageID)
			if err != nil {
				t.Fatalf("pager.Get(space map %d) error = %v", table.spaceMapPageID, err)
			}
			pager.MarkDirty(spaceMapPage)
			clear(spaceMapPage.Data())
			copy(spaceMapPage.Data(), storage.InitSpaceMapPage(uint32(table.spaceMapPageID), table.tableID))
			for _, dataPageID := range table.dataPageIDs {
				dataPage, err := pager.Get(dataPageID)
				if err != nil {
					t.Fatalf("pager.Get(data %d) error = %v", dataPageID, err)
				}
				bucket, err := storage.TablePageFreeSpaceBucket(dataPage.Data())
				if err != nil {
					t.Fatalf("TablePageFreeSpaceBucket(%d) error = %v", dataPageID, err)
				}
				if _, err := storage.AppendSpaceMapEntry(spaceMapPage.Data(), storage.SpaceMapEntry{
					DataPageID:      dataPageID,
					FreeSpaceBucket: bucket,
				}); err != nil {
					t.Fatalf("AppendSpaceMapEntry(%d) error = %v", table.spaceMapPageID, err)
				}
			}
		}
		mappings = append(mappings, storage.DirectoryRootIDMapping{
			ObjectType: storage.DirectoryRootMappingObjectTable,
			ObjectID:   table.tableID,
			RootPageID: uint32(table.rowRootPageID),
		})
		mappings = append(mappings, storage.DirectoryRootIDMapping{
			ObjectType: storage.DirectoryRootMappingObjectTableHeader,
			ObjectID:   table.tableID,
			RootPageID: uint32(table.tableHeaderPageID),
		})
		for _, indexRoot := range table.indexRoots {
			mappings = append(mappings, storage.DirectoryRootIDMapping{
				ObjectType: storage.DirectoryRootMappingObjectIndex,
				ObjectID:   indexRoot.indexID,
				RootPageID: uint32(indexRoot.rootPageID),
			})
		}
		updated = append(updated, table)
	}
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := storage.WriteDirectoryRootIDMappings(file, mappings); err != nil {
		t.Fatalf("WriteDirectoryRootIDMappings() error = %v", err)
	}
	return updated
}

const (
	testDirectoryCATDIRModeOffset       = 40
	testDirectoryCATDIROverflowHeadOff  = 44
	testDirectoryCATDIROverflowCountOff = 48
	testDirectoryCATDIRPayloadBytesOff  = 52
	testDirectoryCatalogOffset          = 56
)

func openRawStorage(t testFataler, path string) (*storage.DBFile, *storage.Pager) {
	t.Helper()

	dbFile, err := storage.OpenOrCreate(path)
	if err != nil {
		t.Fatalf("storage.OpenOrCreate() error = %v", err)
	}
	pager, err := storage.NewPager(dbFile.File())
	if err != nil {
		_ = dbFile.Close()
		t.Fatalf("storage.NewPager() error = %v", err)
	}
	return dbFile, pager
}

func writeMalformedCatalogPage(t testFataler, pager *storage.Pager, data []byte) {
	t.Helper()

	page, err := pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get(0) error = %v", err)
	}
	clear(page.Data())
	copy(page.Data(), storage.InitDirectoryPage(uint32(storage.DirectoryControlPageID), storage.CurrentDBFormatVersion))
	binary.LittleEndian.PutUint32(page.Data()[testDirectoryCATDIRModeOffset:testDirectoryCATDIRModeOffset+4], storage.DirectoryCATDIRStorageModeEmbedded)
	binary.LittleEndian.PutUint32(page.Data()[testDirectoryCATDIROverflowHeadOff:testDirectoryCATDIROverflowHeadOff+4], 0)
	binary.LittleEndian.PutUint32(page.Data()[testDirectoryCATDIROverflowCountOff:testDirectoryCATDIROverflowCountOff+4], 0)
	binary.LittleEndian.PutUint32(page.Data()[testDirectoryCATDIRPayloadBytesOff:testDirectoryCATDIRPayloadBytesOff+4], uint32(len(data)))
	copy(page.Data()[testDirectoryCatalogOffset:], data)
	if err := storage.RecomputePageChecksum(page.Data()); err != nil {
		t.Fatalf("storage.RecomputePageChecksum() error = %v", err)
	}
	page.MarkDirty()
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
}

func writeMalformedCatalogPageWithIDMappings(t testFataler, pager *storage.Pager, data []byte, mappings []storage.DirectoryRootIDMapping) {
	t.Helper()

	page, err := pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get(0) error = %v", err)
	}
	clear(page.Data())
	copy(page.Data(), storage.InitDirectoryPage(uint32(storage.DirectoryControlPageID), storage.CurrentDBFormatVersion))
	binary.LittleEndian.PutUint32(page.Data()[testDirectoryCATDIRModeOffset:testDirectoryCATDIRModeOffset+4], storage.DirectoryCATDIRStorageModeEmbedded)
	binary.LittleEndian.PutUint32(page.Data()[testDirectoryCATDIROverflowHeadOff:testDirectoryCATDIROverflowHeadOff+4], 0)
	binary.LittleEndian.PutUint32(page.Data()[testDirectoryCATDIROverflowCountOff:testDirectoryCATDIROverflowCountOff+4], 0)
	binary.LittleEndian.PutUint32(page.Data()[testDirectoryCATDIRPayloadBytesOff:testDirectoryCATDIRPayloadBytesOff+4], uint32(len(data)))
	copy(page.Data()[testDirectoryCatalogOffset:], data)

	if len(mappings) > 0 {
		offset := testDirectoryCatalogOffset + len(data) + 16
		payload := make([]byte, 0, len(mappings)*9)
		for _, mapping := range mappings {
			payload = append(payload, mapping.ObjectType)
			var raw [4]byte
			binary.LittleEndian.PutUint32(raw[:], mapping.ObjectID)
			payload = append(payload, raw[:]...)
			binary.LittleEndian.PutUint32(raw[:], mapping.RootPageID)
			payload = append(payload, raw[:]...)
		}
		binary.LittleEndian.PutUint32(page.Data()[offset:offset+4], uint32(len(mappings)))
		binary.LittleEndian.PutUint32(page.Data()[offset+4:offset+8], uint32(len(payload)))
		copy(page.Data()[offset+8:], payload)
	}
	if err := storage.RecomputePageChecksum(page.Data()); err != nil {
		t.Fatalf("storage.RecomputePageChecksum() error = %v", err)
	}
	page.MarkDirty()
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
}

func writeOverflowCatalogPageWithIDMappings(t testFataler, pager *storage.Pager, payloadBytes uint32, headPageID storage.PageID, pageCount uint32, freeListHead uint32, mappings []storage.DirectoryRootIDMapping) {
	t.Helper()

	page, err := pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get(0) error = %v", err)
	}
	clear(page.Data())
	copy(page.Data(), storage.InitDirectoryPage(uint32(storage.DirectoryControlPageID), storage.CurrentDBFormatVersion))
	binary.LittleEndian.PutUint32(page.Data()[36:40], freeListHead)
	binary.LittleEndian.PutUint32(page.Data()[testDirectoryCATDIRModeOffset:testDirectoryCATDIRModeOffset+4], storage.DirectoryCATDIRStorageModeOverflow)
	binary.LittleEndian.PutUint32(page.Data()[testDirectoryCATDIROverflowHeadOff:testDirectoryCATDIROverflowHeadOff+4], uint32(headPageID))
	binary.LittleEndian.PutUint32(page.Data()[testDirectoryCATDIROverflowCountOff:testDirectoryCATDIROverflowCountOff+4], pageCount)
	binary.LittleEndian.PutUint32(page.Data()[testDirectoryCATDIRPayloadBytesOff:testDirectoryCATDIRPayloadBytesOff+4], payloadBytes)

	if len(mappings) > 0 {
		offset := testDirectoryCatalogOffset + 16
		payload := make([]byte, 0, len(mappings)*9)
		for _, mapping := range mappings {
			payload = append(payload, mapping.ObjectType)
			var raw [4]byte
			binary.LittleEndian.PutUint32(raw[:], mapping.ObjectID)
			payload = append(payload, raw[:]...)
			binary.LittleEndian.PutUint32(raw[:], mapping.RootPageID)
			payload = append(payload, raw[:]...)
		}
		binary.LittleEndian.PutUint32(page.Data()[offset:offset+4], uint32(len(mappings)))
		binary.LittleEndian.PutUint32(page.Data()[offset+4:offset+8], uint32(len(payload)))
		copy(page.Data()[offset+8:], payload)
	}
	if err := storage.RecomputePageChecksum(page.Data()); err != nil {
		t.Fatalf("storage.RecomputePageChecksum() error = %v", err)
	}
	page.MarkDirty()
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
}

type testFataler interface {
	Helper()
	Fatalf(format string, args ...any)
}

func errInvalidArgumentForTest(msg string) error {
	return fmt.Errorf("test assertion failed: %s", msg)
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
	var id1 int32
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
	var id2 int32
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

func committedLocatorsByIDForTest(t *testing.T, db *DB, tableName string) map[int]storage.RowLocator {
	t.Helper()
	if db == nil || db.pool == nil {
		t.Fatal("committedLocatorsByIDForTest() requires open db with pool")
	}
	table := db.tables[tableName]
	if table == nil {
		t.Fatalf("db.tables[%q] = nil", tableName)
	}
	locators, rows, err := loadCommittedTableRowsAndLocators(db.pool, table)
	if err != nil {
		t.Fatalf("loadCommittedTableRowsAndLocators() error = %v", err)
	}
	if len(locators) != len(rows) {
		t.Fatalf("len(locators) = %d, len(rows) = %d", len(locators), len(rows))
	}
	byID := make(map[int]storage.RowLocator, len(rows))
	for i, row := range rows {
		if !row[0].IsInteger() {
			t.Fatalf("row[0] = %#v, want int value", row[0])
		}
		byID[int(row[0].IntegerValue())] = locators[i]
	}
	return byID
}

func verifyPhysicalTableInventoryMatchesMetadata(t *testing.T, db *DB, tableName string) {
	t.Helper()
	table := db.tables[tableName]
	if table == nil {
		t.Fatalf("db.tables[%q] = nil", tableName)
	}
	spaceMapPageIDs, dataPageIDs, err := committedTablePhysicalStorageInventory(db.pool, table)
	if err != nil {
		t.Fatalf("committedTablePhysicalStorageInventory() error = %v", err)
	}
	if uint32(len(spaceMapPageIDs)) != table.OwnedSpaceMapPageCount() {
		t.Fatalf("len(spaceMapPageIDs) = %d, want %d", len(spaceMapPageIDs), table.OwnedSpaceMapPageCount())
	}
	if uint32(len(dataPageIDs)) != table.OwnedDataPageCount() {
		t.Fatalf("len(dataPageIDs) = %d, want %d", len(dataPageIDs), table.OwnedDataPageCount())
	}
}

func assertMaterializedRowsEqual(t *testing.T, got [][]any, want [][]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d (%#v)", len(got), len(want), got)
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("row %d width = %d, want %d (%#v)", i, len(got[i]), len(want[i]), got[i])
		}
		for j := range want[i] {
			if got[i][j] != want[i][j] {
				t.Fatalf("row %d col %d = %#v, want %#v", i, j, got[i][j], want[i][j])
			}
		}
	}
}
