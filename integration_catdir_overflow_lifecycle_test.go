package rovadb

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestCATDIRDualModeLifecycleAcrossReopens(t *testing.T) {
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
	if err := db.VerifySystemCatalogDigest(); err != nil {
		t.Fatalf("VerifySystemCatalogDigest() baseline error = %v", err)
	}
	baselineDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest() baseline error = %v", err)
	}
	baselineTables := cloneTables(db.tables)
	if err := db.Close(); err != nil {
		t.Fatalf("Close() baseline error = %v", err)
	}

	assertCATDIRModeForPath(t, path, storage.DirectoryCATDIRStorageModeEmbedded)

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() baseline error = %v", err)
	}
	reopenBaselineDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest() reopened baseline error = %v", err)
	}
	if reopenBaselineDigest != baselineDigest {
		t.Fatalf("reopened baseline digest = %q, want %q", reopenBaselineDigest, baselineDigest)
	}
	if err := db.VerifySystemCatalogDigest(); err != nil {
		t.Fatalf("VerifySystemCatalogDigest() reopened baseline error = %v", err)
	}

	largeTables, largePages := buildSyntheticCATDIRTablesForTest(db, baselineTables, 1, "promote")
	if err := db.persistCatalogState(largeTables, largePages); err != nil {
		t.Fatalf("persistCatalogState(large) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() after promotion error = %v", err)
	}

	assertCATDIRModeForPath(t, path, storage.DirectoryCATDIRStorageModeOverflow)

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() overflow error = %v", err)
	}
	overflowDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest() overflow error = %v", err)
	}
	if overflowDigest == baselineDigest {
		t.Fatalf("overflow digest = %q, want different from baseline %q", overflowDigest, baselineDigest)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() overflow error = %v", err)
	}

	assertCATDIRModeForPath(t, path, storage.DirectoryCATDIRStorageModeOverflow)

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second reopen Open() overflow error = %v", err)
	}
	overflowDigestReopen, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest() second overflow reopen error = %v", err)
	}
	if overflowDigestReopen != overflowDigest {
		t.Fatalf("overflow digest after reopen = %q, want %q", overflowDigestReopen, overflowDigest)
	}

	freedPages, err := db.buildFreedPages(stagedPageIDsForTest(largePages)...)
	if err != nil {
		t.Fatalf("buildFreedPages(demote baseline) error = %v", err)
	}
	if err := db.persistCatalogState(cloneTables(baselineTables), freedPages); err != nil {
		t.Fatalf("persistCatalogState(demote baseline) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() after demotion error = %v", err)
	}

	assertCATDIRModeForPath(t, path, storage.DirectoryCATDIRStorageModeEmbedded)

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() demoted error = %v", err)
	}
	defer db.Close()

	demotedDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest() demoted error = %v", err)
	}
	if demotedDigest != baselineDigest {
		t.Fatalf("demoted digest = %q, want baseline %q", demotedDigest, baselineDigest)
	}
	if err := db.VerifySystemCatalogDigest(); err != nil {
		t.Fatalf("VerifySystemCatalogDigest() demoted error = %v", err)
	}
	assertCATDIRModeForPath(t, path, storage.DirectoryCATDIRStorageModeEmbedded)
}

func TestCATDIROverflowRewriteReclaimsAndPagesBecomeReusable(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	baselineTables := cloneTables(db.tables)

	largeA, pagesA := buildSyntheticCATDIRTablesForTest(db, baselineTables, 1, "overflow_a")
	if err := db.persistCatalogState(largeA, pagesA); err != nil {
		t.Fatalf("persistCatalogState(largeA) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() largeA error = %v", err)
	}

	mode, head, count, freeListHead := readCATDIRStateForPath(t, path)
	if mode != storage.DirectoryCATDIRStorageModeOverflow {
		t.Fatalf("CAT/DIR mode after largeA = %d, want %d", mode, storage.DirectoryCATDIRStorageModeOverflow)
	}
	oldChainIDs := readCATDIROverflowChainIDsForPath(t, path, head, count)
	if len(oldChainIDs) == 0 {
		t.Fatal("old CAT/DIR overflow chain ids = empty, want non-empty")
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() after largeA error = %v", err)
	}
	largeB := rewriteSyntheticCATDIRTablesForTest(db.tables, "overflow_b")
	if err := db.persistCatalogState(largeB, nil); err != nil {
		t.Fatalf("persistCatalogState(largeB) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() largeB error = %v", err)
	}

	mode, head, count, freeListHead = readCATDIRStateForPath(t, path)
	if mode != storage.DirectoryCATDIRStorageModeOverflow {
		t.Fatalf("CAT/DIR mode after largeB = %d, want %d", mode, storage.DirectoryCATDIRStorageModeOverflow)
	}
	if freeListHead == 0 {
		t.Fatal("DirectoryFreeListHead() after overflow rewrite = 0, want reusable reclaimed pages")
	}
	reused := allocateFreePagesFromHeadForTest(t, path, freeListHead, 1)
	if len(reused) != 1 {
		t.Fatalf("len(reused free pages) = %d, want 1", len(reused))
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("final reopen Open() error = %v", err)
	}
	defer db.Close()

	digestA, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest() final overflow error = %v", err)
	}
	if digestA == "" {
		t.Fatal("SchemaDigest() final overflow = empty, want non-empty")
	}
	if _, ok := db.tables["users"]; !ok {
		t.Fatal(`db.tables["users"] missing after overflow->overflow lifecycle`)
	}
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
