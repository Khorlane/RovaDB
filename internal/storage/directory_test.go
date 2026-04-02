package storage

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"
)

func TestInitDirectoryPageCreatesValidPage(t *testing.T) {
	page := InitDirectoryPage(uint32(DirectoryControlPageID), CurrentDBFormatVersion)

	if err := ValidateDirectoryPage(page); err != nil {
		t.Fatalf("ValidateDirectoryPage() error = %v", err)
	}
	formatVersion, err := DirectoryFormatVersion(page)
	if err != nil {
		t.Fatalf("DirectoryFormatVersion() error = %v", err)
	}
	if formatVersion != CurrentDBFormatVersion {
		t.Fatalf("DirectoryFormatVersion() = %d, want %d", formatVersion, CurrentDBFormatVersion)
	}
	freeListHead, err := DirectoryFreeListHead(page)
	if err != nil {
		t.Fatalf("DirectoryFreeListHead() error = %v", err)
	}
	if freeListHead != 0 {
		t.Fatalf("DirectoryFreeListHead() = %d, want 0", freeListHead)
	}
}

func TestDirectoryFormatVersionRoundTrip(t *testing.T) {
	page := InitDirectoryPage(uint32(DirectoryControlPageID), CurrentDBFormatVersion)

	if err := SetDirectoryFormatVersion(page, CurrentDBFormatVersion); err != nil {
		t.Fatalf("SetDirectoryFormatVersion() error = %v", err)
	}
	got, err := DirectoryFormatVersion(page)
	if err != nil {
		t.Fatalf("DirectoryFormatVersion() error = %v", err)
	}
	if got != CurrentDBFormatVersion {
		t.Fatalf("DirectoryFormatVersion() = %d, want %d", got, CurrentDBFormatVersion)
	}
}

func TestDirectoryFreeListHeadRoundTrip(t *testing.T) {
	page := InitDirectoryPage(uint32(DirectoryControlPageID), CurrentDBFormatVersion)

	if err := SetDirectoryFreeListHead(page, 27); err != nil {
		t.Fatalf("SetDirectoryFreeListHead() error = %v", err)
	}
	got, err := DirectoryFreeListHead(page)
	if err != nil {
		t.Fatalf("DirectoryFreeListHead() error = %v", err)
	}
	if got != 27 {
		t.Fatalf("DirectoryFreeListHead() = %d, want 27", got)
	}
}

func TestDirectoryCheckpointMetadataRoundTrip(t *testing.T) {
	page, err := BuildCatalogPageData(&CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    1,
				RootPageID: 1,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
			},
		},
	})
	if err != nil {
		t.Fatalf("BuildCatalogPageData() error = %v", err)
	}

	if err := SetDirectoryLastCheckpointLSN(page, 41); err != nil {
		t.Fatalf("SetDirectoryLastCheckpointLSN() error = %v", err)
	}
	if err := SetDirectoryLastCheckpointPageCount(page, 7); err != nil {
		t.Fatalf("SetDirectoryLastCheckpointPageCount() error = %v", err)
	}

	lsn, err := DirectoryLastCheckpointLSN(page)
	if err != nil {
		t.Fatalf("DirectoryLastCheckpointLSN() error = %v", err)
	}
	if lsn != 41 {
		t.Fatalf("DirectoryLastCheckpointLSN() = %d, want 41", lsn)
	}
	pageCount, err := DirectoryLastCheckpointPageCount(page)
	if err != nil {
		t.Fatalf("DirectoryLastCheckpointPageCount() error = %v", err)
	}
	if pageCount != 7 {
		t.Fatalf("DirectoryLastCheckpointPageCount() = %d, want 7", pageCount)
	}
}

func TestOlderWrappedDirectoryPayloadRejectsLegacyCatalogPayload(t *testing.T) {
	catalogPayload := buildLegacyCatalogPageDataForTest(1, &CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				RootPageID: 1,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
			},
		},
	})
	page := InitDirectoryPage(uint32(DirectoryControlPageID), CurrentDBFormatVersion)
	copy(page[directoryCatalogOffset:], catalogPayload)

	if _, err := DirectoryLastCheckpointLSN(page); !errors.Is(err, errUnsupportedCatalogPage) {
		t.Fatalf("DirectoryLastCheckpointLSN() error = %v, want %v", err, errUnsupportedCatalogPage)
	}
	if _, err := DirectoryLastCheckpointPageCount(page); !errors.Is(err, errUnsupportedCatalogPage) {
		t.Fatalf("DirectoryLastCheckpointPageCount() error = %v, want %v", err, errUnsupportedCatalogPage)
	}
}

func TestReadDirectoryCheckpointMetadataPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "directory-checkpoint.db")

	dbFile, err := OpenOrCreate(path)
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	if err := EnsureDirectoryPage(dbFile.File()); err != nil {
		t.Fatalf("EnsureDirectoryPage() error = %v", err)
	}
	pager, err := NewPager(dbFile.File())
	if err != nil {
		t.Fatalf("NewPager() error = %v", err)
	}
	if err := SaveCatalog(pager, &CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    1,
				RootPageID: 1,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
			},
		},
	}); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	page, err := ReadDirectoryPage(dbFile.File())
	if err != nil {
		t.Fatalf("ReadDirectoryPage() error = %v", err)
	}
	if err := SetDirectoryLastCheckpointLSN(page, 99); err != nil {
		t.Fatalf("SetDirectoryLastCheckpointLSN() error = %v", err)
	}
	if err := SetDirectoryLastCheckpointPageCount(page, 3); err != nil {
		t.Fatalf("SetDirectoryLastCheckpointPageCount() error = %v", err)
	}
	if err := WriteDirectoryPage(dbFile.File(), page); err != nil {
		t.Fatalf("WriteDirectoryPage() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	dbFile, err = OpenOrCreate(path)
	if err != nil {
		t.Fatalf("reopen OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	meta, err := ReadDirectoryCheckpointMetadata(dbFile.File())
	if err != nil {
		t.Fatalf("ReadDirectoryCheckpointMetadata() error = %v", err)
	}
	if meta.LastCheckpointLSN != 99 || meta.LastCheckpointPageCount != 3 {
		t.Fatalf("checkpoint metadata = %#v, want LSN=99 pageCount=3", meta)
	}
}

func TestValidateDirectoryPageRejectsWrongPageType(t *testing.T) {
	page := InitDirectoryPage(uint32(DirectoryControlPageID), CurrentDBFormatVersion)
	page[pageHeaderOffsetPageType] = byte(PageTypeTable)
	page[pageHeaderOffsetPageType+1] = 0

	err := ValidateDirectoryPage(page)
	if !errors.Is(err, errCorruptedDirectoryPage) {
		t.Fatalf("ValidateDirectoryPage() error = %v, want %v", err, errCorruptedDirectoryPage)
	}
}

func TestValidateDirectoryPageRejectsInvalidSize(t *testing.T) {
	err := ValidateDirectoryPage(make([]byte, PageSize-1))
	if !errors.Is(err, errCorruptedDirectoryPage) {
		t.Fatalf("ValidateDirectoryPage() error = %v, want %v", err, errCorruptedDirectoryPage)
	}
}

func TestValidateDirectoryPageRejectsUnsupportedFormatVersion(t *testing.T) {
	page := InitDirectoryPage(uint32(DirectoryControlPageID), CurrentDBFormatVersion+1)

	err := ValidateDirectoryPage(page)
	if !errors.Is(err, errCorruptedDirectoryPage) {
		t.Fatalf("ValidateDirectoryPage() error = %v, want %v", err, errCorruptedDirectoryPage)
	}
}

func TestEnsureDirectoryPageInitializesMissingPage(t *testing.T) {
	dbFile, err := OpenOrCreate(filepath.Join(t.TempDir(), "directory.db"))
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	if err := EnsureDirectoryPage(dbFile.File()); err != nil {
		t.Fatalf("EnsureDirectoryPage() error = %v", err)
	}

	page := make([]byte, PageSize)
	if _, err := dbFile.File().ReadAt(page, pageOffset(DirectoryControlPageID)); err != nil {
		t.Fatalf("ReadAt() error = %v", err)
	}
	if err := ValidateDirectoryPage(page); err != nil {
		t.Fatalf("ValidateDirectoryPage() error = %v", err)
	}
}

func TestEnsureDirectoryPageRejectsLegacyCatalogPage(t *testing.T) {
	dbFile, err := OpenOrCreate(filepath.Join(t.TempDir(), "legacy-directory.db"))
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	legacyPage := buildLegacyCatalogPageDataForTest(1, &CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				RootPageID: 1,
				RowCount:   0,
				Columns: []CatalogColumn{
					{Name: "id", Type: CatalogColumnTypeInt},
				},
			},
		},
	})
	if _, err := dbFile.File().WriteAt(legacyPage, pageOffset(DirectoryControlPageID)); err != nil {
		t.Fatalf("WriteAt() error = %v", err)
	}

	if err := EnsureDirectoryPage(dbFile.File()); !errors.Is(err, errCorruptedDirectoryPage) {
		t.Fatalf("EnsureDirectoryPage() error = %v, want %v", err, errCorruptedDirectoryPage)
	}
}

func TestDirectoryFreeListHeadRoundTripDurably(t *testing.T) {
	dbFile, err := OpenOrCreate(filepath.Join(t.TempDir(), "directory-head.db"))
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	if err := EnsureDirectoryPage(dbFile.File()); err != nil {
		t.Fatalf("EnsureDirectoryPage() error = %v", err)
	}
	if err := WriteDirectoryFreeListHead(dbFile.File(), 19); err != nil {
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}

	got, err := ReadDirectoryFreeListHead(dbFile.File())
	if err != nil {
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	if got != 19 {
		t.Fatalf("ReadDirectoryFreeListHead() = %d, want 19", got)
	}
}

func TestDirectoryFreeListHeadPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "directory-reopen.db")

	dbFile, err := OpenOrCreate(path)
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	if err := EnsureDirectoryPage(dbFile.File()); err != nil {
		t.Fatalf("EnsureDirectoryPage() error = %v", err)
	}
	if err := WriteDirectoryFreeListHead(dbFile.File(), 23); err != nil {
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	dbFile, err = OpenOrCreate(path)
	if err != nil {
		t.Fatalf("reopen OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	got, err := ReadDirectoryFreeListHead(dbFile.File())
	if err != nil {
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	if got != 23 {
		t.Fatalf("ReadDirectoryFreeListHead() = %d, want 23", got)
	}
}

func TestValidateDirectoryPageRejectsLegacyNameMappings(t *testing.T) {
	page := InitDirectoryPage(uint32(DirectoryControlPageID), CurrentDBFormatVersion)
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetRootMapCount:directoryBodyOffsetRootMapCount+4], 1)
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetRootMapBytes:directoryBodyOffsetRootMapBytes+4], 2)
	if err := ValidateDirectoryPage(page); !errors.Is(err, errUnsupportedDirectoryPage) {
		t.Fatalf("ValidateDirectoryPage() error = %v, want %v", err, errUnsupportedDirectoryPage)
	}
}

func TestDirectoryRootIDMappingsRoundTrip(t *testing.T) {
	pageData, err := BuildCatalogPageData(&CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    1,
				RootPageID: 1,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
			},
		},
	})
	if err != nil {
		t.Fatalf("BuildCatalogPageData() error = %v", err)
	}
	catalogPayload, err := directoryCatalogPayload(pageData)
	if err != nil {
		t.Fatalf("directoryCatalogPayload() error = %v", err)
	}
	mappings := []DirectoryRootIDMapping{
		{
			ObjectType: DirectoryRootMappingObjectTable,
			ObjectID:   7,
			RootPageID: 13,
		},
		{
			ObjectType: DirectoryRootMappingObjectIndex,
			ObjectID:   11,
			RootPageID: 17,
		},
	}

	page, err := buildDirectoryCatalogPage(catalogPayload, CurrentDBFormatVersion, 19, mappings, DirectoryCheckpointMetadata{})
	if err != nil {
		t.Fatalf("buildDirectoryCatalogPage() error = %v", err)
	}

	got, err := directoryRootIDMappings(page)
	if err != nil {
		t.Fatalf("directoryRootIDMappings() error = %v", err)
	}
	if len(got) != len(mappings) {
		t.Fatalf("len(directoryRootIDMappings()) = %d, want %d", len(got), len(mappings))
	}
	for i := range mappings {
		if got[i] != mappings[i] {
			t.Fatalf("mapping[%d] = %#v, want %#v", i, got[i], mappings[i])
		}
	}
}

func TestBuildDirectoryRootIDMappingsOrdersDeterministically(t *testing.T) {
	catalog := &CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    7,
				RootPageID: 11,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
				Indexes: []CatalogIndex{
					{Name: "idx_users_z", IndexID: 22, RootPageID: 33, Columns: []CatalogIndexColumn{{Name: "id"}}},
					{Name: "idx_users_a", IndexID: 20, RootPageID: 31, Columns: []CatalogIndexColumn{{Name: "id"}}},
				},
			},
			{
				Name:       "accounts",
				TableID:    3,
				RootPageID: 9,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
				Indexes: []CatalogIndex{
					{Name: "idx_accounts_b", IndexID: 21, RootPageID: 32, Columns: []CatalogIndexColumn{{Name: "id"}}},
				},
			},
		},
	}

	got := BuildDirectoryRootIDMappings(catalog)
	wantIDMappings := []DirectoryRootIDMapping{
		{ObjectType: DirectoryRootMappingObjectTable, ObjectID: 3, RootPageID: 9},
		{ObjectType: DirectoryRootMappingObjectTable, ObjectID: 7, RootPageID: 11},
		{ObjectType: DirectoryRootMappingObjectIndex, ObjectID: 20, RootPageID: 31},
		{ObjectType: DirectoryRootMappingObjectIndex, ObjectID: 21, RootPageID: 32},
		{ObjectType: DirectoryRootMappingObjectIndex, ObjectID: 22, RootPageID: 33},
	}

	if len(got) != len(wantIDMappings) {
		t.Fatalf("len(BuildDirectoryRootIDMappings()) = %d, want %d", len(got), len(wantIDMappings))
	}
	for i := range wantIDMappings {
		if got[i] != wantIDMappings[i] {
			t.Fatalf("BuildDirectoryRootIDMappings()[%d] = %#v, want %#v", i, got[i], wantIDMappings[i])
		}
	}
}

func TestBuildCatalogPageDataWritesIDMappingsOnly(t *testing.T) {
	page, err := BuildCatalogPageData(&CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    7,
				RootPageID: 11,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
				Indexes: []CatalogIndex{
					{Name: "idx_users_id", IndexID: 9, RootPageID: 13, Columns: []CatalogIndexColumn{{Name: "id"}}},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("BuildCatalogPageData() error = %v", err)
	}

	if binary.LittleEndian.Uint32(page[directoryBodyOffsetRootMapCount:directoryBodyOffsetRootMapCount+4]) != 0 {
		t.Fatalf("rootMapCount = %d, want 0 on new writes", binary.LittleEndian.Uint32(page[directoryBodyOffsetRootMapCount:directoryBodyOffsetRootMapCount+4]))
	}
	if binary.LittleEndian.Uint32(page[directoryBodyOffsetRootMapBytes:directoryBodyOffsetRootMapBytes+4]) != 0 {
		t.Fatalf("rootMapBytes = %d, want 0 on new writes", binary.LittleEndian.Uint32(page[directoryBodyOffsetRootMapBytes:directoryBodyOffsetRootMapBytes+4]))
	}

	idMappings, err := directoryRootIDMappings(page)
	if err != nil {
		t.Fatalf("directoryRootIDMappings() error = %v", err)
	}
	want := []DirectoryRootIDMapping{
		{ObjectType: DirectoryRootMappingObjectTable, ObjectID: 7, RootPageID: 11},
		{ObjectType: DirectoryRootMappingObjectIndex, ObjectID: 9, RootPageID: 13},
	}
	if len(idMappings) != len(want) {
		t.Fatalf("len(directoryRootIDMappings()) = %d, want %d", len(idMappings), len(want))
	}
	for i := range want {
		if idMappings[i] != want[i] {
			t.Fatalf("idMappings[%d] = %#v, want %#v", i, idMappings[i], want[i])
		}
	}
}

func TestWriteDirectoryRootIDMappingsPersistsDurably(t *testing.T) {
	path := filepath.Join(t.TempDir(), "directory-root-id-map.db")

	dbFile, err := OpenOrCreate(path)
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	if err := EnsureDirectoryPage(dbFile.File()); err != nil {
		t.Fatalf("EnsureDirectoryPage() error = %v", err)
	}
	pager, err := NewPager(dbFile.File())
	if err != nil {
		t.Fatalf("NewPager() error = %v", err)
	}
	if err := SaveCatalog(pager, &CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    5,
				RootPageID: 9,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
			},
		},
	}); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	mappings := []DirectoryRootIDMapping{
		{
			ObjectType: DirectoryRootMappingObjectTable,
			ObjectID:   5,
			RootPageID: 9,
		},
	}
	if err := WriteDirectoryRootIDMappings(dbFile.File(), mappings); err != nil {
		t.Fatalf("WriteDirectoryRootIDMappings() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	dbFile, err = OpenOrCreate(path)
	if err != nil {
		t.Fatalf("reopen OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	got, err := ReadDirectoryRootIDMappings(dbFile.File())
	if err != nil {
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	if len(got) != 1 || got[0] != mappings[0] {
		t.Fatalf("ReadDirectoryRootIDMappings() = %#v, want %#v", got, mappings)
	}
}

func TestApplyDirectoryRootIDMappingsUsesDirectoryRootsOnly(t *testing.T) {
	catalog := &CatalogData{
		Version: catalogVersion,
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    3,
				RootPageID: 5,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
				Indexes: []CatalogIndex{
					{Name: "idx_users_id", IndexID: 9, RootPageID: 7, Columns: []CatalogIndexColumn{{Name: "id"}}},
				},
			},
		},
	}
	mappings := []DirectoryRootIDMapping{
		{
			ObjectType: DirectoryRootMappingObjectTable,
			ObjectID:   3,
			RootPageID: 9,
		},
		{
			ObjectType: DirectoryRootMappingObjectIndex,
			ObjectID:   9,
			RootPageID: 11,
		},
	}

	got, err := ApplyDirectoryRootIDMappings(catalog, mappings)
	if err != nil {
		t.Fatalf("ApplyDirectoryRootIDMappings() error = %v", err)
	}
	if got.Tables[0].RootPageID != 9 {
		t.Fatalf("got.Tables[0].RootPageID = %d, want 9 from directory mapping", got.Tables[0].RootPageID)
	}
	if got.Tables[0].Indexes[0].RootPageID != 11 {
		t.Fatalf("got.Tables[0].Indexes[0].RootPageID = %d, want 11 from directory mapping", got.Tables[0].Indexes[0].RootPageID)
	}
}

func TestApplyDirectoryRootIDMappingsRejectsEmptyMappings(t *testing.T) {
	catalog := &CatalogData{
		Version: catalogVersion,
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    3,
				RootPageID: 5,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
			},
		},
	}

	_, err := ApplyDirectoryRootIDMappings(catalog, nil)
	if !errors.Is(err, errCorruptedDirectoryPage) {
		t.Fatalf("ApplyDirectoryRootIDMappings() error = %v, want %v", err, errCorruptedDirectoryPage)
	}
}

func TestApplyDirectoryRootIDMappingsIgnoresCatalogCarriedRoots(t *testing.T) {
	catalog := &CatalogData{
		Version: catalogVersion,
		Tables: []CatalogTable{
			{
				Name:       "users",
				TableID:    3,
				RootPageID: 99,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
				Indexes: []CatalogIndex{
					{Name: "idx_users_id", IndexID: 9, RootPageID: 101, Columns: []CatalogIndexColumn{{Name: "id"}}},
				},
			},
		},
	}

	got, err := ApplyDirectoryRootIDMappings(catalog, []DirectoryRootIDMapping{
		{ObjectType: DirectoryRootMappingObjectTable, ObjectID: 3, RootPageID: 5},
		{ObjectType: DirectoryRootMappingObjectIndex, ObjectID: 9, RootPageID: 7},
	})
	if err != nil {
		t.Fatalf("ApplyDirectoryRootIDMappings() error = %v", err)
	}
	if got.Tables[0].RootPageID != 5 {
		t.Fatalf("got.Tables[0].RootPageID = %d, want 5 from directory mapping", got.Tables[0].RootPageID)
	}
	if got.Tables[0].Indexes[0].RootPageID != 7 {
		t.Fatalf("got.Tables[0].Indexes[0].RootPageID = %d, want 7 from directory mapping", got.Tables[0].Indexes[0].RootPageID)
	}
}

func TestValidateDirectoryControlStateRejectsInvalidFreeListHeadPageType(t *testing.T) {
	path := filepath.Join(t.TempDir(), "directory-validate-free-head.db")
	dbFile, err := OpenOrCreate(path)
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()
	if err := EnsureDirectoryPage(dbFile.File()); err != nil {
		t.Fatalf("EnsureDirectoryPage() error = %v", err)
	}
	if _, err := dbFile.File().WriteAt(InitializeTablePage(2), pageOffset(2)); err != nil {
		t.Fatalf("WriteAt() error = %v", err)
	}

	err = ValidateDirectoryControlState(dbFile.File(), DirectoryControlState{
		FreeListHead: 2,
	})
	if !errors.Is(err, errCorruptedDirectoryPage) {
		t.Fatalf("ValidateDirectoryControlState() error = %v, want %v", err, errCorruptedDirectoryPage)
	}
}

func TestValidateDirectoryControlStateRejectsInvalidTableRootPageType(t *testing.T) {
	path := filepath.Join(t.TempDir(), "directory-validate-table-root.db")
	dbFile, err := OpenOrCreate(path)
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()
	if err := EnsureDirectoryPage(dbFile.File()); err != nil {
		t.Fatalf("EnsureDirectoryPage() error = %v", err)
	}
	if _, err := dbFile.File().WriteAt(InitFreePage(2, 0), pageOffset(2)); err != nil {
		t.Fatalf("WriteAt() error = %v", err)
	}

	err = ValidateDirectoryControlState(dbFile.File(), DirectoryControlState{
		RootIDMappings: []DirectoryRootIDMapping{{
			ObjectType: DirectoryRootMappingObjectTable,
			ObjectID:   1,
			RootPageID: 2,
		}},
	})
	if !errors.Is(err, errCorruptedTablePage) {
		t.Fatalf("ValidateDirectoryControlState() error = %v, want %v", err, errCorruptedTablePage)
	}
}

func TestValidateDirectoryControlStateRejectsInvalidIndexRootPageType(t *testing.T) {
	path := filepath.Join(t.TempDir(), "directory-validate-index-root.db")
	dbFile, err := OpenOrCreate(path)
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()
	if err := EnsureDirectoryPage(dbFile.File()); err != nil {
		t.Fatalf("EnsureDirectoryPage() error = %v", err)
	}
	if _, err := dbFile.File().WriteAt(InitializeTablePage(2), pageOffset(2)); err != nil {
		t.Fatalf("WriteAt() error = %v", err)
	}

	err = ValidateDirectoryControlState(dbFile.File(), DirectoryControlState{
		RootIDMappings: []DirectoryRootIDMapping{{
			ObjectType: DirectoryRootMappingObjectIndex,
			ObjectID:   1,
			RootPageID: 2,
		}},
	})
	if !errors.Is(err, errCorruptedIndexPage) {
		t.Fatalf("ValidateDirectoryControlState() error = %v, want %v", err, errCorruptedIndexPage)
	}
}

func TestValidateDirectoryControlStateRejectsUnsupportedObjectType(t *testing.T) {
	path := filepath.Join(t.TempDir(), "directory-validate-object-type.db")
	dbFile, err := OpenOrCreate(path)
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()
	if err := EnsureDirectoryPage(dbFile.File()); err != nil {
		t.Fatalf("EnsureDirectoryPage() error = %v", err)
	}
	if _, err := dbFile.File().WriteAt(InitializeTablePage(2), pageOffset(2)); err != nil {
		t.Fatalf("WriteAt() error = %v", err)
	}

	err = ValidateDirectoryControlState(dbFile.File(), DirectoryControlState{
		RootIDMappings: []DirectoryRootIDMapping{{
			ObjectType: 99,
			ObjectID:   1,
			RootPageID: 2,
		}},
	})
	if !errors.Is(err, errCorruptedDirectoryPage) {
		t.Fatalf("ValidateDirectoryControlState() error = %v, want %v", err, errCorruptedDirectoryPage)
	}
}

func TestValidateDirectoryControlStateRejectsDuplicateIDMappings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "directory-validate-id-duplicates.db")
	dbFile, err := OpenOrCreate(path)
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()
	if err := EnsureDirectoryPage(dbFile.File()); err != nil {
		t.Fatalf("EnsureDirectoryPage() error = %v", err)
	}
	if _, err := dbFile.File().WriteAt(InitializeTablePage(2), pageOffset(2)); err != nil {
		t.Fatalf("WriteAt(table) error = %v", err)
	}
	indexPage := InitIndexLeafPage(3)
	if _, err := dbFile.File().WriteAt(indexPage, pageOffset(3)); err != nil {
		t.Fatalf("WriteAt(index) error = %v", err)
	}

	err = ValidateDirectoryControlState(dbFile.File(), DirectoryControlState{
		RootIDMappings: []DirectoryRootIDMapping{
			{ObjectType: DirectoryRootMappingObjectTable, ObjectID: 1, RootPageID: 2},
			{ObjectType: DirectoryRootMappingObjectTable, ObjectID: 1, RootPageID: 2},
		},
	})
	if !errors.Is(err, errCorruptedDirectoryPage) {
		t.Fatalf("ValidateDirectoryControlState(table id dup) error = %v, want %v", err, errCorruptedDirectoryPage)
	}

	err = ValidateDirectoryControlState(dbFile.File(), DirectoryControlState{
		RootIDMappings: []DirectoryRootIDMapping{
			{ObjectType: DirectoryRootMappingObjectIndex, ObjectID: 2, RootPageID: 3},
			{ObjectType: DirectoryRootMappingObjectIndex, ObjectID: 2, RootPageID: 3},
		},
	})
	if !errors.Is(err, errCorruptedDirectoryPage) {
		t.Fatalf("ValidateDirectoryControlState(index id dup) error = %v, want %v", err, errCorruptedDirectoryPage)
	}
}
