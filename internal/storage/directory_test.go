package storage

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"
)

func TestInitDirectoryPageCreatesValidPage(t *testing.T) {
	page := InitDirectoryPage(uint32(DirectoryControlPageID), version)

	if err := ValidateDirectoryPage(page); err != nil {
		t.Fatalf("ValidateDirectoryPage() error = %v", err)
	}
	formatVersion, err := DirectoryFormatVersion(page)
	if err != nil {
		t.Fatalf("DirectoryFormatVersion() error = %v", err)
	}
	if formatVersion != version {
		t.Fatalf("DirectoryFormatVersion() = %d, want %d", formatVersion, version)
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
	page := InitDirectoryPage(uint32(DirectoryControlPageID), version)

	if err := SetDirectoryFormatVersion(page, version); err != nil {
		t.Fatalf("SetDirectoryFormatVersion() error = %v", err)
	}
	got, err := DirectoryFormatVersion(page)
	if err != nil {
		t.Fatalf("DirectoryFormatVersion() error = %v", err)
	}
	if got != version {
		t.Fatalf("DirectoryFormatVersion() = %d, want %d", got, version)
	}
}

func TestDirectoryFreeListHeadRoundTrip(t *testing.T) {
	page := InitDirectoryPage(uint32(DirectoryControlPageID), version)

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

func TestValidateDirectoryPageRejectsWrongPageType(t *testing.T) {
	page := InitDirectoryPage(uint32(DirectoryControlPageID), version)
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

func TestEnsureDirectoryPageUpgradesLegacyCatalogPage(t *testing.T) {
	dbFile, err := OpenOrCreate(filepath.Join(t.TempDir(), "legacy-directory.db"))
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	legacyPage := buildCatalogPageDataV1(&CatalogData{
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
	catalog, err := LoadCatalogPageData(page)
	if err != nil {
		t.Fatalf("LoadCatalogPageData() error = %v", err)
	}
	if len(catalog.Tables) != 1 || catalog.Tables[0].Name != "users" {
		t.Fatalf("catalog = %#v, want upgraded users table metadata", catalog)
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

func TestDirectoryRootMappingsRoundTrip(t *testing.T) {
	mappings := []DirectoryRootMapping{
		{
			ObjectType: DirectoryRootMappingObjectTable,
			TableName:  "users",
			RootPageID: 7,
		},
		{
			ObjectType: DirectoryRootMappingObjectIndex,
			TableName:  "users",
			IndexName:  "idx_users_name",
			RootPageID: 11,
		},
	}

	page, err := buildDirectoryCatalogPage([]byte{1, 2, 3}, version, 19, mappings)
	if err != nil {
		t.Fatalf("buildDirectoryCatalogPage() error = %v", err)
	}

	got, err := directoryRootMappings(page)
	if err != nil {
		t.Fatalf("directoryRootMappings() error = %v", err)
	}
	if len(got) != len(mappings) {
		t.Fatalf("len(directoryRootMappings()) = %d, want %d", len(got), len(mappings))
	}
	for i := range mappings {
		if got[i] != mappings[i] {
			t.Fatalf("mapping[%d] = %#v, want %#v", i, got[i], mappings[i])
		}
	}
}

func TestReadDirectoryRootMappingsEmptyRoundTrip(t *testing.T) {
	page, err := buildDirectoryCatalogPage([]byte{1, 2, 3}, version, 0, nil)
	if err != nil {
		t.Fatalf("buildDirectoryCatalogPage() error = %v", err)
	}

	got, err := directoryRootMappings(page)
	if err != nil {
		t.Fatalf("directoryRootMappings() error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("len(directoryRootMappings()) = %d, want 0", len(got))
	}
}

func TestReadDirectoryRootMappingsRejectsMalformedPayload(t *testing.T) {
	page := InitDirectoryPage(uint32(DirectoryControlPageID), version)
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetRootMapCount:directoryBodyOffsetRootMapCount+4], 1)
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetRootMapBytes:directoryBodyOffsetRootMapBytes+4], 2)
	copy(page[directoryCatalogOffset:], []byte{DirectoryRootMappingObjectTable, 0})

	_, err := directoryRootMappings(page)
	if !errors.Is(err, errCorruptedDirectoryPage) {
		t.Fatalf("directoryRootMappings() error = %v, want %v", err, errCorruptedDirectoryPage)
	}
}

func TestWriteDirectoryRootMappingsPersistsDurably(t *testing.T) {
	path := filepath.Join(t.TempDir(), "directory-root-map.db")

	dbFile, err := OpenOrCreate(path)
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	if err := EnsureDirectoryPage(dbFile.File()); err != nil {
		t.Fatalf("EnsureDirectoryPage() error = %v", err)
	}
	mappings := []DirectoryRootMapping{
		{
			ObjectType: DirectoryRootMappingObjectTable,
			TableName:  "users",
			RootPageID: 5,
		},
	}
	if err := WriteDirectoryRootMappings(dbFile.File(), mappings); err != nil {
		t.Fatalf("WriteDirectoryRootMappings() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	dbFile, err = OpenOrCreate(path)
	if err != nil {
		t.Fatalf("reopen OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	got, err := ReadDirectoryRootMappings(dbFile.File())
	if err != nil {
		t.Fatalf("ReadDirectoryRootMappings() error = %v", err)
	}
	if len(got) != 1 || got[0] != mappings[0] {
		t.Fatalf("ReadDirectoryRootMappings() = %#v, want %#v", got, mappings)
	}
}

func TestApplyDirectoryRootMappingsRejectsMismatch(t *testing.T) {
	catalog := &CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				RootPageID: 5,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
				Indexes: []CatalogIndex{
					{Name: "idx_users_id", RootPageID: 7, Columns: []CatalogIndexColumn{{Name: "id"}}},
				},
			},
		},
	}
	mappings := []DirectoryRootMapping{
		{
			ObjectType: DirectoryRootMappingObjectTable,
			TableName:  "users",
			RootPageID: 9,
		},
	}

	_, err := ApplyDirectoryRootMappings(catalog, mappings)
	if !errors.Is(err, errCorruptedDirectoryPage) {
		t.Fatalf("ApplyDirectoryRootMappings() error = %v, want %v", err, errCorruptedDirectoryPage)
	}
}

func TestApplyDirectoryRootMappingsFallsBackWhenEmpty(t *testing.T) {
	catalog := &CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "users",
				RootPageID: 5,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
			},
		},
	}

	got, err := ApplyDirectoryRootMappings(catalog, nil)
	if err != nil {
		t.Fatalf("ApplyDirectoryRootMappings() error = %v", err)
	}
	if got.Tables[0].RootPageID != 5 {
		t.Fatalf("got.Tables[0].RootPageID = %d, want 5", got.Tables[0].RootPageID)
	}
}
