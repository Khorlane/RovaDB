package rovadb

import (
	"os"
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

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
