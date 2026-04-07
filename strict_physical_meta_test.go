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
		if table.tableHeaderPageID == 0 {
			headerPage := pager.NewPage()
			clear(headerPage.Data())
			copy(headerPage.Data(), storage.InitTableHeaderPage(uint32(headerPage.ID()), table.tableID))
			table.tableHeaderPageID = headerPage.ID()
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
