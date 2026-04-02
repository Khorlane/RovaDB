package rovadb

import (
	"encoding/binary"
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestOpenRejectsPersistedOutOfRangeInt(t *testing.T) {
	path := testDBPath(t)

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	rootPage := pager.NewPage()
	storage.InitTableRootPage(rootPage)

	row := encodedOutOfRangeIntRow(t, 2147483648)
	if err := storage.AppendRowToTablePage(rootPage, row); err != nil {
		t.Fatalf("AppendRowToTablePage() error = %v", err)
	}
	if err := storage.SaveCatalog(pager, &storage.CatalogData{
		Tables: []storage.CatalogTable{
			{
				Name:       "users",
				TableID:    1,
				RootPageID: uint32(rootPage.ID()),
				RowCount:   1,
				Columns: []storage.CatalogColumn{
					{Name: "id", Type: storage.CatalogColumnTypeInt},
				},
			},
		},
	}); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err := Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want corruption error")
	}
	if err.Error() != "storage: corrupted row data" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: corrupted row data")
	}
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
