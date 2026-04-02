package rovadb

import (
	"encoding/binary"
	"fmt"

	"github.com/Khorlane/RovaDB/internal/storage"
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
	copy(page.Data()[48:], data)
	if err := storage.FinalizePageImage(page.Data()); err != nil {
		t.Fatalf("storage.FinalizePageImage() error = %v", err)
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
	copy(page.Data()[48:], data)

	if len(mappings) > 0 {
		offset := 48 + len(data) + 16
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
	if err := storage.FinalizePageImage(page.Data()); err != nil {
		t.Fatalf("storage.FinalizePageImage() error = %v", err)
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
