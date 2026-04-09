package rovadb_test

import (
	"encoding/binary"
	"fmt"

	"github.com/Khorlane/RovaDB/internal/storage"
)

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
