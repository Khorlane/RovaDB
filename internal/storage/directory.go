package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

const (
	// DirectoryControlPageID remains page 0 so existing catalog/root-page numbering stays intact.
	DirectoryControlPageID PageID = catalogPageID

	directoryPageHeaderSize = 32

	directoryBodyOffsetFormatVersion = directoryPageHeaderSize
	directoryBodyOffsetFreeListHead  = directoryPageHeaderSize + 4
	directoryBodyOffsetReserved1     = directoryPageHeaderSize + 8
	directoryBodyOffsetReserved2     = directoryPageHeaderSize + 12
	directoryCatalogOffset           = directoryPageHeaderSize + 16
)

// InitDirectoryPage initializes the durable directory/control page.
func InitDirectoryPage(pageID uint32, formatVersion uint32) []byte {
	page := make([]byte, PageSize)
	binary.LittleEndian.PutUint32(page[pageHeaderOffsetPageID:pageHeaderOffsetPageID+4], pageID)
	binary.LittleEndian.PutUint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2], uint16(PageTypeDirectory))
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetFormatVersion:directoryBodyOffsetFormatVersion+4], formatVersion)
	return page
}

// DirectoryFormatVersion returns the durable directory format version.
func DirectoryFormatVersion(page []byte) (uint32, error) {
	if err := ValidateDirectoryPage(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(page[directoryBodyOffsetFormatVersion : directoryBodyOffsetFormatVersion+4]), nil
}

// SetDirectoryFormatVersion updates the durable directory format version.
func SetDirectoryFormatVersion(page []byte, formatVersion uint32) error {
	if err := ValidateDirectoryPage(page); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetFormatVersion:directoryBodyOffsetFormatVersion+4], formatVersion)
	return nil
}

// DirectoryFreeListHead returns the durable free-list head pointer.
func DirectoryFreeListHead(page []byte) (uint32, error) {
	if err := ValidateDirectoryPage(page); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(page[directoryBodyOffsetFreeListHead : directoryBodyOffsetFreeListHead+4]), nil
}

// SetDirectoryFreeListHead updates the durable free-list head pointer.
func SetDirectoryFreeListHead(page []byte, head uint32) error {
	if err := ValidateDirectoryPage(page); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetFreeListHead:directoryBodyOffsetFreeListHead+4], head)
	return nil
}

// ValidateDirectoryPage validates the shared header and fixed directory body.
func ValidateDirectoryPage(page []byte) error {
	if len(page) != PageSize {
		return errCorruptedDirectoryPage
	}
	if binary.LittleEndian.Uint32(page[pageHeaderOffsetPageID:pageHeaderOffsetPageID+4]) != uint32(DirectoryControlPageID) {
		return errCorruptedDirectoryPage
	}
	if PageType(binary.LittleEndian.Uint16(page[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2])) != PageTypeDirectory {
		return errCorruptedDirectoryPage
	}
	formatVersion := binary.LittleEndian.Uint32(page[directoryBodyOffsetFormatVersion : directoryBodyOffsetFormatVersion+4])
	if formatVersion != version {
		return errCorruptedDirectoryPage
	}
	return nil
}

// EnsureDirectoryPage initializes or upgrades the durable directory page in-place.
func EnsureDirectoryPage(file *os.File) error {
	if file == nil {
		return errCorruptedDirectoryPage
	}

	page := make([]byte, PageSize)
	n, err := file.ReadAt(page, pageOffset(DirectoryControlPageID))
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	if n == 0 || isZeroPage(page) {
		return writeDirectoryPage(file, InitDirectoryPage(uint32(DirectoryControlPageID), version))
	}
	if ValidateDirectoryPage(page) == nil {
		return nil
	}

	cat, err := loadCatalogPayload(page)
	if err != nil {
		return err
	}
	upgraded, err := BuildCatalogPageData(cat)
	if err != nil {
		return err
	}
	return writeDirectoryPage(file, upgraded)
}

func buildDirectoryCatalogPage(catalogPayload []byte, formatVersion uint32, freeListHead uint32) ([]byte, error) {
	if len(catalogPayload) > PageSize-directoryCatalogOffset {
		return nil, errCatalogTooLarge
	}
	page := InitDirectoryPage(uint32(DirectoryControlPageID), formatVersion)
	binary.LittleEndian.PutUint32(page[directoryBodyOffsetFreeListHead:directoryBodyOffsetFreeListHead+4], freeListHead)
	copy(page[directoryCatalogOffset:], catalogPayload)
	return page, nil
}

func directoryCatalogPayload(page []byte) ([]byte, error) {
	if err := ValidateDirectoryPage(page); err != nil {
		return nil, err
	}
	return page[directoryCatalogOffset:], nil
}

func writeDirectoryPage(file *os.File, page []byte) error {
	if _, err := file.WriteAt(page, pageOffset(DirectoryControlPageID)); err != nil {
		return err
	}
	return file.Sync()
}
