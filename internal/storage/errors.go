package storage

import (
	"errors"

	"github.com/Khorlane/RovaDB/internal/dberr"
)

var (
	errCorruptedDatabaseHeader  = dberr.NewStorage("corrupted database header")
	errCorruptedWALHeader       = dberr.NewStorage("corrupted wal header")
	errCorruptedWALFrame        = dberr.NewStorage("corrupted wal frame")
	errCorruptedPageHeader      = dberr.NewStorage("corrupted page header")
	errCorruptedDirectoryPage   = dberr.NewStorage("corrupted directory page")
	errUnsupportedDirectoryPage = dberr.NewStorage("unsupported directory mapping format")
	errCorruptedCatalogPage     = dberr.NewStorage("corrupted catalog page")
	errCorruptedCatalogOverflow = dberr.NewStorage("corrupted catalog overflow page")
	errInvalidCATDIRControl     = dberr.NewStorage("invalid CAT/DIR control metadata")
	errMalformedCATDIROverflow  = dberr.NewStorage("malformed CAT/DIR overflow chain")
	errUnsupportedCatalogPage   = dberr.NewStorage("unsupported catalog payload version")
	errCorruptedTablePage       = dberr.NewStorage("corrupted table page")
	errCorruptedIndexPage       = dberr.NewStorage("corrupted index page")
	errCorruptedRowData         = dberr.NewStorage("corrupted row data")
	errCorruptedIndexMetadata   = dberr.NewStorage("corrupted index metadata")

	errCatalogTooLarge            = dberr.NewStorage("catalog too large")
	errCATDIRExceedsEmbeddedWrite = dberr.NewStorage("catalog metadata exceeds embedded page-0 capacity")
	errTablePageFull              = errors.New("storage: table page full")
	errIndexPageFull              = dberr.NewStorage("index page full")
	errInvalidRowData             = errCorruptedRowData
)
