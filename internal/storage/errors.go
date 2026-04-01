package storage

import (
	"errors"

	"github.com/Khorlane/RovaDB/internal/dberr"
)

var (
	errCorruptedDatabaseHeader = dberr.NewStorage("corrupted database header")
	errCorruptedWALHeader      = dberr.NewStorage("corrupted wal header")
	errCorruptedPageHeader     = dberr.NewStorage("corrupted page header")
	errCorruptedCatalogPage    = dberr.NewStorage("corrupted catalog page")
	errCorruptedTablePage      = dberr.NewStorage("corrupted table page")
	errCorruptedIndexPage      = dberr.NewStorage("corrupted index page")
	errCorruptedRowData        = dberr.NewStorage("corrupted row data")
	errCorruptedIndexMetadata  = dberr.NewStorage("corrupted index metadata")

	errCatalogTooLarge = dberr.NewStorage("catalog too large")
	errTablePageFull   = errors.New("storage: table page full")
	errIndexPageFull   = dberr.NewStorage("index page full")
	errInvalidRowData  = errCorruptedRowData
)
