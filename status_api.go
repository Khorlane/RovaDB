package rovadb

import (
	"encoding/binary"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/storage"
)

// EngineStatus is a cheap diagnostic snapshot of engine control-plane state.
type EngineStatus struct {
	DBFormatVersion         uint32
	WALVersion              uint32
	LastCheckpointLSN       uint64
	LastCheckpointPageCount uint32
	FreeListHead            uint32
	TableCount              int
	IndexCount              int
}

// EngineCheckResult summarizes a small read-only consistency check.
type EngineCheckResult struct {
	OK                bool
	CheckedTableRoots int
	CheckedIndexRoots int
	FreeListHead      uint32
}

// EnginePageUsage summarizes physical page usage by validated page type.
type EnginePageUsage struct {
	TotalPages         int
	TablePages         int
	IndexLeafPages     int
	IndexInternalPages int
	FreePages          int
	DirectoryPages     int
}

// EngineStatus returns a stable in-memory status snapshot for diagnostics.
func (db *DB) EngineStatus() (EngineStatus, error) {
	if db == nil {
		return EngineStatus{}, ErrInvalidArgument
	}
	if db.closed {
		return EngineStatus{}, ErrClosed
	}
	if err := db.validateTxnState(); err != nil {
		return EngineStatus{}, err
	}

	return EngineStatus{
		DBFormatVersion:         storage.DBFormatVersion(),
		WALVersion:              storage.CurrentWALVersion,
		LastCheckpointLSN:       db.lastCheckpointLSN,
		LastCheckpointPageCount: db.lastCheckpointPageCount,
		FreeListHead:            db.freeListHead,
		TableCount:              statusUserTableCount(db.tables),
		IndexCount:              userIndexCount(db.tables),
	}, nil
}

// CheckEngineConsistency runs a small read-only physical/control-plane check.
func (db *DB) CheckEngineConsistency() (EngineCheckResult, error) {
	if db == nil {
		return EngineCheckResult{}, ErrInvalidArgument
	}
	if db.closed {
		return EngineCheckResult{}, ErrClosed
	}
	if err := db.validateTxnState(); err != nil {
		return EngineCheckResult{}, err
	}

	result := EngineCheckResult{
		FreeListHead: db.freeListHead,
	}
	if db.freeListHead != 0 {
		pageData, err := readCommittedPageData(db.pool, storage.PageID(db.freeListHead))
		if err != nil {
			return result, wrapStorageError(err)
		}
		if err := storage.ValidatePageImage(pageData); err != nil {
			return result, wrapStorageError(err)
		}
		if pageTypeOf(pageData) != storage.PageTypeFreePage {
			return result, wrapStorageError(newStorageError("corrupted free page"))
		}
		if _, err := storage.FreePageNext(pageData); err != nil {
			return result, wrapStorageError(err)
		}
	}

	for _, table := range db.tables {
		if table == nil || table.IsSystem {
			continue
		}
		if table.TableID == 0 || table.RootPageID() == 0 {
			return result, wrapStorageError(newStorageError("corrupted catalog page"))
		}
		pageData, err := readCommittedPageData(db.pool, table.RootPageID())
		if err != nil {
			return result, wrapStorageError(err)
		}
		if err := storage.ValidatePageImage(pageData); err != nil {
			return result, wrapStorageError(err)
		}
		if pageTypeOf(pageData) != storage.PageTypeTable {
			return result, wrapStorageError(newStorageError("corrupted table page"))
		}
		if _, err := db.scanTableRows(table); err != nil {
			return result, err
		}
		result.CheckedTableRoots++

		for _, indexDef := range table.IndexDefs {
			if indexDef.IndexID == 0 || indexDef.RootPageID == 0 {
				return result, wrapStorageError(newStorageError("corrupted catalog page"))
			}
			pageData, err := readCommittedPageData(db.pool, storage.PageID(indexDef.RootPageID))
			if err != nil {
				return result, wrapStorageError(err)
			}
			if err := storage.ValidatePageImage(pageData); err != nil {
				return result, wrapStorageError(err)
			}
			pageType := pageTypeOf(pageData)
			if pageType != storage.PageTypeIndexLeaf && pageType != storage.PageTypeIndexInternal {
				return result, wrapStorageError(newStorageError("corrupted index page"))
			}
			result.CheckedIndexRoots++
		}
	}

	result.OK = true
	return result, nil
}

// PageUsage returns a compact validated physical page breakdown.
func (db *DB) PageUsage() (EnginePageUsage, error) {
	if db == nil {
		return EnginePageUsage{}, ErrInvalidArgument
	}
	if db.closed {
		return EnginePageUsage{}, ErrClosed
	}
	if err := db.validateTxnState(); err != nil {
		return EnginePageUsage{}, err
	}
	if db.pager == nil || db.pool == nil {
		return EnginePageUsage{}, ErrInvalidArgument
	}

	totalPages := int(db.pager.NextPageID())
	usage := EnginePageUsage{TotalPages: totalPages}
	for pageID := storage.PageID(0); int(pageID) < totalPages; pageID++ {
		pageData, err := readCommittedPageData(db.pool, pageID)
		if err != nil {
			return usage, wrapStorageError(err)
		}
		if err := storage.ValidatePageImage(pageData); err != nil {
			return usage, wrapStorageError(err)
		}
		switch pageTypeOf(pageData) {
		case storage.PageTypeTable:
			usage.TablePages++
		case storage.PageTypeIndexLeaf:
			usage.IndexLeafPages++
		case storage.PageTypeIndexInternal:
			usage.IndexInternalPages++
		case storage.PageTypeFreePage:
			usage.FreePages++
		case storage.PageTypeDirectory:
			usage.DirectoryPages++
		default:
			return usage, wrapStorageError(newStorageError("corrupted page header"))
		}
	}

	return usage, nil
}

func statusUserTableCount(tables map[string]*executor.Table) int {
	count := 0
	for _, table := range tables {
		if table == nil || table.IsSystem {
			continue
		}
		count++
	}
	return count
}

func pageTypeOf(page []byte) storage.PageType {
	if len(page) < 6 {
		return 0
	}
	return storage.PageType(binary.LittleEndian.Uint16(page[4:6]))
}

func userIndexCount(tables map[string]*executor.Table) int {
	count := 0
	for _, table := range tables {
		if table == nil || table.IsSystem {
			continue
		}
		count += len(table.IndexDefs)
	}
	return count
}
