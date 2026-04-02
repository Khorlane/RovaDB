package rovadb

import (
	"encoding/binary"
	"sort"

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

// EngineTableInfo is a compact logical table inventory entry.
type EngineTableInfo struct {
	TableID    uint32
	TableName  string
	RootPageID uint32
	IndexCount int
}

// EngineIndexInfo is a compact logical index inventory entry.
type EngineIndexInfo struct {
	IndexID    uint32
	TableName  string
	IndexName  string
	RootPageID uint32
	IsUnique   bool
}

// EngineSchemaInventory is a compact logical object inventory.
type EngineSchemaInventory struct {
	Tables  []EngineTableInfo
	Indexes []EngineIndexInfo
}

// EngineSnapshot aggregates the existing engine diagnostic helpers.
type EngineSnapshot struct {
	Status    EngineStatus
	Check     EngineCheckResult
	PageUsage EnginePageUsage
	Inventory EngineSchemaInventory
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

// SchemaInventory returns a compact deterministic inventory of user schema objects.
func (db *DB) SchemaInventory() (EngineSchemaInventory, error) {
	if db == nil {
		return EngineSchemaInventory{}, ErrInvalidArgument
	}
	if db.closed {
		return EngineSchemaInventory{}, ErrClosed
	}
	if err := db.validateTxnState(); err != nil {
		return EngineSchemaInventory{}, err
	}

	inventory := EngineSchemaInventory{
		Tables:  make([]EngineTableInfo, 0),
		Indexes: make([]EngineIndexInfo, 0),
	}
	for _, table := range db.tables {
		if table == nil || table.IsSystem {
			continue
		}
		inventory.Tables = append(inventory.Tables, EngineTableInfo{
			TableID:    table.TableID,
			TableName:  table.Name,
			RootPageID: uint32(table.RootPageID()),
			IndexCount: len(table.IndexDefs),
		})
		for _, indexDef := range table.IndexDefs {
			inventory.Indexes = append(inventory.Indexes, EngineIndexInfo{
				IndexID:    indexDef.IndexID,
				TableName:  table.Name,
				IndexName:  indexDef.Name,
				RootPageID: indexDef.RootPageID,
				IsUnique:   indexDef.Unique,
			})
		}
	}

	sort.Slice(inventory.Tables, func(i, j int) bool {
		return inventory.Tables[i].TableName < inventory.Tables[j].TableName
	})
	sort.Slice(inventory.Indexes, func(i, j int) bool {
		left := inventory.Indexes[i]
		right := inventory.Indexes[j]
		if left.TableName == right.TableName {
			return left.IndexName < right.IndexName
		}
		return left.TableName < right.TableName
	})
	return inventory, nil
}

// EngineSnapshot returns one deterministic aggregate diagnostic snapshot.
func (db *DB) EngineSnapshot() (EngineSnapshot, error) {
	status, err := db.EngineStatus()
	if err != nil {
		return EngineSnapshot{}, err
	}
	check, err := db.CheckEngineConsistency()
	if err != nil {
		return EngineSnapshot{}, err
	}
	pageUsage, err := db.PageUsage()
	if err != nil {
		return EngineSnapshot{}, err
	}
	inventory, err := db.SchemaInventory()
	if err != nil {
		return EngineSnapshot{}, err
	}
	return EngineSnapshot{
		Status:    status,
		Check:     check,
		PageUsage: pageUsage,
		Inventory: inventory,
	}, nil
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
