package rovadb

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

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
	OK                   bool
	CheckedTableRoots    int
	CheckedTableHeaders  int
	CheckedIndexRoots    int
	CheckedSpaceMapPages int
	CheckedDataPages     int
	FreeListHead         uint32
}

// EnginePageUsage summarizes physical page usage by validated page type.
type EnginePageUsage struct {
	TotalPages         int
	HeaderPages        int
	SpaceMapPages      int
	TablePages         int
	IndexLeafPages     int
	IndexInternalPages int
	FreePages          int
	DirectoryPages     int
}

// EngineTableInfo is a compact logical table inventory entry.
type EngineTableInfo struct {
	TableID                 uint32
	TableName               string
	RootPageID              uint32
	TableHeaderPageID       uint32
	FirstSpaceMapPageID     uint32
	OwnedSpaceMapPages      uint32
	EnumeratedSpaceMapPages uint32
	OwnedDataPages          uint32
	EnumeratedDataPages     uint32
	PhysicalMetaPresent     bool
	PhysicalMetaValid       bool
	PhysicalInventoryMatch  bool
	IndexCount              int
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

// String formats the snapshot into a compact deterministic text block.
func (s EngineSnapshot) String() string {
	var b strings.Builder
	b.WriteString("Engine Status\n")
	fmt.Fprintf(&b, "DB format: %d\n", s.Status.DBFormatVersion)
	fmt.Fprintf(&b, "WAL version: %d\n", s.Status.WALVersion)
	fmt.Fprintf(&b, "Checkpoint: LSN=%d pages=%d\n", s.Status.LastCheckpointLSN, s.Status.LastCheckpointPageCount)
	fmt.Fprintf(&b, "Free list head: %d\n", s.Status.FreeListHead)
	fmt.Fprintf(&b, "Tables: %d\n", s.Status.TableCount)
	fmt.Fprintf(&b, "Indexes: %d\n\n", s.Status.IndexCount)

	b.WriteString("Consistency\n")
	fmt.Fprintf(&b, "OK: %t\n", s.Check.OK)
	fmt.Fprintf(&b, "Checked table roots: %d\n", s.Check.CheckedTableRoots)
	fmt.Fprintf(&b, "Checked table headers: %d\n", s.Check.CheckedTableHeaders)
	fmt.Fprintf(&b, "Checked index roots: %d\n", s.Check.CheckedIndexRoots)
	fmt.Fprintf(&b, "Checked space map pages: %d\n", s.Check.CheckedSpaceMapPages)
	fmt.Fprintf(&b, "Checked data pages: %d\n\n", s.Check.CheckedDataPages)

	b.WriteString("Page Usage\n")
	fmt.Fprintf(&b, "Total: %d\n", s.PageUsage.TotalPages)
	fmt.Fprintf(&b, "Header: %d\n", s.PageUsage.HeaderPages)
	fmt.Fprintf(&b, "Space map: %d\n", s.PageUsage.SpaceMapPages)
	fmt.Fprintf(&b, "Table: %d\n", s.PageUsage.TablePages)
	fmt.Fprintf(&b, "Index leaf: %d\n", s.PageUsage.IndexLeafPages)
	fmt.Fprintf(&b, "Index internal: %d\n", s.PageUsage.IndexInternalPages)
	fmt.Fprintf(&b, "Free: %d\n", s.PageUsage.FreePages)
	fmt.Fprintf(&b, "Directory: %d\n\n", s.PageUsage.DirectoryPages)

	b.WriteString("Schema Inventory\n")
	b.WriteString("Tables:\n")
	for _, table := range s.Inventory.Tables {
		physicalState := "missing"
		if table.PhysicalMetaPresent && table.PhysicalMetaValid && table.PhysicalInventoryMatch {
			physicalState = "ok"
		} else if table.PhysicalMetaPresent {
			physicalState = "mismatch"
		}
		fmt.Fprintf(&b, "- %s (id=%d, root=%d, header=%d, first_space_map=%d, space_maps=%d/%d, data_pages=%d/%d, physical=%s, indexes=%d)\n",
			table.TableName,
			table.TableID,
			table.RootPageID,
			table.TableHeaderPageID,
			table.FirstSpaceMapPageID,
			table.OwnedSpaceMapPages,
			table.EnumeratedSpaceMapPages,
			table.OwnedDataPages,
			table.EnumeratedDataPages,
			physicalState,
			table.IndexCount,
		)
	}
	b.WriteString("Indexes:\n")
	for _, index := range s.Inventory.Indexes {
		fmt.Fprintf(&b, "- %s.%s (id=%d, root=%d, unique=%t)\n", index.TableName, index.IndexName, index.IndexID, index.RootPageID, index.IsUnique)
	}
	return b.String()
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
	if err := validateCommittedPhysicalTableStorage(db.pool, db.pager, db.tables, db.freeListHead); err != nil {
		return result, err
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
		if table.TableHeaderPageID() == 0 {
			return result, wrapStorageError(newStorageError("corrupted header page"))
		}
		headerPageData, err := readCommittedPageData(db.pool, table.TableHeaderPageID())
		if err != nil {
			return result, wrapStorageError(err)
		}
		if err := storage.ValidateTableHeaderPage(headerPageData); err != nil {
			return result, wrapStorageError(err)
		}
		result.CheckedTableRoots++
		result.CheckedTableHeaders++

		spaceMapPageIDs, dataPageIDs, err := committedTablePhysicalStorageInventory(db.pool, table)
		if err != nil {
			return result, err
		}
		result.CheckedSpaceMapPages += len(spaceMapPageIDs)
		result.CheckedDataPages += len(dataPageIDs)

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
		case storage.PageTypeHeader:
			usage.HeaderPages++
		case storage.PageTypeSpaceMap:
			usage.SpaceMapPages++
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
		physicalMetaPresent := table.TableHeaderPageID() != 0
		physicalMetaValid := false
		if physicalMetaPresent {
			headerPageData, err := readCommittedPageData(db.pool, table.TableHeaderPageID())
			if err != nil {
				return inventory, wrapStorageError(err)
			}
			if err := storage.ValidateTableHeaderPage(headerPageData); err != nil {
				return inventory, wrapStorageError(err)
			}
			headerTableID, err := storage.TableHeaderTableID(headerPageData)
			if err != nil {
				return inventory, wrapStorageError(err)
			}
			physicalMetaValid = headerTableID == table.TableID
		}
		var enumeratedSpaceMapPages uint32
		var enumeratedDataPages uint32
		physicalInventoryMatch := false
		if table.FirstSpaceMapPageID() == 0 {
			physicalInventoryMatch = physicalMetaPresent && physicalMetaValid && table.OwnedSpaceMapPageCount() == 0 && table.OwnedDataPageCount() == 0
		} else {
			spaceMapPageIDs, dataPageIDs, err := committedTablePhysicalStorageInventory(db.pool, table)
			if err != nil {
				return inventory, err
			}
			enumeratedSpaceMapPages = uint32(len(spaceMapPageIDs))
			enumeratedDataPages = uint32(len(dataPageIDs))
			physicalInventoryMatch = physicalMetaPresent &&
				physicalMetaValid &&
				table.OwnedSpaceMapPageCount() == enumeratedSpaceMapPages &&
				table.OwnedDataPageCount() == enumeratedDataPages
		}
		inventory.Tables = append(inventory.Tables, EngineTableInfo{
			TableID:                 table.TableID,
			TableName:               table.Name,
			RootPageID:              uint32(table.RootPageID()),
			TableHeaderPageID:       uint32(table.TableHeaderPageID()),
			FirstSpaceMapPageID:     uint32(table.FirstSpaceMapPageID()),
			OwnedSpaceMapPages:      table.OwnedSpaceMapPageCount(),
			EnumeratedSpaceMapPages: enumeratedSpaceMapPages,
			OwnedDataPages:          table.OwnedDataPageCount(),
			EnumeratedDataPages:     enumeratedDataPages,
			PhysicalMetaPresent:     physicalMetaPresent,
			PhysicalMetaValid:       physicalMetaValid,
			PhysicalInventoryMatch:  physicalInventoryMatch,
			IndexCount:              len(table.IndexDefs),
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

// EngineReport returns the formatted aggregate engine snapshot.
func (db *DB) EngineReport() (string, error) {
	snapshot, err := db.EngineSnapshot()
	if err != nil {
		return "", err
	}
	return snapshot.String(), nil
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
