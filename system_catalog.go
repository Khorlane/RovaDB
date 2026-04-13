package rovadb

import (
	"math"
	"sort"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

// NOTE: System catalog orchestration belongs to the root API layer because it
// bridges public database lifecycle with executor/storage contracts without
// redefining either layer's ownership.

const (
	systemTableTables       = "sys_tables"
	systemTableColumns      = "sys_tb_columns"
	systemTableIndexes      = "sys_indexes"
	systemTableIndexColumns = "sys_ix_columns"
)

type systemCatalogTableSpec struct {
	name    string
	columns []parser.ColumnDef
}

func systemCatalogTableSpecs() []systemCatalogTableSpec {
	return []systemCatalogTableSpec{
		{
			name: systemTableTables,
			columns: []parser.ColumnDef{
				{Name: "table_id", Type: parser.ColumnTypeInt},
				{Name: "table_name", Type: parser.ColumnTypeText},
			},
		},
		{
			name: systemTableColumns,
			columns: []parser.ColumnDef{
				{Name: "table_id", Type: parser.ColumnTypeInt},
				{Name: "column_name", Type: parser.ColumnTypeText},
				{Name: "column_type", Type: parser.ColumnTypeText},
				{Name: "ordinal_position", Type: parser.ColumnTypeInt},
			},
		},
		{
			name: systemTableIndexes,
			columns: []parser.ColumnDef{
				{Name: "index_id", Type: parser.ColumnTypeInt},
				{Name: "index_name", Type: parser.ColumnTypeText},
				{Name: "table_id", Type: parser.ColumnTypeInt},
				{Name: "is_unique", Type: parser.ColumnTypeBool},
			},
		},
		{
			name: systemTableIndexColumns,
			columns: []parser.ColumnDef{
				{Name: "index_id", Type: parser.ColumnTypeInt},
				{Name: "column_name", Type: parser.ColumnTypeText},
				{Name: "ordinal_position", Type: parser.ColumnTypeInt},
			},
		},
	}
}

func isSystemCatalogTableName(name string) bool {
	switch name {
	case systemTableTables, systemTableColumns, systemTableIndexes, systemTableIndexColumns:
		return true
	default:
		return false
	}
}

func (db *DB) ensureSystemCatalogTables(tables map[string]*executor.Table) (map[storage.PageID]struct{}, bool, error) {
	if db == nil || db.pager == nil {
		return nil, false, nil
	}

	nextFreshID := db.pager.NextPageID()
	newPageIDs := make(map[storage.PageID]struct{})
	changed := false

	for _, spec := range systemCatalogTableSpecs() {
		if table := tables[spec.name]; table != nil {
			table.IsSystem = true
			continue
		}

		rootPageID, isNew, err := db.allocatePageIDFrom(&nextFreshID)
		if err != nil {
			return nil, false, err
		}

		table := &executor.Table{
			Name:      spec.name,
			TableID:   nextTableID(tables),
			IsSystem:  true,
			Columns:   append([]parser.ColumnDef(nil), spec.columns...),
			IndexDefs: nil,
		}
		tableHeaderPageID := nextFreshID
		nextFreshID++
		table.SetStorageMeta(rootPageID, 0)
		table.SetPhysicalTableRootMeta(tableHeaderPageID, storage.CurrentTableStorageFormatVersion, 0, 0, 0)
		tables[spec.name] = table
		if isNew {
			newPageIDs[rootPageID] = struct{}{}
		}
		changed = true
	}

	return newPageIDs, changed, nil
}

func systemCatalogReservedNextFreshID(tables map[string]*executor.Table, nextFreshID storage.PageID) storage.PageID {
	for _, spec := range systemCatalogTableSpecs() {
		table := tables[spec.name]
		if table == nil {
			continue
		}
		if table.RootPageID() >= nextFreshID {
			nextFreshID = table.RootPageID() + 1
		}
		if table.TableHeaderPageID() >= nextFreshID {
			nextFreshID = table.TableHeaderPageID() + 1
		}
	}
	return nextFreshID
}

func (db *DB) rebuildSystemCatalogRows(tables map[string]*executor.Table, newPageIDs map[storage.PageID]struct{}, startNextFreshID storage.PageID) ([]stagedPage, bool, error) {
	if db == nil || db.pager == nil {
		return nil, false, nil
	}

	tableRows, columnRows, indexRows, indexColumnRows, err := buildSystemCatalogRows(tables)
	if err != nil {
		return nil, false, err
	}

	rowSets := map[string][][]parser.Value{
		systemTableTables:       tableRows,
		systemTableColumns:      columnRows,
		systemTableIndexes:      indexRows,
		systemTableIndexColumns: indexColumnRows,
	}

	pages := make([]stagedPage, 0, len(rowSets))
	changed := false
	nextFreshID := startNextFreshID
	freedPhysicalPageIDs := make([]storage.PageID, 0)
	combinedFreeListHead := db.freeListHead
	for _, spec := range systemCatalogTableSpecs() {
		table := tables[spec.name]
		if table == nil || !table.IsSystem {
			return nil, false, newStorageError("corrupted catalog page")
		}
		if _, isNew := newPageIDs[table.RootPageID()]; isNew {
			rootPageData, err := storage.BuildSlottedTablePageData(uint32(table.RootPageID()), nil, nil)
			if err != nil {
				return nil, false, wrapStorageError(err)
			}
			pages = append(pages, stagedPage{
				id:    table.RootPageID(),
				data:  rootPageData,
				isNew: true,
			})
			nextFreshID = nextFreshPageIDAfter(pages, nextFreshID)
		}
		rows := cloneRows(rowSets[spec.name])
		oldPersistedRowCount := table.PersistedRowCount()
		var committedRows [][]parser.Value
		if _, isNew := newPageIDs[table.RootPageID()]; !isNew {
			_, committedRows, err = loadCommittedTableRowsAndLocators(db.pool, table)
			if err != nil {
				return nil, false, err
			}
			spaceMapPageIDs, dataPageIDs, err := committedTablePhysicalStorageInventory(db.pool, table)
			if err != nil {
				return nil, false, err
			}
			freedPhysicalPageIDs = append(freedPhysicalPageIDs, spaceMapPageIDs...)
			freedPhysicalPageIDs = append(freedPhysicalPageIDs, dataPageIDs...)
		}
		table.Rows = rows
		table.SetStorageMeta(table.RootPageID(), uint32(len(rows)))
		db.freeListHead = 0
		tablePages, _, err := db.stageTableRewriteViaPhysicalStorage(table, rows, true, nextFreshID, false)
		if err != nil {
			return nil, false, err
		}
		pages = append(pages, tablePages...)
		nextFreshID = nextFreshPageIDAfter(pages, nextFreshID)
		if !systemCatalogRowsEqual(committedRows, rows) || oldPersistedRowCount != uint32(len(rows)) {
			changed = true
		}
	}
	db.freeListHead = combinedFreeListHead
	if len(freedPhysicalPageIDs) != 0 {
		freedPages, err := db.buildFreedPages(freedPhysicalPageIDs...)
		if err != nil {
			return nil, false, err
		}
		pages = append(pages, freedPages...)
	}
	newPages := make([]stagedPage, 0, len(pages))
	existingPages := make([]stagedPage, 0, len(pages))
	for _, page := range pages {
		if page.isNew {
			newPages = append(newPages, page)
			continue
		}
		existingPages = append(existingPages, page)
	}
	sort.Slice(newPages, func(i, j int) bool {
		return newPages[i].id < newPages[j].id
	})
	pages = append(newPages, existingPages...)

	return pages, changed, nil
}

func systemCatalogRowsEqual(a [][]parser.Value, b [][]parser.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

func buildSystemCatalogRows(tables map[string]*executor.Table) ([][]parser.Value, [][]parser.Value, [][]parser.Value, [][]parser.Value, error) {
	userTables := make([]*executor.Table, 0, len(tables))
	for _, table := range tables {
		if table == nil || table.IsSystem {
			continue
		}
		if table.TableID == 0 {
			return nil, nil, nil, nil, newStorageError("corrupted catalog page")
		}
		userTables = append(userTables, table)
	}
	sort.Slice(userTables, func(i, j int) bool {
		return userTables[i].Name < userTables[j].Name
	})

	sysTables := make([][]parser.Value, 0, len(userTables))
	sysColumns := make([][]parser.Value, 0)
	sysIndexes := make([][]parser.Value, 0)
	sysIndexColumns := make([][]parser.Value, 0)

	for _, table := range userTables {
		tableIDValue, err := systemCatalogIntValue(int64(table.TableID))
		if err != nil {
			return nil, nil, nil, nil, err
		}
		sysTables = append(sysTables, []parser.Value{
			tableIDValue,
			parser.StringValue(table.Name),
		})
		for i, column := range table.Columns {
			ordinalValue, err := systemCatalogIntValue(int64(i + 1))
			if err != nil {
				return nil, nil, nil, nil, err
			}
			sysColumns = append(sysColumns, []parser.Value{
				tableIDValue,
				parser.StringValue(column.Name),
				parser.StringValue(column.Type),
				ordinalValue,
			})
		}

		indexDefs := cloneIndexDefs(table.IndexDefs)
		sort.Slice(indexDefs, func(i, j int) bool {
			return indexDefs[i].Name < indexDefs[j].Name
		})
		for _, indexDef := range indexDefs {
			if indexDef.IndexID == 0 {
				return nil, nil, nil, nil, newStorageError("corrupted catalog page")
			}
			indexIDValue, err := systemCatalogIntValue(int64(indexDef.IndexID))
			if err != nil {
				return nil, nil, nil, nil, err
			}
			sysIndexes = append(sysIndexes, []parser.Value{
				indexIDValue,
				parser.StringValue(indexDef.Name),
				tableIDValue,
				parser.BoolValue(indexDef.Unique),
			})
			for i, indexColumn := range indexDef.Columns {
				ordinalValue, err := systemCatalogIntValue(int64(i + 1))
				if err != nil {
					return nil, nil, nil, nil, err
				}
				sysIndexColumns = append(sysIndexColumns, []parser.Value{
					indexIDValue,
					parser.StringValue(indexColumn.Name),
					ordinalValue,
				})
			}
		}
	}

	return sysTables, sysColumns, sysIndexes, sysIndexColumns, nil
}

func systemCatalogIntValue(v int64) (parser.Value, error) {
	if v < math.MinInt32 || v > math.MaxInt32 {
		return parser.Value{}, newStorageError("corrupted catalog page")
	}
	return parser.IntValue(int32(v)), nil
}
