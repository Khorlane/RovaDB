package rovadb

import (
	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
	"github.com/Khorlane/RovaDB/internal/storage"
)

const (
	systemTableTables       = "__sys_tables"
	systemTableColumns      = "__sys_columns"
	systemTableIndexes      = "__sys_indexes"
	systemTableIndexColumns = "__sys_index_columns"
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

func (db *DB) ensureSystemCatalogTables(tables map[string]*executor.Table) ([]stagedPage, bool, error) {
	if db == nil || db.pager == nil {
		return nil, false, nil
	}

	nextFreshID := db.pager.NextPageID()
	pages := make([]stagedPage, 0, len(systemCatalogTableSpecs()))
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
		rootPageData, err := storage.BuildSlottedTablePageData(uint32(rootPageID), nil)
		if err != nil {
			return nil, false, wrapStorageError(err)
		}

		table := &executor.Table{
			Name:      spec.name,
			TableID:   nextTableID(tables),
			IsSystem:  true,
			Columns:   append([]parser.ColumnDef(nil), spec.columns...),
			Indexes:   make(map[string]*planner.BasicIndex),
			IndexDefs: nil,
		}
		table.SetStorageMeta(rootPageID, 0)
		tables[spec.name] = table
		pages = append(pages, stagedPage{
			id:    rootPageID,
			data:  rootPageData,
			isNew: isNew,
		})
		changed = true
	}

	return pages, changed, nil
}
