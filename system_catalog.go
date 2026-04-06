package rovadb

import (
	"bytes"
	"sort"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

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
		table.SetStorageMeta(rootPageID, 0)
		tables[spec.name] = table
		if isNew {
			newPageIDs[rootPageID] = struct{}{}
		}
		changed = true
	}

	return newPageIDs, changed, nil
}

func (db *DB) rebuildSystemCatalogRows(tables map[string]*executor.Table, newPageIDs map[storage.PageID]struct{}) ([]stagedPage, bool, error) {
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
	for _, spec := range systemCatalogTableSpecs() {
		table := tables[spec.name]
		if table == nil || !table.IsSystem {
			return nil, false, newStorageError("corrupted catalog page")
		}
		rows := cloneRows(rowSets[spec.name])
		pageData, err := storage.BuildSlottedTablePageData(uint32(table.RootPageID()), rows)
		if err != nil {
			return nil, false, wrapStorageError(err)
		}

		currentPageData, err := db.pager.ReadPage(table.RootPageID())
		if err != nil {
			return nil, false, wrapStorageError(err)
		}
		table.Rows = rows
		table.SetStorageMeta(table.RootPageID(), uint32(len(rows)))
		if !bytes.Equal(currentPageData, pageData) || table.PersistedRowCount() != uint32(len(rows)) {
			changed = true
		}
		_, isNew := newPageIDs[table.RootPageID()]
		pages = append(pages, stagedPage{
			id:    table.RootPageID(),
			data:  pageData,
			isNew: isNew,
		})
	}

	return pages, changed, nil
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
		sysTables = append(sysTables, []parser.Value{
			parser.Int64Value(int64(table.TableID)),
			parser.StringValue(table.Name),
		})
		for i, column := range table.Columns {
			sysColumns = append(sysColumns, []parser.Value{
				parser.Int64Value(int64(table.TableID)),
				parser.StringValue(column.Name),
				parser.StringValue(column.Type),
				parser.Int64Value(int64(i + 1)),
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
			sysIndexes = append(sysIndexes, []parser.Value{
				parser.Int64Value(int64(indexDef.IndexID)),
				parser.StringValue(indexDef.Name),
				parser.Int64Value(int64(table.TableID)),
				parser.BoolValue(indexDef.Unique),
			})
			for i, indexColumn := range indexDef.Columns {
				sysIndexColumns = append(sysIndexColumns, []parser.Value{
					parser.Int64Value(int64(indexDef.IndexID)),
					parser.StringValue(indexColumn.Name),
					parser.Int64Value(int64(i + 1)),
				})
			}
		}
	}

	return sysTables, sysColumns, sysIndexes, sysIndexColumns, nil
}
