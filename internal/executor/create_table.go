package executor

import (
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

var (
	errTableAlreadyExists      = newExecError("table already exists")
	errTableDoesNotExist       = newExecError("table not found")
	errWrongValueCount         = newExecError("column count mismatch")
	errColumnDoesNotExist      = newExecError("column not found")
	errTypeMismatch            = newExecError("type mismatch")
	errUnsupportedStatement    = newExecError("unsupported query form")
	errNotImplemented          = newExecError("not implemented")
	errCountOrderByUnsupported = newExecError("unsupported query form")
	errInvalidSelectPlan       = newExecError("invalid select plan")
)

// Table is the tiny in-memory table catalog entry.
type Table struct {
	Name              string
	TableID           uint32
	IsSystem          bool
	Columns           []parser.ColumnDef
	Rows              [][]parser.Value // transient operation rows; persisted storage is authoritative
	IndexDefs         []storage.CatalogIndex
	PrimaryKeyDef     *storage.CatalogPrimaryKey
	ForeignKeyDefs    []storage.CatalogForeignKey
	rootPageID        storage.PageID
	persistedRowCount uint32
	tableHeaderPageID storage.PageID
	tableStorageFmt   uint32
	firstSpaceMapID   storage.PageID
	ownedDataPages    uint32
	ownedSpaceMaps    uint32
}

// SetStorageMeta records the persisted storage metadata for a table.
func (t *Table) SetStorageMeta(rootPageID storage.PageID, rowCount uint32) {
	if t == nil {
		return
	}
	t.rootPageID = rootPageID
	t.persistedRowCount = rowCount
}

// RootPageID reports the reserved logical table root page. Normal runtime rows
// live on table-owned Data pages tracked from the TableHeader/SpaceMap chain.
func (t *Table) RootPageID() storage.PageID {
	if t == nil {
		return 0
	}
	return t.rootPageID
}

// PersistedRowCount reports the durable row count stored in metadata.
func (t *Table) PersistedRowCount() uint32 {
	if t == nil {
		return 0
	}
	return t.persistedRowCount
}

// SetPhysicalTableRootMeta records authoritative durable physical table-root metadata.
func (t *Table) SetPhysicalTableRootMeta(tableHeaderPageID storage.PageID, storageFormatVersion uint32, firstSpaceMapPageID storage.PageID, ownedDataPages uint32, ownedSpaceMapPages uint32) {
	if t == nil {
		return
	}
	t.tableHeaderPageID = tableHeaderPageID
	t.tableStorageFmt = storageFormatVersion
	t.firstSpaceMapID = firstSpaceMapPageID
	t.ownedDataPages = ownedDataPages
	t.ownedSpaceMaps = ownedSpaceMapPages
}

func (t *Table) TableHeaderPageID() storage.PageID {
	if t == nil {
		return 0
	}
	return t.tableHeaderPageID
}

func (t *Table) TableStorageFormatVersion() uint32 {
	if t == nil {
		return 0
	}
	return t.tableStorageFmt
}

func (t *Table) FirstSpaceMapPageID() storage.PageID {
	if t == nil {
		return 0
	}
	return t.firstSpaceMapID
}

func (t *Table) OwnedDataPageCount() uint32 {
	if t == nil {
		return 0
	}
	return t.ownedDataPages
}

func (t *Table) OwnedSpaceMapPageCount() uint32 {
	if t == nil {
		return 0
	}
	return t.ownedSpaceMaps
}

// Execute handles the tiny Stage 1 write statement set.
func Execute(stmt any, tables map[string]*Table) (int64, error) {
	switch s := stmt.(type) {
	case *parser.CreateTableStmt:
		if _, exists := tables[s.Name]; exists {
			return 0, errTableAlreadyExists
		}
		table, err := buildCreateTableDefinition(s, tables)
		if err != nil {
			return 0, err
		}
		tables[s.Name] = table
		return 0, nil
	case *parser.InsertStmt:
		return executeInsert(s, tables)
	case *parser.AlterTableAddColumnStmt:
		return executeAlterTableAddColumn(s, tables)
	case *parser.AlterTableAddPrimaryKeyStmt:
		return executeAlterTableAddPrimaryKey(s, tables)
	case *parser.AlterTableAddForeignKeyStmt:
		return executeAlterTableAddForeignKey(s, tables)
	case *parser.AlterTableDropPrimaryKeyStmt:
		return executeAlterTableDropPrimaryKey(s, tables)
	case *parser.AlterTableDropForeignKeyStmt:
		return executeAlterTableDropForeignKey(s, tables)
	case *parser.DeleteStmt:
		return executeDelete(s, tables)
	case *parser.UpdateStmt:
		return executeUpdate(s, tables)
	default:
		return 0, errUnsupportedStatement
	}
}

func nextCreateTableID(tables map[string]*Table) uint32 {
	var maxID uint32
	for _, table := range tables {
		if table != nil && table.TableID > maxID {
			maxID = table.TableID
		}
	}
	return maxID + 1
}

func nextCreateIndexID(tables map[string]*Table) uint32 {
	var maxID uint32
	for _, table := range tables {
		if table == nil {
			continue
		}
		for _, indexDef := range table.IndexDefs {
			if indexDef.IndexID > maxID {
				maxID = indexDef.IndexID
			}
		}
	}
	return maxID + 1
}
