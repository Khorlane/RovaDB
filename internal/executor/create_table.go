package executor

import (
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
	"github.com/Khorlane/RovaDB/internal/storage"
)

var (
	errTableAlreadyExists      = newExecError("table already exists")
	errTableDoesNotExist       = newExecError("table not found")
	errWrongValueCount         = newExecError("column count mismatch")
	errColumnDoesNotExist      = newExecError("column not found")
	errTypeMismatch            = newExecError("type mismatch")
	errUnsupportedStatement    = newExecError("unsupported query form")
	errCountOrderByUnsupported = newExecError("unsupported query form")
	errInvalidSelectPlan       = newExecError("invalid select plan")
)

// Table is the tiny in-memory table catalog entry.
type Table struct {
	Name              string
	Columns           []parser.ColumnDef
	Rows              [][]parser.Value // transient operation rows; persisted storage is authoritative
	Indexes           map[string]*planner.BasicIndex
	IndexDefs         []storage.CatalogIndex
	rootPageID        storage.PageID
	persistedRowCount uint32
}

// SetStorageMeta records the persisted storage metadata for a table.
func (t *Table) SetStorageMeta(rootPageID storage.PageID, rowCount uint32) {
	if t == nil {
		return
	}
	t.rootPageID = rootPageID
	t.persistedRowCount = rowCount
}

// RootPageID reports the reserved root page for the table's future row storage.
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

// Execute handles the tiny Stage 1 write statement set.
func Execute(stmt any, tables map[string]*Table) (int64, error) {
	switch s := stmt.(type) {
	case *parser.CreateTableStmt:
		if _, exists := tables[s.Name]; exists {
			return 0, errTableAlreadyExists
		}
		tables[s.Name] = &Table{
			Name:    s.Name,
			Columns: append([]parser.ColumnDef(nil), s.Columns...),
		}
		return 0, nil
	case *parser.InsertStmt:
		return executeInsert(s, tables)
	case *parser.AlterTableAddColumnStmt:
		return executeAlterTableAddColumn(s, tables)
	case *parser.DeleteStmt:
		return executeDelete(s, tables)
	case *parser.UpdateStmt:
		return executeUpdate(s, tables)
	default:
		return 0, errUnsupportedStatement
	}
}
