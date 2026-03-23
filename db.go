package rovadb

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
	"github.com/Khorlane/RovaDB/internal/txn"
)

var (
	errDuplicateRootPageID       = errors.New("rovadb: duplicate root page id")
	errPersistedRowCountMismatch = errors.New("rovadb: persisted row count mismatch")
	errInvalidStoredTableMeta    = errors.New("rovadb: invalid stored table metadata")
)

// DB is the top-level handle for a RovaDB database.
type DB struct {
	path   string
	closed bool
	tables map[string]*executor.Table
	file   *storage.DBFile
	pager  *storage.Pager
	txn    *txn.Txn
}

// Open returns a database handle for the given path.
func Open(path string) (*DB, error) {
	if strings.TrimSpace(path) == "" {
		return nil, ErrInvalidArgument
	}

	file, err := storage.OpenOrCreate(path)
	if err != nil {
		return nil, err
	}
	pager, err := storage.NewPager(file.File())
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		_ = pager.Close()
		_ = file.Close()
		return nil, err
	}
	tables, err := tablesFromCatalog(catalog)
	if err != nil {
		_ = pager.Close()
		_ = file.Close()
		return nil, err
	}
	if err := loadPersistedRows(pager, tables); err != nil {
		_ = pager.Close()
		_ = file.Close()
		return nil, err
	}

	return &DB{
		path:   path,
		file:   file,
		pager:  pager,
		tables: tables,
		txn:    nil,
	}, nil
}

// Close releases database resources.
func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	if db.closed {
		return nil
	}

	db.closed = true
	if db.pager != nil {
		if err := db.pager.Close(); err != nil {
			return err
		}
	}
	if db.file != nil {
		return db.file.Close()
	}
	return nil
}

// Exec validates the call and reserves write execution for a later engine pass.
func (db *DB) Exec(ctx context.Context, sql string, args ...any) (Result, error) {
	if ctx == nil {
		return Result{}, ErrInvalidArgument
	}
	if db == nil {
		return Result{}, ErrInvalidArgument
	}
	if db.closed {
		return Result{}, ErrClosed
	}
	if strings.TrimSpace(sql) == "" {
		return Result{}, ErrInvalidArgument
	}
	if db.tables == nil {
		db.tables = make(map[string]*executor.Table)
	}

	stmt, err := parser.Parse(sql)
	if err != nil {
		return Result{}, ErrNotImplemented
	}
	switch stmt := stmt.(type) {
	case *parser.CreateTableStmt:
		var rowsAffected int64
		err := db.execMutatingStatement(func() error {
			var err error
			rowsAffected, err = executor.Execute(stmt, db.tables)
			if err != nil {
				return err
			}

			rootPage := db.pager.NewPage()
			storage.InitTableRootPage(rootPage)
			db.tables[stmt.Name].SetStorageMeta(rootPage.ID(), 0)
			if err := db.persistCatalog(); err != nil {
				delete(db.tables, stmt.Name)
				return err
			}
			return nil
		})
		if err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.InsertStmt:
		var rowsAffected int64
		err := db.execMutatingStatement(func() error {
			var err error
			rowsAffected, err = executor.Execute(stmt, db.tables)
			if err != nil {
				return err
			}
			return db.persistInsertedRow(stmt.TableName)
		})
		if err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.UpdateStmt:
		var rowsAffected int64
		err := db.execMutatingStatement(func() error {
			var err error
			rowsAffected, err = executor.Execute(stmt, db.tables)
			if err != nil {
				return err
			}
			return db.rewritePersistedTable(stmt.TableName)
		})
		if err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.DeleteStmt:
		var rowsAffected int64
		err := db.execMutatingStatement(func() error {
			var err error
			rowsAffected, err = executor.Execute(stmt, db.tables)
			if err != nil {
				return err
			}
			return db.rewritePersistedTable(stmt.TableName)
		})
		if err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	default:
		return Result{}, ErrNotImplemented
	}
}

// Query validates the call and reserves query execution for a later engine pass.
func (db *DB) Query(ctx context.Context, sql string, args ...any) (*Rows, error) {
	if ctx == nil {
		return nil, ErrInvalidArgument
	}
	if db == nil {
		return nil, ErrInvalidArgument
	}
	if db.closed {
		return nil, ErrClosed
	}
	if strings.TrimSpace(sql) == "" {
		return nil, ErrInvalidArgument
	}

	sel, ok := parser.ParseSelectExpr(sql)
	if ok {
		if sel.TableName != "" {
			rows, err := executor.Select(sel, db.tables)
			if err != nil {
				return &Rows{err: err, index: -1}, nil
			}
			return &Rows{values: rows, index: -1}, nil
		}

		value, err := executor.Eval(sel.Expr)
		if err == nil {
			return &Rows{
				index:  -1,
				values: [][]parser.Value{{value}},
			}, nil
		}
	}

	return &Rows{err: ErrNotImplemented, index: -1}, nil
}

func (db *DB) persistCatalog() error {
	if db == nil || db.pager == nil {
		return nil
	}
	if err := storage.SaveCatalog(db.pager, catalogFromTables(db.tables)); err != nil {
		return err
	}
	return db.pager.Flush()
}

func (db *DB) persistInsertedRow(tableName string) error {
	if db == nil || db.pager == nil {
		return nil
	}

	table := db.tables[tableName]
	if table == nil || len(table.Rows) == 0 {
		return nil
	}

	page, err := db.pager.Get(table.RootPageID())
	if err != nil {
		return err
	}
	row, err := storage.EncodeRow(table.Rows[len(table.Rows)-1])
	if err != nil {
		return err
	}
	if err := storage.AppendRowToTablePage(page, row); err != nil {
		return err
	}

	table.SetStorageMeta(table.RootPageID(), storage.TablePageRowCount(page))
	return db.persistCatalog()
}

func (db *DB) rewritePersistedTable(tableName string) error {
	if db == nil || db.pager == nil {
		return nil
	}

	table := db.tables[tableName]
	if table == nil {
		return nil
	}

	encodedRows := make([][]byte, 0, len(table.Rows))
	for _, row := range table.Rows {
		encoded, err := storage.EncodeRow(row)
		if err != nil {
			return err
		}
		encodedRows = append(encodedRows, encoded)
	}

	page, err := db.pager.Get(table.RootPageID())
	if err != nil {
		return err
	}
	if err := storage.RewriteTablePage(page, encodedRows); err != nil {
		return err
	}

	table.SetStorageMeta(table.RootPageID(), storage.TablePageRowCount(page))
	return db.persistCatalog()
}

func (db *DB) beginTxn() {
	if db == nil {
		return
	}
	if db.txn == nil || !db.txn.IsActive() {
		db.txn = txn.NewTxn()
	}
}

func (db *DB) clearTxn() {
	if db == nil {
		return
	}
	db.txn = nil
}

// execMutatingStatement enforces the internal autocommit shape for mutating
// statements. Durability semantics are intentionally unchanged in this slice.
func (db *DB) execMutatingStatement(apply func() error) error {
	if db == nil {
		return ErrInvalidArgument
	}

	db.beginTxn()
	if err := apply(); err != nil {
		if db.txn != nil && db.txn.IsActive() {
			db.txn.Rollback()
		}
		db.clearTxn()
		return err
	}

	if db.txn != nil {
		db.txn.MarkDirty()
	}
	if db.txn != nil && db.txn.IsActive() {
		db.txn.Commit()
	}
	db.clearTxn()
	return nil
}

func catalogFromTables(tables map[string]*executor.Table) *storage.CatalogData {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)

	catalog := &storage.CatalogData{Tables: make([]storage.CatalogTable, 0, len(names))}
	for _, name := range names {
		table := tables[name]
		entry := storage.CatalogTable{
			Name:       table.Name,
			RootPageID: uint32(table.RootPageID()),
			RowCount:   table.PersistedRowCount(),
			Columns:    make([]storage.CatalogColumn, 0, len(table.Columns)),
		}
		for _, column := range table.Columns {
			entry.Columns = append(entry.Columns, storage.CatalogColumn{
				Name: column.Name,
				Type: catalogColumnType(column.Type),
			})
		}
		catalog.Tables = append(catalog.Tables, entry)
	}
	return catalog
}

func tablesFromCatalog(catalog *storage.CatalogData) (map[string]*executor.Table, error) {
	tables := make(map[string]*executor.Table)
	if catalog == nil {
		return tables, nil
	}
	seenRootPageIDs := make(map[storage.PageID]struct{}, len(catalog.Tables))

	for _, table := range catalog.Tables {
		if table.Name == "" || table.RootPageID < 1 || len(table.Columns) == 0 {
			return nil, errInvalidStoredTableMeta
		}
		rootPageID := storage.PageID(table.RootPageID)
		if _, ok := seenRootPageIDs[rootPageID]; ok {
			return nil, errDuplicateRootPageID
		}
		seenRootPageIDs[rootPageID] = struct{}{}

		columns := make([]parser.ColumnDef, 0, len(table.Columns))
		for _, column := range table.Columns {
			if column.Name == "" {
				return nil, errInvalidStoredTableMeta
			}
			columnType, err := parserColumnType(column.Type)
			if err != nil {
				return nil, err
			}
			columns = append(columns, parser.ColumnDef{Name: column.Name, Type: columnType})
		}
		tables[table.Name] = &executor.Table{
			Name:    table.Name,
			Columns: columns,
		}
		tables[table.Name].SetStorageMeta(rootPageID, table.RowCount)
	}

	return tables, nil
}

func loadPersistedRows(pager *storage.Pager, tables map[string]*executor.Table) error {
	for _, table := range tables {
		if table == nil {
			continue
		}

		page, err := pager.Get(table.RootPageID())
		if err != nil {
			return err
		}
		payloads, err := storage.ReadRowsFromTablePage(page)
		if err != nil {
			return err
		}
		if uint32(len(payloads)) != table.PersistedRowCount() {
			return errPersistedRowCountMismatch
		}

		table.Rows = table.Rows[:0]
		for _, payload := range payloads {
			row, err := storage.DecodeRow(payload)
			if err != nil {
				return err
			}
			table.Rows = append(table.Rows, row)
		}
	}

	return nil
}

func catalogColumnType(columnType string) uint8 {
	switch columnType {
	case parser.ColumnTypeInt:
		return storage.CatalogColumnTypeInt
	default:
		return storage.CatalogColumnTypeText
	}
}

func parserColumnType(columnType uint8) (string, error) {
	switch columnType {
	case storage.CatalogColumnTypeInt:
		return parser.ColumnTypeInt, nil
	case storage.CatalogColumnTypeText:
		return parser.ColumnTypeText, nil
	default:
		return "", errors.New("rovadb: invalid catalog column type")
	}
}
