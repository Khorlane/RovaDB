package rovadb

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

// DB is the top-level handle for a RovaDB database.
type DB struct {
	path   string
	closed bool
	tables map[string]*executor.Table
	file   *storage.DBFile
	pager  *storage.Pager
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
	switch stmt.(type) {
	case *parser.CreateTableStmt, *parser.InsertStmt, *parser.DeleteStmt, *parser.UpdateStmt:
	default:
		return Result{}, ErrNotImplemented
	}
	rowsAffected, err := executor.Execute(stmt, db.tables)
	if err != nil {
		return Result{}, err
	}
	if createStmt, ok := stmt.(*parser.CreateTableStmt); ok {
		rootPage := db.pager.NewPage()
		storage.InitTableRootPage(rootPage)
		db.tables[createStmt.Name].SetStorageMeta(rootPage.ID(), 0)
		if err := db.persistCatalog(); err != nil {
			delete(db.tables, createStmt.Name)
			return Result{}, err
		}
	}
	if insertStmt, ok := stmt.(*parser.InsertStmt); ok {
		if err := db.persistInsertedRow(insertStmt.TableName); err != nil {
			return Result{}, err
		}
	}

	return Result{rowsAffected: rowsAffected}, nil
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

	for _, table := range catalog.Tables {
		columns := make([]parser.ColumnDef, 0, len(table.Columns))
		for _, column := range table.Columns {
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
		tables[table.Name].SetStorageMeta(storage.PageID(table.RootPageID), table.RowCount)
	}

	return tables, nil
}

func loadPersistedRows(pager *storage.Pager, tables map[string]*executor.Table) error {
	for _, table := range tables {
		if table == nil || table.PersistedRowCount() == 0 {
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
			return errors.New("rovadb: persisted row count mismatch")
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
