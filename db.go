package rovadb

import (
	"context"
	"errors"
	"os"
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

	afterJournalWriteHook func() error
}

type stagedPage struct {
	id    storage.PageID
	data  []byte
	isNew bool
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
		var committedTables map[string]*executor.Table
		err := db.execMutatingStatement(func() error {
			stagedTables := cloneTables(db.tables)

			var err error
			rowsAffected, err = executor.Execute(stmt, stagedTables)
			if err != nil {
				return err
			}

			if err := db.applyStagedCreate(stagedTables, stmt.Name); err != nil {
				return err
			}
			committedTables = stagedTables
			return nil
		})
		if err != nil {
			return Result{}, err
		}
		db.tables = committedTables
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.InsertStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		err := db.execMutatingStatement(func() error {
			stagedTables := cloneTables(db.tables)

			var err error
			rowsAffected, err = executor.Execute(stmt, stagedTables)
			if err != nil {
				return err
			}

			if err := db.applyStagedTableRewrite(stagedTables, stmt.TableName); err != nil {
				return err
			}
			committedTables = stagedTables
			return nil
		})
		if err != nil {
			return Result{}, err
		}
		db.tables = committedTables
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.UpdateStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		err := db.execMutatingStatement(func() error {
			stagedTables := cloneTables(db.tables)

			var err error
			rowsAffected, err = executor.Execute(stmt, stagedTables)
			if err != nil {
				return err
			}

			if err := db.applyStagedTableRewrite(stagedTables, stmt.TableName); err != nil {
				return err
			}
			committedTables = stagedTables
			return nil
		})
		if err != nil {
			return Result{}, err
		}
		db.tables = committedTables
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.DeleteStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		err := db.execMutatingStatement(func() error {
			stagedTables := cloneTables(db.tables)

			var err error
			rowsAffected, err = executor.Execute(stmt, stagedTables)
			if err != nil {
				return err
			}

			if err := db.applyStagedTableRewrite(stagedTables, stmt.TableName); err != nil {
				return err
			}
			committedTables = stagedTables
			return nil
		})
		if err != nil {
			return Result{}, err
		}
		db.tables = committedTables
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

// rollbackTxn restores pre-commit page images in memory. Commit remains the
// only durability boundary; crash recovery is still future journal work.
func (db *DB) rollbackTxn() {
	if db == nil || db.txn == nil {
		return
	}
	if db.txn.IsActive() {
		db.pager.RestoreDirtyPages()
		db.txn.Rollback()
	}
	db.pager.ClearDirtyTracking()
}

// execMutatingStatement enforces the internal autocommit shape for mutating
// statements. Durability semantics are intentionally unchanged in this slice.
func (db *DB) execMutatingStatement(apply func() error) error {
	if db == nil {
		return ErrInvalidArgument
	}

	db.beginTxn()
	if err := apply(); err != nil {
		db.rollbackTxn()
		db.clearTxn()
		return err
	}

	if db.txn != nil {
		db.txn.MarkDirty()
	}
	if err := db.commitTxn(); err != nil {
		db.rollbackTxn()
		db.clearTxn()
		return err
	}
	db.clearTxn()
	return nil
}

// commitTxn defines the durability boundary for an internal transaction.
// The rollback journal records pre-commit originals before any database-page
// overwrite. Commit is not complete until database pages are synced and the
// journal is removed. Later recovery will use any surviving journal files.
func (db *DB) commitTxn() error {
	if db == nil || db.txn == nil {
		return nil
	}
	if !db.txn.CanCommit() {
		return nil
	}
	if db.txn.IsDirty() {
		journalPages := db.pager.DirtyPagesWithOriginals()
		if len(journalPages) > 0 {
			if err := storage.WriteRollbackJournal(storage.JournalPath(db.path), db.pager.PageSize(), journalPages); err != nil {
				return err
			}
			if db.afterJournalWriteHook != nil {
				if err := db.afterJournalWriteHook(); err != nil {
					return err
				}
			}
		}
		if err := db.pager.FlushDirty(); err != nil {
			return err
		}
		if err := db.pager.Sync(); err != nil {
			return err
		}
		if len(journalPages) > 0 {
			if err := os.Remove(storage.JournalPath(db.path)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	db.pager.ClearDirtyTracking()
	db.txn.Commit()
	return nil
}

func (db *DB) applyStagedCreate(stagedTables map[string]*executor.Table, tableName string) error {
	if db == nil || db.pager == nil {
		return nil
	}

	table := stagedTables[tableName]
	if table == nil {
		return nil
	}

	rootPageID := db.pager.NextPageID()
	table.SetStorageMeta(rootPageID, 0)

	catalogData, err := storage.BuildCatalogPageData(catalogFromTables(stagedTables))
	if err != nil {
		return err
	}
	rootPageData, err := storage.BuildTablePageData(nil)
	if err != nil {
		return err
	}

	if err := db.stageDirtyState(catalogData, []stagedPage{{
		id:    rootPageID,
		data:  rootPageData,
		isNew: true,
	}}); err != nil {
		return err
	}
	return nil
}

func (db *DB) applyStagedTableRewrite(stagedTables map[string]*executor.Table, tableName string) error {
	if db == nil || db.pager == nil {
		return nil
	}

	table := stagedTables[tableName]
	if table == nil {
		return nil
	}

	table.SetStorageMeta(table.RootPageID(), uint32(len(table.Rows)))

	encodedRows, err := encodeRows(table.Rows)
	if err != nil {
		return err
	}
	tablePageData, err := storage.BuildTablePageData(encodedRows)
	if err != nil {
		return err
	}
	catalogData, err := storage.BuildCatalogPageData(catalogFromTables(stagedTables))
	if err != nil {
		return err
	}

	if err := db.stageDirtyState(catalogData, []stagedPage{{
		id:   table.RootPageID(),
		data: tablePageData,
	}}); err != nil {
		return err
	}
	return nil
}

// Stage 5 correctness requires proposal/staging before apply so a single
// statement cannot leak mixed committed and uncommitted visibility. Crash-safe
// durability is still future work.
func (db *DB) stageDirtyState(catalogData []byte, pages []stagedPage) error {
	if db == nil || db.pager == nil {
		return nil
	}

	for _, staged := range pages {
		var page *storage.Page
		if staged.isNew {
			page = db.pager.NewPage()
			if page.ID() != staged.id {
				db.pager.DiscardNewPage(page.ID())
				return errors.New("rovadb: unexpected new page id")
			}
		} else {
			var err error
			page, err = db.pager.Get(staged.id)
			if err != nil {
				return err
			}
		}

		db.pager.MarkDirtyWithOriginal(page)
		clear(page.Data())
		copy(page.Data(), staged.data)
	}

	catalogPage, err := db.pager.Get(0)
	if err != nil {
		return err
	}
	db.pager.MarkDirtyWithOriginal(catalogPage)
	clear(catalogPage.Data())
	copy(catalogPage.Data(), catalogData)
	// Page mutation requires explicit dirty marking; commit-oriented flush
	// eligibility is driven by dirty tracking.
	return nil
}

func encodeRows(rows [][]parser.Value) ([][]byte, error) {
	encodedRows := make([][]byte, 0, len(rows))
	for _, row := range rows {
		encoded, err := storage.EncodeRow(row)
		if err != nil {
			return nil, err
		}
		encodedRows = append(encodedRows, encoded)
	}
	return encodedRows, nil
}

func cloneTables(tables map[string]*executor.Table) map[string]*executor.Table {
	cloned := make(map[string]*executor.Table, len(tables))
	for name, table := range tables {
		cloned[name] = cloneTable(table)
	}
	return cloned
}

func cloneTable(table *executor.Table) *executor.Table {
	if table == nil {
		return nil
	}

	columns := append([]parser.ColumnDef(nil), table.Columns...)
	rows := cloneRows(table.Rows)

	cloned := &executor.Table{
		Name:    table.Name,
		Columns: columns,
		Rows:    rows,
	}
	cloned.SetStorageMeta(table.RootPageID(), table.PersistedRowCount())
	return cloned
}

func cloneRows(rows [][]parser.Value) [][]parser.Value {
	cloned := make([][]parser.Value, 0, len(rows))
	for _, row := range rows {
		cloned = append(cloned, append([]parser.Value(nil), row...))
	}
	return cloned
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
