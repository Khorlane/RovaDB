package rovadb

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
	"github.com/Khorlane/RovaDB/internal/storage"
	"github.com/Khorlane/RovaDB/internal/txn"
)

var (
	errDuplicateRootPageID      = newStorageError("corrupted catalog page")
	errInvalidStoredTableMeta   = newStorageError("corrupted catalog page")
	ErrTxnAlreadyActive         = errors.New("rovadb: transaction already active")
	ErrTxnCommitWithoutActive   = errors.New("rovadb: commit requires active transaction")
	ErrTxnRollbackWithoutActive = errors.New("rovadb: rollback requires active transaction")
	ErrTxnInvariantViolation    = errors.New("rovadb: transaction invariant violation")
)

// DB is the top-level handle for a RovaDB database.
// Mutating statements execute under an internal autocommit discipline.
type DB struct {
	path   string
	closed bool
	tables map[string]*executor.Table
	file   *storage.DBFile
	pager  *storage.Pager
	txn    *txn.Txn

	afterJournalWriteHook func() error
	afterDatabaseSyncHook func() error
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

	// A surviving rollback journal implies an interrupted commit. Recovery
	// restores last-committed page images before catalog or row metadata loads.
	if err := storage.RecoverFromRollbackJournal(path, storage.PageSize); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, wrapStorageError(err)
	}

	file, err := storage.OpenOrCreate(path)
	if err != nil {
		return nil, wrapStorageError(err)
	}
	pager, err := storage.NewPager(file.File())
	if err != nil {
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		_ = pager.Close()
		_ = file.Close()
		return nil, wrapStorageError(err)
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
	if err := rebuildPersistedIndexes(tables); err != nil {
		_ = pager.Close()
		_ = file.Close()
		return nil, err
	}
	if err := validateTables(tables, true); err != nil {
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
	if err := db.validateTxnState(); err != nil {
		return Result{}, err
	}

	stmt, err := parser.Parse(sql)
	if err != nil {
		return Result{}, err
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
		if err := validateTables(committedTables, false); err != nil {
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
		if err := validateTables(committedTables, false); err != nil {
			return Result{}, err
		}
		db.tables = committedTables
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.AlterTableAddColumnStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		err := db.execMutatingStatement(func() error {
			stagedTables := cloneTables(db.tables)

			var err error
			rowsAffected, err = executor.Execute(stmt, stagedTables)
			if err != nil {
				return err
			}

			if err := db.applyStagedCatalogOnly(stagedTables); err != nil {
				return err
			}
			committedTables = stagedTables
			return nil
		})
		if err != nil {
			return Result{}, err
		}
		if err := validateTables(committedTables, false); err != nil {
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
		if err := validateTables(committedTables, false); err != nil {
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
		if err := validateTables(committedTables, false); err != nil {
			return Result{}, err
		}
		db.tables = committedTables
		return Result{rowsAffected: rowsAffected}, nil
	default:
		return Result{}, newExecError("unsupported query form")
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
	if err := db.validateTxnState(); err != nil {
		return &Rows{err: err, idx: -1}, nil
	}

	sel, ok := parser.ParseSelectExpr(sql)
	if ok {
		if sel.TableName != "" {
			if err := validateTables(db.tables, false); err != nil {
				return &Rows{err: err, idx: -1}, nil
			}
			plan, err := planner.PlanSelect(sel, plannerTableMetadata(db.tables))
			if err != nil {
				return &Rows{err: err, idx: -1}, nil
			}
			rows, err := executor.Select(plan, db.tables)
			if err != nil {
				return &Rows{err: err, idx: -1}, nil
			}
			columns, err := executor.ProjectedColumnNames(plan, db.tables[sel.TableName])
			if err != nil {
				return &Rows{err: err, idx: -1}, nil
			}
			return newRows(columns, materializeRows(rows)), nil
		}

		value, err := executor.Eval(sel.Expr)
		if err == nil {
			return newRows(nil, [][]any{{value.Any()}}), nil
		}
	}

	return &Rows{err: classifyQueryParseError(sql), idx: -1}, nil
}

func materializeRows(rows [][]parser.Value) [][]any {
	materialized := make([][]any, 0, len(rows))
	for _, row := range rows {
		values := make([]any, 0, len(row))
		for _, value := range row {
			values = append(values, value.Any())
		}
		materialized = append(materialized, values)
	}
	return materialized
}

func plannerTableMetadata(tables map[string]*executor.Table) map[string]*planner.TableMetadata {
	if len(tables) == 0 {
		return nil
	}

	metadata := make(map[string]*planner.TableMetadata, len(tables))
	for tableName, table := range tables {
		if table == nil {
			continue
		}
		metadata[tableName] = &planner.TableMetadata{
			Indexes: table.Indexes,
		}
	}
	return metadata
}

func (db *DB) beginTxn() error {
	if db == nil {
		return ErrInvalidArgument
	}
	// Transaction state transitions are explicit. Terminal txn objects are not
	// reused; each mutating statement gets a fresh internal txn object.
	if db.txn != nil && db.txn.IsActive() {
		return ErrTxnAlreadyActive
	}
	db.txn = txn.NewTxn()
	return nil
}

func (db *DB) clearTxn() {
	if db == nil {
		return
	}
	db.txn = nil
}

// rollbackTxn restores pre-commit page images in memory. Commit remains the
// only durability boundary; surviving journals are handled on the next open.
func (db *DB) rollbackTxn() error {
	if db == nil {
		return ErrInvalidArgument
	}
	if db.txn == nil {
		return nil
	}
	if db.pager == nil {
		return newExecError("invalid transaction state")
	}
	if db.txn.IsActive() {
		db.pager.RestoreDirtyPages()
		if err := db.txn.Rollback(); err != nil {
			return err
		}
		db.pager.ClearDirtyTracking()
		if len(db.pager.DirtyPages()) != 0 {
			return newExecError("invalid transaction state")
		}
		for _, page := range db.pager.DirtyPagesWithOriginals() {
			if db.pager.HasOriginal(page) {
				return newExecError("invalid transaction state")
			}
		}
		return nil
	}
	if db.txn.CanCommit() {
		return newExecError("invalid transaction state")
	}
	return nil
}

// execMutatingStatement enforces internal autocommit for mutating statements.
func (db *DB) execMutatingStatement(apply func() error) error {
	if db == nil {
		return ErrInvalidArgument
	}

	if err := db.beginTxn(); err != nil {
		return err
	}
	if err := apply(); err != nil {
		if rollbackErr := db.rollbackTxn(); rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}
		db.clearTxn()
		return err
	}

	if db.txn != nil {
		if err := db.txn.MarkDirty(); err != nil {
			if rollbackErr := db.rollbackTxn(); rollbackErr != nil {
				return errors.Join(err, rollbackErr)
			}
			db.clearTxn()
			return err
		}
	}
	if err := db.commitTxn(); err != nil {
		if rollbackErr := db.rollbackTxn(); rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}
		db.clearTxn()
		return err
	}
	db.clearTxn()
	return nil
}

// commitTxn is the only durability boundary for a mutating statement. It
// writes pre-commit originals to the rollback journal, syncs database pages,
// removes the journal, then commits the txn state. Any invariant failure here
// is a correctness bug, not expected runtime flow.
func (db *DB) commitTxn() error {
	if db == nil {
		return ErrInvalidArgument
	}
	if db.txn == nil {
		return nil
	}
	if db.pager == nil {
		return newExecError("invalid transaction state")
	}
	if !db.txn.CanCommit() {
		return newExecError("invalid transaction state")
	}
	if db.txn.IsDirty() {
		journalPages := db.pager.DirtyPagesWithOriginals()
		if len(journalPages) > 0 {
			if err := storage.WriteRollbackJournal(storage.JournalPath(db.path), db.pager.PageSize(), journalPages); err != nil {
				return wrapStorageError(err)
			}
			if db.afterJournalWriteHook != nil {
				if err := db.afterJournalWriteHook(); err != nil {
					return err
				}
			}
		}
		if err := db.pager.FlushDirty(); err != nil {
			return wrapStorageError(err)
		}
		if err := db.pager.Sync(); err != nil {
			return wrapStorageError(err)
		}
		if db.afterDatabaseSyncHook != nil {
			if err := db.afterDatabaseSyncHook(); err != nil {
				return err
			}
		}
		if len(journalPages) > 0 {
			if err := os.Remove(storage.JournalPath(db.path)); err != nil && !os.IsNotExist(err) {
				return wrapStorageError(err)
			}
		}
	}
	db.pager.ClearDirtyTracking()
	if len(db.pager.DirtyPages()) != 0 || len(db.pager.DirtyPagesWithOriginals()) != 0 {
		return newExecError("invalid transaction state")
	}
	if _, err := os.Stat(storage.JournalPath(db.path)); err == nil {
		return newExecError("invalid transaction state")
	} else if !errors.Is(err, os.ErrNotExist) {
		return wrapStorageError(err)
	}
	if err := db.txn.Commit(); err != nil {
		if errors.Is(err, txn.ErrNoActiveTxn) || errors.Is(err, txn.ErrInvalidCommitState) {
			return newExecError("invalid transaction state")
		}
		return err
	}
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
		return wrapStorageError(err)
	}
	rootPageData, err := storage.BuildTablePageData(nil)
	if err != nil {
		return wrapStorageError(err)
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

	// UPDATE/DELETE and any persisted row-content change currently use a full
	// table root-page rewrite. This is the intentional fallback path when the
	// planner/executor cannot use a narrower persistence strategy.
	table.SetStorageMeta(table.RootPageID(), uint32(len(table.Rows)))

	encodedRows, err := encodeRows(table.Rows)
	if err != nil {
		return err
	}
	tablePageData, err := storage.BuildTablePageData(encodedRows)
	if err != nil {
		return wrapStorageError(err)
	}
	catalogData, err := storage.BuildCatalogPageData(catalogFromTables(stagedTables))
	if err != nil {
		return wrapStorageError(err)
	}

	if err := db.stageDirtyState(catalogData, []stagedPage{{
		id:   table.RootPageID(),
		data: tablePageData,
	}}); err != nil {
		return err
	}
	return nil
}

func (db *DB) applyStagedCatalogOnly(stagedTables map[string]*executor.Table) error {
	if db == nil || db.pager == nil {
		return nil
	}

	// Schema metadata changes such as ALTER TABLE ... ADD COLUMN are catalog-only
	// here. Existing stored rows are not rewritten; older rows are padded in
	// memory when materialized against the wider schema.
	catalogData, err := storage.BuildCatalogPageData(catalogFromTables(stagedTables))
	if err != nil {
		return wrapStorageError(err)
	}
	return db.stageDirtyState(catalogData, nil)
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
				return newStorageError("unexpected new page id")
			}
		} else {
			var err error
			page, err = db.pager.Get(staged.id)
			if err != nil {
				return wrapStorageError(err)
			}
		}

		db.pager.MarkDirtyWithOriginal(page)
		clear(page.Data())
		copy(page.Data(), staged.data)
	}

	catalogPage, err := db.pager.Get(0)
	if err != nil {
		return wrapStorageError(err)
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
			return nil, wrapStorageError(err)
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
		Indexes: cloneIndexes(table.Indexes),
	}
	columnNames := columnNamesForTable(cloned)
	for _, index := range cloned.Indexes {
		if index == nil {
			continue
		}
		_ = index.Rebuild(columnNames, cloned.Rows)
	}
	cloned.SetStorageMeta(table.RootPageID(), table.PersistedRowCount())
	return cloned
}

func cloneIndexes(indexes map[string]*planner.BasicIndex) map[string]*planner.BasicIndex {
	if len(indexes) == 0 {
		return nil
	}

	cloned := make(map[string]*planner.BasicIndex, len(indexes))
	for columnName, index := range indexes {
		if index == nil {
			continue
		}
		cloned[columnName] = planner.NewBasicIndex(index.TableName, index.ColumnName)
	}
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
			Indexes:    make([]storage.CatalogIndex, 0, len(table.Indexes)),
		}
		for _, column := range table.Columns {
			entry.Columns = append(entry.Columns, storage.CatalogColumn{
				Name: column.Name,
				Type: catalogColumnType(column.Type),
			})
		}
		indexNames := make([]string, 0, len(table.Indexes))
		for columnName := range table.Indexes {
			indexNames = append(indexNames, columnName)
		}
		sort.Strings(indexNames)
		for _, columnName := range indexNames {
			entry.Indexes = append(entry.Indexes, storage.CatalogIndex{ColumnName: columnName})
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
			Indexes: make(map[string]*planner.BasicIndex),
		}
		tables[table.Name].SetStorageMeta(rootPageID, table.RowCount)
		for _, index := range table.Indexes {
			if index.ColumnName == "" {
				return nil, errInvalidStoredTableMeta
			}
			tables[table.Name].Indexes[index.ColumnName] = planner.NewBasicIndex(table.Name, index.ColumnName)
		}
	}

	return tables, nil
}

func rebuildPersistedIndexes(tables map[string]*executor.Table) error {
	for _, table := range tables {
		if table == nil || len(table.Indexes) == 0 {
			continue
		}
		columnNames := columnNamesForTable(table)
		for columnName, index := range table.Indexes {
			if index == nil {
				table.Indexes[columnName] = planner.NewBasicIndex(table.Name, columnName)
				index = table.Indexes[columnName]
			}
			if err := index.Rebuild(columnNames, table.Rows); err != nil {
				return err
			}
		}
	}
	return nil
}

func columnNamesForTable(table *executor.Table) []string {
	if table == nil {
		return nil
	}
	columnNames := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		columnNames = append(columnNames, column.Name)
	}
	return columnNames
}

func (db *DB) defineBasicIndex(tableName, columnName string) error {
	if db == nil {
		return ErrInvalidArgument
	}

	var committedTables map[string]*executor.Table
	err := db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		table := stagedTables[tableName]
		if table == nil {
			return newExecError("table not found")
		}
		columnNames := columnNamesForTable(table)
		found := false
		for _, column := range table.Columns {
			if column.Name == columnName {
				found = true
			}
		}
		if !found {
			return newExecError("column not found")
		}
		if table.Indexes == nil {
			table.Indexes = make(map[string]*planner.BasicIndex)
		}
		index := planner.NewBasicIndex(tableName, columnName)
		if err := index.Rebuild(columnNames, table.Rows); err != nil {
			return err
		}
		table.Indexes[columnName] = index

		catalogData, err := storage.BuildCatalogPageData(catalogFromTables(stagedTables))
		if err != nil {
			return wrapStorageError(err)
		}
		if err := db.stageDirtyState(catalogData, nil); err != nil {
			return err
		}
		committedTables = stagedTables
		return nil
	})
	if err != nil {
		return err
	}
	if err := validateTables(committedTables, false); err != nil {
		return err
	}
	if committedTables != nil {
		db.tables = committedTables
	}
	return nil
}

func loadPersistedRows(pager *storage.Pager, tables map[string]*executor.Table) error {
	for _, table := range tables {
		if table == nil {
			continue
		}

		page, err := pager.Get(table.RootPageID())
		if err != nil {
			return wrapStorageError(err)
		}
		payloads, err := storage.ReadRowsFromTablePage(page)
		if err != nil {
			return wrapStorageError(err)
		}
		if uint32(len(payloads)) != table.PersistedRowCount() {
			return newStorageError("row count mismatch")
		}

		table.Rows = table.Rows[:0]
		for _, payload := range payloads {
			row, err := storage.DecodeRow(payload)
			if err != nil {
				return wrapStorageError(err)
			}
			if len(row) > len(table.Columns) {
				return newStorageError("row width mismatch")
			}
			table.Rows = append(table.Rows, padRowToSchema(row, len(table.Columns)))
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
		return "", newStorageError("corrupted catalog page")
	}
}

func classifyQueryParseError(sql string) error {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	if strings.HasPrefix(upper, "SELECT ") && strings.Contains(upper, " WHERE ") {
		return newParseError("invalid where clause")
	}
	return newParseError("unsupported query form")
}

func wrapStorageError(err error) error {
	if err == nil {
		return nil
	}
	var dbErr *DBError
	if errors.As(err, &dbErr) {
		return err
	}
	return newStorageError(err.Error())
}

func padRowToSchema(row []parser.Value, width int) []parser.Value {
	if len(row) >= width {
		return row
	}

	padded := append([]parser.Value(nil), row...)
	for len(padded) < width {
		padded = append(padded, parser.NullValue())
	}
	return padded
}

func validateTables(tables map[string]*executor.Table, storageBoundary bool) error {
	for _, table := range tables {
		if err := validateTableRowCount(table, storageBoundary); err != nil {
			return err
		}
		if err := validateIndexConsistency(table); err != nil {
			return err
		}
	}
	return nil
}

func validateTableRowCount(table *executor.Table, storageBoundary bool) error {
	if table == nil {
		return nil
	}
	if uint32(len(table.Rows)) == table.PersistedRowCount() {
		return nil
	}
	if storageBoundary {
		return newStorageError("row count mismatch")
	}
	return newExecError("row count mismatch")
}

func validateIndexConsistency(table *executor.Table) error {
	if table == nil || len(table.Indexes) == 0 {
		return nil
	}

	columnNames := columnNamesForTable(table)
	for columnName, index := range table.Indexes {
		if index == nil {
			return newExecError("index/table mismatch")
		}
		if index.TableName != table.Name || index.ColumnName != columnName {
			return newExecError("index/table mismatch")
		}

		expected := planner.NewBasicIndex(table.Name, columnName)
		if err := expected.Rebuild(columnNames, table.Rows); err != nil {
			return newExecError("index/table mismatch")
		}
		if !reflect.DeepEqual(expected.Entries, index.Entries) {
			return newExecError("index/table mismatch")
		}
	}

	return nil
}

func (db *DB) validateTxnState() error {
	if db == nil {
		return ErrInvalidArgument
	}
	if db.pager == nil {
		if db.txn != nil {
			return newExecError("invalid transaction state")
		}
		return nil
	}

	hasDirty := len(db.pager.DirtyPages()) != 0 || len(db.pager.DirtyPagesWithOriginals()) != 0
	if db.txn == nil {
		if hasDirty {
			return newExecError("invalid transaction state")
		}
		return nil
	}
	if db.txn.IsActive() {
		return nil
	}
	if hasDirty {
		return newExecError("invalid transaction state")
	}
	return nil
}
