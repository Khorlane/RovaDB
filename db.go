package rovadb

import (
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/Khorlane/RovaDB/internal/bufferpool"
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
	pool   *bufferpool.BufferPool
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
	poolSize := int(pager.NextPageID()) + 1
	pool := bufferpool.New(poolSize, pagerPageLoader{pager: pager})
	catalog, err := storage.LoadCatalog(storage.PageReaderFunc(func(pageID storage.PageID) ([]byte, error) {
		return readCommittedPageData(pool, pageID)
	}))
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
	if err := validatePersistedIndexRoots(pool, tables); err != nil {
		_ = pager.Close()
		_ = file.Close()
		return nil, err
	}
	if err := loadPersistedRows(pool, tables); err != nil {
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
	clearLoadedRows(tables)

	return &DB{
		path:   path,
		file:   file,
		pager:  pager,
		pool:   pool,
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

// Exec executes a non-SELECT statement and returns a write result.
func (db *DB) Exec(query string, args ...any) (Result, error) {
	return db.exec(query, args...)
}

func (db *DB) exec(query string, args ...any) (Result, error) {
	if db == nil {
		return Result{}, ErrInvalidArgument
	}
	if db.closed {
		return Result{}, ErrClosed
	}
	if strings.TrimSpace(query) == "" {
		return Result{}, ErrInvalidArgument
	}
	if db.tables == nil {
		db.tables = make(map[string]*executor.Table)
	}
	if err := db.validateTxnState(); err != nil {
		return Result{}, err
	}

	stmt, err := parser.Parse(query)
	if err != nil {
		return Result{}, err
	}
	if err := parser.BindPlaceholders(stmt, args); err != nil {
		return Result{}, err
	}
	switch stmt := stmt.(type) {
	case *parser.SelectExpr:
		return Result{}, ErrExecDisallowsSelect
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
		clearLoadedRows(committedTables)
		db.tables = committedTables
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.InsertStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		err := db.execMutatingStatement(func() error {
			stagedTables := cloneTables(db.tables)
			if err := db.loadRowsIntoTables(stagedTables, stmt.TableName); err != nil {
				return err
			}

			var err error
			rowsAffected, err = executor.Execute(stmt, stagedTables)
			if err != nil {
				return err
			}

			if err := db.applyStagedInsert(stagedTables, stmt.TableName); err != nil {
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
		clearLoadedRows(committedTables)
		db.tables = committedTables
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.AlterTableAddColumnStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		err := db.execMutatingStatement(func() error {
			stagedTables := cloneTables(db.tables)
			if err := db.loadRowsIntoTables(stagedTables, stmt.TableName); err != nil {
				return err
			}

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
		clearLoadedRows(committedTables)
		db.tables = committedTables
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.CreateIndexStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		err := db.execMutatingStatement(func() error {
			stagedTables := cloneTables(db.tables)
			if err := db.loadRowsIntoTables(stagedTables, stmt.TableName); err != nil {
				return err
			}

			var err error
			rowsAffected, committedTables, err = executeCreateIndex(stmt, stagedTables)
			if err != nil {
				return err
			}

			if err := db.applyStagedIndexCreate(stagedTables, stmt.TableName, stmt.Name); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return Result{}, err
		}
		if err := validateTables(committedTables, false); err != nil {
			return Result{}, err
		}
		clearLoadedRows(committedTables)
		db.tables = committedTables
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.DropIndexStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		err := db.execMutatingStatement(func() error {
			stagedTables := cloneTables(db.tables)

			var err error
			rowsAffected, committedTables, err = executeDropIndex(stmt, stagedTables)
			if err != nil {
				return err
			}

			if err := db.applyStagedCatalogOnly(stagedTables); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return Result{}, err
		}
		if err := validateTables(committedTables, false); err != nil {
			return Result{}, err
		}
		clearLoadedRows(committedTables)
		db.tables = committedTables
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.DropTableStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		err := db.execMutatingStatement(func() error {
			stagedTables := cloneTables(db.tables)

			var err error
			rowsAffected, committedTables, err = executeDropTable(stmt, stagedTables)
			if err != nil {
				return err
			}

			if err := db.applyStagedCatalogOnly(stagedTables); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return Result{}, err
		}
		if err := validateTables(committedTables, false); err != nil {
			return Result{}, err
		}
		clearLoadedRows(committedTables)
		db.tables = committedTables
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.UpdateStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		err := db.execMutatingStatement(func() error {
			stagedTables := cloneTables(db.tables)
			if err := db.loadRowsIntoTables(stagedTables, stmt.TableName); err != nil {
				return err
			}

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
		clearLoadedRows(committedTables)
		db.tables = committedTables
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.DeleteStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		err := db.execMutatingStatement(func() error {
			stagedTables := cloneTables(db.tables)
			if err := db.loadRowsIntoTables(stagedTables, stmt.TableName); err != nil {
				return err
			}

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
		clearLoadedRows(committedTables)
		db.tables = committedTables
		return Result{rowsAffected: rowsAffected}, nil
	default:
		return Result{}, newExecError("unsupported query form")
	}
}

// Query executes a SELECT statement and returns a fully materialized row set.
func (db *DB) Query(query string, args ...any) (*Rows, error) {
	return db.query(query, args...)
}

func (db *DB) query(query string, args ...any) (*Rows, error) {
	if db == nil {
		return nil, ErrInvalidArgument
	}
	if db.closed {
		return nil, ErrClosed
	}
	if strings.TrimSpace(query) == "" {
		return nil, ErrInvalidArgument
	}
	if err := db.validateTxnState(); err != nil {
		return &Rows{err: err, idx: -1}, nil
	}

	stmt, err := parser.Parse(query)
	if err != nil {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "SELECT ") {
			return &Rows{err: classifyQueryParseError(query), idx: -1}, nil
		}
		return nil, err
	}
	sel, ok := stmt.(*parser.SelectExpr)
	if !ok {
		return nil, ErrQueryRequiresSelect
	}
	if err := parser.BindPlaceholders(stmt, args); err != nil {
		return &Rows{err: err, idx: -1}, nil
	}

	if sel.TableName != "" {
		if err := validateTables(db.tables, false); err != nil {
			return &Rows{err: err, idx: -1}, nil
		}
		plan, err := planner.PlanSelect(sel, plannerTableMetadata(db.tables))
		if err != nil {
			return &Rows{err: err, idx: -1}, nil
		}
		execTables, err := db.tablesForSelect(plan)
		if err != nil {
			return &Rows{err: err, idx: -1}, nil
		}
		if err := validateTables(execTables, false); err != nil {
			return &Rows{err: err, idx: -1}, nil
		}
		rows, err := executor.Select(plan, execTables)
		if err != nil {
			return &Rows{err: err, idx: -1}, nil
		}
		columns, err := executor.ProjectedColumnNamesForPlan(plan, execTables)
		if err != nil {
			return &Rows{err: err, idx: -1}, nil
		}
		return newRows(columns, materializeRows(rows)), nil
	}

	value, err := executor.Eval(sel.Expr)
	if err != nil {
		return &Rows{err: err, idx: -1}, nil
	}
	return newRows(nil, [][]any{{apiValue(value)}}), nil
}

// QueryRow executes Query and wraps the resulting row set for deferred handling.
func (db *DB) QueryRow(query string, args ...any) *Row {
	rows, err := db.Query(query, args...)
	if err != nil {
		rows = &Rows{
			idx: -1,
			err: err,
		}
	}
	return newRow(rows)
}

func materializeRows(rows [][]parser.Value) [][]any {
	materialized := make([][]any, 0, len(rows))
	for _, row := range rows {
		values := make([]any, 0, len(row))
		for _, value := range row {
			values = append(values, apiValue(value))
		}
		materialized = append(materialized, values)
	}
	return materialized
}

func (db *DB) tablesForSelect(plan *planner.SelectPlan) (map[string]*executor.Table, error) {
	if plan == nil || plan.Stmt == nil || plan.Stmt.TableName == "" {
		return db.tables, nil
	}

	execTables := make(map[string]*executor.Table, len(db.tables))
	for name, existing := range db.tables {
		execTables[name] = existing
	}
	for _, tableName := range tableNamesForSelect(plan) {
		table := db.tables[tableName]
		if table == nil {
			return nil, newExecError("table not found: " + tableName)
		}
		rows, err := db.scanTableRows(table)
		if err != nil {
			return nil, err
		}
		execTables[table.Name] = &executor.Table{
			Name:      table.Name,
			Columns:   append([]parser.ColumnDef(nil), table.Columns...),
			Rows:      rows,
			Indexes:   table.Indexes,
			IndexDefs: cloneIndexDefs(table.IndexDefs),
		}
		execTables[table.Name].SetStorageMeta(table.RootPageID(), table.PersistedRowCount())
	}
	return execTables, nil
}

func (db *DB) scanTableRows(table *executor.Table) ([][]parser.Value, error) {
	if db == nil || table == nil {
		return nil, ErrInvalidArgument
	}

	pageData, err := readCommittedPageData(db.pool, table.RootPageID())
	if err != nil {
		return nil, wrapStorageError(err)
	}
	if pageData == nil {
		return nil, newStorageError("corrupted table page")
	}
	rows, err := decodePersistedTableRows(pageData, table.Columns)
	if err != nil {
		return nil, err
	}
	if uint32(len(rows)) != table.PersistedRowCount() {
		return nil, newStorageError("row count mismatch")
	}
	return cloneRows(rows), nil
}

func tableNamesForSelect(plan *planner.SelectPlan) []string {
	if plan == nil || plan.Stmt == nil {
		return nil
	}
	switch plan.ScanType {
	case planner.ScanTypeJoin:
		if plan.JoinScan == nil {
			return nil
		}
		return []string{plan.JoinScan.LeftTableName, plan.JoinScan.RightTableName}
	case planner.ScanTypeTable, planner.ScanTypeIndex:
		return []string{plan.Stmt.TableName}
	default:
		return nil
	}
}

func apiValue(value parser.Value) any {
	switch value.Kind {
	case parser.ValueKindNull:
		return nil
	case parser.ValueKindInt64:
		return int(value.I64)
	case parser.ValueKindString:
		return value.Str
	case parser.ValueKindBool:
		return value.Bool
	case parser.ValueKindReal:
		return value.F64
	default:
		return value.Any()
	}
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
	db.refreshBufferPool()
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
	rootPageData, err := storage.BuildSlottedTablePageData(uint32(rootPageID), nil)
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

func (db *DB) refreshBufferPool() {
	if db == nil || db.pager == nil {
		return
	}
	poolSize := int(db.pager.NextPageID()) + 1
	db.pool = bufferpool.New(poolSize, pagerPageLoader{pager: db.pager})
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

	tablePageData, locators, err := storage.BuildSlottedTablePageDataWithLocators(uint32(table.RootPageID()), table.Rows)
	if err != nil {
		return wrapStorageError(err)
	}
	if len(locators) != len(table.Rows) {
		return newStorageError("row locator mismatch")
	}

	pages := []stagedPage{{
		id:   table.RootPageID(),
		data: tablePageData,
	}}

	indexPages, err := db.buildRebuiltIndexPages(table, table.Rows, locators)
	if err != nil {
		return err
	}
	pages = append(pages, indexPages...)

	catalogData, err := storage.BuildCatalogPageData(catalogFromTables(stagedTables))
	if err != nil {
		return wrapStorageError(err)
	}

	if err := db.stageDirtyState(catalogData, pages); err != nil {
		return err
	}
	return nil
}

func (db *DB) applyStagedInsert(stagedTables map[string]*executor.Table, tableName string) error {
	if db == nil || db.pager == nil {
		return nil
	}

	table := stagedTables[tableName]
	if table == nil {
		return nil
	}

	table.SetStorageMeta(table.RootPageID(), uint32(len(table.Rows)))
	tablePageData, locators, err := storage.BuildSlottedTablePageDataWithLocators(uint32(table.RootPageID()), table.Rows)
	if err != nil {
		return wrapStorageError(err)
	}
	if len(table.Rows) == 0 || len(locators) != len(table.Rows) {
		return newStorageError("row locator mismatch")
	}

	pages := []stagedPage{{
		id:   table.RootPageID(),
		data: tablePageData,
	}}

	indexPages, err := db.buildInsertedIndexPages(table, table.Rows[len(table.Rows)-1], locators[len(locators)-1])
	if err != nil {
		return err
	}
	pages = append(pages, indexPages...)

	catalogData, err := storage.BuildCatalogPageData(catalogFromTables(stagedTables))
	if err != nil {
		return wrapStorageError(err)
	}
	return db.stageDirtyState(catalogData, pages)
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

func (db *DB) applyStagedIndexCreate(stagedTables map[string]*executor.Table, tableName, indexName string) error {
	if db == nil || db.pager == nil {
		return nil
	}

	table := stagedTables[tableName]
	if table == nil {
		return newExecError("table not found: " + tableName)
	}
	indexDef := table.IndexDefinition(indexName)
	if indexDef == nil {
		return newExecError("index not found")
	}

	var pages []stagedPage
	if indexDef.RootPageID == 0 {
		rootPageID := db.pager.NextPageID()
		indexDef.RootPageID = uint32(rootPageID)
		if columnName, ok := executor.LegacyBasicIndexColumn(*indexDef); ok {
			if index := table.Indexes[columnName]; index != nil {
				index.RootPageID = indexDef.RootPageID
			}
		}
		pages = append(pages, stagedPage{
			id:    rootPageID,
			data:  storage.InitIndexLeafPage(uint32(rootPageID)),
			isNew: true,
		})
	}

	catalogData, err := storage.BuildCatalogPageData(catalogFromTables(stagedTables))
	if err != nil {
		return wrapStorageError(err)
	}
	return db.stageDirtyState(catalogData, pages)
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
		Name:      table.Name,
		Columns:   columns,
		Rows:      rows,
		Indexes:   cloneIndexes(table.Indexes),
		IndexDefs: cloneIndexDefs(table.IndexDefs),
	}
	colNames := columnNamesForTable(cloned)
	for _, index := range cloned.Indexes {
		if index == nil {
			continue
		}
		_ = index.Rebuild(colNames, cloned.Rows)
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
		basic := planner.NewBasicIndex(index.TableName, index.ColumnName)
		basic.RootPageID = index.RootPageID
		cloned[columnName] = basic
	}
	return cloned
}

func cloneIndexDefs(indexDefs []storage.CatalogIndex) []storage.CatalogIndex {
	if len(indexDefs) == 0 {
		return nil
	}

	cloned := make([]storage.CatalogIndex, 0, len(indexDefs))
	for _, indexDef := range indexDefs {
		cloned = append(cloned, storage.CatalogIndex{
			Name:       indexDef.Name,
			Unique:     indexDef.Unique,
			RootPageID: indexDef.RootPageID,
			Columns:    append([]storage.CatalogIndexColumn(nil), indexDef.Columns...),
		})
	}
	return cloned
}

type indexPageStager struct {
	pages    map[storage.PageID]stagedPage
	order    []storage.PageID
	nextPage storage.PageID
}

func newIndexPageStager(nextPage storage.PageID) *indexPageStager {
	return &indexPageStager{
		pages:    make(map[storage.PageID]stagedPage),
		order:    nil,
		nextPage: nextPage,
	}
}

func (s *indexPageStager) stage(page stagedPage) {
	if s == nil {
		return
	}
	if _, exists := s.pages[page.id]; !exists {
		s.order = append(s.order, page.id)
	}
	s.pages[page.id] = page
}

func (s *indexPageStager) allocatePageID() storage.PageID {
	if s == nil {
		return 0
	}
	pageID := s.nextPage
	s.nextPage++
	return pageID
}

func (s *indexPageStager) finalizedPages() []stagedPage {
	if s == nil || len(s.order) == 0 {
		return nil
	}
	pages := make([]stagedPage, 0, len(s.order))
	for _, id := range s.order {
		pages = append(pages, s.pages[id])
	}
	return pages
}

func (db *DB) buildInsertedIndexPages(table *executor.Table, row []parser.Value, locator storage.RowLocator) ([]stagedPage, error) {
	if db == nil || table == nil || len(table.IndexDefs) == 0 {
		return nil, nil
	}

	stager := newIndexPageStager(db.pager.NextPageID())
	for i := range table.IndexDefs {
		indexDef := &table.IndexDefs[i]
		if indexDef.RootPageID == 0 {
			continue
		}

		key, err := encodeIndexKeyForRow(row, table.Columns, *indexDef)
		if err != nil {
			return nil, err
		}
		if err := db.insertIndexRecord(table, indexDef, key, locator, stager); err != nil {
			return nil, err
		}
	}
	return stager.finalizedPages(), nil
}

func (db *DB) buildRebuiltIndexPages(table *executor.Table, rows [][]parser.Value, locators []storage.RowLocator) ([]stagedPage, error) {
	if db == nil || table == nil || len(table.IndexDefs) == 0 {
		return nil, nil
	}
	if len(rows) != len(locators) {
		return nil, newStorageError("row locator mismatch")
	}

	stager := newIndexPageStager(db.pager.NextPageID())
	for i := range table.IndexDefs {
		indexDef := &table.IndexDefs[i]
		if indexDef.RootPageID == 0 {
			continue
		}

		rootPageID := storage.PageID(indexDef.RootPageID)
		stager.stage(stagedPage{
			id:   rootPageID,
			data: storage.InitIndexLeafPage(indexDef.RootPageID),
		})
		if columnName, ok := executor.LegacyBasicIndexColumn(*indexDef); ok {
			if index := table.Indexes[columnName]; index != nil {
				index.RootPageID = indexDef.RootPageID
			}
		}

		for rowIndex, row := range rows {
			key, err := encodeIndexKeyForRow(row, table.Columns, *indexDef)
			if err != nil {
				return nil, err
			}
			if err := db.insertIndexRecord(table, indexDef, key, locators[rowIndex], stager); err != nil {
				return nil, err
			}
		}
	}
	return stager.finalizedPages(), nil
}

func encodeIndexKeyForRow(row []parser.Value, columns []parser.ColumnDef, indexDef storage.CatalogIndex) ([]byte, error) {
	columnPositions := make(map[string]int, len(columns))
	for i, column := range columns {
		columnPositions[column.Name] = i
	}

	values := make([]parser.Value, 0, len(indexDef.Columns))
	for _, indexColumn := range indexDef.Columns {
		position, ok := columnPositions[indexColumn.Name]
		if !ok || position >= len(row) {
			return nil, newExecError("index/table mismatch")
		}
		values = append(values, row[position])
	}
	return storage.EncodeIndexKey(values)
}

type indexInsertPathEntry struct {
	pageID     storage.PageID
	childIndex int
}

func (db *DB) insertIndexRecord(table *executor.Table, indexDef *storage.CatalogIndex, key []byte, locator storage.RowLocator, stager *indexPageStager) error {
	if db == nil || table == nil || indexDef == nil || indexDef.RootPageID == 0 {
		return nil
	}

	rootPageID := storage.PageID(indexDef.RootPageID)
	leafPageID, path, err := db.findIndexInsertPath(rootPageID, key, stager)
	if err != nil {
		return err
	}

	leafPageData, err := db.readIndexPageForInsert(leafPageID, stager)
	if err != nil {
		return err
	}
	leafRecords, err := storage.ReadAllIndexLeafRecords(leafPageData)
	if err != nil {
		return wrapStorageError(err)
	}
	leafRecords = storage.InsertSortedIndexLeafRecords(leafRecords, storage.IndexLeafRecord{
		Key:     key,
		Locator: locator,
	})

	rightSibling, err := storage.IndexLeafRightSibling(leafPageData)
	if err != nil {
		return wrapStorageError(err)
	}
	leftPageData, err := storage.BuildIndexLeafPageData(uint32(leafPageID), leafRecords, rightSibling)
	if err == nil {
		stager.stage(stagedPage{id: leafPageID, data: leftPageData})
		return nil
	}
	var dbErr *DBError
	if !errors.As(err, &dbErr) || dbErr.Kind != "storage" || dbErr.Message != "index page full" {
		return wrapStorageError(err)
	}

	leftRecords, rightRecords, separatorKey, err := storage.SplitIndexLeafRecords(leafRecords)
	if err != nil {
		return wrapStorageError(err)
	}
	rightPageID := stager.allocatePageID()
	leftPageData, err = storage.BuildIndexLeafPageData(uint32(leafPageID), leftRecords, uint32(rightPageID))
	if err != nil {
		return wrapStorageError(err)
	}
	rightPageData, err := storage.BuildIndexLeafPageData(uint32(rightPageID), rightRecords, rightSibling)
	if err != nil {
		return wrapStorageError(err)
	}
	stager.stage(stagedPage{id: leafPageID, data: leftPageData})
	stager.stage(stagedPage{id: rightPageID, data: rightPageData, isNew: true})

	return db.propagateIndexSplit(table, indexDef, path, leafPageID, rightPageID, separatorKey, stager)
}

func (db *DB) propagateIndexSplit(table *executor.Table, indexDef *storage.CatalogIndex, path []indexInsertPathEntry, leftPageID, rightPageID storage.PageID, separatorKey []byte, stager *indexPageStager) error {
	for len(path) > 0 {
		parent := path[len(path)-1]
		path = path[:len(path)-1]

		parentPageData, err := db.readIndexPageForInsert(parent.pageID, stager)
		if err != nil {
			return err
		}
		parentRecords, err := storage.ReadAllIndexInternalRecords(parentPageData)
		if err != nil {
			return wrapStorageError(err)
		}
		if parent.childIndex < 0 || parent.childIndex >= len(parentRecords) {
			return newStorageError("corrupted index page")
		}

		oldKey := append([]byte(nil), parentRecords[parent.childIndex].Key...)
		parentRecords[parent.childIndex].Key = append([]byte(nil), separatorKey...)
		parentRecords[parent.childIndex].ChildPageID = uint32(leftPageID)

		insertRecord := storage.IndexInternalRecord{ChildPageID: uint32(rightPageID)}
		if parent.childIndex == len(parentRecords)-1 {
			insertRecord.Key = append([]byte(nil), separatorKey...)
		} else {
			insertRecord.Key = oldKey
		}
		parentRecords = insertIndexInternalRecord(parentRecords, parent.childIndex+1, insertRecord)

		parentPageRebuilt, err := storage.BuildIndexInternalPageData(uint32(parent.pageID), parentRecords)
		if err == nil {
			stager.stage(stagedPage{id: parent.pageID, data: parentPageRebuilt})
			return nil
		}
		var dbErr *DBError
		if !errors.As(err, &dbErr) || dbErr.Kind != "storage" || dbErr.Message != "index page full" {
			return wrapStorageError(err)
		}

		leftRecords, rightRecords, nextSeparatorKey, err := storage.SplitIndexInternalRecords(parentRecords)
		if err != nil {
			return wrapStorageError(err)
		}
		newRightPageID := stager.allocatePageID()
		leftPageData, err := storage.BuildIndexInternalPageData(uint32(parent.pageID), leftRecords)
		if err != nil {
			return wrapStorageError(err)
		}
		rightPageData, err := storage.BuildIndexInternalPageData(uint32(newRightPageID), rightRecords)
		if err != nil {
			return wrapStorageError(err)
		}
		stager.stage(stagedPage{id: parent.pageID, data: leftPageData})
		stager.stage(stagedPage{id: newRightPageID, data: rightPageData, isNew: true})

		leftPageID = parent.pageID
		rightPageID = newRightPageID
		separatorKey = nextSeparatorKey
	}

	newRootPageID := stager.allocatePageID()
	newRootRecords := []storage.IndexInternalRecord{
		{Key: append([]byte(nil), separatorKey...), ChildPageID: uint32(leftPageID)},
		{Key: append([]byte(nil), separatorKey...), ChildPageID: uint32(rightPageID)},
	}
	newRootPageData, err := storage.BuildIndexInternalPageData(uint32(newRootPageID), newRootRecords)
	if err != nil {
		return wrapStorageError(err)
	}
	stager.stage(stagedPage{id: newRootPageID, data: newRootPageData, isNew: true})
	indexDef.RootPageID = uint32(newRootPageID)
	if columnName, ok := executor.LegacyBasicIndexColumn(*indexDef); ok {
		if index := table.Indexes[columnName]; index != nil {
			index.RootPageID = indexDef.RootPageID
		}
	}
	return nil
}

func (db *DB) readIndexPageForInsert(pageID storage.PageID, stager *indexPageStager) ([]byte, error) {
	if stager != nil {
		if staged, ok := stager.pages[pageID]; ok {
			return append([]byte(nil), staged.data...), nil
		}
	}
	pageData, err := readCommittedPageData(db.pool, pageID)
	if err != nil {
		return nil, wrapStorageError(err)
	}
	if pageData == nil {
		return nil, newStorageError("corrupted index page")
	}
	return pageData, nil
}

func (db *DB) findIndexInsertPath(rootPageID storage.PageID, key []byte, stager *indexPageStager) (storage.PageID, []indexInsertPathEntry, error) {
	if rootPageID == 0 {
		return 0, nil, newStorageError("corrupted index page")
	}

	currentPageID := rootPageID
	path := make([]indexInsertPathEntry, 0)
	for {
		pageData, err := db.readIndexPageForInsert(currentPageID, stager)
		if err != nil {
			return 0, nil, err
		}
		pageType := storage.PageType(binary.LittleEndian.Uint16(pageData[4:6]))
		switch pageType {
		case storage.PageTypeIndexLeaf:
			return currentPageID, path, nil
		case storage.PageTypeIndexInternal:
			records, err := storage.ReadAllIndexInternalRecords(pageData)
			if err != nil {
				return 0, nil, wrapStorageError(err)
			}
			childIndex, childPageID, err := chooseIndexChildRecord(records, key)
			if err != nil {
				return 0, nil, wrapStorageError(err)
			}
			path = append(path, indexInsertPathEntry{pageID: currentPageID, childIndex: childIndex})
			currentPageID = storage.PageID(childPageID)
		default:
			return 0, nil, newStorageError("corrupted index page")
		}
	}
}

func chooseIndexChildRecord(records []storage.IndexInternalRecord, key []byte) (int, uint32, error) {
	if len(records) == 0 {
		return 0, 0, newStorageError("corrupted index page")
	}
	rightmostIndex := len(records) - 1
	for i, record := range records {
		cmp, err := storage.CompareIndexKeys(key, record.Key)
		if err != nil {
			return 0, 0, err
		}
		if cmp < 0 {
			return i, record.ChildPageID, nil
		}
	}
	return rightmostIndex, records[rightmostIndex].ChildPageID, nil
}

func insertIndexInternalRecord(records []storage.IndexInternalRecord, index int, record storage.IndexInternalRecord) []storage.IndexInternalRecord {
	if index < 0 {
		index = 0
	}
	if index > len(records) {
		index = len(records)
	}
	records = append(records, storage.IndexInternalRecord{})
	copy(records[index+1:], records[index:])
	records[index] = storage.IndexInternalRecord{
		Key:         append([]byte(nil), record.Key...),
		ChildPageID: record.ChildPageID,
	}
	return records
}

func cloneRows(rows [][]parser.Value) [][]parser.Value {
	cloned := make([][]parser.Value, 0, len(rows))
	for _, row := range rows {
		cloned = append(cloned, append([]parser.Value(nil), row...))
	}
	return cloned
}

func (db *DB) loadRowsIntoTables(tables map[string]*executor.Table, tableNames ...string) error {
	if db == nil {
		return ErrInvalidArgument
	}

	seen := make(map[string]struct{}, len(tableNames))
	for _, tableName := range tableNames {
		if tableName == "" {
			continue
		}
		if _, ok := seen[tableName]; ok {
			continue
		}
		seen[tableName] = struct{}{}

		table := tables[tableName]
		if table == nil {
			return newExecError("table not found: " + tableName)
		}
		rows, err := db.scanTableRows(table)
		if err != nil {
			return err
		}
		table.Rows = rows
		colNames := columnNamesForTable(table)
		for columnName, index := range table.Indexes {
			if index == nil {
				table.Indexes[columnName] = planner.NewBasicIndex(table.Name, columnName)
				index = table.Indexes[columnName]
			}
			if err := index.Rebuild(colNames, table.Rows); err != nil {
				return err
			}
		}
	}
	return nil
}

func clearLoadedRows(tables map[string]*executor.Table) {
	for _, table := range tables {
		if table == nil {
			continue
		}
		table.Rows = nil
	}
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
			Indexes:    make([]storage.CatalogIndex, 0, len(table.IndexDefs)),
		}
		for _, column := range table.Columns {
			entry.Columns = append(entry.Columns, storage.CatalogColumn{
				Name: column.Name,
				Type: catalogColumnType(column.Type),
			})
		}
		indexNames := make([]string, 0, len(table.IndexDefs))
		indexByName := make(map[string]storage.CatalogIndex, len(table.IndexDefs))
		for _, indexDef := range table.IndexDefs {
			indexNames = append(indexNames, indexDef.Name)
			indexByName[indexDef.Name] = indexDef
		}
		sort.Strings(indexNames)
		for _, indexName := range indexNames {
			entry.Indexes = append(entry.Indexes, indexByName[indexName])
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
			Name:      table.Name,
			Columns:   columns,
			Indexes:   make(map[string]*planner.BasicIndex),
			IndexDefs: cloneIndexDefs(table.Indexes),
		}
		tables[table.Name].SetStorageMeta(rootPageID, table.RowCount)
		for _, index := range table.Indexes {
			columnName, ok := executor.LegacyBasicIndexColumn(index)
			if !ok {
				continue
			}
			basic := planner.NewBasicIndex(table.Name, columnName)
			basic.RootPageID = index.RootPageID
			tables[table.Name].Indexes[columnName] = basic
		}
	}

	return tables, nil
}

func rebuildPersistedIndexes(tables map[string]*executor.Table) error {
	for _, table := range tables {
		if table == nil || len(table.Indexes) == 0 {
			continue
		}
		colNames := columnNamesForTable(table)
		for columnName, index := range table.Indexes {
			if index == nil {
				table.Indexes[columnName] = planner.NewBasicIndex(table.Name, columnName)
				index = table.Indexes[columnName]
			}
			if err := index.Rebuild(colNames, table.Rows); err != nil {
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
	colNames := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		colNames = append(colNames, col.Name)
	}
	return colNames
}

func executeCreateIndex(stmt *parser.CreateIndexStmt, tables map[string]*executor.Table) (int64, map[string]*executor.Table, error) {
	if stmt == nil {
		return 0, nil, newExecError("unsupported query form")
	}
	if indexNameInUse(tables, stmt.Name) {
		return 0, nil, newExecError("index already exists")
	}

	table := tables[stmt.TableName]
	if table == nil {
		return 0, nil, newExecError("table not found: " + stmt.TableName)
	}

	indexDef, err := indexDefinitionFromStmt(table, stmt)
	if err != nil {
		return 0, nil, err
	}
	if table.HasEquivalentIndexDefinition(indexDef) {
		return 0, nil, newExecError("equivalent index already exists")
	}

	table.IndexDefs = append(table.IndexDefs, indexDef)
	if err := executor.ValidateIndexedTextLimitsForTable(table); err != nil {
		table.IndexDefs = table.IndexDefs[:len(table.IndexDefs)-1]
		return 0, nil, err
	}
	if err := executor.ValidateUniqueIndexesForTable(table); err != nil {
		table.IndexDefs = table.IndexDefs[:len(table.IndexDefs)-1]
		return 0, nil, err
	}
	if columnName, ok := executor.LegacyBasicIndexColumn(indexDef); ok {
		if table.Indexes == nil {
			table.Indexes = make(map[string]*planner.BasicIndex)
		}
		index := planner.NewBasicIndex(table.Name, columnName)
		if err := index.Rebuild(columnNamesForTable(table), table.Rows); err != nil {
			return 0, nil, err
		}
		table.Indexes[columnName] = index
	}

	return 0, tables, nil
}

func indexNameInUse(tables map[string]*executor.Table, indexName string) bool {
	for _, table := range tables {
		if table != nil && table.IndexDefinition(indexName) != nil {
			return true
		}
	}
	return false
}

func indexDefinitionFromStmt(table *executor.Table, stmt *parser.CreateIndexStmt) (storage.CatalogIndex, error) {
	if table == nil || stmt == nil {
		return storage.CatalogIndex{}, newExecError("unsupported query form")
	}

	availableColumns := make(map[string]struct{}, len(table.Columns))
	for _, column := range table.Columns {
		availableColumns[column.Name] = struct{}{}
	}

	columns := make([]storage.CatalogIndexColumn, 0, len(stmt.Columns))
	for _, column := range stmt.Columns {
		if _, ok := availableColumns[column.Name]; !ok {
			return storage.CatalogIndex{}, newExecError("column not found")
		}
		columns = append(columns, storage.CatalogIndexColumn{
			Name: column.Name,
			Desc: column.Desc,
		})
	}

	return storage.CatalogIndex{
		Name:    stmt.Name,
		Unique:  stmt.Unique,
		Columns: columns,
	}, nil
}

func executeDropIndex(stmt *parser.DropIndexStmt, tables map[string]*executor.Table) (int64, map[string]*executor.Table, error) {
	if stmt == nil {
		return 0, nil, newExecError("unsupported query form")
	}

	for _, table := range tables {
		if table == nil {
			continue
		}
		indexDef := table.IndexDefinition(stmt.Name)
		if indexDef == nil {
			continue
		}
		if columnName, ok := executor.LegacyBasicIndexColumn(*indexDef); ok && table.Indexes != nil {
			delete(table.Indexes, columnName)
		}
		filtered := make([]storage.CatalogIndex, 0, len(table.IndexDefs)-1)
		for _, existing := range table.IndexDefs {
			if existing.Name != stmt.Name {
				filtered = append(filtered, existing)
			}
		}
		table.IndexDefs = filtered
		return 0, tables, nil
	}

	return 0, nil, newExecError("index not found")
}

func executeDropTable(stmt *parser.DropTableStmt, tables map[string]*executor.Table) (int64, map[string]*executor.Table, error) {
	if stmt == nil {
		return 0, nil, newExecError("unsupported query form")
	}
	if _, ok := tables[stmt.Name]; !ok {
		return 0, nil, newExecError("table not found: " + stmt.Name)
	}
	delete(tables, stmt.Name)
	return 0, tables, nil
}

func (db *DB) defineLegacyBasicIndex(tableName, columnName string) error {
	if db == nil {
		return ErrInvalidArgument
	}

	var committedTables map[string]*executor.Table
	err := db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		if err := db.loadRowsIntoTables(stagedTables, tableName); err != nil {
			return err
		}
		table := stagedTables[tableName]
		if table == nil {
			return newExecError("table not found")
		}
		colNames := columnNamesForTable(table)
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
		indexDef := storage.CatalogIndex{
			Name:   columnName,
			Unique: false,
			Columns: []storage.CatalogIndexColumn{
				{Name: columnName},
			},
		}
		if table.IndexDefinition(indexDef.Name) != nil {
			return newExecError("index already exists")
		}
		if table.HasEquivalentIndexDefinition(indexDef) {
			return newExecError("equivalent index already exists")
		}
		table.IndexDefs = append(table.IndexDefs, indexDef)
		index := planner.NewBasicIndex(tableName, columnName)
		index.RootPageID = indexDef.RootPageID
		if err := index.Rebuild(colNames, table.Rows); err != nil {
			return err
		}
		table.Indexes[columnName] = index

		if err := db.applyStagedIndexCreate(stagedTables, tableName, indexDef.Name); err != nil {
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
		clearLoadedRows(committedTables)
		db.tables = committedTables
	}
	return nil
}

type pagerPageLoader struct {
	pager *storage.Pager
}

func (l pagerPageLoader) ReadPage(pageID bufferpool.PageID) ([]byte, error) {
	if l.pager == nil {
		return nil, nil
	}
	return l.pager.ReadPage(storage.PageID(pageID))
}

func loadPersistedRows(pool *bufferpool.BufferPool, tables map[string]*executor.Table) error {
	for _, table := range tables {
		if table == nil {
			continue
		}

		pageData, err := readCommittedPageData(pool, table.RootPageID())
		if err != nil {
			return wrapStorageError(err)
		}
		if pageData == nil {
			return newStorageError("corrupted table page")
		}
		rows, err := decodePersistedTableRows(pageData, table.Columns)
		if err != nil {
			return err
		}
		if uint32(len(rows)) != table.PersistedRowCount() {
			return newStorageError("row count mismatch")
		}

		table.Rows = table.Rows[:0]
		for _, row := range rows {
			if len(row) > len(table.Columns) {
				return newStorageError("row width mismatch")
			}
			table.Rows = append(table.Rows, padRowToSchema(row, len(table.Columns)))
		}
	}

	return nil
}

func validatePersistedIndexRoots(pool *bufferpool.BufferPool, tables map[string]*executor.Table) error {
	for _, table := range tables {
		if table == nil {
			continue
		}
		for _, indexDef := range table.IndexDefs {
			if indexDef.RootPageID == 0 {
				continue
			}

			pageData, err := readCommittedPageData(pool, storage.PageID(indexDef.RootPageID))
			if err != nil {
				return wrapStorageError(err)
			}
			if pageData == nil {
				return newStorageError("corrupted index page")
			}
			if _, err := storage.IndexPageEntryCount(pageData); err != nil {
				return wrapStorageError(err)
			}
		}
	}
	return nil
}

func readCommittedPageData(pool *bufferpool.BufferPool, pageID storage.PageID) ([]byte, error) {
	frame, err := pool.GetCommittedPage(bufferpool.PageID(pageID))
	if err != nil {
		return nil, err
	}
	if frame == nil {
		return nil, nil
	}

	pageData := append([]byte(nil), frame.Data[:]...)
	pool.UnlatchShared(frame)
	pool.Unpin(frame)
	return pageData, nil
}

func (db *DB) fetchRowByLocator(table *executor.Table, locator storage.RowLocator) ([]parser.Value, error) {
	if db == nil || table == nil {
		return nil, ErrInvalidArgument
	}
	if locator.PageID == 0 || storage.PageID(locator.PageID) != table.RootPageID() {
		return nil, newStorageError("corrupted table page")
	}

	pageData, err := readCommittedPageData(db.pool, storage.PageID(locator.PageID))
	if err != nil {
		return nil, wrapStorageError(err)
	}
	if pageData == nil {
		return nil, newStorageError("corrupted table page")
	}

	row, err := storage.ReadRowByLocatorFromTablePageData(pageData, locator, storageColumnTypes(table.Columns))
	if err != nil {
		return nil, wrapStorageError(err)
	}
	return append([]parser.Value(nil), row...), nil
}

func decodePersistedTableRows(pageData []byte, columns []parser.ColumnDef) ([][]parser.Value, error) {
	if storage.IsSlottedTablePage(pageData) {
		return storage.ReadSlottedRowsFromTablePageData(pageData, storageColumnTypes(columns))
	}

	payloads, err := storage.ReadRowsFromTablePageData(pageData)
	if err != nil {
		return nil, wrapStorageError(err)
	}
	rows := make([][]parser.Value, 0, len(payloads))
	for _, payload := range payloads {
		row, err := storage.DecodeRow(payload)
		if err != nil {
			return nil, wrapStorageError(err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func storageColumnTypes(columns []parser.ColumnDef) []uint8 {
	columnTypes := make([]uint8, 0, len(columns))
	for _, column := range columns {
		columnTypes = append(columnTypes, catalogColumnType(column.Type))
	}
	return columnTypes
}

func catalogColumnType(columnType string) uint8 {
	switch columnType {
	case parser.ColumnTypeInt:
		return storage.CatalogColumnTypeInt
	case parser.ColumnTypeBool:
		return storage.CatalogColumnTypeBool
	case parser.ColumnTypeReal:
		return storage.CatalogColumnTypeReal
	default:
		return storage.CatalogColumnTypeText
	}
}

func parserColumnType(columnType uint8) (string, error) {
	switch columnType {
	case storage.CatalogColumnTypeInt:
		return parser.ColumnTypeInt, nil
	case storage.CatalogColumnTypeBool:
		return parser.ColumnTypeBool, nil
	case storage.CatalogColumnTypeReal:
		return parser.ColumnTypeReal, nil
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
		if err := validateIndexConsistency(table); err != nil {
			return err
		}
	}
	return nil
}

func validateIndexConsistency(table *executor.Table) error {
	if table == nil {
		return nil
	}

	seenIndexNames := make(map[string]struct{}, len(table.IndexDefs))
	legacyIndexDefs := make(map[string]storage.CatalogIndex)
	for _, indexDef := range table.IndexDefs {
		if indexDef.Name == "" {
			return newExecError("index/table mismatch")
		}
		if _, exists := seenIndexNames[indexDef.Name]; exists {
			return newExecError("index/table mismatch")
		}
		seenIndexNames[indexDef.Name] = struct{}{}
		if columnName, ok := executor.LegacyBasicIndexColumn(indexDef); ok {
			if _, exists := legacyIndexDefs[columnName]; exists {
				return newExecError("index/table mismatch")
			}
			legacyIndexDefs[columnName] = indexDef
		}
	}
	if len(table.Indexes) == 0 {
		return nil
	}

	colNames := columnNamesForTable(table)
	for columnName, index := range table.Indexes {
		if index == nil {
			return newExecError("index/table mismatch")
		}
		if _, exists := legacyIndexDefs[columnName]; !exists {
			return newExecError("index/table mismatch")
		}
		if index.RootPageID != legacyIndexDefs[columnName].RootPageID {
			return newExecError("index/table mismatch")
		}
		if index.TableName != table.Name || index.ColumnName != columnName {
			return newExecError("index/table mismatch")
		}
		if len(table.Rows) == 0 && table.PersistedRowCount() != 0 {
			continue
		}

		expected := planner.NewBasicIndex(table.Name, columnName)
		if err := expected.Rebuild(colNames, table.Rows); err != nil {
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
