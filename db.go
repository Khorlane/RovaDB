// Package rovadb is the root API layer: it owns the public embedded API surface
// and contains no engine logic.
package rovadb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/Khorlane/RovaDB/internal/bufferpool"
	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
	"github.com/Khorlane/RovaDB/internal/storage"
	"github.com/Khorlane/RovaDB/internal/txn"
)

// NOTE: This root-layer file owns public API orchestration across parser,
// planner, executor, transaction, and storage boundaries. The imported engine
// packages still own their respective logic; this file only coordinates them.

var (
	errDuplicateRootPageID      = newStorageError("corrupted catalog page")
	errInvalidStoredTableMeta   = newStorageError("corrupted catalog page")
	ErrTxnAlreadyActive         = errors.New("rovadb: transaction already active")
	ErrTxnCommitWithoutActive   = errors.New("rovadb: commit requires active transaction")
	ErrTxnRollbackWithoutActive = errors.New("rovadb: rollback requires active transaction")
	ErrTxnInvariantViolation    = errors.New("rovadb: transaction invariant violation")
)

type checkpointError struct {
	err error
}

func (e *checkpointError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *checkpointError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

var (
	appendWALFrameRecord  = storage.AppendWALFrame
	appendWALCommitRecord = storage.AppendWALCommitRecord
	syncWAL               = storage.SyncWALFile
	resetWAL              = storage.ResetWALFile
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
	tx     *Tx
	txView bool

	afterJournalWriteHook func() error
	afterDatabaseSyncHook func() error

	writerMu     sync.Mutex
	writerActive bool

	pendingPages []stagedPage
	nextWALLSN   uint64
	freeListHead uint32

	lastCheckpointLSN       uint64
	lastCheckpointPageCount uint32
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
	if err := rejectOrphanWALOnCreate(path); err != nil {
		return nil, err
	}

	file, err := storage.OpenOrCreate(path)
	if err != nil {
		return nil, wrapStorageError(err)
	}
	if err := storage.EnsureDirectoryPage(file.File()); err != nil {
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	if err := storage.EnsureWALFile(path, storage.DBFormatVersion()); err != nil {
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	dbFormatVersion, err := storage.ReadDBFormatVersion(file.File())
	if err != nil {
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	directoryPage, err := storage.ReadDirectoryPage(file.File())
	if err != nil {
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	catdirMode, err := storage.DirectoryCATDIRStorageMode(directoryPage)
	if err != nil {
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	catdirOverflowHead, err := storage.DirectoryCATDIROverflowHeadPageID(directoryPage)
	if err != nil {
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	catdirOverflowCount, err := storage.DirectoryCATDIROverflowPageCount(directoryPage)
	if err != nil {
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	catdirPayloadBytes, err := storage.DirectoryCATDIRPayloadByteLength(directoryPage)
	if err != nil {
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	directoryFormatVersion, err := storage.DirectoryFormatVersion(directoryPage)
	if err != nil {
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	walHeader, err := storage.ReadWALHeaderFromPath(path)
	if err != nil {
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	if err := storage.ValidateFormatSignature(storage.FormatSignature{
		DBFormatVersion:        dbFormatVersion,
		DirectoryFormatVersion: directoryFormatVersion,
		WALVersion:             walHeader.WALVersion,
		WALDBFormatVersion:     walHeader.DBFormatVersion,
		PageSize:               walHeader.PageSize,
	}); err != nil {
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	if err := replayCommittedWAL(path); err != nil {
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	freeListHead, err := storage.ReadDirectoryFreeListHead(file.File())
	if err != nil {
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	rootIDMappings, err := storage.ReadDirectoryRootIDMappings(file.File())
	if err != nil {
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	checkpointMeta, err := storage.ReadDirectoryCheckpointMetadata(file.File())
	if err != nil {
		_ = file.Close()
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
	if err := storage.ValidateDirectoryControlState(file.File(), storage.DirectoryControlState{
		FreeListHead:        freeListHead,
		CATDIRStorageMode:   catdirMode,
		CATDIROverflowHead:  catdirOverflowHead,
		CATDIROverflowCount: catdirOverflowCount,
		CATDIRPayloadBytes:  catdirPayloadBytes,
		RootIDMappings:      rootIDMappings,
		CheckpointMeta:      checkpointMeta,
	}); err != nil {
		_ = pager.Close()
		_ = file.Close()
		return nil, wrapStorageError(err)
	}
	catalog, err = storage.ApplyDirectoryRootIDMappings(catalog, rootIDMappings)
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
	if err := loadPhysicalTableRoots(pool, tables, rootIDMappings); err != nil {
		_ = pager.Close()
		_ = file.Close()
		return nil, err
	}
	db := &DB{
		path:                    path,
		file:                    file,
		pager:                   pager,
		pool:                    pool,
		tables:                  tables,
		freeListHead:            freeListHead,
		lastCheckpointLSN:       checkpointMeta.LastCheckpointLSN,
		lastCheckpointPageCount: checkpointMeta.LastCheckpointPageCount,
	}
	if err := db.reconcileSystemCatalogOnOpen(tables); err != nil {
		_ = pager.Close()
		_ = file.Close()
		return nil, err
	}
	if err := validatePersistedIndexRoots(pool, tables); err != nil {
		_ = pager.Close()
		_ = file.Close()
		return nil, err
	}
	if err := validateCommittedPhysicalTableStorage(pool, pager, tables, freeListHead); err != nil {
		_ = pager.Close()
		_ = file.Close()
		return nil, err
	}
	if err := loadPersistedRows(pool, tables); err != nil {
		_ = pager.Close()
		_ = file.Close()
		return nil, err
	}
	if err := validateTables(tables); err != nil {
		_ = pager.Close()
		_ = file.Close()
		return nil, err
	}
	clearLoadedRows(tables)

	nextWALLSN, err := storage.NextWALLSN(path)
	if err != nil {
		_ = pager.Close()
		_ = file.Close()
		return nil, wrapStorageError(err)
	}

	db.nextWALLSN = nextWALLSN
	return db, nil
}

func rejectOrphanWALOnCreate(path string) error {
	if path == "" {
		return nil
	}
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	walPath := storage.WALPath(path)
	if _, err := os.Stat(walPath); err == nil {
		return fmt.Errorf("open: database file does not exist but WAL sidecar exists: %s", walPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (db *DB) reconcileSystemCatalogOnOpen(tables map[string]*executor.Table) error {
	if db == nil {
		return ErrInvalidArgument
	}

	newSystemPageIDs, bootstrapped, err := db.ensureSystemCatalogTables(tables)
	if err != nil {
		return err
	}
	if !bootstrapped {
		return nil
	}
	systemPages, rebuiltSystemRows, err := db.rebuildSystemCatalogRows(tables, newSystemPageIDs, systemCatalogReservedNextFreshID(tables, db.pager.NextPageID()))
	if err != nil {
		return err
	}
	if !bootstrapped && !rebuiltSystemRows {
		return nil
	}
	return db.persistCatalogState(tables, systemPages)
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

func replayCommittedWAL(path string) error {
	frames, err := storage.CommittedWALFrames(path)
	if err != nil {
		return err
	}
	return storage.ApplyWALFramesToDB(path, frames)
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
	if err := db.rejectSystemTableMutation(stmt); err != nil {
		return Result{}, err
	}
	switch stmt := stmt.(type) {
	case *parser.SelectExpr:
		return Result{}, ErrExecDisallowsSelect
	case *parser.CreateTableStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		committed, err := db.execMutatingStatement(func() error {
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
		if committed {
			if err := validateTables(committedTables); err != nil {
				return Result{}, err
			}
			clearLoadedRows(committedTables)
			db.tables = committedTables
		}
		if err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.InsertStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		committed, err := db.execMutatingStatement(func() error {
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
		if committed {
			if err := validateTables(committedTables); err != nil {
				return Result{}, err
			}
			clearLoadedRows(committedTables)
			db.tables = committedTables
		}
		if err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.AlterTableAddColumnStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		committed, err := db.execMutatingStatement(func() error {
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
		if committed {
			if err := validateTables(committedTables); err != nil {
				return Result{}, err
			}
			clearLoadedRows(committedTables)
			db.tables = committedTables
		}
		if err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.CreateIndexStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		committed, err := db.execMutatingStatement(func() error {
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
		if committed {
			if err := validateTables(committedTables); err != nil {
				return Result{}, err
			}
			clearLoadedRows(committedTables)
			db.tables = committedTables
		}
		if err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.DropIndexStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		committed, err := db.execMutatingStatement(func() error {
			stagedTables := cloneTables(db.tables)
			_, rootPageID := droppedIndexRootForName(stagedTables, stmt.Name)

			var err error
			rowsAffected, committedTables, err = executeDropIndex(stmt, stagedTables)
			if err != nil {
				return err
			}

			if err := db.applyStagedDropIndex(stagedTables, rootPageID); err != nil {
				return err
			}
			return nil
		})
		if committed {
			if err := validateTables(committedTables); err != nil {
				return Result{}, err
			}
			clearLoadedRows(committedTables)
			db.tables = committedTables
		}
		if err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.DropTableStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		committed, err := db.execMutatingStatement(func() error {
			stagedTables := cloneTables(db.tables)
			droppedRootPageIDs := droppedTableRootPageIDs(db.pool, stagedTables, stmt.Name)

			var err error
			rowsAffected, committedTables, err = executeDropTable(stmt, stagedTables)
			if err != nil {
				return err
			}

			if err := db.applyStagedDropTable(stagedTables, droppedRootPageIDs); err != nil {
				return err
			}
			return nil
		})
		if committed {
			if err := validateTables(committedTables); err != nil {
				return Result{}, err
			}
			clearLoadedRows(committedTables)
			db.tables = committedTables
		}
		if err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.UpdateStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		committed, err := db.execMutatingStatement(func() error {
			stagedTables := cloneTables(db.tables)
			if err := db.loadRowsIntoTables(stagedTables, stmt.TableName); err != nil {
				return err
			}

			var err error
			rowsAffected, err = executor.Execute(stmt, stagedTables)
			if err != nil {
				return err
			}

			if err := db.applyStagedUpdate(stagedTables, stmt.TableName); err != nil {
				return err
			}
			committedTables = stagedTables
			return nil
		})
		if committed {
			if err := validateTables(committedTables); err != nil {
				return Result{}, err
			}
			clearLoadedRows(committedTables)
			db.tables = committedTables
		}
		if err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.DeleteStmt:
		var rowsAffected int64
		var committedTables map[string]*executor.Table
		committed, err := db.execMutatingStatement(func() error {
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
		if committed {
			if err := validateTables(committedTables); err != nil {
				return Result{}, err
			}
			clearLoadedRows(committedTables)
			db.tables = committedTables
		}
		if err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	default:
		return Result{}, newExecError("unsupported query form")
	}
}

func (db *DB) rejectSystemTableMutation(stmt any) error {
	return rejectSystemTableMutationTables(db.tables, stmt)
}

func rejectSystemTableMutationTables(tables map[string]*executor.Table, stmt any) error {
	if stmt == nil {
		return nil
	}
	switch typed := stmt.(type) {
	case *parser.CreateTableStmt:
		if isSystemCatalogTableName(typed.Name) {
			return newExecError("system tables are read-only")
		}
	case *parser.InsertStmt:
		if isSystemCatalogTableName(typed.TableName) {
			return newExecError("system tables are read-only")
		}
	case *parser.UpdateStmt:
		if isSystemCatalogTableName(typed.TableName) {
			return newExecError("system tables are read-only")
		}
	case *parser.DeleteStmt:
		if isSystemCatalogTableName(typed.TableName) {
			return newExecError("system tables are read-only")
		}
	case *parser.AlterTableAddColumnStmt:
		if isSystemCatalogTableName(typed.TableName) {
			return newExecError("system tables are read-only")
		}
	case *parser.DropTableStmt:
		if isSystemCatalogTableName(typed.Name) {
			return newExecError("system tables are read-only")
		}
	case *parser.CreateIndexStmt:
		if table := tables[typed.TableName]; table != nil && table.IsSystem {
			return newExecError("system tables are read-only")
		}
	case *parser.DropIndexStmt:
		for _, table := range tables {
			if table == nil || !table.IsSystem {
				continue
			}
			if table.IndexDefinition(typed.Name) != nil {
				return newExecError("system tables are read-only")
			}
		}
	}
	return nil
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
		if err := validateTables(db.tables); err != nil {
			return &Rows{err: err, idx: -1}, nil
		}
		plan, err := planner.PlanSelect(sel, plannerTableMetadata(db.tables))
		if err != nil {
			return &Rows{err: err, idx: -1}, nil
		}
		var handoff *executor.SelectExecutionHandoff
		if plan.ScanType == planner.ScanTypeIndexOnly {
			indexOnlyHandoff, err := executor.NewIndexOnlyExecutionHandoff(plan)
			if err != nil {
				return &Rows{err: err, idx: -1}, nil
			}
			if rows, ok, err := db.queryIndexOnly(indexOnlyHandoff); ok {
				if err != nil {
					return &Rows{err: err, idx: -1}, nil
				}
				return rows, nil
			}
			handoff = indexOnlyHandoff.FallbackSelectHandoff()
		} else {
			handoff, err = executor.NewSelectExecutionHandoff(plan)
			if err != nil {
				return &Rows{err: err, idx: -1}, nil
			}
		}
		if handoff == nil {
			return &Rows{err: newExecError("invalid select plan"), idx: -1}, nil
		}
		accessPath := handoff.AccessPath()
		if accessPath.Kind == executor.SelectAccessPathKindIndex {
			table := db.tables[accessPath.IndexLookup.TableName]
			if table == nil {
				return &Rows{err: newExecError("table not found: " + accessPath.IndexLookup.TableName), idx: -1}, nil
			}
			indexDef, err := db.resolveSimpleLogicalIndex(table, accessPath.IndexLookup.ColumnName)
			if err != nil {
				return &Rows{err: err, idx: -1}, nil
			}
			execTable := cloneSelectTableMeta(table)
			if sel.IsCountStar {
				count, err := db.countIndexedRows(execTable, indexDef, accessPath.IndexLookup.LookupValue)
				if err != nil {
					return &Rows{err: err, idx: -1}, nil
				}
				return newRows([]string{"count"}, [][]any{{count}}), nil
			}
			candidateRows, err := db.lookupIndexedRows(table, indexDef, accessPath.IndexLookup.LookupValue)
			if err != nil {
				return &Rows{err: err, idx: -1}, nil
			}
			rows, err := executor.SelectCandidateRowsWithHandoff(handoff, execTable, candidateRows)
			if err != nil {
				return &Rows{err: err, idx: -1}, nil
			}
			columns, err := executor.ProjectedColumnNamesWithHandoff(handoff, execTable)
			if err != nil {
				return &Rows{err: err, idx: -1}, nil
			}
			return newRows(columns, materializeRows(rows)), nil
		}
		if tableName, columnName, ok := simpleEqualityPlanningTarget(sel); ok {
			table := db.tables[tableName]
			if hasMalformedSimpleLogicalIndex(table, columnName) {
				return &Rows{err: newExecError("invalid select plan"), idx: -1}, nil
			}
		}
		execTables, err := db.tablesForSelectHandoff(handoff)
		if err != nil {
			return &Rows{err: err, idx: -1}, nil
		}
		if err := validateTables(execTables); err != nil {
			return &Rows{err: err, idx: -1}, nil
		}
		rows, err := executor.SelectWithHandoff(handoff, execTables)
		if err != nil {
			return &Rows{err: err, idx: -1}, nil
		}
		columns, err := executor.ProjectedColumnNamesForHandoff(handoff, execTables)
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

func (db *DB) queryIndexOnly(handoff *executor.IndexOnlyExecutionHandoff) (*Rows, bool, error) {
	if db == nil || handoff == nil {
		return nil, false, nil
	}
	if !handoff.SupportsDirectExecution() {
		return nil, false, nil
	}
	if handoff.Mode() == executor.IndexOnlyExecutionModeCountStar {
		table := db.tables[handoff.TableName()]
		if table == nil {
			return nil, true, newExecError("table not found: " + handoff.TableName())
		}
		indexDef, err := db.resolveSimpleLogicalIndex(table, handoff.ColumnName())
		if err != nil {
			return nil, true, err
		}
		count, err := db.countAllRowsFromIndexOnly(table, indexDef)
		if err != nil {
			return nil, true, err
		}
		return newRows([]string{"count"}, [][]any{{count}}), true, nil
	}

	table := db.tables[handoff.TableName()]
	if table == nil {
		return nil, true, newExecError("table not found: " + handoff.TableName())
	}
	indexDef, err := db.resolveSimpleLogicalIndex(table, handoff.ColumnName())
	if err != nil {
		return nil, true, err
	}
	rows, err := db.projectAllRowsFromIndexOnly(handoff, table, indexDef)
	if err != nil {
		return nil, true, err
	}
	return rows, true, nil
}

func cloneSelectTableMeta(table *executor.Table) *executor.Table {
	if table == nil {
		return nil
	}
	cloned := &executor.Table{
		Name:      table.Name,
		TableID:   table.TableID,
		IsSystem:  table.IsSystem,
		Columns:   append([]parser.ColumnDef(nil), table.Columns...),
		IndexDefs: cloneIndexDefs(table.IndexDefs),
	}
	cloned.SetStorageMeta(table.RootPageID(), table.PersistedRowCount())
	cloned.SetPhysicalTableRootMeta(table.TableHeaderPageID(), table.TableStorageFormatVersion(), table.FirstSpaceMapPageID(), table.OwnedDataPageCount(), table.OwnedSpaceMapPageCount())
	return cloned
}

// QueryRow executes Query and wraps the resulting row set for deferred handling.
func (db *DB) QueryRow(query string, args ...any) *Row {
	rows, err := db.query(query, args...)
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

func (db *DB) tablesForSelectHandoff(handoff *executor.SelectExecutionHandoff) (map[string]*executor.Table, error) {
	if handoff == nil {
		return db.tables, nil
	}

	execTables := make(map[string]*executor.Table, len(db.tables))
	for name, existing := range db.tables {
		execTables[name] = existing
	}
	for _, tableName := range tableNamesForSelectHandoff(handoff) {
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
			TableID:   table.TableID,
			IsSystem:  table.IsSystem,
			Columns:   append([]parser.ColumnDef(nil), table.Columns...),
			Rows:      rows,
			IndexDefs: cloneIndexDefs(table.IndexDefs),
		}
		execTables[table.Name].SetStorageMeta(table.RootPageID(), table.PersistedRowCount())
		execTables[table.Name].SetPhysicalTableRootMeta(table.TableHeaderPageID(), table.TableStorageFormatVersion(), table.FirstSpaceMapPageID(), table.OwnedDataPageCount(), table.OwnedSpaceMapPageCount())
	}
	return execTables, nil
}

func (db *DB) scanTableRows(table *executor.Table) ([][]parser.Value, error) {
	if db == nil || table == nil {
		return nil, ErrInvalidArgument
	}
	if db.txView && table.Rows != nil {
		return cloneRows(table.Rows), nil
	}
	_, rows, err := loadCommittedTableRowsAndLocators(db.pool, table)
	if err != nil {
		return nil, err
	}
	return cloneRows(rows), nil
}

func (db *DB) lookupIndexedRows(table *executor.Table, indexDef *storage.CatalogIndex, searchValue parser.Value) ([][]parser.Value, error) {
	locators, err := db.lookupIndexedLocators(table, indexDef, searchValue)
	if err != nil {
		return nil, err
	}

	rows := make([][]parser.Value, 0, len(locators))
	for _, locator := range locators {
		row, err := db.fetchRowByLocator(table, locator)
		if err != nil {
			continue
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (db *DB) lookupIndexedLocators(table *executor.Table, indexDef *storage.CatalogIndex, searchValue parser.Value) ([]storage.RowLocator, error) {
	if db == nil || table == nil || indexDef == nil {
		return nil, ErrInvalidArgument
	}
	indexDef, err := db.validateIndexLookupMetadata(table, indexDef)
	if err != nil {
		return nil, err
	}

	locators, err := storage.LookupSimpleIndexExact(db.pageReaderForLookup, indexDef.RootPageID, storageValueFromParser(searchValue))
	if err != nil {
		return nil, wrapStorageError(err)
	}
	return locators, nil
}

func (db *DB) countIndexedRows(table *executor.Table, indexDef *storage.CatalogIndex, searchValue parser.Value) (int, error) {
	locators, err := db.lookupIndexedLocators(table, indexDef, searchValue)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, locator := range locators {
		if _, err := db.fetchRowByLocator(table, locator); err != nil {
			continue
		}
		count++
	}
	return count, nil
}

func (db *DB) countAllRowsFromIndexOnly(table *executor.Table, indexDef *storage.CatalogIndex) (int, error) {
	if db == nil || table == nil || indexDef == nil {
		return 0, ErrInvalidArgument
	}
	indexDef, err := db.validateIndexLookupMetadata(table, indexDef)
	if err != nil {
		return 0, err
	}
	count, err := storage.CountAllSimpleIndexEntries(db.pageReaderForLookup, indexDef.RootPageID)
	if err != nil {
		return 0, wrapStorageError(err)
	}
	return count, nil
}

func (db *DB) projectAllRowsFromIndexOnly(handoff *executor.IndexOnlyExecutionHandoff, table *executor.Table, indexDef *storage.CatalogIndex) (*Rows, error) {
	if db == nil || handoff == nil || table == nil || indexDef == nil {
		return nil, ErrInvalidArgument
	}
	if handoff.ColumnName() == "" {
		return nil, newExecError("invalid select plan")
	}
	indexDef, err := db.validateIndexLookupMetadata(table, indexDef)
	if err != nil {
		return nil, err
	}

	values, err := storage.ReadAllSimpleIndexValuesInOrder(db.pageReaderForLookup, indexDef.RootPageID)
	if err != nil {
		return nil, wrapStorageError(err)
	}

	projected := make([][]any, 0, len(values))
	for _, value := range values {
		projected = append(projected, []any{apiValue(parserValueFromStorage(value))})
	}

	columns, err := executor.ProjectedColumnNamesWithHandoff(handoff.FallbackSelectHandoff(), cloneSelectTableMeta(table))
	if err != nil {
		return nil, err
	}
	return newRows(columns, projected), nil
}

func (db *DB) resolveSimpleLogicalIndex(table *executor.Table, columnName string) (*storage.CatalogIndex, error) {
	if table == nil || columnName == "" {
		return nil, ErrInvalidArgument
	}
	var indexDef *storage.CatalogIndex
	for i := range table.IndexDefs {
		if !isSimpleIndexOnColumn(table.IndexDefs[i], columnName) {
			continue
		}
		if indexDef != nil {
			return nil, newExecError("index/table mismatch")
		}
		indexDef = &table.IndexDefs[i]
	}
	if indexDef == nil || indexDef.IndexID == 0 || indexDef.RootPageID == 0 {
		return nil, newExecError("index/table mismatch")
	}
	return indexDef, nil
}

func (db *DB) validateIndexLookupMetadata(table *executor.Table, indexDef *storage.CatalogIndex) (*storage.CatalogIndex, error) {
	if db == nil || table == nil || indexDef == nil {
		return nil, ErrInvalidArgument
	}
	if db.pool == nil || table.Name == "" || table.TableID == 0 {
		return nil, newExecError("index/table mismatch")
	}
	columnName, ok := simpleIndexColumn(*indexDef)
	if !ok || !tableHasColumn(table, columnName) {
		return nil, newExecError("index/table mismatch")
	}
	resolvedIndexDef, err := db.resolveSimpleLogicalIndex(table, columnName)
	if err != nil {
		return nil, err
	}
	if resolvedIndexDef.Name != indexDef.Name || resolvedIndexDef.IndexID != indexDef.IndexID || resolvedIndexDef.RootPageID != indexDef.RootPageID {
		return nil, newExecError("index/table mismatch")
	}

	if err := storage.ValidateIndexRoot(db.pageReaderForLookup, indexDef.RootPageID); err != nil {
		return nil, wrapStorageError(err)
	}
	return indexDef, nil
}

func tableHasColumn(table *executor.Table, columnName string) bool {
	if table == nil || columnName == "" {
		return false
	}
	for _, column := range table.Columns {
		if column.Name == columnName {
			return true
		}
	}
	return false
}

func hasMalformedSimpleLogicalIndex(table *executor.Table, columnName string) bool {
	if table == nil || columnName == "" {
		return false
	}
	for _, indexDef := range table.IndexDefs {
		if !isSimpleIndexOnColumn(indexDef, columnName) {
			continue
		}
		if indexDef.IndexID == 0 || indexDef.RootPageID == 0 {
			return true
		}
	}
	return false
}

func simpleIndexColumn(indexDef storage.CatalogIndex) (string, bool) {
	if indexDef.Unique || len(indexDef.Columns) != 1 || indexDef.Columns[0].Name == "" || indexDef.Columns[0].Desc {
		return "", false
	}
	return indexDef.Columns[0].Name, true
}

func isSimpleIndexOnColumn(indexDef storage.CatalogIndex, columnName string) bool {
	name, ok := simpleIndexColumn(indexDef)
	return ok && name == columnName
}

func simpleEqualityPlanningTarget(sel *parser.SelectExpr) (string, string, bool) {
	if sel == nil || sel.TableName == "" {
		return "", "", false
	}
	tableRef := sel.PrimaryTableRef()
	if sel.Predicate != nil {
		if sel.Predicate.Kind != parser.PredicateKindComparison || sel.Predicate.Comparison == nil {
			return "", "", false
		}
		cond := sel.Predicate.Comparison
		if cond.Operator != "=" || cond.RightRef != "" {
			return "", "", false
		}
		if cond.LeftExpr != nil && cond.RightExpr != nil {
			leftIsLiteral, leftColumnName, ok := simplePlanningOperandShape(cond.LeftExpr)
			if !ok || leftColumnName == "" || leftIsLiteral {
				return "", "", false
			}
			rightIsLiteral, rightColumnName, ok := simplePlanningOperandShape(cond.RightExpr)
			if !ok || rightColumnName != "" || !rightIsLiteral {
				return "", "", false
			}
			columnName, ok := normalizeSimplePlanningColumnName(leftColumnName, tableRef)
			if !ok {
				return "", "", false
			}
			return sel.TableName, columnName, true
		}
		if cond.LeftExpr != nil || cond.RightExpr != nil {
			return "", "", false
		}
		columnName, ok := normalizeSimplePlanningColumnName(cond.Left, tableRef)
		if !ok {
			return "", "", false
		}
		return sel.TableName, columnName, true
	}
	if sel.Where == nil || len(sel.Where.Items) != 1 {
		return "", "", false
	}
	item := sel.Where.Items[0]
	if item.Op != "" || item.Condition.Operator != "=" || item.Condition.RightRef != "" || item.Condition.LeftExpr != nil || item.Condition.RightExpr != nil {
		return "", "", false
	}
	columnName, ok := normalizeSimplePlanningColumnName(item.Condition.Left, tableRef)
	if !ok {
		return "", "", false
	}
	return sel.TableName, columnName, true
}

func normalizeSimplePlanningColumnName(name string, tableRef *parser.TableRef) (string, bool) {
	if !strings.Contains(name, ".") {
		return name, true
	}
	parts := strings.Split(name, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || tableRef == nil {
		return "", false
	}
	if parts[0] != tableRef.Name && (tableRef.Alias == "" || parts[0] != tableRef.Alias) {
		return "", false
	}
	return parts[1], true
}

func simplePlanningOperandShape(expr *parser.ValueExpr) (bool, string, bool) {
	if expr == nil {
		return false, "", false
	}
	switch expr.Kind {
	case parser.ValueExprKindLiteral:
		return true, "", true
	case parser.ValueExprKindColumnRef:
		if expr.Qualifier != "" {
			return false, expr.Qualifier + "." + expr.Column, true
		}
		return false, expr.Column, true
	case parser.ValueExprKindParen:
		return simplePlanningOperandShape(expr.Inner)
	default:
		return false, "", false
	}
}

func (db *DB) pageReaderForLookup(pageID uint32) ([]byte, error) {
	if db == nil || db.pool == nil {
		return nil, ErrInvalidArgument
	}
	pageData, err := readCommittedPageData(db.pool, storage.PageID(pageID))
	if err != nil {
		return nil, wrapStorageError(err)
	}
	if pageData == nil {
		return nil, newStorageError("corrupted index page")
	}
	return pageData, nil
}

func (db *DB) committedTableLocators(table *executor.Table) ([]storage.RowLocator, error) {
	if db == nil || table == nil {
		return nil, ErrInvalidArgument
	}
	locators, persistedRows, err := loadCommittedTableRowsAndLocators(db.pool, table)
	if err != nil {
		return nil, err
	}
	if len(locators) != len(table.Rows) || len(persistedRows) != len(table.Rows) {
		return nil, newStorageError("row locator mismatch")
	}
	return locators, nil
}

func tableNamesForSelectHandoff(handoff *executor.SelectExecutionHandoff) []string {
	if handoff == nil {
		return nil
	}
	accessPath := handoff.AccessPath()
	switch accessPath.Kind {
	case executor.SelectAccessPathKindJoin:
		return []string{accessPath.JoinLeftTable, accessPath.JoinRightTable}
	case executor.SelectAccessPathKindTable, executor.SelectAccessPathKindIndex:
		if accessPath.SingleTableName == "" {
			return nil
		}
		return []string{accessPath.SingleTableName}
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
		simpleIndexes := make(map[string]planner.SimpleIndex)
		for _, indexDef := range table.IndexDefs {
			columnName, ok := simpleIndexColumn(indexDef)
			if !ok || indexDef.IndexID == 0 || indexDef.RootPageID == 0 {
				continue
			}
			simpleIndexes[columnName] = planner.SimpleIndex{
				TableName:  table.Name,
				ColumnName: columnName,
				IndexID:    indexDef.IndexID,
				RootPageID: indexDef.RootPageID,
			}
		}
		metadata[tableName] = &planner.TableMetadata{
			SimpleIndexes: simpleIndexes,
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
	db.pendingPages = nil
	return nil
}

func (db *DB) clearTxn() {
	if db == nil {
		return
	}
	db.txn = nil
	db.pendingPages = nil
}

func (db *DB) beginWriteTxn() error {
	if db == nil {
		return ErrInvalidArgument
	}
	db.writerMu.Lock()
	defer db.writerMu.Unlock()
	if db.writerActive {
		return newExecError("write conflict")
	}
	db.writerActive = true
	return nil
}

func (db *DB) endWriteTxn() {
	if db == nil {
		return
	}
	db.writerMu.Lock()
	db.writerActive = false
	db.writerMu.Unlock()
}

// rollbackTxn restores pre-commit page images in memory. Commit remains the
// only durability boundary; surviving journals are handled on the next open.
func (db *DB) rollbackTxn() error {
	if db == nil {
		return ErrInvalidArgument
	}
	if db.txn == nil {
		if db.pool != nil {
			db.pool.DiscardPrivatePages()
		}
		return nil
	}
	if db.pager == nil {
		return newExecError("invalid transaction state")
	}
	if db.txn.IsActive() {
		if db.pool != nil {
			db.pool.DiscardPrivatePages()
		}
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
	if db.pool != nil {
		db.pool.DiscardPrivatePages()
	}
	return nil
}

// execMutatingStatement enforces internal autocommit for mutating statements.
func (db *DB) execMutatingStatement(apply func() error) (bool, error) {
	if db == nil {
		return false, ErrInvalidArgument
	}
	if err := db.beginWriteTxn(); err != nil {
		return false, err
	}
	defer db.endWriteTxn()

	if err := db.beginTxn(); err != nil {
		return false, err
	}
	if err := apply(); err != nil {
		if rollbackErr := db.rollbackTxn(); rollbackErr != nil {
			return false, errors.Join(err, rollbackErr)
		}
		db.clearTxn()
		return false, err
	}

	if db.txn != nil {
		if err := db.txn.MarkDirty(); err != nil {
			if rollbackErr := db.rollbackTxn(); rollbackErr != nil {
				return false, errors.Join(err, rollbackErr)
			}
			db.clearTxn()
			return false, err
		}
	}
	if err := db.commitTxn(); err != nil {
		var cpErr *checkpointError
		if errors.As(err, &cpErr) {
			db.clearTxn()
			return true, err
		}
		if rollbackErr := db.rollbackTxn(); rollbackErr != nil {
			return false, errors.Join(err, rollbackErr)
		}
		db.clearTxn()
		return false, err
	}
	db.clearTxn()
	return true, nil
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
	durable := false
	var commitLSN uint64
	var checkpointPageCount uint32
	if db.txn.IsDirty() {
		var err error
		commitLSN, checkpointPageCount, err = db.appendPendingPagesToWAL()
		if err != nil {
			return err
		}
		durable = true
	}
	if err := db.txn.Commit(); err != nil {
		if errors.Is(err, txn.ErrNoActiveTxn) || errors.Is(err, txn.ErrInvalidCommitState) {
			return newExecError("invalid transaction state")
		}
		return err
	}
	if db.pool != nil {
		db.pool.PromotePrivatePages()
	}
	var checkpointErr error
	if durable {
		if err := db.updatePendingCheckpointMetadata(commitLSN, checkpointPageCount); err != nil {
			checkpointErr = err
		}
	}
	if durable && checkpointErr == nil {
		checkpointErr = db.checkpointCommittedPages()
		if checkpointErr == nil {
			if err := resetWAL(db.path, storage.DBFormatVersion()); err != nil {
				checkpointErr = wrapStorageError(err)
			}
		}
	}
	db.pager.ClearDirtyTracking()
	if len(db.pager.DirtyPages()) != 0 || len(db.pager.DirtyPagesWithOriginals()) != 0 {
		return newExecError("invalid transaction state")
	}
	if checkpointErr == nil {
		if _, err := os.Stat(storage.JournalPath(db.path)); err == nil {
			return newExecError("invalid transaction state")
		} else if !errors.Is(err, os.ErrNotExist) {
			return wrapStorageError(err)
		}
	}
	if checkpointErr != nil {
		return &checkpointError{err: checkpointErr}
	}
	return nil
}

func (db *DB) checkpointCommittedPages() error {
	if db == nil || db.pager == nil {
		return nil
	}

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
	if table.TableID == 0 {
		table.TableID = nextTableID(stagedTables)
	}

	nextFreshID := db.pager.NextPageID()
	rootPageID, isNew, err := db.allocatePageIDFrom(&nextFreshID)
	if err != nil {
		return err
	}
	table.SetStorageMeta(rootPageID, 0)
	tableHeaderPageID := nextFreshID
	nextFreshID++
	table.SetPhysicalTableRootMeta(tableHeaderPageID, storage.CurrentTableStorageFormatVersion, 0, 0, 0)

	rootPageData, err := storage.BuildSlottedTablePageData(uint32(rootPageID), nil)
	if err != nil {
		return wrapStorageError(err)
	}
	tableHeaderPageData := storage.InitTableHeaderPage(uint32(tableHeaderPageID), table.TableID)

	if err := db.stageSchemaState(stagedTables, []stagedPage{
		{
			id:    rootPageID,
			data:  rootPageData,
			isNew: isNew,
		},
		{
			id:    tableHeaderPageID,
			data:  tableHeaderPageData,
			isNew: true,
		},
	}); err != nil {
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
	return db.stageSchemaState(stagedTables, nil)
}

func (db *DB) applyStagedDropIndex(stagedTables map[string]*executor.Table, rootPageID storage.PageID) error {
	if db == nil || db.pager == nil {
		return nil
	}

	originalFreeListHead := db.freeListHead
	pages, err := db.buildFreedPages(rootPageID)
	if err != nil {
		db.freeListHead = originalFreeListHead
		return err
	}

	if err := db.stageSchemaState(stagedTables, pages); err != nil {
		db.freeListHead = originalFreeListHead
		return err
	}
	return nil
}

func (db *DB) applyStagedDropTable(stagedTables map[string]*executor.Table, rootPageIDs []storage.PageID) error {
	if db == nil || db.pager == nil {
		return nil
	}

	originalFreeListHead := db.freeListHead
	pages, err := db.buildFreedPages(rootPageIDs...)
	if err != nil {
		db.freeListHead = originalFreeListHead
		return err
	}

	if err := db.stageSchemaState(stagedTables, pages); err != nil {
		db.freeListHead = originalFreeListHead
		return err
	}
	return nil
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
	if indexDef.IndexID == 0 {
		indexDef.IndexID = nextIndexID(stagedTables)
	}

	var pages []stagedPage
	if indexDef.RootPageID == 0 {
		rootPageID, isNew, err := db.allocatePageID()
		if err != nil {
			return err
		}
		indexDef.RootPageID = uint32(rootPageID)
		pages = append(pages, stagedPage{
			id:    rootPageID,
			data:  storage.InitIndexLeafPage(uint32(rootPageID)),
			isNew: isNew,
		})
	}

	if len(table.Rows) > 0 {
		locators, err := db.committedTableLocators(table)
		if err != nil {
			return err
		}
		indexPages, err := db.buildRebuiltIndexPagesForDefs(table, table.Rows, locators, []*storage.CatalogIndex{indexDef}, db.pager.NextPageID())
		if err != nil {
			return err
		}
		pages = append(pages, indexPages...)
	}

	return db.stageSchemaState(stagedTables, pages)
}

// Stage 5 correctness requires proposal/staging before apply so a single
// statement cannot leak mixed committed and uncommitted visibility. Crash-safe
// durability is still future work.
func (db *DB) stageDirtyState(catalogWrite *storage.CatalogWritePlan, pages []stagedPage) error {
	if db == nil || db.pager == nil {
		return nil
	}
	if catalogWrite == nil {
		return ErrInvalidArgument
	}

	stagedPages := make([]stagedPage, 0, len(pages)+len(catalogWrite.OverflowPages)+len(catalogWrite.ReclaimedPages)+1)
	stagedPages = append(stagedPages, pages...)
	for _, overflowPage := range catalogWrite.OverflowPages {
		stagedPages = append(stagedPages, stagedPage{
			id:    overflowPage.PageID,
			data:  overflowPage.Data,
			isNew: overflowPage.IsNew,
		})
	}
	for _, reclaimedPage := range catalogWrite.ReclaimedPages {
		stagedPages = append(stagedPages, stagedPage{
			id:   reclaimedPage.PageID,
			data: reclaimedPage.Data,
		})
	}
	stagedPages = append(stagedPages, stagedPage{id: 0, data: catalogWrite.DirectoryPage})

	for _, staged := range stagedPages {
		if err := db.stagePrivatePageData(staged); err != nil {
			return err
		}
	}
	db.pendingPages = stagedPages
	if err := db.materializePendingPages(); err != nil {
		return err
	}
	db.freeListHead = catalogWrite.FreeListHead
	return nil
}

func (db *DB) stageSchemaState(stagedTables map[string]*executor.Table, pages []stagedPage) error {
	if db == nil || db.pager == nil {
		return nil
	}

	nextFreshID := nextFreshPageIDAfter(pages, db.pager.NextPageID())
	tableHeaderPages, err := db.ensureTableHeaderRoots(stagedTables, nextFreshID)
	if err != nil {
		return err
	}
	systemStartNextFreshID := nextFreshPageIDAfter(tableHeaderPages, nextFreshID)
	systemPages, _, err := db.rebuildSystemCatalogRows(stagedTables, nil, systemStartNextFreshID)
	if err != nil {
		return err
	}
	allPages := make([]stagedPage, 0, len(pages)+len(tableHeaderPages)+len(systemPages))
	allPages = append(allPages, pages...)
	allPages = append(allPages, tableHeaderPages...)
	allPages = append(allPages, systemPages...)

	catalogWrite, err := db.buildCatalogPageData(stagedTables, allPages)
	if err != nil {
		return wrapStorageError(err)
	}
	return db.stageDirtyState(catalogWrite, allPages)
}

func (db *DB) ensureTableHeaderRoots(stagedTables map[string]*executor.Table, nextFreshID storage.PageID) ([]stagedPage, error) {
	if db == nil || db.pager == nil {
		return nil, nil
	}

	tableNames := make([]string, 0, len(stagedTables))
	for name, table := range stagedTables {
		if table == nil {
			continue
		}
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	pages := make([]stagedPage, 0)
	for _, tableName := range tableNames {
		table := stagedTables[tableName]
		if table == nil {
			continue
		}
		if table.TableID == 0 {
			table.TableID = nextTableID(stagedTables)
		}
		if table.TableHeaderPageID() != 0 {
			continue
		}
		tableHeaderPageID := nextFreshID
		nextFreshID++
		table.SetPhysicalTableRootMeta(tableHeaderPageID, storage.CurrentTableStorageFormatVersion, 0, 0, 0)
		pages = append(pages, stagedPage{
			id:    tableHeaderPageID,
			data:  storage.InitTableHeaderPage(uint32(tableHeaderPageID), table.TableID),
			isNew: true,
		})
	}
	return pages, nil
}

func (db *DB) buildCatalogPageData(stagedTables map[string]*executor.Table, pages []stagedPage) (*storage.CatalogWritePlan, error) {
	if db == nil {
		return nil, ErrInvalidArgument
	}
	freeListHead := db.freeListHead
	nextFreshID := db.pager.NextPageID()
	for _, staged := range pages {
		if staged.isNew && staged.id >= nextFreshID {
			nextFreshID = staged.id + 1
		}
	}
	currentMode := storage.DirectoryCATDIRStorageModeEmbedded
	currentOverflowHead := storage.PageID(0)
	currentOverflowCount := uint32(0)
	if pageData, err := db.pager.ReadPage(storage.DirectoryControlPageID); err == nil && storage.ValidateDirectoryPage(pageData) == nil {
		mode, err := storage.DirectoryCATDIRStorageMode(pageData)
		if err != nil {
			return nil, err
		}
		currentMode = mode
		if currentMode == storage.DirectoryCATDIRStorageModeOverflow {
			overflowHead, err := storage.DirectoryCATDIROverflowHeadPageID(pageData)
			if err != nil {
				return nil, err
			}
			currentOverflowHead = storage.PageID(overflowHead)
			currentOverflowCount, err = storage.DirectoryCATDIROverflowPageCount(pageData)
			if err != nil {
				return nil, err
			}
		}
	}
	allocateOverflowPage := func() (storage.PageID, bool, error) {
		allocator := storage.PageAllocator{
			NextPageID: uint32(nextFreshID),
			FreePage: storage.FreePageState{
				HeadPageID: freeListHead,
			},
			ReadFreeNext: func(pageID uint32) (uint32, error) {
				pageData, err := db.pager.ReadPage(storage.PageID(pageID))
				if err != nil {
					return 0, err
				}
				return storage.FreePageNext(pageData)
			},
		}
		allocated, reused, err := allocator.Allocate()
		if err != nil {
			return 0, false, wrapStorageError(err)
		}
		nextFreshID = storage.PageID(allocator.NextPageID)
		freeListHead = allocator.FreePage.HeadPageID
		return storage.PageID(allocated), !reused, nil
	}
	return storage.PrepareCatalogWritePlanWithRootMappings(catalogFromTables(stagedTables), directoryRootMappingsFromTables(stagedTables), currentMode, currentOverflowHead, currentOverflowCount, db.pager, storage.CurrentDBFormatVersion, &freeListHead, storage.DirectoryCheckpointMetadata{
		LastCheckpointLSN:       db.lastCheckpointLSN,
		LastCheckpointPageCount: db.lastCheckpointPageCount,
	}, allocateOverflowPage)
}

func (db *DB) persistCatalogState(stagedTables map[string]*executor.Table, pages []stagedPage) error {
	if db == nil || db.pager == nil {
		return nil
	}

	nextFreshID := nextFreshPageIDAfter(pages, db.pager.NextPageID())
	tableHeaderPages, err := db.ensureTableHeaderRoots(stagedTables, nextFreshID)
	if err != nil {
		return err
	}
	allPages := make([]stagedPage, 0, len(pages)+len(tableHeaderPages))
	allPages = append(allPages, pages...)
	allPages = append(allPages, tableHeaderPages...)

	catalogWrite, err := db.buildCatalogPageData(stagedTables, allPages)
	if err != nil {
		return wrapStorageError(err)
	}

	stagedPages := make([]stagedPage, 0, len(allPages)+len(catalogWrite.OverflowPages)+len(catalogWrite.ReclaimedPages)+1)
	stagedPages = append(stagedPages, allPages...)
	for _, overflowPage := range catalogWrite.OverflowPages {
		stagedPages = append(stagedPages, stagedPage{
			id:    overflowPage.PageID,
			data:  overflowPage.Data,
			isNew: overflowPage.IsNew,
		})
	}
	for _, reclaimedPage := range catalogWrite.ReclaimedPages {
		stagedPages = append(stagedPages, stagedPage{
			id:   reclaimedPage.PageID,
			data: reclaimedPage.Data,
		})
	}
	stagedPages = append(stagedPages, stagedPage{id: 0, data: catalogWrite.DirectoryPage})
	for _, staged := range stagedPages {
		var page *storage.Page
		if staged.isNew {
			page = db.pager.NewPage()
			if page.ID() != staged.id {
				db.pager.DiscardNewPage(page.ID())
				return wrapStorageError(newStorageError("unexpected new page id"))
			}
		} else {
			page, err = db.pager.Get(staged.id)
			if err != nil {
				return wrapStorageError(err)
			}
		}
		db.pager.MarkDirtyWithOriginal(page)
		clear(page.Data())
		copy(page.Data(), staged.data)
	}
	if err := db.pager.FlushDirty(); err != nil {
		return wrapStorageError(err)
	}
	db.freeListHead = catalogWrite.FreeListHead
	return nil
}

func (db *DB) setFreeListHead(head uint32) error {
	if db == nil {
		return ErrInvalidArgument
	}
	db.freeListHead = head
	return nil
}

func (db *DB) buildFreedPages(pageIDs ...storage.PageID) ([]stagedPage, error) {
	if db == nil {
		return nil, ErrInvalidArgument
	}
	if len(pageIDs) == 0 {
		return nil, nil
	}

	pages := make([]stagedPage, 0, len(pageIDs))
	seen := make(map[storage.PageID]struct{}, len(pageIDs))
	for _, pageID := range pageIDs {
		if pageID == 0 {
			continue
		}
		if _, exists := seen[pageID]; exists {
			continue
		}
		seen[pageID] = struct{}{}

		pageData := storage.InitFreePage(uint32(pageID), db.freeListHead)
		pages = append(pages, stagedPage{
			id:   pageID,
			data: pageData,
		})
		if err := db.setFreeListHead(uint32(pageID)); err != nil {
			return nil, err
		}
	}
	return pages, nil
}

func (db *DB) updatePendingCheckpointMetadata(commitLSN uint64, pageCount uint32) error {
	if db == nil {
		return ErrInvalidArgument
	}
	if commitLSN == 0 {
		return nil
	}

	pageData := append([]byte(nil), db.pendingPageData(0)...)
	if len(pageData) == 0 {
		return nil
	}
	if err := storage.SetDirectoryLastCheckpointLSN(pageData, commitLSN); err != nil {
		return wrapStorageError(err)
	}
	if err := storage.SetDirectoryLastCheckpointPageCount(pageData, pageCount); err != nil {
		return wrapStorageError(err)
	}
	if err := db.updatePendingPageImage(0, pageData); err != nil {
		return err
	}
	db.lastCheckpointLSN = commitLSN
	db.lastCheckpointPageCount = pageCount
	return nil
}

func (db *DB) pendingPageData(pageID storage.PageID) []byte {
	if db == nil {
		return nil
	}
	for _, staged := range db.pendingPages {
		if staged.id == pageID {
			return staged.data
		}
	}
	return nil
}

func (db *DB) allocatePageID() (storage.PageID, bool, error) {
	if db == nil || db.pager == nil {
		return 0, false, ErrInvalidArgument
	}
	nextFreshID := db.pager.NextPageID()
	return db.allocatePageIDFrom(&nextFreshID)
}

func nextFreshPageIDAfter(pages []stagedPage, nextFreshID storage.PageID) storage.PageID {
	for _, staged := range pages {
		if staged.isNew && staged.id >= nextFreshID {
			nextFreshID = staged.id + 1
		}
	}
	return nextFreshID
}

func (db *DB) allocatePageIDFrom(nextFreshID *storage.PageID) (storage.PageID, bool, error) {
	if db == nil || db.pager == nil || nextFreshID == nil {
		return 0, false, ErrInvalidArgument
	}
	allocator := storage.PageAllocator{
		NextPageID: uint32(*nextFreshID),
		FreePage: storage.FreePageState{
			HeadPageID: db.freeListHead,
		},
		ReadFreeNext: func(pageID uint32) (uint32, error) {
			pageData, err := db.pager.ReadPage(storage.PageID(pageID))
			if err != nil {
				return 0, err
			}
			return storage.FreePageNext(pageData)
		},
	}
	allocated, reused, err := allocator.Allocate()
	if err != nil {
		return 0, false, wrapStorageError(err)
	}
	if err := db.setFreeListHead(allocator.FreePage.HeadPageID); err != nil {
		return 0, false, err
	}
	*nextFreshID = storage.PageID(allocator.NextPageID)
	return storage.PageID(allocated), !reused, nil
}

func (db *DB) finalCommittedPageImages() []stagedPage {
	if db == nil || len(db.pendingPages) == 0 {
		return nil
	}

	latestByID := make(map[storage.PageID]stagedPage, len(db.pendingPages))
	for _, staged := range db.pendingPages {
		latestByID[staged.id] = stagedPage{
			id:    staged.id,
			data:  append([]byte(nil), staged.data...),
			isNew: staged.isNew,
		}
	}

	ids := make([]int, 0, len(latestByID))
	for pageID := range latestByID {
		ids = append(ids, int(pageID))
	}
	sort.Ints(ids)

	finalPages := make([]stagedPage, 0, len(ids))
	for _, id := range ids {
		finalPages = append(finalPages, latestByID[storage.PageID(id)])
	}
	return finalPages
}

func (db *DB) nextWALRecordLSN() uint64 {
	if db == nil {
		return 0
	}
	lsn := db.nextWALLSN
	if lsn == 0 {
		lsn = 1
	}
	db.nextWALLSN = lsn + 1
	return lsn
}

func (db *DB) appendPendingPagesToWAL() (uint64, uint32, error) {
	if db == nil {
		return 0, 0, ErrInvalidArgument
	}

	finalPages := db.finalCommittedPageImages()
	for _, staged := range finalPages {
		frameLSN := db.nextWALRecordLSN()
		pageData := append([]byte(nil), staged.data...)
		if staged.id != 0 {
			if err := storage.SetPageLSN(pageData, frameLSN); err != nil {
				return 0, 0, wrapStorageError(err)
			}
		}
		if err := storage.FinalizePageImage(pageData); err != nil {
			return 0, 0, wrapStorageError(err)
		}
		if err := db.updatePendingPageImage(staged.id, pageData); err != nil {
			return 0, 0, err
		}

		var frame storage.WALFrame
		frame.FrameLSN = frameLSN
		frame.PageID = uint32(staged.id)
		frame.PageLSN = frameLSN
		copy(frame.PageData[:], pageData)
		if err := appendWALFrameRecord(db.path, frame); err != nil {
			return 0, 0, wrapStorageError(err)
		}
	}

	commitLSN := db.nextWALRecordLSN()
	if err := appendWALCommitRecord(db.path, storage.WALCommitRecord{CommitLSN: commitLSN}); err != nil {
		return 0, 0, wrapStorageError(err)
	}
	if err := syncWAL(db.path); err != nil {
		return 0, 0, wrapStorageError(err)
	}
	return commitLSN, uint32(len(finalPages)), nil
}

func (db *DB) updatePendingPageImage(pageID storage.PageID, pageData []byte) error {
	if db == nil {
		return ErrInvalidArgument
	}

	for i := range db.pendingPages {
		if db.pendingPages[i].id != pageID {
			continue
		}
		db.pendingPages[i].data = append([]byte(nil), pageData...)

		frame, err := db.getWritablePrivateFrame(pageID)
		if err != nil {
			return err
		}
		if frame == nil {
			return newStorageError("corrupted page")
		}
		clear(frame.Data[:])
		copy(frame.Data[:], pageData)
		db.pool.MarkDirty(frame)
		db.pool.UnlatchExclusive(frame)
		db.pool.Unpin(frame)

		page, err := db.pager.Get(pageID)
		if err != nil {
			return wrapStorageError(err)
		}
		clear(page.Data())
		copy(page.Data(), pageData)
		return nil
	}
	return nil
}

// stagePrivatePageData is the writer-side page helper. It resolves only
// private frames before materializing them into the existing pager flow.
func (db *DB) stagePrivatePageData(staged stagedPage) error {
	if db == nil || db.pool == nil {
		return nil
	}

	var (
		frame *bufferpool.Frame
		err   error
	)
	if staged.isNew {
		frame, err = db.pool.InstallPrivatePage(bufferpool.PageID(staged.id), staged.data)
	} else {
		frame, err = db.getWritablePrivateFrame(staged.id)
		if err == nil && frame != nil {
			clear(frame.Data[:])
			copy(frame.Data[:], staged.data)
		}
	}
	if err != nil {
		return wrapStorageError(err)
	}
	if frame == nil {
		return newStorageError("corrupted page")
	}
	db.pool.MarkDirty(frame)
	db.pool.UnlatchExclusive(frame)
	db.pool.Unpin(frame)
	return nil
}

func (db *DB) getWritablePrivateFrame(pageID storage.PageID) (*bufferpool.Frame, error) {
	if db == nil || db.pool == nil {
		return nil, nil
	}
	frame, err := db.pool.GetPrivatePage(bufferpool.PageID(pageID))
	if err != nil {
		return nil, wrapStorageError(err)
	}
	if err := bufferpool.RequirePrivateFrame(frame); err != nil {
		if frame != nil {
			db.pool.UnlatchExclusive(frame)
			db.pool.Unpin(frame)
		}
		return nil, wrapStorageError(err)
	}
	return frame, nil
}

func (db *DB) materializePendingPages() error {
	if db == nil || db.pager == nil || len(db.pendingPages) == 0 {
		return nil
	}

	for _, staged := range db.pendingPages {
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

		frame, err := db.getWritablePrivateFrame(staged.id)
		if err != nil {
			return err
		}
		if frame == nil {
			return newStorageError("corrupted page")
		}

		db.pager.MarkDirtyWithOriginal(page)
		clear(page.Data())
		copy(page.Data(), frame.Data[:])
		db.pool.UnlatchExclusive(frame)
		db.pool.Unpin(frame)
	}
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
		Name:           table.Name,
		TableID:        table.TableID,
		IsSystem:       table.IsSystem,
		Columns:        columns,
		Rows:           rows,
		IndexDefs:      cloneIndexDefs(table.IndexDefs),
		PrimaryKeyDef:  clonePrimaryKeyDef(table.PrimaryKeyDef),
		ForeignKeyDefs: cloneForeignKeyDefs(table.ForeignKeyDefs),
	}
	cloned.SetStorageMeta(table.RootPageID(), table.PersistedRowCount())
	cloned.SetPhysicalTableRootMeta(table.TableHeaderPageID(), table.TableStorageFormatVersion(), table.FirstSpaceMapPageID(), table.OwnedDataPageCount(), table.OwnedSpaceMapPageCount())
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
			IndexID:    indexDef.IndexID,
			RootPageID: indexDef.RootPageID,
			Columns:    append([]storage.CatalogIndexColumn(nil), indexDef.Columns...),
		})
	}
	return cloned
}

func clonePrimaryKeyDef(pk *storage.CatalogPrimaryKey) *storage.CatalogPrimaryKey {
	if pk == nil {
		return nil
	}
	cloned := *pk
	cloned.Columns = append([]string(nil), pk.Columns...)
	return &cloned
}

func cloneForeignKeyDefs(fks []storage.CatalogForeignKey) []storage.CatalogForeignKey {
	if len(fks) == 0 {
		return nil
	}
	cloned := make([]storage.CatalogForeignKey, 0, len(fks))
	for _, fk := range fks {
		clonedFK := fk
		clonedFK.ChildColumns = append([]string(nil), fk.ChildColumns...)
		clonedFK.ParentColumns = append([]string(nil), fk.ParentColumns...)
		cloned = append(cloned, clonedFK)
	}
	return cloned
}

func nextTableID(tables map[string]*executor.Table) uint32 {
	var maxID uint32
	for _, table := range tables {
		if table != nil && table.TableID > maxID {
			maxID = table.TableID
		}
	}
	if maxID == 0 {
		return 1
	}
	return maxID + 1
}

func nextIndexID(tables map[string]*executor.Table) uint32 {
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
	if maxID == 0 {
		return 1
	}
	return maxID + 1
}

type indexPageStager struct {
	pages       map[storage.PageID]stagedPage
	order       []storage.PageID
	allocate    func() (storage.PageID, bool, error)
	nextFreshID storage.PageID
}

func newIndexPageStager(nextPage storage.PageID, allocate func() (storage.PageID, bool, error)) *indexPageStager {
	return &indexPageStager{
		pages:       make(map[storage.PageID]stagedPage),
		order:       nil,
		allocate:    allocate,
		nextFreshID: nextPage,
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

func (s *indexPageStager) allocatePageID() (storage.PageID, bool, error) {
	if s == nil {
		return 0, false, nil
	}
	if s.allocate == nil {
		pageID := s.nextFreshID
		s.nextFreshID++
		return pageID, true, nil
	}
	return s.allocate()
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

func (db *DB) buildInsertedIndexPages(table *executor.Table, row []parser.Value, locator storage.RowLocator, startNextFreshID storage.PageID) ([]stagedPage, error) {
	if db == nil || table == nil || len(table.IndexDefs) == 0 {
		return nil, nil
	}

	nextFreshID := startNextFreshID
	stager := newIndexPageStager(nextFreshID, func() (storage.PageID, bool, error) {
		return db.allocatePageIDFrom(&nextFreshID)
	})
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

func (db *DB) persistPublicTxState(stagedTables map[string]*executor.Table) error {
	if db == nil {
		return ErrInvalidArgument
	}
	if db.pager == nil {
		return nil
	}

	originalFreeListHead := db.freeListHead
	newRoots, err := db.assignPublicTxSnapshotMetadata(stagedTables)
	if err != nil {
		db.freeListHead = originalFreeListHead
		return err
	}

	tableNames := make([]string, 0, len(stagedTables))
	for name, table := range stagedTables {
		if table == nil || table.IsSystem {
			continue
		}
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	pages := make([]stagedPage, 0, len(tableNames))
	for _, tableName := range tableNames {
		table := stagedTables[tableName]
		table.SetStorageMeta(table.RootPageID(), uint32(len(table.Rows)))

		tablePages, locators, err := db.stageTableRewriteViaPhysicalStorage(table, table.Rows, true, db.pager.NextPageID(), true)
		if err != nil {
			db.freeListHead = originalFreeListHead
			return err
		}
		pages = append(pages, tablePages...)

		indexPages, err := db.buildPublicTxRebuiltIndexPages(table, table.Rows, locators, newRoots, nextFreshPageIDAfter(pages, db.pager.NextPageID()))
		if err != nil {
			db.freeListHead = originalFreeListHead
			return err
		}
		pages = append(pages, indexPages...)
	}

	if err := db.stageSchemaState(stagedTables, pages); err != nil {
		db.freeListHead = originalFreeListHead
		return err
	}
	return nil
}

func (db *DB) assignPublicTxSnapshotMetadata(stagedTables map[string]*executor.Table) (map[storage.PageID]bool, error) {
	if db == nil {
		return nil, ErrInvalidArgument
	}
	newRoots := make(map[storage.PageID]bool)

	tableNames := make([]string, 0, len(stagedTables))
	for name, table := range stagedTables {
		if table == nil || table.IsSystem {
			continue
		}
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	for _, tableName := range tableNames {
		table := stagedTables[tableName]
		if table.TableID == 0 {
			table.TableID = nextTableID(stagedTables)
		}
		if table.RootPageID() == 0 {
			rootPageID, isNew, err := db.allocatePageID()
			if err != nil {
				return nil, err
			}
			table.SetStorageMeta(rootPageID, table.PersistedRowCount())
			newRoots[rootPageID] = isNew
		}
		if table.TableHeaderPageID() == 0 {
			tableHeaderPageID, _, err := db.allocatePageID()
			if err != nil {
				return nil, err
			}
			table.SetPhysicalTableRootMeta(tableHeaderPageID, storage.CurrentTableStorageFormatVersion, 0, 0, 0)
		}
		for i := range table.IndexDefs {
			indexDef := &table.IndexDefs[i]
			if indexDef.IndexID == 0 {
				indexDef.IndexID = nextIndexID(stagedTables)
			}
			if indexDef.RootPageID != 0 {
				continue
			}
			rootPageID, isNew, err := db.allocatePageID()
			if err != nil {
				return nil, err
			}
			indexDef.RootPageID = uint32(rootPageID)
			newRoots[rootPageID] = isNew
		}
	}
	return newRoots, nil
}

func (db *DB) buildPublicTxRebuiltIndexPages(table *executor.Table, rows [][]parser.Value, locators []storage.RowLocator, newRoots map[storage.PageID]bool, startNextFreshID storage.PageID) ([]stagedPage, error) {
	if db == nil || table == nil || len(table.IndexDefs) == 0 {
		return nil, nil
	}
	if len(rows) != len(locators) {
		return nil, newStorageError("row locator mismatch")
	}

	nextFreshID := startNextFreshID
	for pageID, isNew := range newRoots {
		if isNew && pageID >= nextFreshID {
			nextFreshID = pageID + 1
		}
	}
	stager := newIndexPageStager(nextFreshID, func() (storage.PageID, bool, error) {
		return db.allocatePageIDFrom(&nextFreshID)
	})
	for i := range table.IndexDefs {
		indexDef := &table.IndexDefs[i]
		if indexDef.RootPageID == 0 {
			continue
		}

		rootPageID := storage.PageID(indexDef.RootPageID)
		stager.stage(stagedPage{
			id:    rootPageID,
			data:  storage.InitIndexLeafPage(indexDef.RootPageID),
			isNew: newRoots[rootPageID],
		})
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

func (db *DB) buildRebuiltIndexPages(table *executor.Table, rows [][]parser.Value, locators []storage.RowLocator, startNextFreshID storage.PageID) ([]stagedPage, error) {
	if db == nil || table == nil || len(table.IndexDefs) == 0 {
		return nil, nil
	}
	indexDefs := make([]*storage.CatalogIndex, 0, len(table.IndexDefs))
	for i := range table.IndexDefs {
		indexDefs = append(indexDefs, &table.IndexDefs[i])
	}
	return db.buildRebuiltIndexPagesForDefs(table, rows, locators, indexDefs, startNextFreshID)
}

func (db *DB) buildRebuiltIndexPagesForDefs(table *executor.Table, rows [][]parser.Value, locators []storage.RowLocator, indexDefs []*storage.CatalogIndex, startNextFreshID storage.PageID) ([]stagedPage, error) {
	if db == nil || table == nil || len(table.IndexDefs) == 0 {
		return nil, nil
	}
	if len(rows) != len(locators) {
		return nil, newStorageError("row locator mismatch")
	}

	nextFreshID := startNextFreshID
	stager := newIndexPageStager(nextFreshID, func() (storage.PageID, bool, error) {
		return db.allocatePageIDFrom(&nextFreshID)
	})
	for _, indexDef := range indexDefs {
		if indexDef == nil {
			continue
		}
		if indexDef.RootPageID == 0 {
			continue
		}

		rootPageID := storage.PageID(indexDef.RootPageID)
		stager.stage(stagedPage{
			id:   rootPageID,
			data: storage.InitIndexLeafPage(indexDef.RootPageID),
		})

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
	return storage.EncodeIndexKey(storageValuesFromParser(values))
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
	rightPageID, rightIsNew, err := stager.allocatePageID()
	if err != nil {
		return err
	}
	leftPageData, err = storage.BuildIndexLeafPageData(uint32(leafPageID), leftRecords, uint32(rightPageID))
	if err != nil {
		return wrapStorageError(err)
	}
	rightPageData, err := storage.BuildIndexLeafPageData(uint32(rightPageID), rightRecords, rightSibling)
	if err != nil {
		return wrapStorageError(err)
	}
	stager.stage(stagedPage{id: leafPageID, data: leftPageData})
	stager.stage(stagedPage{id: rightPageID, data: rightPageData, isNew: rightIsNew})

	return db.propagateIndexSplit(indexDef, path, leafPageID, rightPageID, separatorKey, stager)
}

func (db *DB) propagateIndexSplit(indexDef *storage.CatalogIndex, path []indexInsertPathEntry, leftPageID, rightPageID storage.PageID, separatorKey []byte, stager *indexPageStager) error {
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
		newRightPageID, newRightIsNew, err := stager.allocatePageID()
		if err != nil {
			return err
		}
		leftPageData, err := storage.BuildIndexInternalPageData(uint32(parent.pageID), leftRecords)
		if err != nil {
			return wrapStorageError(err)
		}
		rightPageData, err := storage.BuildIndexInternalPageData(uint32(newRightPageID), rightRecords)
		if err != nil {
			return wrapStorageError(err)
		}
		stager.stage(stagedPage{id: parent.pageID, data: leftPageData})
		stager.stage(stagedPage{id: newRightPageID, data: rightPageData, isNew: newRightIsNew})

		leftPageID = parent.pageID
		rightPageID = newRightPageID
		separatorKey = nextSeparatorKey
	}

	newRootPageID, newRootIsNew, err := stager.allocatePageID()
	if err != nil {
		return err
	}
	newRootRecords := []storage.IndexInternalRecord{
		{Key: append([]byte(nil), separatorKey...), ChildPageID: uint32(leftPageID)},
		{Key: append([]byte(nil), separatorKey...), ChildPageID: uint32(rightPageID)},
	}
	newRootPageData, err := storage.BuildIndexInternalPageData(uint32(newRootPageID), newRootRecords)
	if err != nil {
		return wrapStorageError(err)
	}
	stager.stage(stagedPage{id: newRootPageID, data: newRootPageData, isNew: newRootIsNew})
	indexDef.RootPageID = uint32(newRootPageID)
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
			Name:        table.Name,
			TableID:     table.TableID,
			RootPageID:  uint32(table.RootPageID()),
			RowCount:    table.PersistedRowCount(),
			Columns:     make([]storage.CatalogColumn, 0, len(table.Columns)),
			Indexes:     make([]storage.CatalogIndex, 0, len(table.IndexDefs)),
			PrimaryKey:  clonePrimaryKeyDef(table.PrimaryKeyDef),
			ForeignKeys: cloneForeignKeyDefs(table.ForeignKeyDefs),
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
		if len(entry.ForeignKeys) > 1 {
			sort.Slice(entry.ForeignKeys, func(i, j int) bool {
				return entry.ForeignKeys[i].Name < entry.ForeignKeys[j].Name
			})
		}
		catalog.Tables = append(catalog.Tables, entry)
	}
	return catalog
}

func directoryRootMappingsFromTables(tables map[string]*executor.Table) []storage.DirectoryRootIDMapping {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)

	mappings := make([]storage.DirectoryRootIDMapping, 0, len(names)*3)
	for _, name := range names {
		table := tables[name]
		if table == nil || table.TableID == 0 {
			continue
		}
		if table.RootPageID() != 0 {
			mappings = append(mappings, storage.DirectoryRootIDMapping{
				ObjectType: storage.DirectoryRootMappingObjectTable,
				ObjectID:   table.TableID,
				RootPageID: uint32(table.RootPageID()),
			})
		}
		if table.TableHeaderPageID() != 0 {
			mappings = append(mappings, storage.DirectoryRootIDMapping{
				ObjectType: storage.DirectoryRootMappingObjectTableHeader,
				ObjectID:   table.TableID,
				RootPageID: uint32(table.TableHeaderPageID()),
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
			indexDef := indexByName[indexName]
			if indexDef.IndexID == 0 || indexDef.RootPageID == 0 {
				continue
			}
			mappings = append(mappings, storage.DirectoryRootIDMapping{
				ObjectType: storage.DirectoryRootMappingObjectIndex,
				ObjectID:   indexDef.IndexID,
				RootPageID: indexDef.RootPageID,
			})
		}
	}
	return mappings
}

func tablesFromCatalog(catalog *storage.CatalogData) (map[string]*executor.Table, error) {
	tables := make(map[string]*executor.Table)
	if catalog == nil {
		return tables, nil
	}
	seenRootPageIDs := make(map[storage.PageID]struct{}, len(catalog.Tables))
	strictDirectoryRoots := catalog.Version >= 6

	for _, table := range catalog.Tables {
		if table.Name == "" || len(table.Columns) == 0 {
			return nil, errInvalidStoredTableMeta
		}
		if strictDirectoryRoots && table.RootPageID < 1 {
			return nil, errInvalidStoredTableMeta
		}
		rootPageID := storage.PageID(table.RootPageID)
		if rootPageID != 0 {
			if _, ok := seenRootPageIDs[rootPageID]; ok {
				return nil, errDuplicateRootPageID
			}
			seenRootPageIDs[rootPageID] = struct{}{}
		}

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
			Name:           table.Name,
			TableID:        table.TableID,
			IsSystem:       isSystemCatalogTableName(table.Name),
			Columns:        columns,
			IndexDefs:      cloneIndexDefs(table.Indexes),
			PrimaryKeyDef:  clonePrimaryKeyDef(table.PrimaryKey),
			ForeignKeyDefs: cloneForeignKeyDefs(table.ForeignKeys),
		}
		tables[table.Name].SetStorageMeta(rootPageID, table.RowCount)
		for _, index := range table.Indexes {
			if strictDirectoryRoots && index.RootPageID == 0 {
				return nil, errInvalidStoredTableMeta
			}
		}
	}

	return tables, nil
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

func droppedIndexRootForName(tables map[string]*executor.Table, indexName string) (string, storage.PageID) {
	for tableName, table := range tables {
		if table == nil {
			continue
		}
		indexDef := table.IndexDefinition(indexName)
		if indexDef == nil {
			continue
		}
		return tableName, storage.PageID(indexDef.RootPageID)
	}
	return "", 0
}

func droppedTableRootPageIDs(pool *bufferpool.BufferPool, tables map[string]*executor.Table, tableName string) []storage.PageID {
	table := tables[tableName]
	if table == nil {
		return nil
	}

	indexNames := make([]string, 0, len(table.IndexDefs))
	indexRoots := make(map[string]storage.PageID, len(table.IndexDefs))
	for _, indexDef := range table.IndexDefs {
		if indexDef.RootPageID == 0 {
			continue
		}
		indexNames = append(indexNames, indexDef.Name)
		indexRoots[indexDef.Name] = storage.PageID(indexDef.RootPageID)
	}
	sort.Strings(indexNames)

	pageIDs := make([]storage.PageID, 0, len(indexNames)+2+int(table.OwnedSpaceMapPageCount())+int(table.OwnedDataPageCount()))
	if table.TableHeaderPageID() != 0 {
		pageIDs = append(pageIDs, table.TableHeaderPageID())
	}
	if pool != nil && table.FirstSpaceMapPageID() != 0 {
		spaceMapPageIDs, dataPageIDs, err := committedTablePhysicalStorageInventory(pool, table)
		if err == nil {
			pageIDs = append(pageIDs, spaceMapPageIDs...)
			pageIDs = append(pageIDs, dataPageIDs...)
		}
	}
	for _, indexName := range indexNames {
		pageIDs = append(pageIDs, indexRoots[indexName])
	}
	if table.RootPageID() != 0 {
		pageIDs = append(pageIDs, table.RootPageID())
	}
	return pageIDs
}

func loadPhysicalTableRoots(pool *bufferpool.BufferPool, tables map[string]*executor.Table, mappings []storage.DirectoryRootIDMapping) error {
	if pool == nil {
		return ErrInvalidArgument
	}

	headerMappings := make(map[uint32]storage.PageID, len(mappings))
	for _, mapping := range mappings {
		switch mapping.ObjectType {
		case storage.DirectoryRootMappingObjectTable:
			continue
		case storage.DirectoryRootMappingObjectTableHeader:
			if _, exists := headerMappings[mapping.ObjectID]; exists {
				return wrapStorageError(newPhysicalStorageDiagnosticError("corrupted header page", "duplicate table-header mapping for table id %d", mapping.ObjectID))
			}
			headerMappings[mapping.ObjectID] = storage.PageID(mapping.RootPageID)
		case storage.DirectoryRootMappingObjectIndex:
			continue
		default:
			return wrapStorageError(newStorageError("corrupted directory page"))
		}
	}

	for _, table := range tables {
		if table == nil || table.TableID == 0 {
			continue
		}
		headerPageID, ok := headerMappings[table.TableID]
		if !ok || headerPageID == 0 {
			return wrapStorageError(newTablePhysicalStorageError(table, "corrupted header page", "missing table-header mapping"))
		}
		pageData, err := readCommittedPageData(pool, headerPageID)
		if err != nil {
			return wrapStorageError(err)
		}
		if err := storage.ValidateTableHeaderPage(pageData); err != nil {
			return wrapStorageError(newTablePhysicalStorageError(table, "corrupted header page", "header page %d is invalid", headerPageID))
		}
		tableID, err := storage.TableHeaderTableID(pageData)
		if err != nil {
			return wrapStorageError(newTablePhysicalStorageError(table, "corrupted header page", "header page %d has unreadable owning table id", headerPageID))
		}
		if tableID != table.TableID {
			return wrapStorageError(newTablePhysicalStorageError(table, "corrupted header page", "header page %d claims table id %d", headerPageID, tableID))
		}
		storageVersion, err := storage.TableHeaderStorageFormatVersion(pageData)
		if err != nil {
			return wrapStorageError(err)
		}
		firstSpaceMapPageID, err := storage.TableHeaderFirstSpaceMapPageID(pageData)
		if err != nil {
			return wrapStorageError(err)
		}
		ownedDataPages, err := storage.TableHeaderOwnedDataPageCount(pageData)
		if err != nil {
			return wrapStorageError(err)
		}
		ownedSpaceMapPages, err := storage.TableHeaderOwnedSpaceMapPageCount(pageData)
		if err != nil {
			return wrapStorageError(err)
		}
		table.SetPhysicalTableRootMeta(headerPageID, storageVersion, storage.PageID(firstSpaceMapPageID), ownedDataPages, ownedSpaceMapPages)
		delete(headerMappings, table.TableID)
	}
	if len(headerMappings) != 0 {
		tableIDs := make([]int, 0, len(headerMappings))
		for tableID := range headerMappings {
			tableIDs = append(tableIDs, int(tableID))
		}
		sort.Ints(tableIDs)
		tableID := uint32(tableIDs[0])
		return wrapStorageError(newPhysicalStorageDiagnosticError("corrupted header page", "orphan table-header mapping for table id %d points to page %d", tableID, headerMappings[tableID]))
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
		_, rows, err := loadCommittedTableRowsAndLocators(pool, table)
		if err != nil {
			return err
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

// readCommittedPageData is the reader-side page helper. It never resolves
// writer-private frames.
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
	if locator.PageID == 0 {
		return nil, newStorageError("corrupted table page")
	}
	if table.FirstSpaceMapPageID() == 0 {
		return nil, newStorageError("corrupted header page")
	}

	dataPageIDs, err := committedTableDataPageIDs(db.pool, table)
	if err != nil {
		return nil, err
	}
	locatorPageID := storage.PageID(locator.PageID)
	ownedPage := false
	for _, pageID := range dataPageIDs {
		if pageID == locatorPageID {
			ownedPage = true
			break
		}
	}
	if !ownedPage {
		return nil, newStorageError("corrupted table page")
	}

	pageData, err := readCommittedPageData(db.pool, locatorPageID)
	if err != nil {
		return nil, wrapStorageError(err)
	}
	if pageData == nil {
		return nil, newStorageError("corrupted table page")
	}
	if err := storage.ValidateOwnedDataPage(pageData, table.TableID); err != nil {
		return nil, wrapStorageError(err)
	}

	row, err := storage.ReadRowByLocatorFromTablePageData(pageData, locator, storageColumnTypes(table.Columns))
	if err != nil {
		return nil, wrapStorageError(err)
	}
	return append([]parser.Value(nil), parserValuesFromStorage(row)...), nil
}

func loadCommittedTableRowsAndLocators(pool *bufferpool.BufferPool, table *executor.Table) ([]storage.RowLocator, [][]parser.Value, error) {
	if pool == nil || table == nil {
		return nil, nil, ErrInvalidArgument
	}
	if table.FirstSpaceMapPageID() == 0 {
		if table.PersistedRowCount() == 0 {
			return nil, nil, nil
		}
		return nil, nil, newStorageError("corrupted header page")
	}

	_, dataPageIDs, err := committedTablePhysicalStorageInventory(pool, table)
	if err != nil {
		return nil, nil, err
	}
	locators := make([]storage.RowLocator, 0)
	rows := make([][]parser.Value, 0)
	for _, pageID := range dataPageIDs {
		pageData, err := readCommittedPageData(pool, pageID)
		if err != nil {
			return nil, nil, wrapStorageError(err)
		}
		if pageData == nil {
			return nil, nil, newStorageError("corrupted table page")
		}
		if err := storage.ValidateOwnedDataPage(pageData, table.TableID); err != nil {
			return nil, nil, wrapStorageError(err)
		}
		pageLocators, pageRows, err := storage.ReadSlottedRowsWithLocators(pageData, uint32(pageID), storageColumnTypes(table.Columns))
		if err != nil {
			return nil, nil, wrapStorageError(err)
		}
		locators = append(locators, pageLocators...)
		rows = append(rows, parserRowsFromStorage(pageRows)...)
	}
	if uint32(len(rows)) != table.PersistedRowCount() {
		return nil, nil, newStorageError("row count mismatch")
	}
	return locators, rows, nil
}

func committedTableDataPageIDs(pool *bufferpool.BufferPool, table *executor.Table) ([]storage.PageID, error) {
	_, dataPageIDs, err := committedTablePhysicalStorageInventory(pool, table)
	return dataPageIDs, err
}

func committedTablePhysicalStorageInventory(pool *bufferpool.BufferPool, table *executor.Table) ([]storage.PageID, []storage.PageID, error) {
	if table == nil {
		return nil, nil, ErrInvalidArgument
	}
	if table.FirstSpaceMapPageID() == 0 {
		return nil, nil, nil
	}
	if pool == nil {
		return nil, nil, newTablePhysicalStorageError(table, "corrupted space map page", "buffer pool unavailable for physical inventory")
	}

	seenSpaceMaps := make(map[storage.PageID]struct{})
	spaceMapPageIDs := make([]storage.PageID, 0, table.OwnedSpaceMapPageCount())
	dataPageIDs := make([]storage.PageID, 0, table.OwnedDataPageCount())
	seenDataPages := make(map[storage.PageID]struct{}, table.OwnedDataPageCount())
	nextSpaceMapID := table.FirstSpaceMapPageID()
	for nextSpaceMapID != 0 {
		if _, exists := seenSpaceMaps[nextSpaceMapID]; exists {
			return nil, nil, newTablePhysicalStorageError(table, "corrupted space map page", "space-map chain revisits page %d", nextSpaceMapID)
		}
		seenSpaceMaps[nextSpaceMapID] = struct{}{}
		spaceMapPageIDs = append(spaceMapPageIDs, nextSpaceMapID)

		pageData, err := readCommittedPageData(pool, nextSpaceMapID)
		if err != nil {
			return nil, nil, wrapStorageError(err)
		}
		if pageData == nil {
			return nil, nil, newTablePhysicalStorageError(table, "corrupted space map page", "space-map page %d is missing", nextSpaceMapID)
		}
		if err := storage.ValidateSpaceMapPage(pageData); err != nil {
			return nil, nil, wrapStorageError(newTablePhysicalStorageError(table, "corrupted space map page", "space-map page %d is invalid", nextSpaceMapID))
		}
		owningTableID, err := storage.SpaceMapOwningTableID(pageData)
		if err != nil {
			return nil, nil, wrapStorageError(newTablePhysicalStorageError(table, "corrupted space map page", "space-map page %d has unreadable owning table id", nextSpaceMapID))
		}
		if owningTableID != table.TableID {
			return nil, nil, newTablePhysicalStorageError(table, "corrupted space map page", "space-map page %d claims table id %d", nextSpaceMapID, owningTableID)
		}
		entryCount, err := storage.SpaceMapEntryCount(pageData)
		if err != nil {
			return nil, nil, wrapStorageError(err)
		}
		for entryID := 0; entryID < entryCount; entryID++ {
			entry, err := storage.SpaceMapPageEntry(pageData, entryID)
			if err != nil {
				return nil, nil, wrapStorageError(err)
			}
			if _, exists := seenDataPages[entry.DataPageID]; exists {
				return nil, nil, newTablePhysicalStorageError(table, "corrupted space map page", "duplicate data page %d in space-map inventory", entry.DataPageID)
			}
			dataPageData, err := readCommittedPageData(pool, entry.DataPageID)
			if err != nil {
				return nil, nil, wrapStorageError(err)
			}
			if dataPageData == nil {
				return nil, nil, newTablePhysicalStorageError(table, "corrupted table page", "space-map inventory references missing data page %d", entry.DataPageID)
			}
			if err := storage.ValidateOwnedDataPage(dataPageData, table.TableID); err != nil {
				return nil, nil, wrapStorageError(newTablePhysicalStorageError(table, "corrupted table page", "data page %d is invalid or has the wrong owning table id", entry.DataPageID))
			}
			seenDataPages[entry.DataPageID] = struct{}{}
			dataPageIDs = append(dataPageIDs, entry.DataPageID)
		}
		nextPageID, err := storage.SpaceMapNextPageID(pageData)
		if err != nil {
			return nil, nil, wrapStorageError(err)
		}
		nextSpaceMapID = storage.PageID(nextPageID)
	}
	if uint32(len(spaceMapPageIDs)) != table.OwnedSpaceMapPageCount() {
		return nil, nil, newTablePhysicalStorageError(table, "corrupted header page", "owned space-map page count mismatch: header=%d inventory=%d", table.OwnedSpaceMapPageCount(), len(spaceMapPageIDs))
	}
	if uint32(len(dataPageIDs)) != table.OwnedDataPageCount() {
		return nil, nil, newTablePhysicalStorageError(table, "corrupted header page", "owned data page count mismatch: header=%d inventory=%d", table.OwnedDataPageCount(), len(dataPageIDs))
	}
	return spaceMapPageIDs, dataPageIDs, nil
}

func validateCommittedPhysicalTableStorage(pool *bufferpool.BufferPool, pager *storage.Pager, tables map[string]*executor.Table, freeListHead uint32) error {
	if pool == nil || pager == nil {
		return ErrInvalidArgument
	}

	freePages, err := committedFreePageSet(pool, storage.PageID(freeListHead))
	if err != nil {
		return err
	}

	headerOwners := make(map[storage.PageID]uint32)
	spaceMapOwners := make(map[storage.PageID]uint32)
	dataPageOwners := make(map[storage.PageID]uint32)
	legacyRootPages := make(map[storage.PageID]struct{})

	for _, table := range tables {
		if table == nil || table.TableID == 0 {
			continue
		}
		if table.RootPageID() != 0 {
			legacyRootPages[table.RootPageID()] = struct{}{}
		}
		if table.TableHeaderPageID() == 0 {
			return wrapStorageError(newTablePhysicalStorageError(table, "corrupted header page", "missing table-header root metadata"))
		}
		if _, free := freePages[table.TableHeaderPageID()]; free {
			return wrapStorageError(newTablePhysicalStorageError(table, "corrupted header page", "header page %d is also present in the free list", table.TableHeaderPageID()))
		}
		if existingOwner, exists := headerOwners[table.TableHeaderPageID()]; exists && existingOwner != table.TableID {
			return wrapStorageError(newTablePhysicalStorageError(table, "corrupted header page", "header page %d is already owned by table id %d", table.TableHeaderPageID(), existingOwner))
		}
		headerOwners[table.TableHeaderPageID()] = table.TableID

		headerPageData, err := readCommittedPageData(pool, table.TableHeaderPageID())
		if err != nil {
			return wrapStorageError(err)
		}
		if err := storage.ValidateTableHeaderPage(headerPageData); err != nil {
			return wrapStorageError(newTablePhysicalStorageError(table, "corrupted header page", "header page %d is invalid", table.TableHeaderPageID()))
		}
		headerTableID, err := storage.TableHeaderTableID(headerPageData)
		if err != nil {
			return wrapStorageError(newTablePhysicalStorageError(table, "corrupted header page", "header page %d has unreadable owning table id", table.TableHeaderPageID()))
		}
		if headerTableID != table.TableID {
			return wrapStorageError(newTablePhysicalStorageError(table, "corrupted header page", "header page %d claims table id %d", table.TableHeaderPageID(), headerTableID))
		}

		firstSpaceMapPageID := table.FirstSpaceMapPageID()
		ownedDataPages := table.OwnedDataPageCount()
		ownedSpaceMapPages := table.OwnedSpaceMapPageCount()
		if firstSpaceMapPageID == 0 {
			if ownedDataPages != 0 || ownedSpaceMapPages != 0 {
				return wrapStorageError(newTablePhysicalStorageError(table, "corrupted header page", "header reports owned pages without a first space-map root"))
			}
		} else if ownedSpaceMapPages == 0 {
			return wrapStorageError(newTablePhysicalStorageError(table, "corrupted header page", "header references first space-map page %d but reports zero owned space-map pages", firstSpaceMapPageID))
		}

		spaceMapPageIDs, dataPageIDs, err := committedTablePhysicalStorageInventory(pool, table)
		if err != nil {
			return err
		}
		for _, pageID := range spaceMapPageIDs {
			if _, free := freePages[pageID]; free {
				return wrapStorageError(newTablePhysicalStorageError(table, "corrupted space map page", "space-map page %d is also present in the free list", pageID))
			}
			if existingOwner, exists := spaceMapOwners[pageID]; exists && existingOwner != table.TableID {
				return wrapStorageError(newTablePhysicalStorageError(table, "corrupted space map page", "space-map page %d is already owned by table id %d", pageID, existingOwner))
			}
			spaceMapOwners[pageID] = table.TableID
		}
		for _, pageID := range dataPageIDs {
			if _, free := freePages[pageID]; free {
				return wrapStorageError(newTablePhysicalStorageError(table, "corrupted table page", "data page %d is also present in the free list", pageID))
			}
			if existingOwner, exists := dataPageOwners[pageID]; exists && existingOwner != table.TableID {
				return wrapStorageError(newTablePhysicalStorageError(table, "corrupted table page", "data page %d is already owned by table id %d", pageID, existingOwner))
			}
			dataPageOwners[pageID] = table.TableID
		}
	}

	for pageID := storage.PageID(0); pageID < pager.NextPageID(); pageID++ {
		pageData, err := readCommittedPageData(pool, pageID)
		if err != nil {
			return wrapStorageError(err)
		}
		if pageData == nil {
			return wrapStorageError(newStorageError("corrupted page header"))
		}
		if err := storage.ValidatePageImage(pageData); err != nil {
			if _, legacyRoot := legacyRootPages[pageID]; legacyRoot {
				continue
			}
			return wrapStorageError(err)
		}
		switch pageTypeOf(pageData) {
		case storage.PageTypeHeader:
			role, err := storage.HeaderPageRoleValue(pageData)
			if err != nil {
				return wrapStorageError(err)
			}
			if role != storage.HeaderPageRoleTable {
				continue
			}
			if pageID == 0 {
				return wrapStorageError(newPhysicalStorageDiagnosticError("corrupted header page", "page 0 cannot be a table-header page"))
			}
			if _, ok := headerOwners[pageID]; !ok {
				return wrapStorageError(newPhysicalStorageDiagnosticError("corrupted header page", "orphan table-header page %d is not referenced by any table", pageID))
			}
		case storage.PageTypeSpaceMap:
			if _, ok := spaceMapOwners[pageID]; !ok {
				return wrapStorageError(newPhysicalStorageDiagnosticError("corrupted space map page", "orphan space-map page %d is not referenced by any table", pageID))
			}
		case storage.PageTypeTable:
			owningTableID, err := storage.TablePageOwningTableID(pageData)
			if err != nil {
				return wrapStorageError(err)
			}
			if owningTableID == 0 {
				continue
			}
			if owner, ok := dataPageOwners[pageID]; !ok || owner != owningTableID {
				return wrapStorageError(newPhysicalStorageDiagnosticError("corrupted table page", "orphan owned data page %d claims table id %d", pageID, owningTableID))
			}
		}
	}
	return nil
}

func physicalTableDiagnosticLabel(table *executor.Table) string {
	if table == nil {
		return "table <nil>"
	}
	if table.Name == "" {
		return fmt.Sprintf("table id=%d", table.TableID)
	}
	return fmt.Sprintf("table %q (id=%d)", table.Name, table.TableID)
}

func newPhysicalStorageDiagnosticError(pageFamily string, format string, args ...any) error {
	return newStorageError(fmt.Sprintf("%s: %s", pageFamily, fmt.Sprintf(format, args...)))
}

func newTablePhysicalStorageError(table *executor.Table, pageFamily string, format string, args ...any) error {
	detailArgs := append([]any{physicalTableDiagnosticLabel(table)}, args...)
	return newPhysicalStorageDiagnosticError(pageFamily, "%s "+format, detailArgs...)
}

func committedFreePageSet(pool *bufferpool.BufferPool, head storage.PageID) (map[storage.PageID]struct{}, error) {
	freePages := make(map[storage.PageID]struct{})
	for head != 0 {
		if _, exists := freePages[head]; exists {
			return nil, wrapStorageError(newStorageError("corrupted free page"))
		}
		pageData, err := readCommittedPageData(pool, head)
		if err != nil {
			return nil, wrapStorageError(err)
		}
		if pageData == nil {
			return nil, wrapStorageError(newStorageError("corrupted free page"))
		}
		if err := storage.ValidatePageImage(pageData); err != nil {
			return nil, wrapStorageError(err)
		}
		if pageTypeOf(pageData) != storage.PageTypeFreePage {
			return nil, wrapStorageError(newStorageError("corrupted free page"))
		}
		next, err := storage.FreePageNext(pageData)
		if err != nil {
			return nil, wrapStorageError(err)
		}
		freePages[head] = struct{}{}
		head = storage.PageID(next)
	}
	return freePages, nil
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

func validateTables(tables map[string]*executor.Table) error {
	for _, table := range tables {
		if err := validateIndexConsistency(table); err != nil {
			return err
		}
		if err := validateTableConstraintConsistency(table); err != nil {
			return err
		}
	}
	if err := validateForeignKeyDependencies(tables); err != nil {
		return err
	}
	return nil
}

func validateIndexConsistency(table *executor.Table) error {
	if table == nil {
		return nil
	}

	seenIndexNames := make(map[string]struct{}, len(table.IndexDefs))
	for _, indexDef := range table.IndexDefs {
		if indexDef.Name == "" {
			return newExecError("index/table mismatch")
		}
		if _, exists := seenIndexNames[indexDef.Name]; exists {
			return newExecError("index/table mismatch")
		}
		seenIndexNames[indexDef.Name] = struct{}{}
		if columnName, ok := simpleIndexColumn(indexDef); ok {
			for _, other := range table.IndexDefs {
				if other.Name == indexDef.Name {
					continue
				}
				if otherColumn, ok := simpleIndexColumn(other); ok && otherColumn == columnName {
					return newExecError("index/table mismatch")
				}
			}
		}
	}
	return nil
}

func validateTableConstraintConsistency(table *executor.Table) error {
	if table == nil {
		return nil
	}

	indexesByID := make(map[uint32]storage.CatalogIndex, len(table.IndexDefs))
	for _, indexDef := range table.IndexDefs {
		if indexDef.IndexID != 0 {
			indexesByID[indexDef.IndexID] = indexDef
		}
	}
	columnNames := make(map[string]struct{}, len(table.Columns))
	for _, column := range table.Columns {
		columnNames[column.Name] = struct{}{}
	}

	constraintNames := make(map[string]struct{}, 1+len(table.ForeignKeyDefs))
	if table.PrimaryKeyDef != nil {
		pk := table.PrimaryKeyDef
		if pk.Name == "" || pk.TableID == 0 || pk.TableID != table.TableID || len(pk.Columns) == 0 || pk.IndexID == 0 {
			return newExecError("constraint/table mismatch")
		}
		constraintNames[pk.Name] = struct{}{}
		if _, ok := indexesByID[pk.IndexID]; !ok {
			return newExecError("constraint/table mismatch")
		}
		if hasDuplicateStrings(pk.Columns) || !allColumnsExist(pk.Columns, columnNames) {
			return newExecError("constraint/table mismatch")
		}
	}
	for _, fk := range table.ForeignKeyDefs {
		if fk.Name == "" || fk.ChildTableID == 0 || fk.ChildTableID != table.TableID || len(fk.ChildColumns) == 0 || fk.ParentTableID == 0 || len(fk.ParentColumns) == 0 || fk.ParentPrimaryKeyName == "" || fk.ChildIndexID == 0 {
			return newExecError("constraint/table mismatch")
		}
		if fk.OnDeleteAction != storage.CatalogForeignKeyDeleteActionRestrict && fk.OnDeleteAction != storage.CatalogForeignKeyDeleteActionCascade {
			return newExecError("constraint/table mismatch")
		}
		if _, exists := constraintNames[fk.Name]; exists {
			return newExecError("constraint/table mismatch")
		}
		constraintNames[fk.Name] = struct{}{}
		if _, ok := indexesByID[fk.ChildIndexID]; !ok {
			return newExecError("constraint/table mismatch")
		}
		if hasDuplicateStrings(fk.ChildColumns) || !allColumnsExist(fk.ChildColumns, columnNames) {
			return newExecError("constraint/table mismatch")
		}
		if hasDuplicateStrings(fk.ParentColumns) {
			return newExecError("constraint/table mismatch")
		}
	}
	return nil
}

func validateForeignKeyDependencies(tables map[string]*executor.Table) error {
	tablesByID := make(map[uint32]*executor.Table, len(tables))
	for _, table := range tables {
		if table == nil || table.TableID == 0 {
			continue
		}
		if _, exists := tablesByID[table.TableID]; exists {
			return newExecError("constraint/table mismatch")
		}
		tablesByID[table.TableID] = table
	}
	for _, table := range tables {
		if table == nil {
			continue
		}
		for _, fk := range table.ForeignKeyDefs {
			parentTable := tablesByID[fk.ParentTableID]
			if parentTable == nil || parentTable.PrimaryKeyDef == nil {
				return newExecError("constraint/table mismatch")
			}
			if parentTable.PrimaryKeyDef.Name != fk.ParentPrimaryKeyName {
				return newExecError("constraint/table mismatch")
			}
			parentColumns := make(map[string]struct{}, len(parentTable.Columns))
			for _, column := range parentTable.Columns {
				parentColumns[column.Name] = struct{}{}
			}
			if !allColumnsExist(fk.ParentColumns, parentColumns) {
				return newExecError("constraint/table mismatch")
			}
		}
	}
	return nil
}

func hasDuplicateStrings(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			return true
		}
		if _, exists := seen[value]; exists {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}

func allColumnsExist(values []string, columns map[string]struct{}) bool {
	for _, value := range values {
		if _, ok := columns[value]; !ok {
			return false
		}
	}
	return true
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
