package rovadb

import (
	"strings"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

// Tx is the public transaction handle reserved for explicit transaction support.
type Tx struct {
	db       *DB
	finished bool
	tables   map[string]*executor.Table
}

// Begin starts an explicit transaction.
//
// Explicit transaction execution remains in-memory for now. Existing DB-level
// autocommit behavior remains unchanged until later slices wire commit and
// rollback through durable closeout semantics.
func (db *DB) Begin() (*Tx, error) {
	if db == nil {
		return nil, ErrInvalidArgument
	}
	if db.closed {
		return nil, ErrClosed
	}
	if db.tx != nil && !db.tx.finished {
		return nil, ErrTxnAlreadyActive
	}

	tables, err := db.snapshotTablesForTx()
	if err != nil {
		return nil, err
	}

	tx := &Tx{db: db, tables: tables}
	db.tx = tx
	return tx, nil
}

// Exec executes a non-SELECT statement within the explicit transaction snapshot.
func (tx *Tx) Exec(query string, args ...any) (Result, error) {
	if err := tx.ensureActive(); err != nil {
		return Result{}, err
	}
	if strings.TrimSpace(query) == "" {
		return Result{}, ErrInvalidArgument
	}
	if tx.tables == nil {
		tx.tables = make(map[string]*executor.Table)
	}

	stmt, err := parser.Parse(query)
	if err != nil {
		return Result{}, err
	}
	if err := parser.BindPlaceholders(stmt, args); err != nil {
		return Result{}, err
	}
	if err := rejectSystemTableMutationTables(tx.tables, stmt); err != nil {
		return Result{}, err
	}

	switch stmt := stmt.(type) {
	case *parser.SelectExpr:
		return Result{}, ErrExecDisallowsSelect
	case *parser.CreateTableStmt, *parser.InsertStmt, *parser.AlterTableAddColumnStmt, *parser.UpdateStmt, *parser.DeleteStmt:
		rowsAffected, err := executor.Execute(stmt, tx.tables)
		if err != nil {
			return Result{}, err
		}
		if err := validateTables(tx.tables, false); err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.CreateIndexStmt:
		rowsAffected, updated, err := executeCreateIndex(stmt, tx.tables)
		if err != nil {
			return Result{}, err
		}
		tx.tables = updated
		if err := validateTables(tx.tables, false); err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.DropIndexStmt:
		rowsAffected, updated, err := executeDropIndex(stmt, tx.tables)
		if err != nil {
			return Result{}, err
		}
		tx.tables = updated
		if err := validateTables(tx.tables, false); err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	case *parser.DropTableStmt:
		rowsAffected, updated, err := executeDropTable(stmt, tx.tables)
		if err != nil {
			return Result{}, err
		}
		tx.tables = updated
		if err := validateTables(tx.tables, false); err != nil {
			return Result{}, err
		}
		return Result{rowsAffected: rowsAffected}, nil
	default:
		return Result{}, newExecError("unsupported query form")
	}
}

// Query executes a SELECT statement within the explicit transaction snapshot.
func (tx *Tx) Query(query string, args ...any) (*Rows, error) {
	if err := tx.ensureActive(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(query) == "" {
		return nil, ErrInvalidArgument
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
		if err := validateTables(tx.tables, false); err != nil {
			return &Rows{err: err, idx: -1}, nil
		}
		plan, err := planner.PlanSelect(sel, plannerTableMetadata(tx.tables))
		if err != nil {
			return &Rows{err: err, idx: -1}, nil
		}
		execTables := cloneTables(tx.tables)
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

// QueryRow executes Query within the explicit transaction snapshot and wraps
// the resulting row set for deferred handling.
func (tx *Tx) QueryRow(query string, args ...any) *Row {
	rows, err := tx.Query(query, args...)
	if err != nil {
		rows = &Rows{
			idx: -1,
			err: err,
		}
	}
	return newRow(rows)
}

// Commit finalizes an explicit transaction and installs its logical state.
func (tx *Tx) Commit() error {
	if tx == nil || tx.db == nil || tx.finished {
		return ErrTxnCommitWithoutActive
	}
	tx.db.tables = cloneTables(tx.tables)
	tx.db.txView = true
	tx.finished = true
	if tx.db.tx == tx {
		tx.db.tx = nil
	}
	return nil
}

// Rollback abandons an explicit transaction and discards its logical state.
func (tx *Tx) Rollback() error {
	if tx == nil || tx.db == nil || tx.finished {
		return ErrTxnRollbackWithoutActive
	}
	tx.finished = true
	if tx.db.tx == tx {
		tx.db.tx = nil
	}
	return nil
}

func (tx *Tx) ensureActive() error {
	if tx == nil || tx.db == nil {
		return ErrInvalidArgument
	}
	if tx.finished {
		return ErrTxNotActive
	}
	return nil
}

func (db *DB) snapshotTablesForTx() (map[string]*executor.Table, error) {
	if db == nil {
		return nil, ErrInvalidArgument
	}
	snapshot := cloneTables(db.tables)
	if len(snapshot) == 0 {
		return snapshot, nil
	}

	names := make([]string, 0, len(snapshot))
	for name := range snapshot {
		names = append(names, name)
	}
	if err := db.loadRowsIntoTables(snapshot, names...); err != nil {
		return nil, err
	}
	return snapshot, nil
}
