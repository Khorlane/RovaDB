package rovadb

import (
	"context"
	"strings"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
)

// DB is the top-level handle for a RovaDB database.
type DB struct {
	path   string
	closed bool
	tables map[string]*executor.Table
}

// Open returns a database handle for the given path.
func Open(path string) (*DB, error) {
	if strings.TrimSpace(path) == "" {
		return nil, ErrInvalidArgument
	}

	return &DB{path: path}, nil
}

// Close releases database resources.
func (db *DB) Close() error {
	if db == nil {
		return nil
	}

	db.closed = true
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
	if _, ok := stmt.(*parser.CreateTableStmt); !ok {
		return Result{}, ErrNotImplemented
	}
	if err := executor.Execute(stmt, db.tables); err != nil {
		return Result{}, err
	}

	return Result{}, nil
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
		value, err := executor.Eval(sel.Expr)
		if err == nil {
			return &Rows{value: value}, nil
		}
	}

	return &Rows{err: ErrNotImplemented}, nil
}
