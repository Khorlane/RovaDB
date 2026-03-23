package rovadb

import (
	"context"
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

	return &DB{path: path, file: file, pager: pager}, nil
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
