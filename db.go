package rovadb

import (
	"context"
	"strconv"
	"strings"
)

// DB is the top-level handle for a RovaDB database.
type DB struct {
	path   string
	closed bool
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

	return Result{}, ErrNotImplemented
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

	tokens := strings.Fields(strings.TrimSpace(sql))
	if len(tokens) == 2 && strings.EqualFold(tokens[0], "SELECT") {
		value, err := strconv.ParseInt(tokens[1], 10, 64)
		if err == nil {
			return &Rows{value: value}, nil
		}
	}

	return &Rows{err: ErrNotImplemented}, nil
}
