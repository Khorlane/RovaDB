package rovadb

import (
	"context"
	"errors"
	"testing"
)

func TestQuerySelectLiteral(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		value int64
	}{
		{name: "select one", sql: "SELECT 1", value: 1},
		{name: "select forty two", sql: "SELECT 42", value: 42},
		{name: "select trimmed mixed case", sql: " select 999 ", value: 999},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open("test.db")
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			rows, err := db.Query(context.Background(), tc.sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			if !rows.Next() {
				t.Fatal("Next() = false, want true")
			}

			var got int64
			if err := rows.Scan(&got); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			if got != tc.value {
				t.Fatalf("Scan() got %d, want %d", got, tc.value)
			}

			if rows.Next() {
				t.Fatal("Next() = true after first row, want false")
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("Err() = %v, want nil", err)
			}
		})
	}
}

func TestQueryUnsupportedLiteral(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "select identifier", sql: "SELECT abc"},
		{name: "extra token", sql: "SELECT 1 2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open("test.db")
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			rows, err := db.Query(context.Background(), tc.sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			if rows.Next() {
				t.Fatal("Next() = true, want false")
			}
			if !errors.Is(rows.Err(), ErrNotImplemented) {
				t.Fatalf("Err() = %v, want ErrNotImplemented", rows.Err())
			}
		})
	}
}

func TestQueryNilContext(t *testing.T) {
	db, err := Open("test.db")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	var nilCtx context.Context
	rows, err := db.Query(nilCtx, "SELECT 1")
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Query() error = %v, want ErrInvalidArgument", err)
	}
	if rows != nil {
		t.Fatalf("Query() rows = %v, want nil", rows)
	}
}

func TestQueryClosedDB(t *testing.T) {
	db, err := Open("test.db")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT 1")
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Query() error = %v, want ErrClosed", err)
	}
	if rows != nil {
		t.Fatalf("Query() rows = %v, want nil", rows)
	}
}
