package rovadb

import (
	"context"
	"errors"
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestQuerySelectLiteral(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		value int64
	}{
		{name: "select one", sql: "SELECT 1", value: 1},
		{name: "select forty two", sql: "SELECT 42", value: 42},
		{name: "select minus one", sql: "SELECT -1", value: -1},
		{name: "select minus forty two", sql: "SELECT -42", value: -42},
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

func TestQuerySelectStringLiteral(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		value string
	}{
		{name: "select hello", sql: "SELECT 'hello'", value: "hello"},
		{name: "select rovadb", sql: "SELECT 'RovaDB'", value: "RovaDB"},
		{name: "select empty string", sql: "SELECT ''", value: ""},
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

			var got string
			if err := rows.Scan(&got); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			if got != tc.value {
				t.Fatalf("Scan() got %q, want %q", got, tc.value)
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
		{name: "select plus one", sql: "SELECT +1"},
		{name: "select double quoted string", sql: `SELECT "hello"`},
		{name: "select string with spaces", sql: "SELECT 'hello world'"},
		{name: "select unterminated string", sql: "SELECT 'unterminated"},
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

func TestRowsScanStringIntoAny(t *testing.T) {
	db, err := Open("test.db")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query(context.Background(), "SELECT 'hello'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var got any
	if err := rows.Scan(&got); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if got != "hello" {
		t.Fatalf("Scan() got %#v, want %q", got, "hello")
	}
}

func TestRowsScanStringIntoInt64(t *testing.T) {
	db, err := Open("test.db")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query(context.Background(), "SELECT 'hello'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var got int64
	err = rows.Scan(&got)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Scan() error = %v, want ErrInvalidArgument", err)
	}
}

func TestParseSelectLiteralDirect(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		value any
		ok    bool
	}{
		{name: "select integer", sql: "SELECT 1", value: int64(1), ok: true},
		{name: "select negative integer", sql: "SELECT -1", value: int64(-1), ok: true},
		{name: "select negative forty two", sql: "SELECT -42", value: int64(-42), ok: true},
		{name: "select string", sql: "SELECT 'hello'", value: "hello", ok: true},
		{name: "select identifier", sql: "SELECT abc", ok: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parser.ParseSelectLiteral(tc.sql)
			if ok != tc.ok {
				t.Fatalf("ParseSelectLiteral() ok = %v, want %v", ok, tc.ok)
			}
			if !tc.ok {
				if got != nil {
					t.Fatalf("ParseSelectLiteral() = %#v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("ParseSelectLiteral() = nil, want value")
			}
			if got.Value != tc.value {
				t.Fatalf("ParseSelectLiteral().Value = %#v, want %#v", got.Value, tc.value)
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
