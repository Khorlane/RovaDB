package rovadb

import (
	"errors"
	"testing"
)

func TestQuerySelectLiteral(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		value int
	}{
		{name: "select one", sql: "SELECT 1", value: 1},
		{name: "select forty two", sql: "SELECT 42", value: 42},
		{name: "select minus one", sql: "SELECT -1", value: -1},
		{name: "select minus forty two", sql: "SELECT -42", value: -42},
		{name: "select one plus two", sql: "SELECT 1+2", value: 3},
		{name: "select five minus three", sql: "SELECT 5-3", value: 2},
		{name: "select minus one plus two", sql: "SELECT -1+2", value: 1},
		{name: "select ten plus minus three", sql: "SELECT 10+-3", value: 7},
		{name: "select one plus two spaced", sql: "SELECT 1 + 2", value: 3},
		{name: "select five minus three spaced", sql: "SELECT 5 - 3", value: 2},
		{name: "select minus one plus two spaced", sql: "SELECT -1 + 2", value: 1},
		{name: "select ten plus minus three spaced", sql: "SELECT 10 + -3", value: 7},
		{name: "select parenthesized one plus two", sql: "SELECT (1+2)", value: 3},
		{name: "select parenthesized one plus two spaced", sql: "SELECT (1 + 2)", value: 3},
		{name: "select parenthesized minus one plus two", sql: "SELECT (-1+2)", value: 1},
		{name: "select trimmed mixed case", sql: " select 999 ", value: 999},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(testDBPath(t))
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			if !rows.Next() {
				t.Fatal("Next() = false, want true")
			}

			var got int
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
			db, err := Open(testDBPath(t))
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			rows, err := db.Query(tc.sql)
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

func TestQuerySelectBoolLiteral(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		value bool
	}{
		{name: "select true", sql: "SELECT TRUE", value: true},
		{name: "select false", sql: "SELECT FALSE", value: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(testDBPath(t))
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			if !rows.Next() {
				t.Fatal("Next() = false, want true")
			}

			var got bool
			if err := rows.Scan(&got); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			if got != tc.value {
				t.Fatalf("Scan() got %v, want %v", got, tc.value)
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

func TestQuerySelectRealLiteral(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		value float64
	}{
		{name: "select pi-ish", sql: "SELECT 3.14", value: 3.14},
		{name: "select negative real", sql: "SELECT -2.5", value: -2.5},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(testDBPath(t))
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			if !rows.Next() {
				t.Fatal("Next() = false, want true")
			}

			var got float64
			if err := rows.Scan(&got); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			if got != tc.value {
				t.Fatalf("Scan() got %v, want %v", got, tc.value)
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

func TestRowsScanBoolIntoAny(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT TRUE")
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
	if got != true {
		t.Fatalf("Scan() got %#v, want true", got)
	}
}

func TestRowsScanRealIntoAny(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 3.14")
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
	if got != 3.14 {
		t.Fatalf("Scan() got %#v, want 3.14", got)
	}
}

func TestQueryRowSelectRealLiteral(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	var got float64
	if err := db.QueryRow("SELECT 3.14").Scan(&got); err != nil {
		t.Fatalf("QueryRow().Scan() error = %v", err)
	}
	if got != 3.14 {
		t.Fatalf("QueryRow().Scan() got %v, want 3.14", got)
	}
}

func TestRowsScanRealIntoWrongDestination(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 3.14")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var got int
	err = rows.Scan(&got)
	if !errors.Is(err, ErrUnsupportedScanType) {
		t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
	}
}

func TestQuerySelectNullLiteralRegression(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT NULL")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "parse: unsupported query form" {
		t.Fatalf("Err() = %v, want %q", rows.Err(), "parse: unsupported query form")
	}
}

func TestQueryUnsupportedLiteral(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "select identifier", sql: "SELECT abc"},
		{name: "select plus one", sql: "SELECT +1"},
		{name: "select chained expression", sql: "SELECT 1+2+3"},
		{name: "select chained expression spaced", sql: "SELECT 1 + 2 + 3"},
		{name: "select incomplete expression", sql: "SELECT 1 +"},
		{name: "select missing left operand", sql: "SELECT + 1"},
		{name: "select string expression", sql: "SELECT 'a'+'b'"},
		{name: "select multiply expression spaced", sql: "SELECT 1 * 2"},
		{name: "select multiply expression", sql: "SELECT 1*2"},
		{name: "select nested parenthesized expression", sql: "SELECT ((1+2))"},
		{name: "select unterminated parenthesized expression", sql: "SELECT (1+2"},
		{name: "select trailing parenthesized expression", sql: "SELECT 1+2)"},
		{name: "select missing from table", sql: "SELECT id, name users"},
		{name: "select invalid from format", sql: "SELECT id name FROM users"},
		{name: "select double quoted string", sql: `SELECT "hello"`},
		{name: "select string with spaces", sql: "SELECT 'hello world'"},
		{name: "select unterminated string", sql: "SELECT 'unterminated"},
		{name: "extra token", sql: "SELECT 1 2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(testDBPath(t))
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			if rows.Next() {
				t.Fatal("Next() = true, want false")
			}
			if rows.Err() == nil || rows.Err().Error() != "parse: unsupported query form" {
				t.Fatalf("Err() = %v, want %q", rows.Err(), "parse: unsupported query form")
			}
		})
	}
}

func TestRowsScanStringIntoAny(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 'hello'")
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

func TestRowsScanStringIntoInt(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 'hello'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var got int
	err = rows.Scan(&got)
	if !errors.Is(err, ErrUnsupportedScanType) {
		t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
	}
}

func TestQueryEmptySQL(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query(" ")
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Query() error = %v, want ErrInvalidArgument", err)
	}
	if rows != nil {
		t.Fatalf("Query() rows = %v, want nil", rows)
	}
}

func TestQueryClosedDB(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rows, err := db.Query("SELECT 1")
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Query() error = %v, want ErrClosed", err)
	}
	if rows != nil {
		t.Fatalf("Query() rows = %v, want nil", rows)
	}
}
