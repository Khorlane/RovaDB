package rovadb

import (
	"errors"
	"testing"
)

func TestAPIErrorContractQueryAndExecRouting(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("CREATE TABLE users (id INT)")
	if !errors.Is(err, ErrQueryRequiresSelect) {
		t.Fatalf("Query(non-select) error = %v, want ErrQueryRequiresSelect", err)
	}
	if rows != nil {
		t.Fatalf("Query(non-select) rows = %v, want nil", rows)
	}

	result, err := db.Exec("SELECT 1")
	if !errors.Is(err, ErrExecDisallowsSelect) {
		t.Fatalf("Exec(select) error = %v, want ErrExecDisallowsSelect", err)
	}
	if result != (Result{}) {
		t.Fatalf("Exec(select) result = %#v, want zero Result", result)
	}
}

func TestAPIErrorContractRowsStateErrors(t *testing.T) {
	rows := newRows(nil, [][]any{{1}})

	var i int
	if err := rows.Scan(&i); !errors.Is(err, ErrScanBeforeNext) {
		t.Fatalf("Scan() before Next = %v, want ErrScanBeforeNext", err)
	}

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	if err := rows.Scan(); !errors.Is(err, ErrScanMismatch) {
		t.Fatalf("Scan() count mismatch = %v, want ErrScanMismatch", err)
	}

	var s string
	if err := rows.Scan(&s); !errors.Is(err, ErrUnsupportedScanType) {
		t.Fatalf("Scan() unsupported type = %v, want ErrUnsupportedScanType", err)
	}

	if err := rows.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := rows.Scan(&i); !errors.Is(err, ErrRowsClosed) {
		t.Fatalf("Scan() after Close = %v, want ErrRowsClosed", err)
	}
}

func TestAPIErrorContractRowStrictness(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	row := db.QueryRow("SELECT id FROM users WHERE id = 999")
	var i int
	if err := row.Scan(&i); !errors.Is(err, ErrNoRows) {
		t.Fatalf("QueryRow(no rows).Scan() = %v, want ErrNoRows", err)
	}

	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	row = db.QueryRow("SELECT id FROM users ORDER BY id")
	if err := row.Scan(&i); !errors.Is(err, ErrMultipleRows) {
		t.Fatalf("QueryRow(multi-row).Scan() = %v, want ErrMultipleRows", err)
	}
}

func TestAPIErrorContractDeferredPassthrough(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	row := db.QueryRow("SELECT * FROM users WHERE id =")
	var i int
	err = row.Scan(&i)
	if err == nil || err.Error() != "parse: invalid where clause" {
		t.Fatalf("QueryRow(malformed).Scan() = %v, want %q", err, "parse: invalid where clause")
	}
	if errors.Is(err, ErrNoRows) {
		t.Fatal("QueryRow(malformed).Scan() matched ErrNoRows, want passthrough parse error")
	}

	rows, err := db.Query("SELEC 1")
	if err == nil || err.Error() != "parse: unsupported query form" {
		t.Fatalf("Query(malformed) error = %v, want %q", err, "parse: unsupported query form")
	}
	if rows != nil {
		t.Fatalf("Query(malformed) rows = %v, want nil", rows)
	}

	row = db.QueryRow("SELECT * FROM users")
	err = row.Scan(&i)
	if err == nil || err.Error() != "execution: table not found" {
		t.Fatalf("QueryRow(exec error).Scan() = %v, want %q", err, "execution: table not found")
	}
	if errors.Is(err, ErrNoRows) {
		t.Fatal("QueryRow(exec error).Scan() matched ErrNoRows, want passthrough execution error")
	}
}
