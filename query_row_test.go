package rovadb

import (
	"errors"
	"testing"
)

func TestQueryRowReturnsWrapperForLiteralSelect(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	row := db.QueryRow("SELECT 1")
	if row == nil {
		t.Fatal("QueryRow() = nil, want value")
	}
	if row.rows == nil {
		t.Fatal("QueryRow().rows = nil, want value")
	}
	if row.rows.err != nil {
		t.Fatalf("QueryRow().rows.err = %v, want nil", row.rows.err)
	}
	if row.rows.idx != -1 {
		t.Fatalf("QueryRow().rows.idx = %d, want -1", row.rows.idx)
	}
	if len(row.rows.data) != 1 || len(row.rows.data[0]) != 1 || row.rows.data[0][0] != 1 {
		t.Fatalf("QueryRow().rows.data = %#v, want [[1]]", row.rows.data)
	}
}

func TestQueryRowDefersNonSelectQueryError(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	row := db.QueryRow("CREATE TABLE users (id INT)")
	if row == nil {
		t.Fatal("QueryRow() = nil, want value")
	}
	if row.rows == nil {
		t.Fatal("QueryRow().rows = nil, want value")
	}
	if !errors.Is(row.rows.err, ErrQueryRequiresSelect) {
		t.Fatalf("QueryRow().rows.err = %v, want ErrQueryRequiresSelect", row.rows.err)
	}
	if row.rows.idx != -1 {
		t.Fatalf("QueryRow().rows.idx = %d, want -1", row.rows.idx)
	}
}

func TestQueryRowDefersMalformedQueryError(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	row := db.QueryRow("SELECT * FROM users WHERE id =")
	if row == nil {
		t.Fatal("QueryRow() = nil, want value")
	}
	if row.rows == nil {
		t.Fatal("QueryRow().rows = nil, want value")
	}
	if row.rows.err == nil || row.rows.err.Error() != "parse: invalid where clause" {
		t.Fatalf("QueryRow().rows.err = %v, want %q", row.rows.err, "parse: invalid where clause")
	}
}

func TestQueryRowDefersClosedDBError(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	row := db.QueryRow("SELECT 1")
	if row == nil {
		t.Fatal("QueryRow() = nil, want value")
	}
	if row.rows == nil {
		t.Fatal("QueryRow().rows = nil, want value")
	}
	if !errors.Is(row.rows.err, ErrClosed) {
		t.Fatalf("QueryRow().rows.err = %v, want ErrClosed", row.rows.err)
	}
}

func TestQueryRowDefersNilDBError(t *testing.T) {
	var db *DB

	row := db.QueryRow("SELECT 1")
	if row == nil {
		t.Fatal("QueryRow() = nil, want value")
	}
	if row.rows == nil {
		t.Fatal("QueryRow().rows = nil, want value")
	}
	if !errors.Is(row.rows.err, ErrInvalidArgument) {
		t.Fatalf("QueryRow().rows.err = %v, want ErrInvalidArgument", row.rows.err)
	}
}
