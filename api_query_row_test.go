package rovadb

import (
	"errors"
	"testing"
)

func TestRowScanSuccessSingleRow(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	row := db.QueryRow("SELECT 1")

	var i int
	if err := row.Scan(&i); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if i != 1 {
		t.Fatalf("Scan() got %d, want 1", i)
	}
	if row.rows == nil || !row.rows.closed {
		t.Fatalf("row.rows.closed = %v, want true", row.rows != nil && row.rows.closed)
	}
}

func TestRowScanSuccessSingleRowMultipleColumns(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	row := db.QueryRow("SELECT id, name FROM users WHERE id = 1")
	var i int
	var s string
	if err := row.Scan(&i, &s); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if i != 1 || s != "alice" {
		t.Fatalf("Scan() = (%d, %q), want (1, %q)", i, s, "alice")
	}
}

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

func TestRowScanNoRows(t *testing.T) {
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
		t.Fatalf("Scan() error = %v, want ErrNoRows", err)
	}
	if row.rows == nil || !row.rows.closed {
		t.Fatalf("row.rows.closed = %v, want true", row.rows != nil && row.rows.closed)
	}
}

func TestRowScanMultipleRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	row := db.QueryRow("SELECT id FROM users ORDER BY id")
	var i int
	if err := row.Scan(&i); !errors.Is(err, ErrMultipleRows) {
		t.Fatalf("Scan() error = %v, want ErrMultipleRows", err)
	}
	if row.rows == nil || !row.rows.closed {
		t.Fatalf("row.rows.closed = %v, want true", row.rows != nil && row.rows.closed)
	}
}

func TestRowScanDeferredQueryErrorPassthrough(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	row := db.QueryRow("CREATE TABLE users (id INT)")
	var i int
	if err := row.Scan(&i); !errors.Is(err, ErrQueryRequiresSelect) {
		t.Fatalf("Scan() error = %v, want ErrQueryRequiresSelect", err)
	}

	row = db.QueryRow("SELECT * FROM users WHERE id =")
	if err := row.Scan(&i); err == nil || err.Error() != "parse: invalid where clause" {
		t.Fatalf("Scan() error = %v, want %q", err, "parse: invalid where clause")
	}
}

func TestRowScanClosedAndNilDBDeferredErrors(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	var i int
	row := db.QueryRow("SELECT 1")
	if err := row.Scan(&i); !errors.Is(err, ErrClosed) {
		t.Fatalf("Scan() error = %v, want ErrClosed", err)
	}

	var nilDB *DB
	row = nilDB.QueryRow("SELECT 1")
	if err := row.Scan(&i); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Scan() error = %v, want ErrInvalidArgument", err)
	}
}

func TestRowScanMismatchAndTypeMismatchPassthrough(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	row := db.QueryRow("SELECT id, name FROM users")
	var i int
	if err := row.Scan(&i); !errors.Is(err, ErrScanMismatch) {
		t.Fatalf("Scan() error = %v, want ErrScanMismatch", err)
	}

	row = db.QueryRow("SELECT name FROM users")
	if err := row.Scan(&i); !errors.Is(err, ErrUnsupportedScanType) {
		t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
	}
}

func TestRowScanNilReceiver(t *testing.T) {
	var row *Row
	var i int

	if err := row.Scan(&i); !errors.Is(err, ErrNoRows) {
		t.Fatalf("Scan() error = %v, want ErrNoRows", err)
	}
}

func TestQueryRowPlaceholderArgsWhereClause(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	row := db.QueryRow("SELECT name FROM users WHERE id = ?", 1)
	var name string
	if err := row.Scan(&name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if name != "alice" {
		t.Fatalf("name = %q, want %q", name, "alice")
	}
}

func TestQueryRowPlaceholderArgsReflectsUpdatedRow(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (?, ?)", 1, "steve"); err != nil {
		t.Fatalf("Exec(insert with placeholders) error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET name = ? WHERE id = ?", "sam", 1); err != nil {
		t.Fatalf("Exec(update with placeholders) error = %v", err)
	}

	row := db.QueryRow("SELECT name FROM users WHERE id = ?", 1)
	var name string
	if err := row.Scan(&name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if name != "sam" {
		t.Fatalf("name = %q, want %q", name, "sam")
	}
}

func TestQueryRowIndexedEqualityUsesDurableLookupPath(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	db.tables["users"].Indexes["name"].Entries = nil

	row := db.QueryRow("SELECT id FROM users WHERE name = 'bob'")
	var id int
	if err := row.Scan(&id); err != nil {
		t.Fatalf("QueryRow(indexed equality).Scan() error = %v", err)
	}
	if id != 2 {
		t.Fatalf("QueryRow(indexed equality).Scan() got %d, want 2", id)
	}
}

func TestQueryRowIndexedEqualityDuplicateMatchesRemainMultipleRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'alice')",
		"INSERT INTO users VALUES (3, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	db.tables["users"].Indexes["name"].Entries = nil

	row := db.QueryRow("SELECT id FROM users WHERE name = 'alice'")
	var id int
	if err := row.Scan(&id); !errors.Is(err, ErrMultipleRows) {
		t.Fatalf("QueryRow(duplicate indexed equality).Scan() = %v, want ErrMultipleRows", err)
	}
}

func TestQueryRowIndexedEqualityNoMatchRemainsNoRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	db.tables["users"].Indexes["name"].Entries = nil

	row := db.QueryRow("SELECT id FROM users WHERE name = 'zoe'")
	var id int
	if err := row.Scan(&id); !errors.Is(err, ErrNoRows) {
		t.Fatalf("QueryRow(no-match indexed equality).Scan() = %v, want ErrNoRows", err)
	}
}

func TestQueryRowNonIndexPathRemainsUnchanged(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	row := db.QueryRow("SELECT name FROM users WHERE id > 1")
	var name string
	if err := row.Scan(&name); !errors.Is(err, ErrMultipleRows) {
		t.Fatalf("QueryRow(non-index path).Scan() = %v, want ErrMultipleRows", err)
	}
}

func TestQueryRowPlaceholderArgsCountMismatchIsDeferred(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	row := db.QueryRow("SELECT 1 WHERE 1 = ?")
	if row == nil || row.rows == nil || row.rows.err == nil {
		t.Fatalf("row = %#v, want deferred bind error", row)
	}
}
