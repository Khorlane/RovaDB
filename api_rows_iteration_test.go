package rovadb

import (
	"errors"
	"testing"
)

func TestRowsNextIteratesUntilExhausted(t *testing.T) {
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

	rows, err := db.Query("SELECT id FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if !rows.Next() {
		t.Fatal("first Next() = false, want true")
	}
	if rows.idx != 0 {
		t.Fatalf("rows.idx after first Next() = %d, want 0", rows.idx)
	}
	if !rows.Next() {
		t.Fatal("second Next() = false, want true")
	}
	if rows.idx != 1 {
		t.Fatalf("rows.idx after second Next() = %d, want 1", rows.idx)
	}
	if rows.Next() {
		t.Fatal("third Next() = true, want false")
	}
	if rows.idx != len(rows.data) {
		t.Fatalf("rows.idx after exhaustion = %d, want %d", rows.idx, len(rows.data))
	}
	if rows.Next() {
		t.Fatal("Next() after exhaustion = true, want false")
	}
	if rows.idx != len(rows.data) {
		t.Fatalf("rows.idx after repeated exhaustion = %d, want %d", rows.idx, len(rows.data))
	}
}

func TestRowsNextEmptyResultSet(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() != nil {
		t.Fatalf("Err() = %v, want nil", rows.Err())
	}
}

func TestRowsDeferredErrorBlocksIteration(t *testing.T) {
	wantErr := errors.New("boom")
	rows := &Rows{
		idx: -1,
		err: wantErr,
	}

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if !errors.Is(rows.Err(), wantErr) {
		t.Fatalf("Err() = %v, want %v", rows.Err(), wantErr)
	}
}

func TestRowsCloseLifecycle(t *testing.T) {
	rows := newRows(nil, [][]any{{1}})

	if err := rows.Close(); err != nil {
		t.Fatalf("first Close() error = %v, want nil", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want nil", err)
	}
	if rows.Next() {
		t.Fatal("Next() after Close() = true, want false")
	}
	if rows.Err() != nil {
		t.Fatalf("Err() after Close() = %v, want nil", rows.Err())
	}

	rows = &Rows{idx: -1, err: wantErrRowsClose}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close() with deferred error = %v, want nil", err)
	}
	if !errors.Is(rows.Err(), wantErrRowsClose) {
		t.Fatalf("Err() after Close() = %v, want %v", rows.Err(), wantErrRowsClose)
	}
}

var wantErrRowsClose = errors.New("deferred")

func TestRowsNilReceiverLifecycle(t *testing.T) {
	var rows *Rows

	if rows.Next() {
		t.Fatal("nil Rows Next() = true, want false")
	}
	if rows.Err() != nil {
		t.Fatalf("nil Rows Err() = %v, want nil", rows.Err())
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("nil Rows Close() = %v, want nil", err)
	}
}
