package rovadb

import (
	"strings"
	"testing"
)

func TestQueryRowReturnsBoundaryIntResult(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	var got int
	if err := db.QueryRow("SELECT 2147483647").Scan(&got); err != nil {
		t.Fatalf("QueryRow().Scan() error = %v", err)
	}
	if got != 2147483647 {
		t.Fatalf("QueryRow().Scan() got %d, want 2147483647", got)
	}
}

func TestQueryIntArithmeticOverflowIsDeferred(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 2147483647 + 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: integer out of range" {
		t.Fatalf("Err() = %v, want %q", rows.Err(), "execution: integer out of range")
	}
}

func TestQueryAbsOverflowIsDeferred(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (-2147483648)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT ABS(id) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: integer out of range" {
		t.Fatalf("Err() = %v, want %q", rows.Err(), "execution: integer out of range")
	}
}

func TestUpdateIntArithmeticOverflowDoesNotChangeState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (2147483647)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	if _, err := db.Exec("UPDATE users SET id = id + 1 WHERE id = 2147483647"); err == nil || !strings.Contains(err.Error(), "integer out of range") {
		t.Fatalf("Exec(update overflow) error = %v, want integer out of range", err)
	}

	assertSelectIntRows(t, db, "SELECT id FROM users", 2147483647)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT id FROM users", 2147483647)
}
