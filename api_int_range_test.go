package rovadb

import "testing"

func TestExecRejectsOutOfRangeIntPlaceholderInsert(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	if _, err := db.Exec("INSERT INTO users VALUES (?)", 2147483648); err == nil {
		t.Fatal("Exec(insert out-of-range int placeholder) error = nil, want error")
	}
}

func TestExecRejectsOutOfRangeIntPlaceholderUpdate(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	if _, err := db.Exec("UPDATE users SET id = ? WHERE id = 1", -2147483649); err == nil {
		t.Fatal("Exec(update out-of-range int placeholder) error = nil, want error")
	}
}

func TestQueryRejectsOutOfRangeIntLiteral(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 2147483648")
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

func TestQueryAcceptsBoundaryIntLiteral(t *testing.T) {
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
