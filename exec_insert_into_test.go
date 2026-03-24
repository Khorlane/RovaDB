package rovadb

import (
	"errors"
	"testing"
)

func TestExecInsertInto(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	table := db.tables["users"]
	if table == nil || len(table.Rows) != 1 {
		t.Fatalf("users rows = %#v, want one row", table)
	}
}

func TestExecInsertIntoWithColumnListReordered(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users (name, id) VALUES ('steve', 1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var id int
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if id != 1 || name != "steve" {
		t.Fatalf("row = (%d, %q), want (1, %q)", id, name, "steve")
	}
}

func TestExecInsertIntoWrongType(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES ('steve', 'bob')"); err == nil {
		t.Fatal("Exec(insert) error = nil, want type error")
	}
}

func TestExecInsertIntoBoolWrongTypeRejected(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE flags (flag BOOL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	tests := []string{
		"INSERT INTO flags VALUES (1)",
		"INSERT INTO flags VALUES (0)",
		"INSERT INTO flags VALUES ('true')",
		"INSERT INTO flags VALUES ('false')",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := db.Exec(sql)
			var dbErr *DBError
			if !errors.As(err, &dbErr) || dbErr.Kind != ErrExec {
				t.Fatalf("Exec(%q) error = %v, want exec-type mismatch error", sql, err)
			}
		})
	}
}
