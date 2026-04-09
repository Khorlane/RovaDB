package rovadb_test

import (
	"testing"
)

func TestAlterTableAddColumnBasic(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"ALTER TABLE users ADD COLUMN age INT",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id, age FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id1 int
	var age1 any
	if err := rows.Scan(&id1, &age1); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if id1 != 1 || age1 != nil {
		t.Fatalf("first row = (%d, %#v), want (1, nil)", id1, age1)
	}
	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	var id2 int
	var age2 any
	if err := rows.Scan(&id2, &age2); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id2 != 2 || age2 != nil {
		t.Fatalf("second row = (%d, %#v), want (2, nil)", id2, age2)
	}
}

func TestAlterTableAddColumnInsertAndUpdate(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"ALTER TABLE users ADD COLUMN age INT",
		"UPDATE users SET age = 30 WHERE id = 1",
		"INSERT INTO users VALUES (2, 'bob', 40)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query("SELECT id, age FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id1 int
	var age1 int
	if err := rows.Scan(&id1, &age1); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if id1 != 1 || age1 != 30 {
		t.Fatalf("first row = (%d, %d), want (1, 30)", id1, age1)
	}
	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	var id2 int
	var age2 int
	if err := rows.Scan(&id2, &age2); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id2 != 2 || age2 != 40 {
		t.Fatalf("second row = (%d, %d), want (2, 40)", id2, age2)
	}
}

func TestAlterTableAddColumnReopenAndWhere(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"ALTER TABLE users ADD COLUMN age INT",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query("SELECT id FROM users WHERE age = NULL")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var id int
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if id != 1 {
		t.Fatalf("id = %d, want 1", id)
	}
}

func TestAlterTableUnsupportedForms(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"ALTER TABLE users DROP COLUMN age",
		"ALTER TABLE users ADD age INT",
	} {
		if _, err := db.Exec(sql); err == nil || err.Error() != "parse: unsupported alter table form" {
			t.Fatalf("Exec(%q) error = %v, want %q", sql, err, "parse: unsupported alter table form")
		}
	}
}
