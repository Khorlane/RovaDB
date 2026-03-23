package rovadb

import (
	"context"
	"testing"
)

func TestExecDeleteFromWhere(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert 1) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (2, 'bob')"); err != nil {
		t.Fatalf("Exec(insert 2) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (3, 'sam')"); err != nil {
		t.Fatalf("Exec(insert 3) error = %v", err)
	}

	result, err := db.Exec(context.Background(), "DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	if result.RowsAffected() != 1 {
		t.Fatalf("Exec(delete).RowsAffected() = %d, want 1", result.RowsAffected())
	}

	rows, err := db.Query(context.Background(), "SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id1 int64
	var name1 string
	if err := rows.Scan(&id1, &name1); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if id1 != 2 || name1 != "bob" {
		t.Fatalf("first row = (%d, %q), want (2, %q)", id1, name1, "bob")
	}

	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	var id2 int64
	var name2 string
	if err := rows.Scan(&id2, &name2); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id2 != 3 || name2 != "sam" {
		t.Fatalf("second row = (%d, %q), want (3, %q)", id2, name2, "sam")
	}

	if rows.Next() {
		t.Fatal("Next() third = true, want false")
	}
}
