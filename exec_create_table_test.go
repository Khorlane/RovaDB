package rovadb

import (
	"context"
	"testing"
)

func TestExecCreateTable(t *testing.T) {
	db, err := Open("test.db")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	result, err := db.Exec(context.Background(), "CREATE TABLE users (id, name)")
	if err != nil {
		t.Fatalf("Exec() error = %v", err)
	}
	if result.RowsAffected() != 0 {
		t.Fatalf("Exec().RowsAffected() = %d, want 0", result.RowsAffected())
	}
	if db.tables == nil || db.tables["users"] == nil {
		t.Fatal("Exec() did not create users table")
	}
}

func TestExecCreateTableDuplicate(t *testing.T) {
	db, err := Open("test.db")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id, name)"); err != nil {
		t.Fatalf("first Exec() error = %v", err)
	}

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id, name)"); err == nil {
		t.Fatal("second Exec() error = nil, want duplicate table error")
	}
}
