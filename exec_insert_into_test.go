package rovadb

import (
	"context"
	"testing"
)

func TestExecInsertInto(t *testing.T) {
	db, err := Open("test.db")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id, name)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	table := db.tables["users"]
	if table == nil || len(table.Rows) != 1 {
		t.Fatalf("users rows = %#v, want one row", table)
	}
}
