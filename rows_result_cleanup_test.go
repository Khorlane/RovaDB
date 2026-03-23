package rovadb

import (
	"context"
	"errors"
	"testing"
)

func TestRowsScanBeforeNext(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	var got int64
	err = rows.Scan(&got)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Scan() error = %v, want ErrInvalidArgument", err)
	}
}

func TestRowsScanAfterIterationEnds(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var got int64
	if err := rows.Scan(&got); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if rows.Next() {
		t.Fatal("Next() second = true, want false")
	}

	err = rows.Scan(&got)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Scan() after end error = %v, want ErrInvalidArgument", err)
	}
}

func TestResultRowsAffectedCleanup(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	insertResult, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, 'steve')")
	if err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if insertResult.RowsAffected() != 1 {
		t.Fatalf("Exec(insert).RowsAffected() = %d, want 1", insertResult.RowsAffected())
	}

	updateResult, err := db.Exec(context.Background(), "UPDATE users SET name = 'sam' WHERE id = 999")
	if err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if updateResult.RowsAffected() != 0 {
		t.Fatalf("Exec(update).RowsAffected() = %d, want 0", updateResult.RowsAffected())
	}

	deleteResult, err := db.Exec(context.Background(), "DELETE FROM users WHERE id = 999")
	if err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	if deleteResult.RowsAffected() != 0 {
		t.Fatalf("Exec(delete).RowsAffected() = %d, want 0", deleteResult.RowsAffected())
	}
}

func TestQueryZeroRowSelectCleanup(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}

	var id int64
	var name string
	err = rows.Scan(&id, &name)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Scan() error = %v, want ErrInvalidArgument", err)
	}
}
