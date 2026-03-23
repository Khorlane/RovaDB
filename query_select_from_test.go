package rovadb

import (
	"context"
	"errors"
	"testing"
)

func TestQuerySelectFromTable(t *testing.T) {
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
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (2, 'sam')"); err != nil {
		t.Fatalf("Exec(insert 2) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT id, name FROM users")
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
	if id1 != 1 || name1 != "steve" {
		t.Fatalf("first row = (%d, %q), want (1, %q)", id1, name1, "steve")
	}

	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	var id2 int64
	var name2 string
	if err := rows.Scan(&id2, &name2); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id2 != 2 || name2 != "sam" {
		t.Fatalf("second row = (%d, %q), want (2, %q)", id2, name2, "sam")
	}

	if rows.Next() {
		t.Fatal("Next() third = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func TestQuerySelectAllFromTable(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if got := rows.Columns(); len(got) != 2 || got[0] != "id" || got[1] != "name" {
		t.Fatalf("Columns() = %#v, want [id name]", got)
	}

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var id int64
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if id != 1 || name != "steve" {
		t.Fatalf("row = (%d, %q), want (1, %q)", id, name, "steve")
	}
}

func TestQuerySelectInvalidColumn(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT email FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil {
		t.Fatal("Err() = nil, want column error")
	}
}

func TestQuerySelectFromEmptyTable(t *testing.T) {
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
	if rows.Err() != nil {
		t.Fatalf("Err() = %v, want nil", rows.Err())
	}
}

func TestQuerySelectSubsetOrder(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT name, id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if got := rows.Columns(); len(got) != 2 || got[0] != "name" || got[1] != "id" {
		t.Fatalf("Columns() = %#v, want [name id]", got)
	}

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var name string
	var id int64
	if err := rows.Scan(&name, &id); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if name != "steve" || id != 1 {
		t.Fatalf("row = (%q, %d), want (%q, %d)", name, id, "steve", 1)
	}
}

func TestQuerySelectSingleProjectedColumn(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if got := rows.Columns(); len(got) != 1 || got[0] != "id" {
		t.Fatalf("Columns() = %#v, want [id]", got)
	}
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var id int64
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if id != 1 {
		t.Fatalf("id = %d, want 1", id)
	}
}

func TestQuerySelectMissingTable(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query(context.Background(), "SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil {
		t.Fatal("Err() = nil, want missing table error")
	}
}

func TestQuerySelectWrongScanShape(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var id int64
	err = rows.Scan(&id)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Scan() error = %v, want ErrInvalidArgument", err)
	}
}

func TestQuerySelectWhereIntEquality(t *testing.T) {
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

	rows, err := db.Query(context.Background(), "SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if name != "steve" {
		t.Fatalf("Scan() got %q, want %q", name, "steve")
	}
	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
}

func TestQuerySelectWhereNumericComparisons(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query(context.Background(), "SELECT id FROM users WHERE id >= 2")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id int64
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if id != 2 {
		t.Fatalf("first id = %d, want 2", id)
	}
	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id != 3 {
		t.Fatalf("second id = %d, want 3", id)
	}
	if rows.Next() {
		t.Fatal("Next() third = true, want false")
	}
}

func TestQuerySelectWhereStringEquality(t *testing.T) {
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

	rows, err := db.Query(context.Background(), "SELECT name FROM users WHERE name = 'bob'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if name != "bob" {
		t.Fatalf("Scan() got %q, want %q", name, "bob")
	}
	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
}

func TestQuerySelectWhereStringNotEqual(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert 1) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (2, 'bob')"); err != nil {
		t.Fatalf("Exec(insert 2) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT name FROM users WHERE name != 'bob'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if name != "alice" {
		t.Fatalf("Scan() got %q, want %q", name, "alice")
	}
	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
}

func TestQuerySelectWhereTypeMismatch(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT * FROM users WHERE id = 'abc'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil {
		t.Fatal("Err() = nil, want type mismatch error")
	}
}

func TestQuerySelectWhereAndConditions(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
		"INSERT INTO users VALUES (4, 'dina')",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query(context.Background(), "SELECT id FROM users WHERE id > 1 AND id < 4")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id int64
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if id != 2 {
		t.Fatalf("first id = %d, want 2", id)
	}
	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id != 3 {
		t.Fatalf("second id = %d, want 3", id)
	}
	if rows.Next() {
		t.Fatal("Next() third = true, want false")
	}
}

func TestQuerySelectWhereOrConditions(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query(context.Background(), "SELECT id FROM users WHERE id = 1 OR id = 3")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 1, 3)
}

func TestQuerySelectWhereOrNoMatches(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT * FROM users WHERE id = 2 OR name = 'bob'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() != nil {
		t.Fatalf("Err() = %v, want nil", rows.Err())
	}
}

func TestQuerySelectWhereLeftToRightWithoutPrecedence(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (1, 'bob')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query(context.Background(), "SELECT name FROM users WHERE id = 1 OR id = 2 AND name = 'bob'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsStringSequence(t, rows, "bob", "bob")
}

func TestQuerySelectMalformedWhereBooleanChain(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query(context.Background(), "SELECT * FROM users WHERE id = 1 OR")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if !errors.Is(rows.Err(), ErrNotImplemented) {
		t.Fatalf("Err() = %v, want ErrNotImplemented", rows.Err())
	}
}

func TestQuerySelectOrderByIntAsc(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (3, 'cara')",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query(context.Background(), "SELECT id FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 1, 2, 3)
}

func TestQuerySelectOrderByIntDesc(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (3, 'cara')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query(context.Background(), "SELECT id FROM users ORDER BY id DESC")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 3, 2, 1)
}

func TestQuerySelectOrderByStringAsc(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query(context.Background(), "SELECT name FROM users ORDER BY name ASC")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsStringSequence(t, rows, "alice", "bob", "cara")
}

func TestQuerySelectOrderByStringDesc(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query(context.Background(), "SELECT name FROM users ORDER BY name DESC")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsStringSequence(t, rows, "cara", "bob", "alice")
}

func TestQuerySelectOrderByWithWhere(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (3, 'cara')",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query(context.Background(), "SELECT name FROM users WHERE id > 1 ORDER BY id DESC")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsStringSequence(t, rows, "cara", "bob")
}

func TestQuerySelectOrderByUnknownColumn(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT * FROM users ORDER BY age")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil {
		t.Fatal("Err() = nil, want order by column error")
	}
}

func assertRowsIntSequence(t *testing.T, rows *Rows, want ...int64) {
	t.Helper()

	for i, wantValue := range want {
		if !rows.Next() {
			t.Fatalf("Next() row %d = false, want true", i)
		}
		var got int64
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan() row %d error = %v", i, err)
		}
		if got != wantValue {
			t.Fatalf("row %d = %d, want %d", i, got, wantValue)
		}
	}
	if rows.Next() {
		t.Fatal("Next() after expected rows = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func assertRowsStringSequence(t *testing.T, rows *Rows, want ...string) {
	t.Helper()

	for i, wantValue := range want {
		if !rows.Next() {
			t.Fatalf("Next() row %d = false, want true", i)
		}
		var got string
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan() row %d error = %v", i, err)
		}
		if got != wantValue {
			t.Fatalf("row %d = %q, want %q", i, got, wantValue)
		}
	}
	if rows.Next() {
		t.Fatal("Next() after expected rows = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func TestQuerySelectWhereNoMatches(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT * FROM users WHERE name = 'bob'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() != nil {
		t.Fatalf("Err() = %v, want nil", rows.Err())
	}
}

func TestQuerySelectNullRoundTrip(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, NULL)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var got any
	if err := rows.Scan(&got); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if got != nil {
		t.Fatalf("Scan() got %#v, want nil", got)
	}
}

func TestQuerySelectWhereEqualsNull(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, NULL)",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query(context.Background(), "SELECT id FROM users WHERE name = NULL")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 1)
}

func TestQuerySelectWhereNotEqualsNull(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, NULL)",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query(context.Background(), "SELECT id FROM users WHERE name != NULL")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 2)
}

func TestQuerySelectWhereLessThanNullErrors(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, NULL)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT * FROM users WHERE name < NULL")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil {
		t.Fatal("Err() = nil, want comparison error")
	}
}

func TestQueryUpdateSetsNull(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "UPDATE users SET name = NULL WHERE id = 1"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var got any
	if err := rows.Scan(&got); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if got != nil {
		t.Fatalf("Scan() got %#v, want nil", got)
	}
}

func TestQueryNullPersistsAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, NULL)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query(context.Background(), "SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var got any
	if err := rows.Scan(&got); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if got != nil {
		t.Fatalf("Scan() got %#v, want nil", got)
	}
}
