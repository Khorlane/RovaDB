package rovadb

import (
	"testing"
)

func TestInsertSelectAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert 1) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (2, 'bob')"); err != nil {
		t.Fatalf("Exec(insert 2) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id1 int
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
	var id2 int
	var name2 string
	if err := rows.Scan(&id2, &name2); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id2 != 2 || name2 != "bob" {
		t.Fatalf("second row = (%d, %q), want (2, %q)", id2, name2, "bob")
	}

	if rows.Next() {
		t.Fatal("Next() third = true, want false")
	}
}

func TestMultipleTablesReloadRowsOnOpen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert users) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO teams VALUES (10, 'ravens')"); err != nil {
		t.Fatalf("Exec(insert teams) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	userRows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query(users) error = %v", err)
	}
	defer userRows.Close()

	if !userRows.Next() {
		t.Fatal("users Next() = false, want true")
	}
	var userID int
	var userName string
	if err := userRows.Scan(&userID, &userName); err != nil {
		t.Fatalf("users Scan() error = %v", err)
	}
	if userID != 1 || userName != "steve" {
		t.Fatalf("users row = (%d, %q), want (1, %q)", userID, userName, "steve")
	}

	teamRows, err := db.Query("SELECT * FROM teams")
	if err != nil {
		t.Fatalf("Query(teams) error = %v", err)
	}
	defer teamRows.Close()

	if !teamRows.Next() {
		t.Fatal("teams Next() = false, want true")
	}
	var teamID int
	var teamName string
	if err := teamRows.Scan(&teamID, &teamName); err != nil {
		t.Fatalf("teams Scan() error = %v", err)
	}
	if teamID != 10 || teamName != "ravens" {
		t.Fatalf("teams row = (%d, %q), want (10, %q)", teamID, teamName, "ravens")
	}
}

func TestBoolRowsRoundTripAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE flags (id INT, active BOOL, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO flags VALUES (1, TRUE, 'alice')"); err != nil {
		t.Fatalf("Exec(insert true) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO flags VALUES (2, FALSE, 'bob')"); err != nil {
		t.Fatalf("Exec(insert false) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO flags VALUES (3, NULL, 'cara')"); err != nil {
		t.Fatalf("Exec(insert null) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT * FROM flags ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id1 int
	var active1 bool
	var name1 string
	if err := rows.Scan(&id1, &active1, &name1); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if id1 != 1 || active1 != true || name1 != "alice" {
		t.Fatalf("first row = (%d, %v, %q), want (1, true, %q)", id1, active1, name1, "alice")
	}

	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	var id2 int
	var active2 bool
	var name2 string
	if err := rows.Scan(&id2, &active2, &name2); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id2 != 2 || active2 != false || name2 != "bob" {
		t.Fatalf("second row = (%d, %v, %q), want (2, false, %q)", id2, active2, name2, "bob")
	}

	if !rows.Next() {
		t.Fatal("Next() third = false, want true")
	}
	var id3 int
	var active3 any
	var name3 string
	if err := rows.Scan(&id3, &active3, &name3); err != nil {
		t.Fatalf("Scan() third error = %v", err)
	}
	if id3 != 3 || active3 != nil || name3 != "cara" {
		t.Fatalf("third row = (%d, %#v, %q), want (3, nil, %q)", id3, active3, name3, "cara")
	}

	if rows.Next() {
		t.Fatal("Next() fourth = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}
