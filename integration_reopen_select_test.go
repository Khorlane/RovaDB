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

func TestBoolRowsUpdateRoundTripAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT, active BOOL)",
		"INSERT INTO users VALUES (1, 'alice', TRUE)",
		"INSERT INTO users VALUES (2, 'bob', FALSE)",
		"INSERT INTO users VALUES (3, 'cara', TRUE)",
		"UPDATE users SET active = FALSE WHERE id = 1",
		"UPDATE users SET active = TRUE WHERE id = 2",
		"UPDATE users SET active = NULL WHERE id = 3",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT id, name, active FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id1 int
	var name1 string
	var active1 bool
	if err := rows.Scan(&id1, &name1, &active1); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if id1 != 1 || name1 != "alice" || active1 != false {
		t.Fatalf("first row = (%d, %q, %v), want (1, %q, false)", id1, name1, active1, "alice")
	}

	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	var id2 int
	var name2 string
	var active2 bool
	if err := rows.Scan(&id2, &name2, &active2); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id2 != 2 || name2 != "bob" || active2 != true {
		t.Fatalf("second row = (%d, %q, %v), want (2, %q, true)", id2, name2, active2, "bob")
	}

	if !rows.Next() {
		t.Fatal("Next() third = false, want true")
	}
	var id3 int
	var name3 string
	var active3 any
	if err := rows.Scan(&id3, &name3, &active3); err != nil {
		t.Fatalf("Scan() third error = %v", err)
	}
	if id3 != 3 || name3 != "cara" || active3 != nil {
		t.Fatalf("third row = (%d, %q, %#v), want (3, %q, nil)", id3, name3, active3, "cara")
	}

	if rows.Next() {
		t.Fatal("Next() fourth = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func TestRealRowsRoundTripAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE measurements (id INT, x REAL, label TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO measurements VALUES (1, 0.0, 'zero')"); err != nil {
		t.Fatalf("Exec(insert zero) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO measurements VALUES (2, 3.14, 'pi')"); err != nil {
		t.Fatalf("Exec(insert pi) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO measurements VALUES (3, -2.5, 'neg')"); err != nil {
		t.Fatalf("Exec(insert neg) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO measurements VALUES (4, NULL, 'missing')"); err != nil {
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

	rows, err := db.Query("SELECT id, x, label FROM measurements ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id1 int
	var x1 float64
	var label1 string
	if err := rows.Scan(&id1, &x1, &label1); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if id1 != 1 || x1 != 0.0 || label1 != "zero" {
		t.Fatalf("first row = (%d, %v, %q), want (1, 0.0, %q)", id1, x1, label1, "zero")
	}

	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	var id2 int
	var x2 float64
	var label2 string
	if err := rows.Scan(&id2, &x2, &label2); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id2 != 2 || x2 != 3.14 || label2 != "pi" {
		t.Fatalf("second row = (%d, %v, %q), want (2, 3.14, %q)", id2, x2, label2, "pi")
	}

	if !rows.Next() {
		t.Fatal("Next() third = false, want true")
	}
	var id3 int
	var x3 float64
	var label3 string
	if err := rows.Scan(&id3, &x3, &label3); err != nil {
		t.Fatalf("Scan() third error = %v", err)
	}
	if id3 != 3 || x3 != -2.5 || label3 != "neg" {
		t.Fatalf("third row = (%d, %v, %q), want (3, -2.5, %q)", id3, x3, label3, "neg")
	}

	if !rows.Next() {
		t.Fatal("Next() fourth = false, want true")
	}
	var id4 int
	var x4 any
	var label4 string
	if err := rows.Scan(&id4, &x4, &label4); err != nil {
		t.Fatalf("Scan() fourth error = %v", err)
	}
	if id4 != 4 || x4 != nil || label4 != "missing" {
		t.Fatalf("fourth row = (%d, %#v, %q), want (4, nil, %q)", id4, x4, label4, "missing")
	}

	if rows.Next() {
		t.Fatal("Next() fifth = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func TestRealRowsUpdateDeleteRoundTripAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()
	for _, sql := range []string{
		"CREATE TABLE measurements (id INT, x REAL, label TEXT)",
		"INSERT INTO measurements VALUES (1, 0.0, 'zero')",
		"INSERT INTO measurements VALUES (2, 3.14, 'pi')",
		"INSERT INTO measurements VALUES (3, -2.5, 'neg')",
		"INSERT INTO measurements VALUES (4, 10.25, 'hi')",
		"DELETE FROM measurements WHERE x < 0.0",
		"UPDATE measurements SET x = 1.25 WHERE id = 1",
		"UPDATE measurements SET x = NULL WHERE id = 2",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	assertSelectRealRows(t, db, "SELECT id, x, label FROM measurements ORDER BY id", [][3]any{
		{int(1), 1.25, "zero"},
		{int(2), nil, "pi"},
		{int(4), 10.25, "hi"},
	})
}

func assertSelectRealRows(t *testing.T, db *DB, sql string, want [][3]any) {
	t.Helper()

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q) error = %v", sql, err)
	}
	defer rows.Close()

	got := make([][3]any, 0, len(want))
	for rows.Next() {
		var id int
		var x any
		var label string
		if err := rows.Scan(&id, &x, &label); err != nil {
			t.Fatalf("Scan(%q) error = %v", sql, err)
		}
		got = append(got, [3]any{id, x, label})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err(%q) = %v", sql, err)
	}
	if len(got) != len(want) {
		t.Fatalf("rows(%q) len = %d, want %d; got = %#v", sql, len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rows(%q)[%d] = %#v, want %#v", sql, i, got[i], want[i])
		}
	}
}
