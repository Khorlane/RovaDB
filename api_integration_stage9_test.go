package rovadb

import (
	"errors"
	"testing"
)

func TestStage9APICanonicalLifecycleAndReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	createResult, err := db.Exec("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if createResult.RowsAffected() != 0 {
		t.Fatalf("create RowsAffected() = %d, want 0", createResult.RowsAffected())
	}

	for _, tc := range []struct {
		sql          string
		rowsAffected int
	}{
		{sql: "INSERT INTO users VALUES (1, 'alice')", rowsAffected: 1},
		{sql: "INSERT INTO users VALUES (2, 'bob')", rowsAffected: 1},
	} {
		result, err := db.Exec(tc.sql)
		if err != nil {
			t.Fatalf("Exec(%q) error = %v", tc.sql, err)
		}
		if result.RowsAffected() != tc.rowsAffected {
			t.Fatalf("Exec(%q).RowsAffected() = %d, want %d", tc.sql, result.RowsAffected(), tc.rowsAffected)
		}
	}

	rows, err := db.Query("SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if got := rows.Columns(); len(got) != 2 || got[0] != "id" || got[1] != "name" {
		t.Fatalf("Columns() = %#v, want [id name]", got)
	}

	var gotRows [][2]any
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		gotRows = append(gotRows, [2]any{id, name})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v, want nil", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close() = %v, want nil", err)
	}
	wantRows := [][2]any{{1, "alice"}, {2, "bob"}}
	if len(gotRows) != len(wantRows) {
		t.Fatalf("row count = %d, want %d", len(gotRows), len(wantRows))
	}
	for i := range wantRows {
		if gotRows[i] != wantRows[i] {
			t.Fatalf("row %d = %#v, want %#v", i, gotRows[i], wantRows[i])
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	row := db.QueryRow("SELECT name FROM users WHERE id = 2")
	var name string
	if err := row.Scan(&name); err != nil {
		t.Fatalf("QueryRow().Scan() error = %v", err)
	}
	if name != "bob" {
		t.Fatalf("QueryRow().Scan() got %q, want %q", name, "bob")
	}
}

func TestStage9APIStrictRoutingAndSingleRowSemantics(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("CREATE TABLE users (id INT)")
	if !errors.Is(err, ErrQueryRequiresSelect) {
		t.Fatalf("Query(non-select) error = %v, want ErrQueryRequiresSelect", err)
	}
	if rows != nil {
		t.Fatalf("Query(non-select) rows = %v, want nil", rows)
	}

	result, err := db.Exec("SELECT 1")
	if !errors.Is(err, ErrExecDisallowsSelect) {
		t.Fatalf("Exec(select) error = %v, want ErrExecDisallowsSelect", err)
	}
	if result != (Result{}) {
		t.Fatalf("Exec(select) result = %#v, want zero Result", result)
	}

	if _, err := db.Exec("CREATE TABLE people (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create people) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO people VALUES (1, 'amy')",
		"INSERT INTO people VALUES (2, 'ben')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	var name string
	if err := db.QueryRow("SELECT name FROM people WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("QueryRow(one row).Scan() error = %v", err)
	}
	if name != "amy" {
		t.Fatalf("QueryRow(one row).Scan() got %q, want %q", name, "amy")
	}

	if err := db.QueryRow("SELECT name FROM people WHERE id = 999").Scan(&name); !errors.Is(err, ErrNoRows) {
		t.Fatalf("QueryRow(no rows).Scan() = %v, want ErrNoRows", err)
	}
	if err := db.QueryRow("SELECT name FROM people ORDER BY id").Scan(&name); !errors.Is(err, ErrMultipleRows) {
		t.Fatalf("QueryRow(multiple rows).Scan() = %v, want ErrMultipleRows", err)
	}
}

func TestStage9APIBoolExampleFlow(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, active BOOL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'Alice', TRUE)",
		"INSERT INTO users VALUES (2, 'Bob', FALSE)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id, name FROM users WHERE active = TRUE ORDER BY id")
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
	var id int
	var userName string
	if err := rows.Scan(&id, &userName); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if id != 1 || userName != "Alice" {
		t.Fatalf("row = (%d, %q), want (1, %q)", id, userName, "Alice")
	}
	if rows.Next() {
		t.Fatal("Next() second = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func TestStage9APIRealExampleFlow(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, active BOOL, score REAL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'Alice', TRUE, 3.14)",
		"INSERT INTO users VALUES (2, 'Bob', FALSE, 1.25)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id, name FROM users WHERE active = TRUE ORDER BY id")
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
	var id int
	var userName string
	if err := rows.Scan(&id, &userName); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if id != 1 || userName != "Alice" {
		t.Fatalf("row = (%d, %q), want (1, %q)", id, userName, "Alice")
	}
	if rows.Next() {
		t.Fatal("Next() second = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}

	var score float64
	if err := db.QueryRow("SELECT score FROM users WHERE id = 1").Scan(&score); err != nil {
		t.Fatalf("QueryRow(score).Scan() error = %v", err)
	}
	if score != 3.14 {
		t.Fatalf("QueryRow(score).Scan() got %v, want 3.14", score)
	}
}
