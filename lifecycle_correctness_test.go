package rovadb

import (
	"context"
	"errors"
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestLifecycleWriteCloseReopenQuery(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertSelectRowsWithNames(t, db, "SELECT id, name FROM users", [][2]any{
		{int64(1), "alice"},
		{int64(2), "bob"},
	})
}

func TestLifecycleUpdateCloseReopenQuery(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"UPDATE users SET name = 'bobby' WHERE id = 2",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertSelectRowsWithNames(t, db, "SELECT id, name FROM users", [][2]any{
		{int64(1), "alice"},
		{int64(2), "bobby"},
	})
}

func TestLifecycleDeleteCloseReopenQuery(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
		"DELETE FROM users WHERE id = 2",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertSelectRowsWithNames(t, db, "SELECT id, name FROM users", [][2]any{
		{int64(1), "alice"},
		{int64(3), "cara"},
	})
}

func TestLifecycleRollbackCloseReopenKeepsCommittedState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	err = db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		table := stagedTables["users"]
		table.Rows[0][1] = parser.StringValue("rolled-back")
		if err := db.applyStagedTableRewrite(stagedTables, "users"); err != nil {
			return err
		}
		return errors.New("force rollback")
	})
	if err == nil || err.Error() != "force rollback" {
		t.Fatalf("execMutatingStatement() error = %v, want %q", err, "force rollback")
	}

	assertSelectRowsWithNames(t, db, "SELECT id, name FROM users", [][2]any{
		{int64(1), "alice"},
	})

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertSelectRowsWithNames(t, db, "SELECT id, name FROM users", [][2]any{
		{int64(1), "alice"},
	})
}

func TestLifecycleMultipleWritesAcrossReopenBoundaries(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	db = reopenDB(t, path)
	if _, err := db.Exec(context.Background(), "UPDATE users SET name = 'bobby' WHERE id = 2"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	db = reopenDB(t, path)
	if _, err := db.Exec(context.Background(), "DELETE FROM users WHERE id = 1"); err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("third Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertSelectRowsWithNames(t, db, "SELECT id, name FROM users", [][2]any{
		{int64(2), "bobby"},
	})
}

func TestLifecycleIndexedQueryAfterReopenRemainsCorrect(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'alice')",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.defineBasicIndex("users", "name"); err != nil {
		t.Fatalf("defineBasicIndex() error = %v", err)
	}

	assertQueryIntRows(t, db, "SELECT id FROM users WHERE name = 'alice' ORDER BY id", 1, 3)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertQueryIntRows(t, db, "SELECT id FROM users WHERE name = 'alice' ORDER BY id", 1, 3)
}

func assertSelectRowsWithNames(t *testing.T, db *DB, sql string, want [][2]any) {
	t.Helper()

	rows, err := db.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("Query(%q) error = %v", sql, err)
	}
	defer rows.Close()

	got := make([][2]any, 0, len(want))
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan(%q) error = %v", sql, err)
		}
		got = append(got, [2]any{id, name})
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
