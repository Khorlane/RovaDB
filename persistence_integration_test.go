package rovadb

import (
	"context"
	"testing"
)

// Stage 4 invariants:
// - writes are immediately durable
// - no transaction boundary exists
// - mutations are visible immediately
// - close/reopen preserves full state

func reopenDB(t *testing.T, path string) *DB {
	t.Helper()

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() after reopen error = %v", err)
	}
	return db
}

func TestCreateTablePersistence(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertIntRows(t, rows)
}

func TestInsertPersistence(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
		"INSERT INTO t VALUES (3)",
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

	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertIntRows(t, rows, 1, 2, 3)
}

func TestUpdatePersistence(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
		"INSERT INTO t VALUES (3)",
		"UPDATE t SET id = 10 WHERE id = 1",
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

	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertIntRows(t, rows, 10, 2, 3)
}

func TestDeletePersistence(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
		"INSERT INTO t VALUES (3)",
		"DELETE FROM t WHERE id = 2",
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

	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertIntRows(t, rows, 1, 3)
}

func TestMixedMutationPersistence(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
		"INSERT INTO t VALUES (3)",
		"INSERT INTO t VALUES (4)",
		"UPDATE t SET id = 10 WHERE id = 1",
		"UPDATE t SET id = 30 WHERE id = 3",
		"DELETE FROM t WHERE id = 2",
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

	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertIntRows(t, rows, 10, 30, 4)
}

func TestMutationsStillPersistUnderAutocommit(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
		"INSERT INTO t VALUES (3)",
		"UPDATE t SET id = 10 WHERE id = 1",
		"DELETE FROM t WHERE id = 2",
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

	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertIntRows(t, rows, 10, 3)
}

func TestFailedMutatingStatementClearsTxn(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO t VALUES (1, 2)"); err == nil {
		t.Fatal("Exec(insert) error = nil, want failure")
	}
	if db.txn != nil {
		t.Fatalf("db.txn = %#v, want nil", db.txn)
	}
}

func TestSuccessfulMutatingStatementClearsTxn(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if db.txn != nil {
		t.Fatalf("db.txn = %#v, want nil", db.txn)
	}
}

func TestFailedInsertDoesNotChangeState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert baseline) error = %v", err)
	}

	if _, err := db.Exec(context.Background(), "INSERT INTO t VALUES (1, 2)"); err == nil {
		t.Fatal("Exec(failing insert) error = nil, want failure")
	}

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestFailedUpdateDoesNotChangeState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec(context.Background(), "UPDATE t SET missing = 10 WHERE id = 1"); err == nil {
		t.Fatal("Exec(failing update) error = nil, want failure")
	}

	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)
}

func TestFailedDeleteDoesNotChangeState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec(context.Background(), "DELETE FROM t WHERE missing = 2"); err == nil {
		t.Fatal("Exec(failing delete) error = nil, want failure")
	}

	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)
}

func TestFailedCreateTableDoesNotPartiallyRegisterTable(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err == nil {
		t.Fatal("Exec(duplicate create) error = nil, want failure")
	}
	if len(db.tables) != 1 {
		t.Fatalf("len(db.tables) = %d, want 1", len(db.tables))
	}
	assertSelectIntRows(t, db, "SELECT * FROM t")

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if len(db.tables) != 1 {
		t.Fatalf("len(reopened db.tables) = %d, want 1", len(db.tables))
	}
	assertSelectIntRows(t, db, "SELECT * FROM t")
}

func assertIntRows(t *testing.T, rows *Rows, want ...int64) {
	t.Helper()

	got := make([]int64, 0, len(want))
	for rows.Next() {
		var value int64
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		got = append(got, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err() = %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d (rows = %v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d = %d, want %d (rows = %v)", i, got[i], want[i], got)
		}
	}
}

func assertSelectIntRows(t *testing.T, db *DB, sql string, want ...int64) {
	t.Helper()

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q) error = %v", sql, err)
	}
	defer rows.Close()

	assertIntRows(t, rows, want...)
}
