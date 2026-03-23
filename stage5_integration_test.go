package rovadb

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestCommitDurabilityAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	for _, sql := range []string{
		"CREATE TABLE t (id INT)",
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
	assertSelectIntRows(t, db, "SELECT * FROM t", 10, 3)
}

func TestFailedMutationDoesNotLeakState(t *testing.T) {
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

	for _, sql := range []string{
		"INSERT INTO t VALUES (1, 2)",
		"UPDATE t SET missing = 10 WHERE id = 1",
		"DELETE FROM t WHERE missing = 2",
		"CREATE TABLE t (id INT)",
	} {
		if _, err := db.Exec(context.Background(), sql); err == nil {
			t.Fatalf("Exec(%q) error = nil, want failure", sql)
		}
		assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)
		assertAutocommitClean(t, db, path)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)
}

func TestInterruptedCommitRecoversLastCommittedState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	db.afterDatabaseSyncHook = func() error {
		return errors.New("boom after db sync")
	}
	if _, err := db.Exec(context.Background(), "UPDATE t SET id = 2 WHERE id = 1"); err == nil {
		t.Fatal("Exec(update) error = nil, want interrupted commit failure")
	}
	if _, err := os.Stat(storage.JournalPath(path)); err != nil {
		t.Fatalf("journal stat error = %v, want surviving journal", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
	if _, err := os.Stat(storage.JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists after recovery", err)
	}
}

func TestMultipleCommittedMutationsAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE t (id INT)",
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	if _, err := db.Exec(context.Background(), "UPDATE t SET id = 10 WHERE id = 1"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	assertSelectIntRows(t, db, "SELECT * FROM t", 10, 2)
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	if _, err := db.Exec(context.Background(), "DELETE FROM t WHERE id = 2"); err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO t VALUES (30)"); err != nil {
		t.Fatalf("Exec(insert 30) error = %v", err)
	}
	assertSelectIntRows(t, db, "SELECT * FROM t", 10, 30)
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 10, 30)
}

func TestJournalAbsentAfterSuccessfulCommitAndRecovery(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	assertAutocommitClean(t, db, path)

	db.afterDatabaseSyncHook = func() error {
		return errors.New("boom after db sync")
	}
	if _, err := db.Exec(context.Background(), "UPDATE t SET id = 2 WHERE id = 1"); err == nil {
		t.Fatal("Exec(update) error = nil, want interrupted commit failure")
	}
	if _, err := os.Stat(storage.JournalPath(path)); err != nil {
		t.Fatalf("journal stat error = %v, want surviving journal", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertAutocommitClean(t, db, path)
}

func TestAutocommitMutationLeavesNoTxnOrTracking(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE t (id INT)",
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET id = 10 WHERE id = 1",
		"DELETE FROM t WHERE id = 10",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
		assertAutocommitClean(t, db, path)
	}
}

func assertAutocommitClean(t *testing.T, db *DB, path string) {
	t.Helper()

	if db.txn != nil {
		t.Fatalf("db.txn = %#v, want nil", db.txn)
	}
	if len(db.pager.DirtyPages()) != 0 {
		t.Fatalf("len(db.pager.DirtyPages()) = %d, want 0", len(db.pager.DirtyPages()))
	}
	if len(db.pager.DirtyPagesWithOriginals()) != 0 {
		t.Fatalf("len(db.pager.DirtyPagesWithOriginals()) = %d, want 0", len(db.pager.DirtyPagesWithOriginals()))
	}
	if _, err := os.Stat(storage.JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists", err)
	}
}
