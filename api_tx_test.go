package rovadb

import (
	"errors"
	"testing"
)

func TestPublicTransactionAPIMethodShapesCompile(t *testing.T) {
	var begin func(*DB) (*Tx, error) = (*DB).Begin
	var commit func(*Tx) error = (*Tx).Commit
	var rollback func(*Tx) error = (*Tx).Rollback
	var exec func(*Tx, string, ...any) (Result, error) = (*Tx).Exec
	var query func(*Tx, string, ...any) (*Rows, error) = (*Tx).Query
	var queryRow func(*Tx, string, ...any) *Row = (*Tx).QueryRow

	if begin == nil || commit == nil || rollback == nil || exec == nil || query == nil || queryRow == nil {
		t.Fatal("public transaction method expressions should be non-nil")
	}
}

func TestBeginOnOpenDBReturnsActiveTx(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v, want nil", err)
	}
	if tx == nil {
		t.Fatal("Begin() tx = nil, want value")
	}
	if tx.db != db {
		t.Fatalf("tx.db = %#v, want originating db %#v", tx.db, db)
	}
	if tx.finished {
		t.Fatal("tx.finished = true, want false for active transaction")
	}
	if db.tx != tx {
		t.Fatalf("db.tx = %#v, want returned tx %#v", db.tx, tx)
	}
}

func TestBeginOnClosedDBReturnsClosed(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	tx, err := db.Begin()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Begin() error = %v, want %v", err, ErrClosed)
	}
	if tx != nil {
		t.Fatalf("Begin() tx = %#v, want nil on closed DB", tx)
	}
}

func TestSecondBeginWhileTxActiveIsRejected(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	first, err := db.Begin()
	if err != nil {
		t.Fatalf("first Begin() error = %v", err)
	}

	second, err := db.Begin()
	if !errors.Is(err, ErrTxnAlreadyActive) {
		t.Fatalf("second Begin() error = %v, want %v", err, ErrTxnAlreadyActive)
	}
	if second != nil {
		t.Fatalf("second Begin() tx = %#v, want nil", second)
	}
	if db.tx != first {
		t.Fatalf("db.tx = %#v, want first tx %#v", db.tx, first)
	}
}

func TestCommitAppliesTransactionLocalStateToLaterDBReads(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := tx.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Tx.Exec(create) error = %v", err)
	}
	if _, err := tx.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Tx.Exec(insert) error = %v", err)
	}

	var txName string
	if err := tx.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&txName); err != nil {
		t.Fatalf("Tx.QueryRow().Scan() before commit error = %v", err)
	}
	if txName != "alice" {
		t.Fatalf("tx-scoped name = %q, want %q", txName, "alice")
	}

	dbRows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("DB.Query() before commit error = %v", err)
	}
	if dbRows.Next() {
		t.Fatal("DB.Query() before commit unexpectedly saw uncommitted rows")
	}
	if dbRows.Err() == nil || dbRows.Err().Error() != "execution: table not found: users" {
		t.Fatalf("DB.Query() before commit Err() = %v, want %q", dbRows.Err(), "execution: table not found: users")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v, want nil", err)
	}
	if !tx.finished {
		t.Fatal("tx.finished = false, want true after Commit()")
	}
	if db.tx != nil {
		t.Fatalf("db.tx = %#v, want nil after Commit()", db.tx)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxnCommitWithoutActive) {
		t.Fatalf("second Commit() error = %v, want %v", err, ErrTxnCommitWithoutActive)
	}

	var dbName string
	if err := db.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&dbName); err != nil {
		t.Fatalf("DB.QueryRow().Scan() after commit error = %v", err)
	}
	if dbName != "alice" {
		t.Fatalf("committed name = %q, want %q", dbName, "alice")
	}
	if _, err := tx.Exec("CREATE TABLE later (id INT)"); !errors.Is(err, ErrTxNotActive) {
		t.Fatalf("Exec() after Commit() error = %v, want %v", err, ErrTxNotActive)
	}
	if _, err := tx.Query("SELECT 1"); !errors.Is(err, ErrTxNotActive) {
		t.Fatalf("Query() after Commit() error = %v, want %v", err, ErrTxNotActive)
	}
	if err := tx.QueryRow("SELECT 1").Scan(new(any)); !errors.Is(err, ErrTxNotActive) {
		t.Fatalf("QueryRow().Scan() after Commit() error = %v, want %v", err, ErrTxNotActive)
	}

	next, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() after Commit() error = %v", err)
	}
	if next == nil {
		t.Fatal("Begin() after Commit() tx = nil, want value")
	}
}

func TestRollbackDiscardsTransactionLocalStateFromLaterDBReads(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := tx.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Tx.Exec(create) error = %v", err)
	}
	if _, err := tx.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Tx.Exec(insert) error = %v", err)
	}

	var txName string
	if err := tx.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&txName); err != nil {
		t.Fatalf("Tx.QueryRow().Scan() before rollback error = %v", err)
	}
	if txName != "alice" {
		t.Fatalf("tx-scoped name = %q, want %q", txName, "alice")
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v, want nil", err)
	}
	if !tx.finished {
		t.Fatal("tx.finished = false, want true after Rollback()")
	}
	if db.tx != nil {
		t.Fatalf("db.tx = %#v, want nil after Rollback()", db.tx)
	}
	if err := tx.Rollback(); !errors.Is(err, ErrTxnRollbackWithoutActive) {
		t.Fatalf("second Rollback() error = %v, want %v", err, ErrTxnRollbackWithoutActive)
	}

	dbRows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("DB.Query() after rollback error = %v", err)
	}
	if dbRows.Next() {
		t.Fatal("DB.Query() after rollback unexpectedly saw discarded rows")
	}
	if dbRows.Err() == nil || dbRows.Err().Error() != "execution: table not found: users" {
		t.Fatalf("DB.Query() after rollback Err() = %v, want %q", dbRows.Err(), "execution: table not found: users")
	}
	if _, err := tx.Exec("CREATE TABLE later (id INT)"); !errors.Is(err, ErrTxNotActive) {
		t.Fatalf("Exec() after Rollback() error = %v, want %v", err, ErrTxNotActive)
	}
	if _, err := tx.Query("SELECT 1"); !errors.Is(err, ErrTxNotActive) {
		t.Fatalf("Query() after Rollback() error = %v, want %v", err, ErrTxNotActive)
	}
	if err := tx.QueryRow("SELECT 1").Scan(new(any)); !errors.Is(err, ErrTxNotActive) {
		t.Fatalf("QueryRow().Scan() after Rollback() error = %v, want %v", err, ErrTxNotActive)
	}

	next, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() after Rollback() error = %v", err)
	}
	if next == nil {
		t.Fatal("Begin() after Rollback() tx = nil, want value")
	}
}

func TestTxExecAndQueryOperateWithinExplicitTransactionSnapshot(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := tx.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Tx.Exec(create) error = %v", err)
	}
	if _, err := tx.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Tx.Exec(insert) error = %v", err)
	}

	rows, err := tx.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Tx.Query() error = %v", err)
	}
	if got := rows.Columns(); len(got) != 2 || got[0] != "id" || got[1] != "name" {
		t.Fatalf("rows.Columns() = %#v, want [id name]", got)
	}
	if !rows.Next() {
		t.Fatal("rows.Next() = false, want one row")
	}
	var id int
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if id != 1 || name != "alice" {
		t.Fatalf("scanned row = (%d, %q), want (1, %q)", id, name, "alice")
	}
	if rows.Next() {
		t.Fatal("rows.Next() = true, want one row only")
	}

	dbRows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("DB.Query() error = %v", err)
	}
	if dbRows.Next() {
		t.Fatal("DB.Query() unexpectedly saw Tx-only rows")
	}
	if dbRows.Err() == nil || dbRows.Err().Error() != "execution: table not found: users" {
		t.Fatalf("DB.Query() Err() = %v, want %q", dbRows.Err(), "execution: table not found: users")
	}
}

func TestTxQueryRowOperatesWithinExplicitTransactionSnapshot(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := tx.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Tx.Exec(create) error = %v", err)
	}
	if _, err := tx.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Tx.Exec(insert) error = %v", err)
	}

	var name string
	if err := tx.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("Tx.QueryRow().Scan() error = %v", err)
	}
	if name != "alice" {
		t.Fatalf("name = %q, want %q", name, "alice")
	}
}

func TestDBAutocommitBehaviorRemainsUnchangedWithoutBegin(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("DB.Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("DB.Exec(insert) error = %v", err)
	}

	var name string
	if err := db.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("DB.QueryRow().Scan() error = %v", err)
	}
	if name != "alice" {
		t.Fatalf("name = %q, want %q", name, "alice")
	}
}
