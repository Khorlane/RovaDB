package rovadb

import (
	"errors"
	"testing"
)

func TestPublicTransactionAPIMethodShapesCompile(t *testing.T) {
	var begin func(*DB) (*Tx, error) = (*DB).Begin
	var commit func(*Tx) error = (*Tx).Commit
	var rollback func(*Tx) error = (*Tx).Rollback

	if begin == nil || commit == nil || rollback == nil {
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

func TestCommitReleasesBeginOwnershipWithoutRoutingExecution(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
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
}

func TestRollbackReleasesBeginOwnershipWithoutRoutingExecution(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
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
}
