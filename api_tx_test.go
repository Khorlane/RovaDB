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

func TestBeginReturnsNotImplementedForNow(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("Begin() error = %v, want %v", err, ErrNotImplemented)
	}
	if tx != nil {
		t.Fatalf("Begin() tx = %#v, want nil while explicit transactions are stubbed", tx)
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

func TestPublicTxMethodsReturnNotImplementedForNow(t *testing.T) {
	var tx Tx

	if err := tx.Commit(); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("Commit() error = %v, want %v", err, ErrNotImplemented)
	}
	if err := tx.Rollback(); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("Rollback() error = %v, want %v", err, ErrNotImplemented)
	}
}
