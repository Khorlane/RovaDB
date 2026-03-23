package txn

import (
	"errors"
	"testing"
)

func TestNewTxnStartsActive(t *testing.T) {
	txn := NewTxn()
	if txn == nil {
		t.Fatal("NewTxn() = nil")
	}
	if !txn.IsActive() {
		t.Fatal("IsActive() = false, want true")
	}
	if txn.state != TxnStateActive {
		t.Fatalf("state = %v, want %v", txn.state, TxnStateActive)
	}
	if !txn.CanCommit() {
		t.Fatal("CanCommit() = false, want true")
	}
}

func TestCommitTransitionsToCommitted(t *testing.T) {
	txn := NewTxn()
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	if txn.state != TxnStateCommitted {
		t.Fatalf("state = %v, want %v", txn.state, TxnStateCommitted)
	}
	if txn.IsActive() {
		t.Fatal("IsActive() = true, want false")
	}
	if txn.CanCommit() {
		t.Fatal("CanCommit() = true, want false")
	}
}

func TestRollbackTransitionsToRolledBack(t *testing.T) {
	txn := NewTxn()
	if err := txn.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	if txn.state != TxnStateRolledBack {
		t.Fatalf("state = %v, want %v", txn.state, TxnStateRolledBack)
	}
	if txn.IsActive() {
		t.Fatal("IsActive() = true, want false")
	}
	if txn.CanCommit() {
		t.Fatal("CanCommit() = true, want false")
	}
}

func TestMarkDirtySetsDirtyFlag(t *testing.T) {
	txn := NewTxn()
	if err := txn.MarkDirty(); err != nil {
		t.Fatalf("MarkDirty() error = %v", err)
	}

	if !txn.dirty {
		t.Fatal("dirty = false, want true")
	}
}

func TestMarkDirtyOnNonActiveTxnErrors(t *testing.T) {
	txn := NewTxn()
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	err := txn.MarkDirty()
	if !errors.Is(err, ErrDirtyNonActiveTxn) {
		t.Fatalf("MarkDirty() error = %v, want %v", err, ErrDirtyNonActiveTxn)
	}
}

func TestCommitOnTerminalTxnErrors(t *testing.T) {
	committed := NewTxn()
	if err := committed.Commit(); err != nil {
		t.Fatalf("Commit() initial error = %v", err)
	}
	if err := committed.Commit(); !errors.Is(err, ErrInvalidCommitState) {
		t.Fatalf("Commit() on committed txn error = %v, want %v", err, ErrInvalidCommitState)
	}

	rolledBack := NewTxn()
	if err := rolledBack.Rollback(); err != nil {
		t.Fatalf("Rollback() initial error = %v", err)
	}
	if err := rolledBack.Commit(); !errors.Is(err, ErrInvalidCommitState) {
		t.Fatalf("Commit() on rolled back txn error = %v, want %v", err, ErrInvalidCommitState)
	}
}

func TestRollbackOnTerminalTxnErrors(t *testing.T) {
	committed := NewTxn()
	if err := committed.Commit(); err != nil {
		t.Fatalf("Commit() initial error = %v", err)
	}
	if err := committed.Rollback(); !errors.Is(err, ErrInvalidRollbackState) {
		t.Fatalf("Rollback() on committed txn error = %v, want %v", err, ErrInvalidRollbackState)
	}

	rolledBack := NewTxn()
	if err := rolledBack.Rollback(); err != nil {
		t.Fatalf("Rollback() initial error = %v", err)
	}
	if err := rolledBack.Rollback(); !errors.Is(err, ErrInvalidRollbackState) {
		t.Fatalf("Rollback() on rolled back txn error = %v, want %v", err, ErrInvalidRollbackState)
	}
}
