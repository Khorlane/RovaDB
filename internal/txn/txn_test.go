package txn

import "testing"

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
}

func TestCommitTransitionsToCommitted(t *testing.T) {
	txn := NewTxn()
	txn.Commit()

	if txn.state != TxnStateCommitted {
		t.Fatalf("state = %v, want %v", txn.state, TxnStateCommitted)
	}
	if txn.IsActive() {
		t.Fatal("IsActive() = true, want false")
	}
}

func TestRollbackTransitionsToRolledBack(t *testing.T) {
	txn := NewTxn()
	txn.Rollback()

	if txn.state != TxnStateRolledBack {
		t.Fatalf("state = %v, want %v", txn.state, TxnStateRolledBack)
	}
	if txn.IsActive() {
		t.Fatal("IsActive() = true, want false")
	}
}

func TestMarkDirtySetsDirtyFlag(t *testing.T) {
	txn := NewTxn()
	txn.MarkDirty()

	if !txn.dirty {
		t.Fatal("dirty = false, want true")
	}
}
