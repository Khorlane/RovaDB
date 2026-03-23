package txn

import "errors"

// TxnState describes the lifecycle state of an internal transaction.
type TxnState int

const (
	TxnStateIdle TxnState = iota
	TxnStateActive
	TxnStateCommitted
	TxnStateRolledBack
)

var (
	ErrNoActiveTxn          = errors.New("txn: no active transaction")
	ErrInvalidCommitState   = errors.New("txn: commit requires active transaction")
	ErrInvalidRollbackState = errors.New("txn: rollback requires active transaction")
	ErrDirtyNonActiveTxn    = errors.New("txn: dirty mark requires active transaction")
)

// Txn is the minimal internal transaction state holder.
type Txn struct {
	state TxnState
	dirty bool
}

// NewTxn creates a new active internal transaction.
func NewTxn() *Txn {
	return &Txn{
		state: TxnStateActive,
	}
}

// IsActive reports whether the transaction is currently active.
func (t *Txn) IsActive() bool {
	return t != nil && t.state == TxnStateActive
}

// CanCommit reports whether the transaction is currently eligible to commit.
func (t *Txn) CanCommit() bool {
	return t != nil && t.state == TxnStateActive
}

// IsDirty reports whether the transaction has staged writes.
func (t *Txn) IsDirty() bool {
	return t != nil && t.dirty
}

// MarkDirty records that work occurred within the transaction.
func (t *Txn) MarkDirty() error {
	if t == nil {
		return ErrNoActiveTxn
	}
	if t.state != TxnStateActive {
		return ErrDirtyNonActiveTxn
	}
	t.dirty = true
	return nil
}

// Commit transitions an active transaction to committed.
func (t *Txn) Commit() error {
	if t == nil {
		return ErrNoActiveTxn
	}
	if t.state != TxnStateActive {
		return ErrInvalidCommitState
	}
	t.state = TxnStateCommitted
	return nil
}

// Rollback transitions an active transaction to rolled back.
func (t *Txn) Rollback() error {
	if t == nil {
		return ErrNoActiveTxn
	}
	if t.state != TxnStateActive {
		return ErrInvalidRollbackState
	}
	t.state = TxnStateRolledBack
	return nil
}
