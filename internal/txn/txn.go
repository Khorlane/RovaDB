package txn

// TxnState describes the lifecycle state of an internal transaction.
type TxnState int

const (
	TxnStateIdle TxnState = iota
	TxnStateActive
	TxnStateCommitted
	TxnStateRolledBack
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
func (t *Txn) MarkDirty() {
	if t == nil {
		return
	}
	t.dirty = true
}

// Commit transitions an active transaction to committed.
func (t *Txn) Commit() {
	if t == nil || t.state != TxnStateActive {
		return
	}
	t.state = TxnStateCommitted
}

// Rollback transitions an active transaction to rolled back.
func (t *Txn) Rollback() {
	if t == nil || t.state != TxnStateActive {
		return
	}
	t.state = TxnStateRolledBack
}
