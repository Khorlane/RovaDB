package rovadb

// Tx is the public transaction handle reserved for explicit transaction support.
type Tx struct {
	db       *DB
	finished bool
}

// Begin starts an explicit transaction.
//
// Explicit transaction execution is not implemented yet. Existing autocommit
// behavior remains unchanged until later slices wire the public API through the
// engine.
func (db *DB) Begin() (*Tx, error) {
	if db == nil {
		return nil, ErrInvalidArgument
	}
	if db.closed {
		return nil, ErrClosed
	}
	if db.tx != nil && !db.tx.finished {
		return nil, ErrTxnAlreadyActive
	}
	tx := &Tx{db: db}
	db.tx = tx
	return tx, nil
}

// Commit finalizes an explicit transaction.
func (tx *Tx) Commit() error {
	if tx == nil || tx.db == nil || tx.finished {
		return ErrTxnCommitWithoutActive
	}
	tx.finished = true
	if tx.db.tx == tx {
		tx.db.tx = nil
	}
	return nil
}

// Rollback abandons an explicit transaction.
func (tx *Tx) Rollback() error {
	if tx == nil || tx.db == nil || tx.finished {
		return ErrTxnRollbackWithoutActive
	}
	tx.finished = true
	if tx.db.tx == tx {
		tx.db.tx = nil
	}
	return nil
}
