package rovadb

// Tx is the public transaction handle reserved for explicit transaction support.
type Tx struct{}

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
	return nil, ErrNotImplemented
}

// Commit finalizes an explicit transaction.
func (tx *Tx) Commit() error {
	return ErrNotImplemented
}

// Rollback abandons an explicit transaction.
func (tx *Tx) Rollback() error {
	return ErrNotImplemented
}
