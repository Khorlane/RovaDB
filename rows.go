package rovadb

// Rows represents a result stream from a query.
type Rows struct {
	err error
}

// Next reports whether another row is available.
func (r *Rows) Next() bool {
	return false
}

// Scan decodes the current row into destination values.
func (r *Rows) Scan(dest ...any) error {
	return ErrNotImplemented
}

// Close releases any resources held by the row stream.
func (r *Rows) Close() error {
	return nil
}

// Err reports any terminal row iteration error.
func (r *Rows) Err() error {
	if r == nil {
		return nil
	}

	return r.err
}
