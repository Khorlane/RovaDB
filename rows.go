package rovadb

// Rows represents a result stream from a query.
type Rows struct {
	err   error
	done  bool
	value any
}

// Next reports whether another row is available.
func (r *Rows) Next() bool {
	if r == nil {
		return false
	}
	if r.err != nil {
		return false
	}
	if r.done {
		return false
	}

	r.done = true
	return true
}

// Scan decodes the current row into destination values.
func (r *Rows) Scan(dest ...any) error {
	if r == nil {
		return ErrInvalidArgument
	}
	if r.err != nil {
		return r.err
	}
	if len(dest) != 1 {
		return ErrInvalidArgument
	}

	switch v := dest[0].(type) {
	case *int:
		if v == nil {
			return ErrInvalidArgument
		}
		if value, ok := r.value.(int64); ok {
			*v = int(value)
			return nil
		}
	case *int64:
		if v == nil {
			return ErrInvalidArgument
		}
		if value, ok := r.value.(int64); ok {
			*v = value
			return nil
		}
	case *string:
		if v == nil {
			return ErrInvalidArgument
		}
		if value, ok := r.value.(string); ok {
			*v = value
			return nil
		}
	case *any:
		if v == nil {
			return ErrInvalidArgument
		}
		*v = r.value
		return nil
	}

	return ErrInvalidArgument
}

// Close releases any resources held by the row stream.
func (r *Rows) Close() error {
	return nil
}

// Err reports any terminal row iteration error.
func (r *Rows) Err() error {
	return r.err
}
