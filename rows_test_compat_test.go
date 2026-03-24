package rovadb

func (r *Rows) Next() bool {
	if r == nil || r.closed {
		return false
	}
	if r.err != nil {
		return false
	}
	if r.idx+1 >= len(r.data) {
		r.idx = len(r.data)
		return false
	}

	r.idx++
	return true
}

func (r *Rows) Scan(dest ...any) error {
	if r == nil {
		return ErrInvalidArgument
	}
	if r.err != nil {
		return r.err
	}
	if r.closed {
		return ErrClosed
	}
	if r.idx < 0 || r.idx >= len(r.data) {
		return ErrInvalidArgument
	}

	row := r.data[r.idx]
	if len(dest) != len(row) {
		return ErrInvalidArgument
	}

	for i, value := range row {
		if err := scanValueForTest(dest[i], value); err != nil {
			return err
		}
	}

	return nil
}

func (r *Rows) Close() error {
	if r == nil {
		return nil
	}
	r.closed = true
	return nil
}

func (r *Rows) Err() error {
	if r == nil {
		return nil
	}
	return r.err
}

func (r *Rows) Columns() []string {
	if r == nil {
		return nil
	}
	return append([]string(nil), r.columns...)
}

func scanValueForTest(dest any, value any) error {
	switch v := dest.(type) {
	case *int:
		if v == nil {
			return ErrInvalidArgument
		}
		switch n := value.(type) {
		case int:
			*v = n
			return nil
		case int64:
			*v = int(n)
			return nil
		}
	case *int64:
		if v == nil {
			return ErrInvalidArgument
		}
		switch n := value.(type) {
		case int:
			*v = int64(n)
			return nil
		case int64:
			*v = n
			return nil
		}
	case *string:
		if v == nil {
			return ErrInvalidArgument
		}
		s, ok := value.(string)
		if !ok {
			return ErrInvalidArgument
		}
		*v = s
		return nil
	case *any:
		if v == nil {
			return ErrInvalidArgument
		}
		*v = value
		return nil
	}

	return ErrInvalidArgument
}
