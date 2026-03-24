package rovadb

// Rows represents a fully materialized query result.
type Rows struct {
	columns []string
	data    [][]any
	idx     int
	err     error
	closed  bool
}

// Row is a thin QueryRow wrapper over Rows.
type Row struct {
	rows *Rows
}

func newRows(cols []string, data [][]any) *Rows {
	return &Rows{
		columns: cols,
		data:    data,
		idx:     -1,
	}
}

func newRow(r *Rows) *Row {
	return &Row{rows: r}
}

// Next reports whether another row is available.
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

// Scan decodes the current row into destination values.
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
		if err := scanValue(dest[i], value); err != nil {
			return err
		}
	}

	return nil
}

func scanValue(dest any, value any) error {
	switch v := dest.(type) {
	case *int:
		if v == nil {
			return ErrInvalidArgument
		}
		n, ok := value.(int64)
		if !ok {
			return ErrInvalidArgument
		}
		*v = int(n)
		return nil
	case *int64:
		if v == nil {
			return ErrInvalidArgument
		}
		n, ok := value.(int64)
		if !ok {
			return ErrInvalidArgument
		}
		*v = n
		return nil
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

// Close releases any resources held by the row stream.
func (r *Rows) Close() error {
	if r == nil {
		return nil
	}
	r.closed = true
	return nil
}

// Err reports any terminal row iteration error.
func (r *Rows) Err() error {
	if r == nil {
		return nil
	}
	return r.err
}

// Columns reports the projected column names in scan order.
func (r *Rows) Columns() []string {
	if r == nil {
		return nil
	}
	return append([]string(nil), r.columns...)
}
