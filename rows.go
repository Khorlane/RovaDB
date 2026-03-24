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

// Next advances to the next row in the fully materialized result set.
func (r *Rows) Next() bool {
	if r == nil {
		return false
	}
	if r.closed {
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

// Close marks the row set closed. Rows are fully in memory, so this is lifecycle hygiene only.
func (r *Rows) Close() error {
	if r == nil {
		return nil
	}
	r.closed = true
	return nil
}

// Err reports any deferred query error associated with the row set.
func (r *Rows) Err() error {
	if r == nil {
		return nil
	}
	return r.err
}

// Columns reports the projected column names in query order.
func (r *Rows) Columns() []string {
	if r == nil {
		return nil
	}
	if len(r.columns) == 0 {
		return nil
	}

	cols := make([]string, len(r.columns))
	copy(cols, r.columns)
	return cols
}

// Scan copies the current row into destination pointers using strict positional matching.
func (r *Rows) Scan(dest ...any) error {
	if r == nil {
		return ErrScanBeforeNext
	}
	if r.err != nil {
		return r.err
	}
	if r.closed {
		return ErrRowsClosed
	}
	if r.idx < 0 || r.idx >= len(r.data) {
		return ErrScanBeforeNext
	}

	row := r.data[r.idx]
	if len(dest) != len(row) {
		return ErrScanMismatch
	}

	type assignment struct {
		dest  any
		value any
	}

	assignments := make([]assignment, 0, len(dest))
	for i := range dest {
		value, err := scanAssignableValue(dest[i], row[i])
		if err != nil {
			return err
		}
		assignments = append(assignments, assignment{
			dest:  dest[i],
			value: value,
		})
	}

	for _, assignment := range assignments {
		switch d := assignment.dest.(type) {
		case *int:
			*d = assignment.value.(int)
		case *string:
			*d = assignment.value.(string)
		case *bool:
			*d = assignment.value.(bool)
		case *any:
			*d = assignment.value
		default:
			return ErrUnsupportedScanType
		}
	}

	return nil
}

func scanAssignableValue(dest any, src any) (any, error) {
	switch d := dest.(type) {
	case *int:
		if d == nil {
			return nil, ErrUnsupportedScanType
		}
		n, ok := src.(int)
		if !ok {
			return nil, ErrUnsupportedScanType
		}
		return n, nil
	case *string:
		if d == nil {
			return nil, ErrUnsupportedScanType
		}
		s, ok := src.(string)
		if !ok {
			return nil, ErrUnsupportedScanType
		}
		return s, nil
	case *bool:
		if d == nil {
			return nil, ErrUnsupportedScanType
		}
		b, ok := src.(bool)
		if !ok {
			return nil, ErrUnsupportedScanType
		}
		return b, nil
	case *any:
		if d == nil {
			return nil, ErrUnsupportedScanType
		}
		return src, nil
	default:
		return nil, ErrUnsupportedScanType
	}
}

// Scan reads exactly one row from the wrapped Rows result.
func (r *Row) Scan(dest ...any) error {
	if r == nil || r.rows == nil {
		return ErrNoRows
	}
	if r.rows.err != nil {
		return r.rows.err
	}

	if !r.rows.Next() {
		defer r.rows.Close()
		if err := r.rows.Err(); err != nil {
			return err
		}
		return ErrNoRows
	}

	if err := r.rows.Scan(dest...); err != nil {
		_ = r.rows.Close()
		return err
	}

	if r.rows.Next() {
		_ = r.rows.Close()
		return ErrMultipleRows
	}
	if err := r.rows.Err(); err != nil {
		_ = r.rows.Close()
		return err
	}
	_ = r.rows.Close()
	return nil
}
