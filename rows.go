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
