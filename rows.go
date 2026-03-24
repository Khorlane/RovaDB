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
