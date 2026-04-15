package rovadb

import "time"

// Rows represents a fully materialized query result.
type Rows struct {
	columns   []string
	scanTypes []string
	data      [][]any
	idx       int
	err       error
	closed    bool
}

// Row is a thin QueryRow wrapper over Rows.
type Row struct {
	rows *Rows
}

func newRows(cols []string, data [][]any) *Rows {
	return newRowsWithScanTypes(cols, data, nil)
}

func newRowsWithScanTypes(cols []string, data [][]any, scanTypes []string) *Rows {
	return &Rows{
		columns:   cols,
		scanTypes: append([]string(nil), scanTypes...),
		data:      data,
		idx:       -1,
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
		value, err := scanAssignableValue(dest[i], row[i], r.scanTypeAt(i))
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
		case *int16:
			*d = assignment.value.(int16)
		case *int32:
			*d = assignment.value.(int32)
		case *int64:
			*d = assignment.value.(int64)
		case *string:
			*d = assignment.value.(string)
		case *bool:
			*d = assignment.value.(bool)
		case *float64:
			*d = assignment.value.(float64)
		case *Time:
			*d = assignment.value.(Time)
		case *time.Time:
			*d = assignment.value.(time.Time)
		case *any:
			*d = assignment.value
		default:
			return ErrUnsupportedScanType
		}
	}

	return nil
}

func (r *Rows) scanTypeAt(idx int) string {
	if r == nil || idx < 0 || idx >= len(r.scanTypes) {
		return ""
	}
	return r.scanTypes[idx]
}

func scanAssignableValue(dest any, src any, scanType string) (any, error) {
	switch d := dest.(type) {
	case *int16:
		if d == nil {
			return nil, ErrUnsupportedScanType
		}
		if scanType != "" && scanType != "SMALLINT" {
			return nil, ErrUnsupportedScanType
		}
		n, ok := src.(int16)
		if !ok {
			return nil, ErrUnsupportedScanType
		}
		return n, nil
	case *int32:
		if d == nil {
			return nil, ErrUnsupportedScanType
		}
		if scanType != "" && scanType != "INT" {
			return nil, ErrUnsupportedScanType
		}
		n, ok := src.(int32)
		if !ok {
			return nil, ErrUnsupportedScanType
		}
		return n, nil
	case *int64:
		if d == nil {
			return nil, ErrUnsupportedScanType
		}
		if scanType != "" && scanType != "BIGINT" {
			return nil, ErrUnsupportedScanType
		}
		n, ok := src.(int64)
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
	case *float64:
		if d == nil {
			return nil, ErrUnsupportedScanType
		}
		f, ok := src.(float64)
		if !ok {
			return nil, ErrUnsupportedScanType
		}
		return f, nil
	case *Time:
		if d == nil {
			return nil, ErrUnsupportedScanType
		}
		if scanType != "" && scanType != "TIME" {
			return nil, ErrUnsupportedScanType
		}
		tv, ok := src.(Time)
		if !ok {
			return nil, ErrUnsupportedScanType
		}
		return tv, nil
	case *time.Time:
		if d == nil {
			return nil, ErrUnsupportedScanType
		}
		if scanType != "" && scanType != "DATE" && scanType != "TIMESTAMP" {
			return nil, ErrUnsupportedScanType
		}
		tv, ok := src.(time.Time)
		if !ok {
			return nil, ErrUnsupportedScanType
		}
		return tv, nil
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
