package rovadb

// Result represents the outcome of a write operation.
type Result struct {
	rowsAffected int64
}

// RowsAffected reports the number of rows changed by a write operation.
func (r Result) RowsAffected() int {
	return int(r.rowsAffected)
}
