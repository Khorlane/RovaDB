package rovadb

// Rows represents a result stream from a query.
type Rows struct{}

// Close releases any resources held by the row stream.
func (r *Rows) Close() error {
	return nil
}
