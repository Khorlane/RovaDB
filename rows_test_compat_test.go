package rovadb

func (r *Rows) Columns() []string {
	if r == nil {
		return nil
	}
	return append([]string(nil), r.columns...)
}
