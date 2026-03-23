package rovadb

import "github.com/Khorlane/RovaDB/internal/parser"

// Rows represents a result stream from a query.
type Rows struct {
	err    error
	index  int
	values [][]parser.Value
}

// Next reports whether another row is available.
func (r *Rows) Next() bool {
	if r == nil {
		return false
	}
	if r.err != nil {
		return false
	}
	if r.index+1 >= len(r.values) {
		r.index = len(r.values)
		return false
	}

	r.index++
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
	if r.index < 0 || r.index >= len(r.values) {
		return ErrInvalidArgument
	}
	row := r.values[r.index]
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

func scanValue(dest any, value parser.Value) error {
	switch v := dest.(type) {
	case *int:
		if v == nil {
			return ErrInvalidArgument
		}
		if value.Kind == parser.ValueKindInt64 {
			*v = int(value.I64)
			return nil
		}
	case *int64:
		if v == nil {
			return ErrInvalidArgument
		}
		if value.Kind == parser.ValueKindInt64 {
			*v = value.I64
			return nil
		}
	case *string:
		if v == nil {
			return ErrInvalidArgument
		}
		if value.Kind == parser.ValueKindString {
			*v = value.Str
			return nil
		}
	case *any:
		if v == nil {
			return ErrInvalidArgument
		}
		*v = value.Any()
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
