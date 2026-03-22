package rovadb

import "github.com/Khorlane/RovaDB/internal/parser"

// Rows represents a result stream from a query.
type Rows struct {
	err   error
	done  bool
	value parser.Value
}

// Next reports whether another row is available.
func (r *Rows) Next() bool {
	if r == nil {
		return false
	}
	if r.err != nil {
		return false
	}
	if r.done {
		return false
	}

	r.done = true
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
	if len(dest) != 1 {
		return ErrInvalidArgument
	}

	switch v := dest[0].(type) {
	case *int:
		if v == nil {
			return ErrInvalidArgument
		}
		if r.value.Kind == parser.ValueKindInt64 {
			*v = int(r.value.I64)
			return nil
		}
	case *int64:
		if v == nil {
			return ErrInvalidArgument
		}
		if r.value.Kind == parser.ValueKindInt64 {
			*v = r.value.I64
			return nil
		}
	case *string:
		if v == nil {
			return ErrInvalidArgument
		}
		if r.value.Kind == parser.ValueKindString {
			*v = r.value.Str
			return nil
		}
	case *any:
		if v == nil {
			return ErrInvalidArgument
		}
		*v = r.value.Any()
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
