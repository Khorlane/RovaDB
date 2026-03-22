package rovadb

import "errors"

// ErrNotImplemented marks API surface that exists before engine behavior does.
var ErrNotImplemented = errors.New("rovadb: not implemented")
