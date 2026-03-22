package rovadb

import "errors"

var (
	// ErrNotImplemented marks API surface that exists before engine behavior does.
	ErrNotImplemented = errors.New("rovadb: not implemented")
	// ErrClosed reports use of a closed database handle.
	ErrClosed = errors.New("rovadb: closed")
	// ErrInvalidArgument reports invalid input provided to the API.
	ErrInvalidArgument = errors.New("rovadb: invalid argument")
)
