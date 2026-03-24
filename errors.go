package rovadb

import (
	"errors"

	"github.com/Khorlane/RovaDB/internal/dberr"
)

type ErrorKind = dberr.ErrorKind

const (
	ErrParse   ErrorKind = dberr.ErrParse
	ErrPlan    ErrorKind = dberr.ErrPlan
	ErrExec    ErrorKind = dberr.ErrExec
	ErrStorage ErrorKind = dberr.ErrStorage
)

type DBError = dberr.DBError

func newParseError(msg string) error {
	return dberr.NewParse(msg)
}

func newExecError(msg string) error {
	return dberr.NewExec(msg)
}

func newStorageError(msg string) error {
	return dberr.NewStorage(msg)
}

var (
	// ErrNotImplemented marks API surface that exists before engine behavior does.
	ErrNotImplemented = errors.New("rovadb: not implemented")
	// ErrClosed reports use of a closed database handle.
	ErrClosed = errors.New("rovadb: closed")
	// ErrInvalidArgument reports invalid input provided to the API.
	ErrInvalidArgument = errors.New("rovadb: invalid argument")
	// ErrQueryRequiresSelect reports Query use with a non-SELECT statement.
	ErrQueryRequiresSelect = errors.New("rovadb: Query requires SELECT statement")
)
