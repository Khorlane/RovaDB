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
	// ErrExecDisallowsSelect reports Exec use with a SELECT statement.
	ErrExecDisallowsSelect = errors.New("rovadb: Exec disallows SELECT statement")
	// ErrScanMismatch reports that Scan destination count does not match the current row width.
	ErrScanMismatch = errors.New("rovadb: scan destination count mismatch")
	// ErrUnsupportedScanType reports unsupported or incompatible Scan destination/value types.
	ErrUnsupportedScanType = errors.New("rovadb: unsupported scan type")
	// ErrScanBeforeNext reports Scan without a current row selected by Next.
	ErrScanBeforeNext = errors.New("rovadb: Scan called before Next")
	// ErrRowsClosed reports Scan on a closed Rows value.
	ErrRowsClosed = errors.New("rovadb: rows closed")
	// ErrNoRows reports that QueryRow.Scan found no rows.
	ErrNoRows = errors.New("rovadb: no rows")
	// ErrMultipleRows reports that QueryRow.Scan found more than one row.
	ErrMultipleRows = errors.New("rovadb: multiple rows")
	// ErrTxNotActive reports use of a finished explicit transaction handle.
	ErrTxNotActive = errors.New("rovadb: transaction is not active")
)
