package executor

import "github.com/Khorlane/RovaDB/internal/dberr"

func newExecError(msg string) error {
	return dberr.NewExec(msg)
}
