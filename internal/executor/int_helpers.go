package executor

import "github.com/Khorlane/RovaDB/internal/parser"

var errIntOutOfRange = newExecError("integer out of range")

func publicIntResult(v int64) (parser.Value, error) {
	value, err := parser.PublicIntValue(v)
	if err != nil {
		return parser.Value{}, errIntOutOfRange
	}
	return value, nil
}
