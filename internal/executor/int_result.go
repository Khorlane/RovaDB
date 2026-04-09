package executor

import "github.com/Khorlane/RovaDB/internal/parser"

var errIntOutOfRange = newExecError("integer out of range")

// publicIntResult keeps public integer result shaping in the execution layer
// even though the underlying range contract is defined by parser.Value.
func publicIntResult(v int64) (parser.Value, error) {
	value, err := parser.PublicIntValue(v)
	if err != nil {
		return parser.Value{}, errIntOutOfRange
	}
	return value, nil
}
