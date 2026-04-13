package executor

import "github.com/Khorlane/RovaDB/internal/parser"

var errIntOutOfRange = newExecError("integer out of range")

// publicIntResult keeps executor-owned integer expression results as untyped
// SQL integer literals until a later typed context forces resolution.
func publicIntResult(v int64) (parser.Value, error) {
	if !parser.PublicIntInRange(v) {
		return parser.Value{}, errIntOutOfRange
	}
	return parser.IntegerLiteralValue(v), nil
}
