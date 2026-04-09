package executor

import "github.com/Khorlane/RovaDB/internal/parser"

// cloneRows is execution-owned row materialization plumbing for statement
// evaluation. It intentionally stays out of root API row wrappers.
func cloneRows(rows [][]parser.Value) [][]parser.Value {
	cloned := make([][]parser.Value, 0, len(rows))
	for _, row := range rows {
		cloned = append(cloned, append([]parser.Value(nil), row...))
	}
	return cloned
}
