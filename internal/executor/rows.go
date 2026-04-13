package executor

import "github.com/Khorlane/RovaDB/internal/parser"

// cloneRows is execution-owned row materialization plumbing for statement
// evaluation. It intentionally stays out of planner types and root API row
// wrappers.
func cloneRows(rows [][]parser.Value) [][]parser.Value {
	cloned := make([][]parser.Value, 0, len(rows))
	for _, row := range rows {
		cloned = append(cloned, append([]parser.Value(nil), row...))
	}
	return cloned
}

// ExpandRowToSchema materializes trailing columns that are absent from an older
// row image using the current schema metadata. Existing stored values remain
// untouched; only missing trailing columns are filled.
func ExpandRowToSchema(row []parser.Value, columns []parser.ColumnDef) []parser.Value {
	if len(row) >= len(columns) {
		return row
	}

	expanded := append([]parser.Value(nil), row...)
	for _, column := range columns[len(row):] {
		if column.HasDefault {
			expanded = append(expanded, column.DefaultValue)
			continue
		}
		expanded = append(expanded, parser.NullValue())
	}
	return expanded
}
