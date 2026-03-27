package executor

import (
	"github.com/Khorlane/RovaDB/internal/parser"
)

const indexedTextByteLimit = 512

var errIndexedTextTooLarge = newExecError("indexed TEXT column value exceeds 512-byte limit")

// ValidateIndexedTextLimitsForTable validates all indexed TEXT values in the
// table's current row set against the V1 byte-length limit.
func ValidateIndexedTextLimitsForTable(table *Table) error {
	if table == nil {
		return nil
	}
	return validateIndexedTextLimits(table, table.Rows)
}

func validateIndexedTextLimits(table *Table, rows [][]parser.Value) error {
	if table == nil || len(table.IndexDefs) == 0 {
		return nil
	}

	textColumnPositions := make(map[int]struct{})
	columnPositions := make(map[string]int, len(table.Columns))
	for i, column := range table.Columns {
		columnPositions[column.Name] = i
		if column.Type == parser.ColumnTypeText {
			textColumnPositions[i] = struct{}{}
		}
	}

	indexedTextPositions := make(map[int]struct{})
	for _, indexDef := range table.IndexDefs {
		for _, indexColumn := range indexDef.Columns {
			position, ok := columnPositions[indexColumn.Name]
			if !ok {
				return errColumnDoesNotExist
			}
			if _, ok := textColumnPositions[position]; ok {
				indexedTextPositions[position] = struct{}{}
			}
		}
	}
	if len(indexedTextPositions) == 0 {
		return nil
	}

	for _, row := range rows {
		for position := range indexedTextPositions {
			if position >= len(row) {
				continue
			}
			value := row[position]
			if value.Kind == parser.ValueKindNull {
				continue
			}
			if value.Kind == parser.ValueKindString && len(value.Str) > indexedTextByteLimit {
				return errIndexedTextTooLarge
			}
		}
	}

	return nil
}
