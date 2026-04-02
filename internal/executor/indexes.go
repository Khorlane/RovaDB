package executor

import "github.com/Khorlane/RovaDB/internal/parser"

func rebuildIndexesForTable(table *Table) error {
	return nil
}

func padRowToWidth(row []parser.Value, width int) []parser.Value {
	if len(row) >= width {
		return row
	}

	padded := append([]parser.Value(nil), row...)
	for len(padded) < width {
		padded = append(padded, parser.NullValue())
	}
	return padded
}
