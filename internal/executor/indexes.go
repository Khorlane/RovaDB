package executor

import (
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

func rebuildIndexesForTable(table *Table) error {
	if table == nil || len(table.Indexes) == 0 {
		return nil
	}

	columnNames := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		columnNames = append(columnNames, column.Name)
	}

	for columnName, index := range table.Indexes {
		if index == nil {
			table.Indexes[columnName] = planner.NewBasicIndex(table.Name, columnName)
			index = table.Indexes[columnName]
		}
		if err := index.Rebuild(columnNames, table.Rows); err != nil {
			return err
		}
	}

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
