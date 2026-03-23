package executor

import "github.com/Khorlane/RovaDB/internal/parser"

func executeDelete(stmt *parser.DeleteStmt, tables map[string]*Table) (int64, error) {
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, errTableDoesNotExist
	}

	if stmt.Where == nil {
		affected := int64(len(table.Rows))
		table.Rows = nil
		return affected, nil
	}

	whereIndex, err := resolveColumnIndex(stmt.Where.Left, table)
	if err != nil {
		return 0, err
	}

	kept := make([][]parser.Value, 0, len(table.Rows))
	var affected int64
	for _, row := range table.Rows {
		match, err := compareValues(stmt.Where.Operator, row[whereIndex], stmt.Where.Right)
		if err != nil {
			return 0, err
		}
		if match {
			affected++
			continue
		}
		kept = append(kept, row)
	}

	table.Rows = kept
	return affected, nil
}
