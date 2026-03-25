package executor

import "github.com/Khorlane/RovaDB/internal/parser"

func executeDelete(stmt *parser.DeleteStmt, tables map[string]*Table) (int64, error) {
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, errTableDoesNotExist
	}

	if stmt.Where == nil && stmt.Predicate == nil {
		affected := int64(len(table.Rows))
		table.Rows = nil
		if err := rebuildIndexesForTable(table); err != nil {
			return 0, err
		}
		return affected, nil
	}
	if err := validatePredicateOrWhereColumns(stmt.Predicate, stmt.Where, table); err != nil {
		return 0, err
	}

	kept := make([][]parser.Value, 0, len(table.Rows))
	var affected int64
	for _, row := range table.Rows {
		match, err := evalPredicateOrWhere(row, table, stmt.Predicate, stmt.Where)
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
	if err := rebuildIndexesForTable(table); err != nil {
		return 0, err
	}
	return affected, nil
}
