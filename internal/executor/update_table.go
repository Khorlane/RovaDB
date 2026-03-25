package executor

import "github.com/Khorlane/RovaDB/internal/parser"

func executeUpdate(stmt *parser.UpdateStmt, tables map[string]*Table) (int64, error) {
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, errTableDoesNotExist
	}

	assignments := make([]struct {
		index int
		value parser.Value
	}, 0, len(stmt.Assignments))
	for _, assignment := range stmt.Assignments {
		idx, err := resolveColumnIndex(assignment.Column, table)
		if err != nil {
			return 0, err
		}
		if !valueMatchesColumnType(assignment.Value, table.Columns[idx].Type) {
			return 0, errTypeMismatch
		}
		assignments = append(assignments, struct {
			index int
			value parser.Value
		}{index: idx, value: assignment.Value})
	}
	if err := validatePredicateOrWhereColumns(stmt.Predicate, stmt.Where, table); err != nil {
		return 0, err
	}

	var affected int64
	for _, row := range table.Rows {
		match, err := evalPredicateOrWhere(row, table, stmt.Predicate, stmt.Where)
		if err != nil {
			return 0, err
		}
		if !match {
			continue
		}
		for _, assignment := range assignments {
			row[assignment.index] = assignment.value
		}
		affected++
	}
	if err := rebuildIndexesForTable(table); err != nil {
		return 0, err
	}

	return affected, nil
}
