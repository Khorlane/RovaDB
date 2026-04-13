package executor

import "github.com/Khorlane/RovaDB/internal/parser"

func executeUpdate(stmt *parser.UpdateStmt, tables map[string]*Table) (int64, error) {
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, newTableNotFoundError(stmt.TableName)
	}

	assignments := make([]struct {
		index int
		expr  *parser.ValueExpr
		value parser.Value
	}, 0, len(stmt.Assignments))
	for _, assignment := range stmt.Assignments {
		idx, err := resolveColumnIndex(assignment.Column, table)
		if err != nil {
			return 0, err
		}
		expr := assignment.Expr
		if expr != nil {
			if err := validateValueExprColumns(expr, table); err != nil {
				return 0, err
			}
		} else {
			expr = &parser.ValueExpr{Kind: parser.ValueExprKindLiteral, Value: assignment.Value}
		}
		assignments = append(assignments, struct {
			index int
			expr  *parser.ValueExpr
			value parser.Value
		}{index: idx, expr: expr, value: assignment.Value})
	}
	if err := validateFilterColumns(stmt.Predicate, stmt.Where, table); err != nil {
		return 0, err
	}

	var affected int64
	updatedRows := cloneRows(table.Rows)
	for _, row := range updatedRows {
		match, err := evalFilter(row, table, stmt.Predicate, stmt.Where)
		if err != nil {
			return 0, err
		}
		if !match {
			continue
		}
		for _, assignment := range assignments {
			value, err := evalValueExpr(row, table, assignment.expr)
			if err != nil {
				return 0, err
			}
			normalized, err := normalizeColumnValue(table, assignment.index, value)
			if err != nil {
				return 0, err
			}
			row[assignment.index] = normalized
		}
		affected++
	}
	if err := validateUniqueIndexes(table, updatedRows); err != nil {
		return 0, err
	}
	if err := validateIndexedTextLimits(table, updatedRows); err != nil {
		return 0, err
	}
	table.Rows = updatedRows
	if err := rebuildIndexesForTable(table); err != nil {
		return 0, err
	}

	return affected, nil
}
