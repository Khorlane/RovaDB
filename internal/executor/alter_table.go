package executor

import "github.com/Khorlane/RovaDB/internal/parser"

func executeAlterTableAddColumn(stmt *parser.AlterTableAddColumnStmt, tables map[string]*Table) (int64, error) {
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, newTableNotFoundError(stmt.TableName)
	}

	for _, column := range table.Columns {
		if column.Name == stmt.Column.Name {
			return 0, errColumnDoesNotExist
		}
	}

	table.Columns = append(table.Columns, stmt.Column)
	for i := range table.Rows {
		table.Rows[i] = padRowToWidth(table.Rows[i], len(table.Columns))
	}
	if err := rebuildIndexesForTable(table); err != nil {
		return 0, err
	}

	return 0, nil
}
