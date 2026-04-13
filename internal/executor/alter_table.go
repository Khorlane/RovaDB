package executor

import (
	"fmt"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

func executeAlterTableAddColumn(stmt *parser.AlterTableAddColumnStmt, tables map[string]*Table) (int64, error) {
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, newTableNotFoundError(stmt.TableName)
	}
	column := stmt.Column

	for _, column := range table.Columns {
		if column.Name == stmt.Column.Name {
			return 0, errColumnDoesNotExist
		}
	}
	if column.NotNull && !column.HasDefault && tableHasRows(table) {
		return 0, newExecError("cannot add NOT NULL column without DEFAULT to non-empty table")
	}
	if column.HasDefault {
		normalized, err := normalizeColumnValueForDef(column, column.DefaultValue)
		if err != nil {
			return 0, err
		}
		column.DefaultValue = normalized
	}

	table.Columns = append(table.Columns, column)
	for i := range table.Rows {
		table.Rows[i] = ExpandRowToSchema(table.Rows[i], table.Columns)
	}
	if err := rebuildIndexesForTable(table); err != nil {
		return 0, err
	}

	return 0, nil
}

func tableHasRows(table *Table) bool {
	if table == nil {
		return false
	}
	if table.Rows != nil {
		return len(table.Rows) != 0
	}
	return table.PersistedRowCount() != 0
}

func executeAlterTableAddPrimaryKey(stmt *parser.AlterTableAddPrimaryKeyStmt, tables map[string]*Table) (int64, error) {
	if stmt == nil {
		return 0, errUnsupportedStatement
	}
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, newTableNotFoundError(stmt.TableName)
	}
	if err := applyPrimaryKeyConstraint(table, tables, &stmt.PrimaryKey); err != nil {
		return 0, err
	}
	return 0, nil
}

func executeAlterTableAddForeignKey(stmt *parser.AlterTableAddForeignKeyStmt, tables map[string]*Table) (int64, error) {
	if stmt == nil {
		return 0, errUnsupportedStatement
	}
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, newTableNotFoundError(stmt.TableName)
	}
	if err := applyForeignKeyConstraint(table, tables, &stmt.ForeignKey); err != nil {
		return 0, err
	}
	return 0, nil
}

func executeAlterTableDropPrimaryKey(stmt *parser.AlterTableDropPrimaryKeyStmt, tables map[string]*Table) (int64, error) {
	if stmt == nil {
		return 0, errUnsupportedStatement
	}
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, newTableNotFoundError(stmt.TableName)
	}
	if table.PrimaryKeyDef == nil {
		return 0, newExecError(fmt.Sprintf("primary key not found: table=%s", stmt.TableName))
	}

	parentTableID := table.TableID
	parentPrimaryKeyName := table.PrimaryKeyDef.Name
	table.PrimaryKeyDef = nil

	for _, other := range tables {
		if other == nil || len(other.ForeignKeyDefs) == 0 {
			continue
		}
		filtered := other.ForeignKeyDefs[:0]
		for _, fk := range other.ForeignKeyDefs {
			if fk.ParentTableID == parentTableID && fk.ParentPrimaryKeyName == parentPrimaryKeyName {
				continue
			}
			filtered = append(filtered, fk)
		}
		other.ForeignKeyDefs = filtered
	}

	return 0, nil
}

func executeAlterTableDropForeignKey(stmt *parser.AlterTableDropForeignKeyStmt, tables map[string]*Table) (int64, error) {
	if stmt == nil {
		return 0, errUnsupportedStatement
	}
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, newTableNotFoundError(stmt.TableName)
	}
	filtered := make([]storage.CatalogForeignKey, 0, len(table.ForeignKeyDefs))
	removed := false
	for _, fk := range table.ForeignKeyDefs {
		if fk.Name == stmt.ConstraintName {
			removed = true
			continue
		}
		filtered = append(filtered, fk)
	}
	if !removed {
		return 0, newExecError(fmt.Sprintf("foreign key not found: table=%s constraint=%s", stmt.TableName, stmt.ConstraintName))
	}
	table.ForeignKeyDefs = filtered
	return 0, nil
}
