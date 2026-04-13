package executor

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

func buildCreateTableDefinition(stmt *parser.CreateTableStmt, tables map[string]*Table) (*Table, error) {
	if stmt == nil {
		return nil, errUnsupportedStatement
	}

	table := &Table{
		Name:    stmt.Name,
		TableID: nextCreateTableID(tables),
		Columns: append([]parser.ColumnDef(nil), stmt.Columns...),
	}
	for i, column := range table.Columns {
		if !column.HasDefault {
			continue
		}
		normalized, err := normalizeColumnValueForDef(column, column.DefaultValue)
		if err != nil {
			return nil, err
		}
		column.DefaultValue = normalized
		table.Columns[i] = column
	}

	nextIndexID := nextCreateIndexID(tables)
	if stmt.PrimaryKey != nil {
		indexDef := storage.CatalogIndex{
			Name:    stmt.PrimaryKey.IndexName,
			Unique:  true,
			IndexID: nextIndexID,
			Columns: make([]storage.CatalogIndexColumn, 0, len(stmt.PrimaryKey.Columns)),
		}
		nextIndexID++
		for _, column := range stmt.PrimaryKey.Columns {
			indexDef.Columns = append(indexDef.Columns, storage.CatalogIndexColumn{Name: column})
		}
		table.IndexDefs = append(table.IndexDefs, indexDef)
	}
	for _, fk := range stmt.ForeignKeys {
		indexDef := storage.CatalogIndex{
			Name:    fk.IndexName,
			IndexID: nextIndexID,
			Columns: make([]storage.CatalogIndexColumn, 0, len(fk.Columns)),
		}
		nextIndexID++
		for _, column := range fk.Columns {
			indexDef.Columns = append(indexDef.Columns, storage.CatalogIndexColumn{Name: column})
		}
		if !table.HasEquivalentIndexDefinition(indexDef) {
			table.IndexDefs = append(table.IndexDefs, indexDef)
		}
	}

	constraintTables := make(map[string]*Table, len(tables)+1)
	for name, existing := range tables {
		constraintTables[name] = existing
	}
	constraintTables[table.Name] = table

	if err := applyPrimaryKeyConstraint(table, constraintTables, stmt.PrimaryKey); err != nil {
		return nil, err
	}
	for i := range stmt.ForeignKeys {
		if err := applyForeignKeyConstraint(table, constraintTables, &stmt.ForeignKeys[i]); err != nil {
			return nil, err
		}
	}

	return table, nil
}

func applyPrimaryKeyConstraint(table *Table, tables map[string]*Table, pk *parser.PrimaryKeyDef) error {
	if table == nil || pk == nil {
		return nil
	}
	if table.PrimaryKeyDef != nil {
		return newExecError("multiple primary keys not allowed")
	}
	if err := validateConstraintNameAvailable(table, pk.Name); err != nil {
		return err
	}
	indexDef, err := resolveIndexDefinitionForConstraint(table, tables, pk.IndexName)
	if err != nil {
		return err
	}
	if !indexDef.Unique {
		return newExecError("primary key supporting index must be unique")
	}
	if err := requireExactIndexColumns(indexDef, pk.Columns); err != nil {
		return err
	}
	if err := validateExistingPrimaryKeyRows(table, pk.Columns); err != nil {
		return err
	}

	table.PrimaryKeyDef = &storage.CatalogPrimaryKey{
		Name:       pk.Name,
		TableID:    table.TableID,
		Columns:    append([]string(nil), pk.Columns...),
		IndexID:    indexDef.IndexID,
		ImplicitNN: true,
	}
	return nil
}

func applyForeignKeyConstraint(table *Table, tables map[string]*Table, fk *parser.ForeignKeyDef) error {
	if table == nil || fk == nil {
		return nil
	}
	if err := validateConstraintNameAvailable(table, fk.Name); err != nil {
		return err
	}
	if hasDuplicateConstraintColumns(fk.Columns) {
		return newExecError("foreign key child columns must be unique")
	}
	if hasDuplicateConstraintColumns(fk.ParentColumns) {
		return newExecError("foreign key parent columns must be unique")
	}

	parentTable := tables[fk.ParentTable]
	if parentTable == nil {
		return newExecError("foreign key parent table not found")
	}
	if parentTable.PrimaryKeyDef == nil {
		return newExecError("foreign key parent primary key not found")
	}
	if len(fk.Columns) != len(fk.ParentColumns) {
		return newExecError("foreign key column count mismatch")
	}
	if err := requireExactColumnList(parentTable.PrimaryKeyDef.Columns, fk.ParentColumns, "foreign key parent columns must match parent primary key exactly"); err != nil {
		return err
	}
	if err := requireExactTypeMatch(table, fk.Columns, parentTable, fk.ParentColumns); err != nil {
		return err
	}

	indexDef, err := resolveIndexDefinitionForConstraint(table, tables, fk.IndexName)
	if err != nil {
		return err
	}
	if err := requireIndexLeftmostPrefix(indexDef, fk.Columns); err != nil {
		return err
	}
	if err := validateExistingForeignKeyRows(table, fk.Columns, parentTable, fk.ParentColumns); err != nil {
		return err
	}

	table.ForeignKeyDefs = append(table.ForeignKeyDefs, storage.CatalogForeignKey{
		Name:                 fk.Name,
		ChildTableID:         table.TableID,
		ChildColumns:         append([]string(nil), fk.Columns...),
		ParentTableID:        parentTable.TableID,
		ParentColumns:        append([]string(nil), fk.ParentColumns...),
		ParentPrimaryKeyName: parentTable.PrimaryKeyDef.Name,
		ChildIndexID:         indexDef.IndexID,
		OnDeleteAction:       catalogForeignKeyDeleteAction(fk.OnDelete),
	})
	return nil
}

func validateConstraintNameAvailable(table *Table, name string) error {
	if table == nil || name == "" {
		return newExecError("constraint name is required")
	}
	if table.PrimaryKeyDef != nil && table.PrimaryKeyDef.Name == name {
		return newExecError("duplicate constraint name on table")
	}
	for _, fk := range table.ForeignKeyDefs {
		if fk.Name == name {
			return newExecError("duplicate constraint name on table")
		}
	}
	return nil
}

func resolveIndexDefinitionForConstraint(table *Table, tables map[string]*Table, indexName string) (*storage.CatalogIndex, error) {
	if table == nil {
		return nil, errUnsupportedStatement
	}
	if strings.TrimSpace(indexName) == "" {
		return nil, newExecError("supporting index is required")
	}
	indexDef := table.IndexDefinition(indexName)
	if indexDef == nil {
		for _, other := range tables {
			if other == nil || other == table {
				continue
			}
			if other.IndexDefinition(indexName) != nil {
				return nil, newExecError("supporting index belongs to different table")
			}
		}
		return nil, newExecError("supporting index not found")
	}
	return indexDef, nil
}

func requireExactIndexColumns(indexDef *storage.CatalogIndex, columns []string) error {
	if indexDef == nil {
		return newExecError("supporting index not found")
	}
	if len(indexDef.Columns) != len(columns) {
		return newExecError("supporting index shape mismatch")
	}
	for i := range columns {
		if indexDef.Columns[i].Desc || indexDef.Columns[i].Name != columns[i] {
			return newExecError("supporting index shape mismatch")
		}
	}
	return nil
}

func requireIndexLeftmostPrefix(indexDef *storage.CatalogIndex, columns []string) error {
	if indexDef == nil {
		return newExecError("supporting index not found")
	}
	if len(indexDef.Columns) < len(columns) {
		return newExecError("foreign key supporting index shape mismatch")
	}
	for i := range columns {
		if indexDef.Columns[i].Desc || indexDef.Columns[i].Name != columns[i] {
			return newExecError("foreign key supporting index shape mismatch")
		}
	}
	return nil
}

func requireExactColumnList(expected, actual []string, errMsg string) error {
	if len(expected) != len(actual) {
		return newExecError(errMsg)
	}
	for i := range expected {
		if expected[i] != actual[i] {
			return newExecError(errMsg)
		}
	}
	return nil
}

func requireExactTypeMatch(childTable *Table, childColumns []string, parentTable *Table, parentColumns []string) error {
	if len(childColumns) != len(parentColumns) {
		return newExecError("foreign key column count mismatch")
	}
	childPositions, err := columnPositions(childTable)
	if err != nil {
		return err
	}
	parentPositions, err := columnPositions(parentTable)
	if err != nil {
		return err
	}
	for i := range childColumns {
		childPos, ok := childPositions[childColumns[i]]
		if !ok {
			return newColumnNotFoundError(childColumns[i])
		}
		parentPos, ok := parentPositions[parentColumns[i]]
		if !ok {
			return newColumnNotFoundError(parentColumns[i])
		}
		if childTable.Columns[childPos].Type != parentTable.Columns[parentPos].Type {
			return newExecError("foreign key column type mismatch")
		}
	}
	return nil
}

func validateExistingPrimaryKeyRows(table *Table, columns []string) error {
	if table == nil {
		return nil
	}
	positions, err := columnPositions(table)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(table.Rows))
	for _, row := range table.Rows {
		key, err := buildConstraintTupleKey(row, positions, columns, "primary key existing NULL violation")
		if err != nil {
			return err
		}
		if _, exists := seen[key]; exists {
			return newExecError("primary key existing duplicate violation")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateExistingForeignKeyRows(childTable *Table, childColumns []string, parentTable *Table, parentColumns []string) error {
	childPositions, err := columnPositions(childTable)
	if err != nil {
		return err
	}
	parentPositions, err := columnPositions(parentTable)
	if err != nil {
		return err
	}
	parentKeys := make(map[string]struct{}, len(parentTable.Rows))
	for _, row := range parentTable.Rows {
		key, err := buildConstraintTupleKey(row, parentPositions, parentColumns, "")
		if err != nil {
			return err
		}
		parentKeys[key] = struct{}{}
	}
	for _, row := range childTable.Rows {
		key, err := buildConstraintTupleKey(row, childPositions, childColumns, "foreign key existing NULL child row violation")
		if err != nil {
			return err
		}
		if _, exists := parentKeys[key]; !exists {
			return newExecError("foreign key existing row violation")
		}
	}
	return nil
}

func columnPositions(table *Table) (map[string]int, error) {
	if table == nil {
		return nil, errUnsupportedStatement
	}
	positions := make(map[string]int, len(table.Columns))
	for i, column := range table.Columns {
		positions[column.Name] = i
	}
	return positions, nil
}

func buildConstraintTupleKey(row []parser.Value, positions map[string]int, columns []string, nullErr string) (string, error) {
	parts := make([]string, 0, len(columns))
	for _, columnName := range columns {
		position, ok := positions[columnName]
		if !ok {
			return "", newColumnNotFoundError(columnName)
		}
		if position >= len(row) || row[position].Kind == parser.ValueKindNull {
			if nullErr != "" {
				return "", newExecError(nullErr)
			}
			return "", nil
		}
		parts = append(parts, constraintTupleKeyPart(row[position]))
	}
	return strings.Join(parts, "|"), nil
}

func constraintTupleKeyPart(value parser.Value) string {
	switch value.Kind {
	case parser.ValueKindIntegerLiteral, parser.ValueKindSmallInt, parser.ValueKindInt, parser.ValueKindBigInt:
		return "int:" + strconv.FormatInt(value.IntegerValue(), 10)
	case parser.ValueKindString:
		return "string:" + value.Str
	case parser.ValueKindBool:
		if value.Bool {
			return "bool:true"
		}
		return "bool:false"
	case parser.ValueKindReal:
		return "real:" + strconv.FormatUint(math.Float64bits(value.F64), 10)
	default:
		return fmt.Sprintf("kind:%d", value.Kind)
	}
}

func catalogForeignKeyDeleteAction(action parser.ForeignKeyDeleteAction) uint8 {
	switch action {
	case parser.ForeignKeyDeleteActionCascade:
		return storage.CatalogForeignKeyDeleteActionCascade
	default:
		return storage.CatalogForeignKeyDeleteActionRestrict
	}
}

func hasDuplicateConstraintColumns(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			return true
		}
		if _, exists := seen[value]; exists {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}
