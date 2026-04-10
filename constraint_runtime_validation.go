package rovadb

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

const (
	relViolationPrimaryKeyNull            = "primary_key_null"
	relViolationPrimaryKeyDuplicate       = "primary_key_duplicate"
	relViolationPrimaryKeyUpdateForbidden = "primary_key_update_forbidden"
	relViolationForeignKeyNull            = "foreign_key_null"
	relViolationForeignKeyMissingParent   = "foreign_key_missing_parent"
	relViolationForeignKeyRestrict        = "foreign_key_restrict"
	relBoundaryForeignKeyCascadeDeferred  = "foreign_key_delete_cascade_deferred"
)

func writeValidationLoadTargets(tables map[string]*executor.Table, stmt any) []string {
	targets := make([]string, 0)
	seen := make(map[string]struct{})
	add := func(name string) {
		if name == "" {
			return
		}
		if _, exists := seen[name]; exists {
			return
		}
		seen[name] = struct{}{}
		targets = append(targets, name)
	}

	switch typed := stmt.(type) {
	case *parser.InsertStmt:
		add(typed.TableName)
		addParentValidationTargets(tables, typed.TableName, add)
	case *parser.UpdateStmt:
		add(typed.TableName)
		addParentValidationTargets(tables, typed.TableName, add)
	case *parser.DeleteStmt:
		add(typed.TableName)
		addReferencingChildTargets(tables, typed.TableName, add)
	}
	return targets
}

func addParentValidationTargets(tables map[string]*executor.Table, tableName string, add func(string)) {
	table := tables[tableName]
	if table == nil {
		return
	}
	tablesByID := tableNamesByID(tables)
	for _, fk := range table.ForeignKeyDefs {
		add(tablesByID[fk.ParentTableID])
	}
}

func addReferencingChildTargets(tables map[string]*executor.Table, tableName string, add func(string)) {
	table := tables[tableName]
	if table == nil || table.PrimaryKeyDef == nil {
		return
	}
	for _, childName := range sortedTableNames(tables) {
		child := tables[childName]
		if child == nil {
			continue
		}
		for _, fk := range child.ForeignKeyDefs {
			if fk.ParentTableID == table.TableID && fk.ParentPrimaryKeyName == table.PrimaryKeyDef.Name {
				add(childName)
				break
			}
		}
	}
}

func validateWriteConstraints(stagedTables map[string]*executor.Table, stmt any, originalTargetRows [][]parser.Value) error {
	switch typed := stmt.(type) {
	case *parser.InsertStmt:
		return validateInsertWriteConstraints(stagedTables, typed.TableName)
	case *parser.UpdateStmt:
		return validateUpdateWriteConstraints(stagedTables, typed.TableName, originalTargetRows)
	case *parser.DeleteStmt:
		return validateDeleteWriteConstraints(stagedTables, typed.TableName)
	default:
		return nil
	}
}

func validateInsertWriteConstraints(tables map[string]*executor.Table, tableName string) error {
	table := tables[tableName]
	if table == nil {
		return newExecError("table not found: " + tableName)
	}
	if err := validateTablePrimaryKeyRows(table); err != nil {
		return err
	}
	return validateTableForeignKeyRows(table, tables)
}

func validateUpdateWriteConstraints(stagedTables map[string]*executor.Table, tableName string, originalTargetRows [][]parser.Value) error {
	stagedTable := stagedTables[tableName]
	if stagedTable == nil {
		return newExecError("table not found: " + tableName)
	}
	if err := validatePrimaryKeyUpdateImmutability(stagedTable, originalTargetRows); err != nil {
		return err
	}
	if err := validateTablePrimaryKeyRows(stagedTable); err != nil {
		return err
	}
	return validateTableForeignKeyRows(stagedTable, stagedTables)
}

func validateDeleteWriteConstraints(tables map[string]*executor.Table, tableName string) error {
	parentTable := tables[tableName]
	if parentTable == nil {
		return newExecError("table not found: " + tableName)
	}
	if parentTable.PrimaryKeyDef == nil {
		return nil
	}

	parentPositions, err := tableColumnPositions(parentTable)
	if err != nil {
		return err
	}
	parentKeys := make(map[string]struct{}, len(parentTable.Rows))
	for _, row := range parentTable.Rows {
		key, err := buildRuntimeConstraintTupleKey(row, parentPositions, parentTable.PrimaryKeyDef.Columns, "")
		if err != nil {
			return err
		}
		parentKeys[key] = struct{}{}
	}

	for _, childName := range sortedTableNames(tables) {
		childTable := tables[childName]
		if childTable == nil {
			continue
		}
		childPositions, err := tableColumnPositions(childTable)
		if err != nil {
			return err
		}
		for _, fk := range sortedReferencingForeignKeys(childTable, parentTable) {
			for _, row := range childTable.Rows {
				key, err := buildRuntimeConstraintTupleKey(row, childPositions, fk.ChildColumns, "")
				if err != nil {
					return err
				}
				if _, exists := parentKeys[key]; exists {
					continue
				}
				if fk.OnDeleteAction == storage.CatalogForeignKeyDeleteActionCascade {
					return newDeferredCascadeDeleteError(childTable.Name, fk.Name)
				}
				return newRelationalConstraintViolation(childTable.Name, fk.Name, relViolationForeignKeyRestrict)
			}
		}
	}
	return nil
}

func validateTablePrimaryKeyRows(table *executor.Table) error {
	if table == nil || table.PrimaryKeyDef == nil {
		return nil
	}
	positions, err := tableColumnPositions(table)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(table.Rows))
	for _, row := range table.Rows {
		key, err := buildRuntimeConstraintTupleKey(row, positions, table.PrimaryKeyDef.Columns, relViolationPrimaryKeyNull)
		if err != nil {
			if relErr, ok := err.(*relationalConstraintError); ok {
				return newRelationalConstraintViolation(table.Name, table.PrimaryKeyDef.Name, relErr.violationType)
			}
			return err
		}
		if _, exists := seen[key]; exists {
			return newRelationalConstraintViolation(table.Name, table.PrimaryKeyDef.Name, relViolationPrimaryKeyDuplicate)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateTableForeignKeyRows(table *executor.Table, tables map[string]*executor.Table) error {
	if table == nil || len(table.ForeignKeyDefs) == 0 {
		return nil
	}
	childPositions, err := tableColumnPositions(table)
	if err != nil {
		return err
	}
	tablesByID := tablesByID(tables)
	for _, fk := range table.ForeignKeyDefs {
		parentTable := tablesByID[fk.ParentTableID]
		if parentTable == nil || parentTable.PrimaryKeyDef == nil {
			return newExecError("constraint/table mismatch")
		}
		parentPositions, err := tableColumnPositions(parentTable)
		if err != nil {
			return err
		}
		parentKeys := make(map[string]struct{}, len(parentTable.Rows))
		for _, row := range parentTable.Rows {
			key, err := buildRuntimeConstraintTupleKey(row, parentPositions, fk.ParentColumns, "")
			if err != nil {
				return err
			}
			parentKeys[key] = struct{}{}
		}
		for _, row := range table.Rows {
			key, err := buildRuntimeConstraintTupleKey(row, childPositions, fk.ChildColumns, relViolationForeignKeyNull)
			if err != nil {
				if relErr, ok := err.(*relationalConstraintError); ok {
					return newRelationalConstraintViolation(table.Name, fk.Name, relErr.violationType)
				}
				return err
			}
			if _, exists := parentKeys[key]; !exists {
				return newRelationalConstraintViolation(table.Name, fk.Name, relViolationForeignKeyMissingParent)
			}
		}
	}
	return nil
}

func validatePrimaryKeyUpdateImmutability(stagedTable *executor.Table, originalRows [][]parser.Value) error {
	if stagedTable == nil || stagedTable.PrimaryKeyDef == nil {
		return nil
	}
	if len(originalRows) != len(stagedTable.Rows) {
		return newStorageError("row locator mismatch")
	}
	positions, err := tableColumnPositions(stagedTable)
	if err != nil {
		return err
	}
	for i := range originalRows {
		beforeKey, err := buildRuntimeConstraintTupleKey(originalRows[i], positions, stagedTable.PrimaryKeyDef.Columns, relViolationPrimaryKeyNull)
		if err != nil {
			if relErr, ok := err.(*relationalConstraintError); ok {
				return newRelationalConstraintViolation(stagedTable.Name, stagedTable.PrimaryKeyDef.Name, relErr.violationType)
			}
			return err
		}
		afterKey, err := buildRuntimeConstraintTupleKey(stagedTable.Rows[i], positions, stagedTable.PrimaryKeyDef.Columns, relViolationPrimaryKeyNull)
		if err != nil {
			if relErr, ok := err.(*relationalConstraintError); ok {
				return newRelationalConstraintViolation(stagedTable.Name, stagedTable.PrimaryKeyDef.Name, relErr.violationType)
			}
			return err
		}
		if beforeKey != afterKey {
			return newRelationalConstraintViolation(stagedTable.Name, stagedTable.PrimaryKeyDef.Name, relViolationPrimaryKeyUpdateForbidden)
		}
	}
	return nil
}

func tableColumnPositions(table *executor.Table) (map[string]int, error) {
	if table == nil {
		return nil, newExecError("constraint/table mismatch")
	}
	positions := make(map[string]int, len(table.Columns))
	for i, column := range table.Columns {
		positions[column.Name] = i
	}
	return positions, nil
}

func buildRuntimeConstraintTupleKey(row []parser.Value, positions map[string]int, columns []string, nullViolationType string) (string, error) {
	parts := make([]string, 0, len(columns))
	for _, columnName := range columns {
		position, ok := positions[columnName]
		if !ok {
			return "", newExecError("constraint/table mismatch")
		}
		if position >= len(row) || row[position].Kind == parser.ValueKindNull {
			if nullViolationType != "" {
				return "", &relationalConstraintError{violationType: nullViolationType}
			}
			return "", nil
		}
		parts = append(parts, runtimeConstraintTupleKeyPart(row[position]))
	}
	return strings.Join(parts, "|"), nil
}

func runtimeConstraintTupleKeyPart(value parser.Value) string {
	switch value.Kind {
	case parser.ValueKindInt64:
		return "int:" + strconv.FormatInt(value.I64, 10)
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

type relationalConstraintError struct {
	tableName      string
	constraintName string
	violationType  string
	deferred       bool
}

func (e *relationalConstraintError) Error() string {
	if e == nil {
		return ""
	}
	if e.deferred {
		return fmt.Sprintf("constraint deferred: table=%s constraint=%s type=%s", e.tableName, e.constraintName, e.violationType)
	}
	return fmt.Sprintf("constraint violation: table=%s constraint=%s type=%s", e.tableName, e.constraintName, e.violationType)
}

func newRelationalConstraintViolation(tableName, constraintName, violationType string) error {
	return newExecError((&relationalConstraintError{
		tableName:      tableName,
		constraintName: constraintName,
		violationType:  violationType,
	}).Error())
}

func newDeferredCascadeDeleteError(tableName, constraintName string) error {
	return fmt.Errorf("%w: %s", ErrNotImplemented, (&relationalConstraintError{
		tableName:      tableName,
		constraintName: constraintName,
		violationType:  relBoundaryForeignKeyCascadeDeferred,
		deferred:       true,
	}).Error())
}

func sortedTableNames(tables map[string]*executor.Table) []string {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func tablesByID(tables map[string]*executor.Table) map[uint32]*executor.Table {
	byID := make(map[uint32]*executor.Table, len(tables))
	for _, table := range tables {
		if table == nil || table.TableID == 0 {
			continue
		}
		byID[table.TableID] = table
	}
	return byID
}

func tableNamesByID(tables map[string]*executor.Table) map[uint32]string {
	byID := make(map[uint32]string, len(tables))
	for name, table := range tables {
		if table == nil || table.TableID == 0 {
			continue
		}
		byID[table.TableID] = name
	}
	return byID
}

func sortedReferencingForeignKeys(childTable, parentTable *executor.Table) []storage.CatalogForeignKey {
	if childTable == nil || parentTable == nil || parentTable.PrimaryKeyDef == nil {
		return nil
	}
	fks := make([]storage.CatalogForeignKey, 0)
	for _, fk := range childTable.ForeignKeyDefs {
		if fk.ParentTableID == parentTable.TableID && fk.ParentPrimaryKeyName == parentTable.PrimaryKeyDef.Name {
			fks = append(fks, fk)
		}
	}
	sort.Slice(fks, func(i, j int) bool {
		return fks[i].Name < fks[j].Name
	})
	return fks
}
