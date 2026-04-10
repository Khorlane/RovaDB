package executor

import "github.com/Khorlane/RovaDB/internal/storage"

// PrimaryKeyDefinition returns the table-owned primary-key definition when present.
func (t *Table) PrimaryKeyDefinition() *storage.CatalogPrimaryKey {
	if t == nil || t.PrimaryKeyDef == nil {
		return nil
	}
	cloned := *t.PrimaryKeyDef
	cloned.Columns = append([]string(nil), t.PrimaryKeyDef.Columns...)
	return &cloned
}

// ForeignKeyDefinitions returns child foreign-key definitions in table-owned order.
func (t *Table) ForeignKeyDefinitions() []storage.CatalogForeignKey {
	if t == nil || len(t.ForeignKeyDefs) == 0 {
		return nil
	}
	cloned := make([]storage.CatalogForeignKey, 0, len(t.ForeignKeyDefs))
	for _, fk := range t.ForeignKeyDefs {
		cloned = append(cloned, cloneForeignKeyDefinition(fk))
	}
	return cloned
}

// ConstraintDefinitions returns table-owned constraint metadata for later DDL/runtime slices.
func (t *Table) ConstraintDefinitions() (*storage.CatalogPrimaryKey, []storage.CatalogForeignKey) {
	return t.PrimaryKeyDefinition(), t.ForeignKeyDefinitions()
}

// ForeignKeysReferencingTable returns foreign keys whose parent table matches parentTableID.
func ForeignKeysReferencingTable(tables map[string]*Table, parentTableID uint32) []storage.CatalogForeignKey {
	if parentTableID == 0 {
		return nil
	}
	matches := make([]storage.CatalogForeignKey, 0)
	for _, table := range tables {
		if table == nil {
			continue
		}
		for _, fk := range table.ForeignKeyDefs {
			if fk.ParentTableID == parentTableID {
				matches = append(matches, cloneForeignKeyDefinition(fk))
			}
		}
	}
	return matches
}

// ForeignKeysReferencingPrimaryKey returns foreign keys whose parent PK dependency matches exactly.
func ForeignKeysReferencingPrimaryKey(tables map[string]*Table, parentTableID uint32, parentPrimaryKeyName string) []storage.CatalogForeignKey {
	if parentTableID == 0 || parentPrimaryKeyName == "" {
		return nil
	}
	matches := make([]storage.CatalogForeignKey, 0)
	for _, fk := range ForeignKeysReferencingTable(tables, parentTableID) {
		if fk.ParentPrimaryKeyName == parentPrimaryKeyName {
			matches = append(matches, fk)
		}
	}
	return matches
}

// IndexReferencedByConstraint reports whether a PK or FK depends on the given index.
func IndexReferencedByConstraint(tables map[string]*Table, indexID uint32) bool {
	if indexID == 0 {
		return false
	}
	for _, table := range tables {
		if table == nil {
			continue
		}
		if table.PrimaryKeyDef != nil && table.PrimaryKeyDef.IndexID == indexID {
			return true
		}
		for _, fk := range table.ForeignKeyDefs {
			if fk.ChildIndexID == indexID {
				return true
			}
		}
	}
	return false
}

func clonePrimaryKeyDefinition(pk *storage.CatalogPrimaryKey) *storage.CatalogPrimaryKey {
	if pk == nil {
		return nil
	}
	cloned := *pk
	cloned.Columns = append([]string(nil), pk.Columns...)
	return &cloned
}

func cloneForeignKeyDefinition(fk storage.CatalogForeignKey) storage.CatalogForeignKey {
	fk.ChildColumns = append([]string(nil), fk.ChildColumns...)
	fk.ParentColumns = append([]string(nil), fk.ParentColumns...)
	return fk
}
