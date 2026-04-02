package executor

import "github.com/Khorlane/RovaDB/internal/storage"

// IndexDefinition returns the named index definition when present.
func (t *Table) IndexDefinition(name string) *storage.CatalogIndex {
	if t == nil {
		return nil
	}
	for i := range t.IndexDefs {
		if t.IndexDefs[i].Name == name {
			return &t.IndexDefs[i]
		}
	}
	return nil
}

// HasEquivalentIndexDefinition reports whether the given definition already exists.
func (t *Table) HasEquivalentIndexDefinition(candidate storage.CatalogIndex) bool {
	if t == nil {
		return false
	}
	for _, existing := range t.IndexDefs {
		if EquivalentIndexDefinition(existing, candidate) {
			return true
		}
	}
	return false
}

// EquivalentIndexDefinition reports semantic equality between two definitions.
func EquivalentIndexDefinition(left, right storage.CatalogIndex) bool {
	if left.Unique != right.Unique || len(left.Columns) != len(right.Columns) {
		return false
	}
	for i := range left.Columns {
		if left.Columns[i].Name != right.Columns[i].Name || left.Columns[i].Desc != right.Columns[i].Desc {
			return false
		}
	}
	return true
}
