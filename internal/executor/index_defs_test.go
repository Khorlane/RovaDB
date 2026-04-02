package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestEquivalentIndexDefinitionIgnoresNameButRespectsShape(t *testing.T) {
	left := storage.CatalogIndex{
		Name:   "idx_a",
		Unique: true,
		Columns: []storage.CatalogIndexColumn{
			{Name: "first"},
			{Name: "last", Desc: true},
		},
	}
	right := storage.CatalogIndex{
		Name:   "idx_b",
		Unique: true,
		Columns: []storage.CatalogIndexColumn{
			{Name: "first"},
			{Name: "last", Desc: true},
		},
	}
	if !EquivalentIndexDefinition(left, right) {
		t.Fatal("EquivalentIndexDefinition() = false, want true")
	}
}

func TestTableIndexDefinitionHelpers(t *testing.T) {
	table := &Table{
		IndexDefs: []storage.CatalogIndex{
			{
				Name: "idx_users_name",
				Columns: []storage.CatalogIndexColumn{
					{Name: "name"},
				},
			},
		},
	}

	if table.IndexDefinition("idx_users_name") == nil {
		t.Fatal("IndexDefinition(idx_users_name) = nil, want non-nil")
	}
	if table.IndexDefinition("missing") != nil {
		t.Fatal("IndexDefinition(missing) != nil, want nil")
	}
	if !table.HasEquivalentIndexDefinition(storage.CatalogIndex{
		Name: "idx_duplicate_shape",
		Columns: []storage.CatalogIndexColumn{
			{Name: "name"},
		},
	}) {
		t.Fatal("HasEquivalentIndexDefinition() = false, want true")
	}
}
