package executor

import (
	"strings"
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestValidateCascadeGraphLegalityRejectsCascadeCycles(t *testing.T) {
	t.Run("self cycle", func(t *testing.T) {
		tables := cascadeGraphTables(
			cascadeGraphTable("nodes", 1, cascadeFK("nodes", 1, 1, "fk_nodes_parent", storage.CatalogForeignKeyDeleteActionCascade)),
		)

		err := ValidateCascadeGraphLegality(tables)
		if err == nil || !strings.Contains(err.Error(), "cascade cycle detected") || !strings.Contains(err.Error(), "nodes.fk_nodes_parent") {
			t.Fatalf("ValidateCascadeGraphLegality() error = %v, want self-cycle rejection", err)
		}
	})

	t.Run("multi table cycle", func(t *testing.T) {
		tables := cascadeGraphTables(
			cascadeGraphTable("a", 1, cascadeFK("b", 2, 1, "fk_b_a", storage.CatalogForeignKeyDeleteActionCascade)),
			cascadeGraphTable("b", 2, cascadeFK("c", 3, 2, "fk_c_b", storage.CatalogForeignKeyDeleteActionCascade)),
			cascadeGraphTable("c", 3, cascadeFK("a", 1, 3, "fk_a_c", storage.CatalogForeignKeyDeleteActionCascade)),
		)

		err := ValidateCascadeGraphLegality(tables)
		if err == nil || !strings.Contains(err.Error(), "cascade cycle detected") {
			t.Fatalf("ValidateCascadeGraphLegality() error = %v, want cycle rejection", err)
		}
		for _, label := range []string{"a.fk_b_a", "b.fk_c_b", "c.fk_a_c"} {
			if !strings.Contains(err.Error(), label) {
				t.Fatalf("ValidateCascadeGraphLegality() error = %v, want %s in cycle error", err, label)
			}
		}
	})
}

func TestValidateCascadeGraphLegalityAllowsRestrictBrokenCycles(t *testing.T) {
	tables := cascadeGraphTables(
		cascadeGraphTable("a", 1, cascadeFK("b", 2, 1, "fk_b_a", storage.CatalogForeignKeyDeleteActionCascade)),
		cascadeGraphTable("b", 2, cascadeFK("a", 1, 2, "fk_a_b", storage.CatalogForeignKeyDeleteActionRestrict)),
	)

	if err := ValidateCascadeGraphLegality(tables); err != nil {
		t.Fatalf("ValidateCascadeGraphLegality() error = %v, want nil", err)
	}
}

func TestValidateCascadeGraphLegalityRejectsMultipleCascadePaths(t *testing.T) {
	tables := cascadeGraphTables(
		cascadeGraphTable("a", 1, cascadeFK("b", 2, 1, "fk_b_a", storage.CatalogForeignKeyDeleteActionCascade), cascadeFK("c", 3, 1, "fk_c_a", storage.CatalogForeignKeyDeleteActionCascade)),
		cascadeGraphTable("b", 2, cascadeFK("d", 4, 2, "fk_d_b", storage.CatalogForeignKeyDeleteActionCascade)),
		cascadeGraphTable("c", 3, cascadeFK("d", 4, 3, "fk_d_c", storage.CatalogForeignKeyDeleteActionCascade)),
		cascadeGraphTable("d", 4),
	)

	err := ValidateCascadeGraphLegality(tables)
	if err == nil || !strings.Contains(err.Error(), "multiple cascade paths detected") {
		t.Fatalf("ValidateCascadeGraphLegality() error = %v, want multiple-path rejection", err)
	}
	for _, part := range []string{"source=a", "target=d", "b.fk_d_b", "c.fk_d_c"} {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("ValidateCascadeGraphLegality() error = %v, want %s in multiple-path error", err, part)
		}
	}
}

func TestValidateCascadeGraphLegalityAllowsSingleCascadePathWithRestrictAlternative(t *testing.T) {
	tables := cascadeGraphTables(
		cascadeGraphTable("a", 1, cascadeFK("b", 2, 1, "fk_b_a", storage.CatalogForeignKeyDeleteActionCascade), cascadeFK("c", 3, 1, "fk_c_a", storage.CatalogForeignKeyDeleteActionRestrict)),
		cascadeGraphTable("b", 2, cascadeFK("d", 4, 2, "fk_d_b", storage.CatalogForeignKeyDeleteActionCascade)),
		cascadeGraphTable("c", 3, cascadeFK("d", 4, 3, "fk_d_c", storage.CatalogForeignKeyDeleteActionRestrict)),
		cascadeGraphTable("d", 4),
	)

	if err := ValidateCascadeGraphLegality(tables); err != nil {
		t.Fatalf("ValidateCascadeGraphLegality() error = %v, want nil", err)
	}
}

func cascadeGraphTables(tables ...*Table) map[string]*Table {
	out := make(map[string]*Table, len(tables))
	for _, table := range tables {
		out[table.Name] = table
	}
	return out
}

func cascadeGraphTable(name string, tableID uint32, fks ...storage.CatalogForeignKey) *Table {
	return &Table{
		Name:           name,
		TableID:        tableID,
		ForeignKeyDefs: append([]storage.CatalogForeignKey(nil), fks...),
	}
}

func cascadeFK(_ string, childID uint32, parentID uint32, constraintName string, action uint8) storage.CatalogForeignKey {
	return storage.CatalogForeignKey{
		Name:           constraintName,
		ChildTableID:   childID,
		ParentTableID:  parentID,
		ChildColumns:   []string{"id"},
		ParentColumns:  []string{"id"},
		ChildIndexID:   childID,
		OnDeleteAction: action,
	}
}
