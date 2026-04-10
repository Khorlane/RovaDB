package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestTableConstraintHelpers(t *testing.T) {
	table := &Table{
		PrimaryKeyDef: &storage.CatalogPrimaryKey{
			Name:       "pk_users",
			TableID:    7,
			Columns:    []string{"id"},
			IndexID:    11,
			ImplicitNN: true,
		},
		ForeignKeyDefs: []storage.CatalogForeignKey{
			{
				Name:                 "fk_users_team",
				ChildTableID:         7,
				ChildColumns:         []string{"team_id"},
				ParentTableID:        3,
				ParentColumns:        []string{"id"},
				ParentPrimaryKeyName: "pk_teams",
				ChildIndexID:         13,
				OnDeleteAction:       storage.CatalogForeignKeyDeleteActionCascade,
			},
		},
	}

	pk := table.PrimaryKeyDefinition()
	if pk == nil || pk.Name != "pk_users" || len(pk.Columns) != 1 || pk.Columns[0] != "id" {
		t.Fatalf("PrimaryKeyDefinition() = %#v, want pk_users(id)", pk)
	}
	fks := table.ForeignKeyDefinitions()
	if len(fks) != 1 || fks[0].Name != "fk_users_team" {
		t.Fatalf("ForeignKeyDefinitions() = %#v, want fk_users_team", fks)
	}
	allPK, allFKs := table.ConstraintDefinitions()
	if allPK == nil || len(allFKs) != 1 {
		t.Fatalf("ConstraintDefinitions() = (%#v, %#v), want both definitions", allPK, allFKs)
	}
}

func TestConstraintDependencyHelpers(t *testing.T) {
	tables := map[string]*Table{
		"teams": {
			Name: "teams",
			PrimaryKeyDef: &storage.CatalogPrimaryKey{
				Name:       "pk_teams",
				TableID:    3,
				Columns:    []string{"id"},
				IndexID:    5,
				ImplicitNN: true,
			},
		},
		"users": {
			Name: "users",
			PrimaryKeyDef: &storage.CatalogPrimaryKey{
				Name:       "pk_users",
				TableID:    7,
				Columns:    []string{"id"},
				IndexID:    11,
				ImplicitNN: true,
			},
			ForeignKeyDefs: []storage.CatalogForeignKey{
				{
					Name:                 "fk_users_team",
					ChildTableID:         7,
					ChildColumns:         []string{"team_id"},
					ParentTableID:        3,
					ParentColumns:        []string{"id"},
					ParentPrimaryKeyName: "pk_teams",
					ChildIndexID:         13,
					OnDeleteAction:       storage.CatalogForeignKeyDeleteActionRestrict,
				},
			},
		},
	}

	if got := ForeignKeysReferencingTable(tables, 3); len(got) != 1 || got[0].Name != "fk_users_team" {
		t.Fatalf("ForeignKeysReferencingTable() = %#v, want fk_users_team", got)
	}
	if got := ForeignKeysReferencingPrimaryKey(tables, 3, "pk_teams"); len(got) != 1 || got[0].Name != "fk_users_team" {
		t.Fatalf("ForeignKeysReferencingPrimaryKey() = %#v, want fk_users_team", got)
	}
	if !IndexReferencedByConstraint(tables, 5) {
		t.Fatal("IndexReferencedByConstraint(pk index) = false, want true")
	}
	if !IndexReferencedByConstraint(tables, 13) {
		t.Fatal("IndexReferencedByConstraint(fk index) = false, want true")
	}
	if IndexReferencedByConstraint(tables, 99) {
		t.Fatal("IndexReferencedByConstraint(unused index) = true, want false")
	}
}
