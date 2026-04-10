package executor

import (
	"strings"
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestApplyPrimaryKeyConstraintRejectsSupportingIndexOnDifferentTable(t *testing.T) {
	child := &Table{
		Name:    "users",
		TableID: 1,
		Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}},
	}
	parent := &Table{
		Name:    "teams",
		TableID: 2,
		Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}},
		IndexDefs: []storage.CatalogIndex{{
			Name:    "idx_shared_pk",
			Unique:  true,
			IndexID: 7,
			Columns: []storage.CatalogIndexColumn{{Name: "id"}},
		}},
	}

	err := applyPrimaryKeyConstraint(child, map[string]*Table{"users": child, "teams": parent}, &parser.PrimaryKeyDef{
		Name:      "pk_users",
		Columns:   []string{"id"},
		IndexName: "idx_shared_pk",
	})
	if err == nil || !strings.Contains(err.Error(), "different table") {
		t.Fatalf("applyPrimaryKeyConstraint() error = %v, want different table rejection", err)
	}
}

func TestApplyForeignKeyConstraintRejectsSupportingIndexOnDifferentTable(t *testing.T) {
	parent := &Table{
		Name:    "teams",
		TableID: 1,
		Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}},
		PrimaryKeyDef: &storage.CatalogPrimaryKey{
			Name:       "pk_teams",
			TableID:    1,
			Columns:    []string{"id"},
			IndexID:    9,
			ImplicitNN: true,
		},
	}
	child := &Table{
		Name:    "users",
		TableID: 2,
		Columns: []parser.ColumnDef{{Name: "team_id", Type: parser.ColumnTypeInt}},
	}
	other := &Table{
		Name:    "audit",
		TableID: 3,
		Columns: []parser.ColumnDef{{Name: "team_id", Type: parser.ColumnTypeInt}},
		IndexDefs: []storage.CatalogIndex{{
			Name:    "idx_users_team",
			IndexID: 11,
			Columns: []storage.CatalogIndexColumn{{Name: "team_id"}},
		}},
	}

	err := applyForeignKeyConstraint(child, map[string]*Table{"teams": parent, "users": child, "audit": other}, &parser.ForeignKeyDef{
		Name:          "fk_users_team",
		Columns:       []string{"team_id"},
		ParentTable:   "teams",
		ParentColumns: []string{"id"},
		IndexName:     "idx_users_team",
		OnDelete:      parser.ForeignKeyDeleteActionRestrict,
	})
	if err == nil || !strings.Contains(err.Error(), "different table") {
		t.Fatalf("applyForeignKeyConstraint() error = %v, want different table rejection", err)
	}
}

func TestApplyPrimaryKeyConstraintRejectsExistingNullsAndDuplicates(t *testing.T) {
	t.Run("nulls", func(t *testing.T) {
		table := &Table{
			Name:    "users",
			TableID: 1,
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}},
			Rows: [][]parser.Value{
				{parser.Int64Value(1)},
				{parser.NullValue()},
			},
			IndexDefs: []storage.CatalogIndex{{
				Name:    "idx_users_pk",
				Unique:  true,
				IndexID: 7,
				Columns: []storage.CatalogIndexColumn{{Name: "id"}},
			}},
		}

		err := applyPrimaryKeyConstraint(table, map[string]*Table{"users": table}, &parser.PrimaryKeyDef{
			Name:      "pk_users",
			Columns:   []string{"id"},
			IndexName: "idx_users_pk",
		})
		if err == nil || !strings.Contains(err.Error(), "NULL violation") {
			t.Fatalf("applyPrimaryKeyConstraint() error = %v, want NULL violation", err)
		}
	})

	t.Run("duplicates", func(t *testing.T) {
		table := &Table{
			Name:    "users",
			TableID: 1,
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}},
			Rows: [][]parser.Value{
				{parser.Int64Value(1)},
				{parser.Int64Value(1)},
			},
			IndexDefs: []storage.CatalogIndex{{
				Name:    "idx_users_pk",
				Unique:  true,
				IndexID: 7,
				Columns: []storage.CatalogIndexColumn{{Name: "id"}},
			}},
		}

		err := applyPrimaryKeyConstraint(table, map[string]*Table{"users": table}, &parser.PrimaryKeyDef{
			Name:      "pk_users",
			Columns:   []string{"id"},
			IndexName: "idx_users_pk",
		})
		if err == nil || !strings.Contains(err.Error(), "duplicate violation") {
			t.Fatalf("applyPrimaryKeyConstraint() error = %v, want duplicate violation", err)
		}
	})
}
