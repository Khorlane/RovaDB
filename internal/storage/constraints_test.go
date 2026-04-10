package storage

import "testing"

func TestCatalogConstraintMetadataRoundTrip(t *testing.T) {
	payload, err := encodeCatalogPayload(&CatalogData{
		Tables: []CatalogTable{
			{
				Name:       "teams",
				TableID:    3,
				RootPageID: 1,
				Columns: []CatalogColumn{
					{Name: "id", Type: CatalogColumnTypeInt},
				},
				Indexes: []CatalogIndex{
					{Name: "idx_teams_pk", IndexID: 5, Columns: []CatalogIndexColumn{{Name: "id"}}},
				},
				PrimaryKey: &CatalogPrimaryKey{
					Name:       "pk_teams",
					TableID:    3,
					Columns:    []string{"id"},
					IndexID:    5,
					ImplicitNN: true,
				},
			},
			{
				Name:       "users",
				TableID:    7,
				RootPageID: 2,
				Columns: []CatalogColumn{
					{Name: "id", Type: CatalogColumnTypeInt},
					{Name: "team_id", Type: CatalogColumnTypeInt},
				},
				Indexes: []CatalogIndex{
					{Name: "idx_users_pk", IndexID: 11, Columns: []CatalogIndexColumn{{Name: "id"}}},
					{Name: "idx_users_team", IndexID: 13, Columns: []CatalogIndexColumn{{Name: "team_id"}}},
				},
				PrimaryKey: &CatalogPrimaryKey{
					Name:       "pk_users",
					TableID:    7,
					Columns:    []string{"id"},
					IndexID:    11,
					ImplicitNN: true,
				},
				ForeignKeys: []CatalogForeignKey{
					{
						Name:                 "fk_users_team",
						ChildTableID:         7,
						ChildColumns:         []string{"team_id"},
						ParentTableID:        3,
						ParentColumns:        []string{"id"},
						ParentPrimaryKeyName: "pk_teams",
						ChildIndexID:         13,
						OnDeleteAction:       CatalogForeignKeyDeleteActionCascade,
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("encodeCatalogPayload() error = %v", err)
	}

	_, got, err := decodeCatalogPayload(payload)
	if err != nil {
		t.Fatalf("decodeCatalogPayload() error = %v", err)
	}
	if got.Tables[0].PrimaryKey == nil || got.Tables[0].PrimaryKey.Name != "pk_teams" {
		t.Fatalf("teams PrimaryKey = %#v, want pk_teams", got.Tables[0].PrimaryKey)
	}
	if got.Tables[1].PrimaryKey == nil || got.Tables[1].PrimaryKey.Name != "pk_users" {
		t.Fatalf("users PrimaryKey = %#v, want pk_users", got.Tables[1].PrimaryKey)
	}
	if len(got.Tables[1].ForeignKeys) != 1 {
		t.Fatalf("len(users ForeignKeys) = %d, want 1", len(got.Tables[1].ForeignKeys))
	}
	fk := got.Tables[1].ForeignKeys[0]
	if fk.OnDeleteAction != CatalogForeignKeyDeleteActionCascade {
		t.Fatalf("fk.OnDeleteAction = %d, want %d", fk.OnDeleteAction, CatalogForeignKeyDeleteActionCascade)
	}
	if fk.ParentPrimaryKeyName != "pk_teams" || fk.ChildIndexID != 13 {
		t.Fatalf("fk = %#v, want parent pk dependency and child index preserved", fk)
	}
}

func TestDecodeCatalogPayloadRejectsMalformedConstraintMetadata(t *testing.T) {
	tests := []struct {
		name  string
		table CatalogTable
	}{
		{
			name: "pk missing name",
			table: CatalogTable{
				Name:       "users",
				TableID:    7,
				RootPageID: 1,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
				Indexes:    []CatalogIndex{{Name: "idx_users_pk", IndexID: 11, Columns: []CatalogIndexColumn{{Name: "id"}}}},
				PrimaryKey: &CatalogPrimaryKey{TableID: 7, Columns: []string{"id"}, IndexID: 11, ImplicitNN: true},
			},
		},
		{
			name: "fk empty child columns",
			table: CatalogTable{
				Name:       "users",
				TableID:    7,
				RootPageID: 1,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}, {Name: "team_id", Type: CatalogColumnTypeInt}},
				Indexes:    []CatalogIndex{{Name: "idx_users_team", IndexID: 13, Columns: []CatalogIndexColumn{{Name: "team_id"}}}},
				ForeignKeys: []CatalogForeignKey{{
					Name:                 "fk_users_team",
					ChildTableID:         7,
					ParentTableID:        3,
					ParentColumns:        []string{"id"},
					ParentPrimaryKeyName: "pk_teams",
					ChildIndexID:         13,
					OnDeleteAction:       CatalogForeignKeyDeleteActionRestrict,
				}},
			},
		},
		{
			name: "fk invalid delete action",
			table: CatalogTable{
				Name:       "users",
				TableID:    7,
				RootPageID: 1,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}, {Name: "team_id", Type: CatalogColumnTypeInt}},
				Indexes:    []CatalogIndex{{Name: "idx_users_team", IndexID: 13, Columns: []CatalogIndexColumn{{Name: "team_id"}}}},
				ForeignKeys: []CatalogForeignKey{{
					Name:                 "fk_users_team",
					ChildTableID:         7,
					ChildColumns:         []string{"team_id"},
					ParentTableID:        3,
					ParentColumns:        []string{"id"},
					ParentPrimaryKeyName: "pk_teams",
					ChildIndexID:         13,
					OnDeleteAction:       99,
				}},
			},
		},
		{
			name: "pk missing index reference",
			table: CatalogTable{
				Name:       "users",
				TableID:    7,
				RootPageID: 1,
				Columns:    []CatalogColumn{{Name: "id", Type: CatalogColumnTypeInt}},
				Indexes:    []CatalogIndex{{Name: "idx_users_pk", IndexID: 11, Columns: []CatalogIndexColumn{{Name: "id"}}}},
				PrimaryKey: &CatalogPrimaryKey{Name: "pk_users", TableID: 7, Columns: []string{"id"}, ImplicitNN: true},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := encodeCatalogPayload(&CatalogData{Tables: []CatalogTable{tc.table}})
			if err == nil {
				t.Fatal("encodeCatalogPayload() error = nil, want malformed constraint metadata rejection")
			}
		})
	}
}
