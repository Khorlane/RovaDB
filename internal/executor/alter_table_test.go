package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestExecuteAlterTableAddColumn(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name: "users",
			Columns: []parser.ColumnDef{
				{Name: "id", Type: parser.ColumnTypeInt},
			},
			Rows: [][]parser.Value{
				{parser.Int64Value(1)},
				{parser.Int64Value(2)},
			},
		},
	}

	affected, err := Execute(&parser.AlterTableAddColumnStmt{
		TableName: "users",
		Column:    parser.ColumnDef{Name: "name", Type: parser.ColumnTypeText},
	}, tables)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if affected != 0 {
		t.Fatalf("Execute() affected = %d, want 0", affected)
	}
	if len(tables["users"].Columns) != 2 {
		t.Fatalf("len(table.Columns) = %d, want 2", len(tables["users"].Columns))
	}
	if got := tables["users"].Rows[0]; len(got) != 2 || got[1] != parser.NullValue() {
		t.Fatalf("row 0 = %#v, want padded NULL column", got)
	}
}

func TestExecuteAlterTableAddColumnAppliesNullabilityAndDefaults(t *testing.T) {
	tests := []struct {
		name         string
		table        *Table
		column       parser.ColumnDef
		wantErr      string
		wantRows     [][]parser.Value
		wantRowWidth int
	}{
		{
			name: "nullable without default on populated table",
			table: &Table{
				Name:    "users",
				Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}},
				Rows:    [][]parser.Value{{parser.Int64Value(1)}, {parser.Int64Value(2)}},
			},
			column:       parser.ColumnDef{Name: "name", Type: parser.ColumnTypeText},
			wantRows:     [][]parser.Value{{parser.Int64Value(1), parser.NullValue()}, {parser.Int64Value(2), parser.NullValue()}},
			wantRowWidth: 2,
		},
		{
			name: "nullable with default on populated table",
			table: &Table{
				Name:    "users",
				Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}},
				Rows:    [][]parser.Value{{parser.Int64Value(1)}, {parser.Int64Value(2)}},
			},
			column:       parser.ColumnDef{Name: "name", Type: parser.ColumnTypeText, HasDefault: true, DefaultValue: parser.StringValue("ready")},
			wantRows:     [][]parser.Value{{parser.Int64Value(1), parser.StringValue("ready")}, {parser.Int64Value(2), parser.StringValue("ready")}},
			wantRowWidth: 2,
		},
		{
			name: "not null without default on empty table",
			table: &Table{
				Name:    "users",
				Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}},
			},
			column:       parser.ColumnDef{Name: "active", Type: parser.ColumnTypeBool, NotNull: true},
			wantRowWidth: 2,
		},
		{
			name: "not null without default on populated table fails",
			table: &Table{
				Name:    "users",
				Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}},
				Rows:    [][]parser.Value{{parser.Int64Value(1)}},
			},
			column:  parser.ColumnDef{Name: "active", Type: parser.ColumnTypeBool, NotNull: true},
			wantErr: "execution: cannot add NOT NULL column without DEFAULT to non-empty table",
		},
		{
			name: "not null without default uses persisted row count fallback",
			table: func() *Table {
				table := &Table{
					Name:    "users",
					Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}},
				}
				table.SetStorageMeta(0, 1)
				return table
			}(),
			column:  parser.ColumnDef{Name: "active", Type: parser.ColumnTypeBool, NotNull: true},
			wantErr: "execution: cannot add NOT NULL column without DEFAULT to non-empty table",
		},
		{
			name: "not null with default on populated table",
			table: &Table{
				Name:    "users",
				Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}},
				Rows:    [][]parser.Value{{parser.Int64Value(1)}, {parser.Int64Value(2)}},
			},
			column:       parser.ColumnDef{Name: "active", Type: parser.ColumnTypeBool, NotNull: true, HasDefault: true, DefaultValue: parser.BoolValue(true)},
			wantRows:     [][]parser.Value{{parser.Int64Value(1), parser.BoolValue(true)}, {parser.Int64Value(2), parser.BoolValue(true)}},
			wantRowWidth: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tables := map[string]*Table{"users": tc.table}
			_, err := Execute(&parser.AlterTableAddColumnStmt{
				TableName: "users",
				Column:    tc.column,
			}, tables)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("Execute() error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if len(tc.table.Columns) != tc.wantRowWidth {
				t.Fatalf("len(table.Columns) = %d, want %d", len(tc.table.Columns), tc.wantRowWidth)
			}
			if len(tc.table.Rows) != len(tc.wantRows) {
				t.Fatalf("len(table.Rows) = %d, want %d", len(tc.table.Rows), len(tc.wantRows))
			}
			for i := range tc.wantRows {
				got := tc.table.Rows[i]
				want := tc.wantRows[i]
				if len(got) != len(want) {
					t.Fatalf("row %d len = %d, want %d", i, len(got), len(want))
				}
				for j := range want {
					if got[j] != want[j] {
						t.Fatalf("row %d column %d = %#v, want %#v", i, j, got[j], want[j])
					}
				}
			}
		})
	}
}

func TestExecuteAlterTableAddKeyForms(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			TableID: 7,
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "team_id", Type: parser.ColumnTypeInt}},
			IndexDefs: []storage.CatalogIndex{
				{Name: "idx_users_pk", Unique: true, IndexID: 11, Columns: []storage.CatalogIndexColumn{{Name: "id"}}},
				{Name: "idx_users_team", IndexID: 13, Columns: []storage.CatalogIndexColumn{{Name: "team_id"}}},
			},
		},
		"teams": {
			Name:    "teams",
			TableID: 9,
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}},
			IndexDefs: []storage.CatalogIndex{
				{Name: "idx_teams_pk", Unique: true, IndexID: 15, Columns: []storage.CatalogIndexColumn{{Name: "id"}}},
			},
			PrimaryKeyDef: &storage.CatalogPrimaryKey{
				Name:       "pk_teams",
				TableID:    9,
				Columns:    []string{"id"},
				IndexID:    15,
				ImplicitNN: true,
			},
		},
	}

	tests := []struct {
		name  string
		stmt  any
		check func(t *testing.T, tables map[string]*Table)
	}{
		{
			name: "add primary key",
			stmt: &parser.AlterTableAddPrimaryKeyStmt{
				TableName: "users",
				PrimaryKey: parser.PrimaryKeyDef{
					Name:      "pk_users",
					Columns:   []string{"id"},
					IndexName: "idx_users_pk",
				},
			},
			check: func(t *testing.T, tables map[string]*Table) {
				t.Helper()
				if tables["users"].PrimaryKeyDef == nil || tables["users"].PrimaryKeyDef.Name != "pk_users" {
					t.Fatalf("users.PrimaryKeyDef = %#v, want pk_users", tables["users"].PrimaryKeyDef)
				}
			},
		},
		{
			name: "add foreign key",
			stmt: &parser.AlterTableAddForeignKeyStmt{
				TableName: "users",
				ForeignKey: parser.ForeignKeyDef{
					Name:          "fk_users_team",
					Columns:       []string{"team_id"},
					ParentTable:   "teams",
					ParentColumns: []string{"id"},
					IndexName:     "idx_users_team",
					OnDelete:      parser.ForeignKeyDeleteActionCascade,
				},
			},
			check: func(t *testing.T, tables map[string]*Table) {
				t.Helper()
				if len(tables["users"].ForeignKeyDefs) != 1 || tables["users"].ForeignKeyDefs[0].Name != "fk_users_team" {
					t.Fatalf("users.ForeignKeyDefs = %#v, want fk_users_team", tables["users"].ForeignKeyDefs)
				}
			},
		},
		{
			name: "drop primary key",
			stmt: &parser.AlterTableDropPrimaryKeyStmt{TableName: "users"},
			check: func(t *testing.T, tables map[string]*Table) {
				t.Helper()
				if tables["users"].PrimaryKeyDef != nil {
					t.Fatalf("users.PrimaryKeyDef = %#v, want nil after drop", tables["users"].PrimaryKeyDef)
				}
			},
		},
		{
			name: "drop foreign key",
			stmt: &parser.AlterTableDropForeignKeyStmt{TableName: "users", ConstraintName: "fk_users_team"},
			check: func(t *testing.T, tables map[string]*Table) {
				t.Helper()
				if len(tables["users"].ForeignKeyDefs) != 0 {
					t.Fatalf("users.ForeignKeyDefs = %#v, want empty after drop", tables["users"].ForeignKeyDefs)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			execTables := cloneAlterTableTestTables(tables)
			switch tc.name {
			case "drop primary key":
				execTables["users"].PrimaryKeyDef = &storage.CatalogPrimaryKey{
					Name:       "pk_users",
					TableID:    7,
					Columns:    []string{"id"},
					IndexID:    11,
					ImplicitNN: true,
				}
				execTables["users"].ForeignKeyDefs = nil
				execTables["teams"].ForeignKeyDefs = []storage.CatalogForeignKey{{
					Name:                 "fk_teams_parent",
					ChildTableID:         9,
					ChildColumns:         []string{"id"},
					ParentTableID:        7,
					ParentColumns:        []string{"id"},
					ParentPrimaryKeyName: "pk_users",
					ChildIndexID:         15,
					OnDeleteAction:       storage.CatalogForeignKeyDeleteActionRestrict,
				}}
			case "drop foreign key":
				execTables["users"].PrimaryKeyDef = nil
				execTables["users"].ForeignKeyDefs = []storage.CatalogForeignKey{{
					Name:                 "fk_users_team",
					ChildTableID:         7,
					ChildColumns:         []string{"team_id"},
					ParentTableID:        9,
					ParentColumns:        []string{"id"},
					ParentPrimaryKeyName: "pk_teams",
					ChildIndexID:         13,
					OnDeleteAction:       storage.CatalogForeignKeyDeleteActionCascade,
				}}
			}
			_, err := Execute(tc.stmt, execTables)
			if err != nil {
				t.Fatalf("Execute() error = %v, want nil", err)
			}
			tc.check(t, execTables)
		})
	}
}

func cloneAlterTableTestTables(src map[string]*Table) map[string]*Table {
	cloned := make(map[string]*Table, len(src))
	for name, table := range src {
		if table == nil {
			cloned[name] = nil
			continue
		}
		clonedTable := &Table{
			Name:           table.Name,
			TableID:        table.TableID,
			IsSystem:       table.IsSystem,
			Columns:        append([]parser.ColumnDef(nil), table.Columns...),
			Rows:           append([][]parser.Value(nil), table.Rows...),
			IndexDefs:      append([]storage.CatalogIndex(nil), table.IndexDefs...),
			PrimaryKeyDef:  clonePrimaryKeyDefinition(table.PrimaryKeyDef),
			ForeignKeyDefs: cloneAlterTableForeignKeyDefs(table.ForeignKeyDefs),
		}
		cloned[name] = clonedTable
	}
	return cloned
}

func cloneAlterTableForeignKeyDefs(src []storage.CatalogForeignKey) []storage.CatalogForeignKey {
	if len(src) == 0 {
		return nil
	}
	cloned := make([]storage.CatalogForeignKey, 0, len(src))
	for _, fk := range src {
		cloned = append(cloned, cloneForeignKeyDefinition(fk))
	}
	return cloned
}
