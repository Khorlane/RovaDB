package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
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

func TestExecuteAlterTableKeyFormsNotImplementedYet(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			TableID: 7,
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "team_id", Type: parser.ColumnTypeInt}},
		},
	}

	tests := []struct {
		name string
		stmt any
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
		},
		{
			name: "drop primary key",
			stmt: &parser.AlterTableDropPrimaryKeyStmt{TableName: "users"},
		},
		{
			name: "drop foreign key",
			stmt: &parser.AlterTableDropForeignKeyStmt{TableName: "users", ConstraintName: "fk_users_team"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Execute(tc.stmt, tables)
			if err != errNotImplemented {
				t.Fatalf("Execute() error = %v, want %v", err, errNotImplemented)
			}
		})
	}
}
