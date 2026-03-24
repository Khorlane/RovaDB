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
