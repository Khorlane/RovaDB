package parser

import "testing"

func TestParseCreateTable(t *testing.T) {
	got, err := parseCreateTable("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("parseCreateTable() error = %v", err)
	}
	if got.Name != "users" {
		t.Fatalf("parseCreateTable().Name = %q, want %q", got.Name, "users")
	}
	if len(got.Columns) != 2 || got.Columns[0] != (ColumnDef{Name: "id", Type: ColumnTypeInt}) || got.Columns[1] != (ColumnDef{Name: "name", Type: ColumnTypeText}) {
		t.Fatalf("parseCreateTable().Columns = %#v, want typed id/name columns", got.Columns)
	}
}

func TestParseCreateTableInvalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "missing parens", input: "CREATE TABLE users id INT, name TEXT"},
		{name: "unsupported type", input: "CREATE TABLE users (id BOOL, name TEXT)"},
		{name: "missing type", input: "CREATE TABLE users (id, name TEXT)"},
		{name: "duplicate column", input: "CREATE TABLE users (id INT, id TEXT)"},
		{name: "empty column list", input: "CREATE TABLE users ()"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCreateTable(tc.input)
			if err == nil {
				t.Fatalf("parseCreateTable() = %#v, want error", got)
			}
		})
	}
}
