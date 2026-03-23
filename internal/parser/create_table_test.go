package parser

import "testing"

func TestParseCreateTable(t *testing.T) {
	got, err := parseCreateTable("CREATE TABLE users (id, name)")
	if err != nil {
		t.Fatalf("parseCreateTable() error = %v", err)
	}
	if got.Name != "users" {
		t.Fatalf("parseCreateTable().Name = %q, want %q", got.Name, "users")
	}
	if len(got.Columns) != 2 || got.Columns[0] != "id" || got.Columns[1] != "name" {
		t.Fatalf("parseCreateTable().Columns = %#v, want []string{\"id\", \"name\"}", got.Columns)
	}
}

func TestParseCreateTableInvalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "missing parens", input: "CREATE TABLE users id, name"},
		{name: "empty column", input: "CREATE TABLE users (id, )"},
		{name: "duplicate column", input: "CREATE TABLE users (id, id)"},
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
