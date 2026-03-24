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

func TestParseCreateTableBool(t *testing.T) {
	got, err := parseCreateTable("CREATE TABLE t (flag BOOL)")
	if err != nil {
		t.Fatalf("parseCreateTable() error = %v", err)
	}
	if got.Name != "t" {
		t.Fatalf("parseCreateTable().Name = %q, want %q", got.Name, "t")
	}
	if len(got.Columns) != 1 || got.Columns[0] != (ColumnDef{Name: "flag", Type: ColumnTypeBool}) {
		t.Fatalf("parseCreateTable().Columns = %#v, want BOOL column", got.Columns)
	}
}

func TestParseCreateTableMixedBoolSchema(t *testing.T) {
	got, err := parseCreateTable("CREATE TABLE t (id INT, name TEXT, active BOOL)")
	if err != nil {
		t.Fatalf("parseCreateTable() error = %v", err)
	}
	want := []ColumnDef{
		{Name: "id", Type: ColumnTypeInt},
		{Name: "name", Type: ColumnTypeText},
		{Name: "active", Type: ColumnTypeBool},
	}
	if len(got.Columns) != len(want) {
		t.Fatalf("len(parseCreateTable().Columns) = %d, want %d", len(got.Columns), len(want))
	}
	for i := range want {
		if got.Columns[i] != want[i] {
			t.Fatalf("parseCreateTable().Columns[%d] = %#v, want %#v", i, got.Columns[i], want[i])
		}
	}
}

func TestParseCreateTableMultipleBoolColumns(t *testing.T) {
	got, err := parseCreateTable("CREATE TABLE t (a BOOL, b BOOL, c INT)")
	if err != nil {
		t.Fatalf("parseCreateTable() error = %v", err)
	}
	want := []ColumnDef{
		{Name: "a", Type: ColumnTypeBool},
		{Name: "b", Type: ColumnTypeBool},
		{Name: "c", Type: ColumnTypeInt},
	}
	if len(got.Columns) != len(want) {
		t.Fatalf("len(parseCreateTable().Columns) = %d, want %d", len(got.Columns), len(want))
	}
	for i := range want {
		if got.Columns[i] != want[i] {
			t.Fatalf("parseCreateTable().Columns[%d] = %#v, want %#v", i, got.Columns[i], want[i])
		}
	}
}

func TestParseCreateTableReal(t *testing.T) {
	got, err := parseCreateTable("CREATE TABLE t (x REAL)")
	if err != nil {
		t.Fatalf("parseCreateTable() error = %v", err)
	}
	if got.Name != "t" {
		t.Fatalf("parseCreateTable().Name = %q, want %q", got.Name, "t")
	}
	if len(got.Columns) != 1 || got.Columns[0] != (ColumnDef{Name: "x", Type: ColumnTypeReal}) {
		t.Fatalf("parseCreateTable().Columns = %#v, want REAL column", got.Columns)
	}
}

func TestParseCreateTableMixedRealSchema(t *testing.T) {
	got, err := parseCreateTable("CREATE TABLE t (a INT, b REAL, c TEXT, d BOOL)")
	if err != nil {
		t.Fatalf("parseCreateTable() error = %v", err)
	}
	want := []ColumnDef{
		{Name: "a", Type: ColumnTypeInt},
		{Name: "b", Type: ColumnTypeReal},
		{Name: "c", Type: ColumnTypeText},
		{Name: "d", Type: ColumnTypeBool},
	}
	if len(got.Columns) != len(want) {
		t.Fatalf("len(parseCreateTable().Columns) = %d, want %d", len(got.Columns), len(want))
	}
	for i := range want {
		if got.Columns[i] != want[i] {
			t.Fatalf("parseCreateTable().Columns[%d] = %#v, want %#v", i, got.Columns[i], want[i])
		}
	}
}

func TestParseCreateTableInvalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "missing parens", input: "CREATE TABLE users id INT, name TEXT"},
		{name: "unsupported type boolean", input: "CREATE TABLE users (id BOOLEAN, name TEXT)"},
		{name: "unsupported type boole", input: "CREATE TABLE users (id BOOLE, name TEXT)"},
		{name: "unsupported type boll", input: "CREATE TABLE users (id BOLL, name TEXT)"},
		{name: "unsupported type float", input: "CREATE TABLE users (id FLOAT, name TEXT)"},
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
