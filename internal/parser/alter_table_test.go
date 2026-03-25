package parser

import "testing"

func TestParseAlterTableAddColumn(t *testing.T) {
	got, err := parseAlterTable("ALTER TABLE users ADD COLUMN age INT")
	if err != nil {
		t.Fatalf("parseAlterTable() error = %v", err)
	}
	if got.TableName != "users" {
		t.Fatalf("parseAlterTable().TableName = %q, want %q", got.TableName, "users")
	}
	if got.Column != (ColumnDef{Name: "age", Type: ColumnTypeInt}) {
		t.Fatalf("parseAlterTable().Column = %#v, want age INT", got.Column)
	}
}

func TestParseAlterTableViaParse(t *testing.T) {
	stmt, err := Parse("ALTER TABLE users ADD COLUMN age INT")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	got, ok := stmt.(*AlterTableAddColumnStmt)
	if !ok {
		t.Fatalf("Parse() stmt type = %T, want *AlterTableAddColumnStmt", stmt)
	}
	if got.TableName != "users" {
		t.Fatalf("Parse().TableName = %q, want %q", got.TableName, "users")
	}
	if got.Column != (ColumnDef{Name: "age", Type: ColumnTypeInt}) {
		t.Fatalf("Parse().Column = %#v, want age INT", got.Column)
	}
}

func TestParseAlterTableTokensAddColumn(t *testing.T) {
	got, err := parseAlterTableTokens("ALTER TABLE users ADD COLUMN age INT")
	if err != nil {
		t.Fatalf("parseAlterTableTokens() error = %v", err)
	}
	if got.TableName != "users" {
		t.Fatalf("parseAlterTableTokens().TableName = %q, want %q", got.TableName, "users")
	}
	if got.Column != (ColumnDef{Name: "age", Type: ColumnTypeInt}) {
		t.Fatalf("parseAlterTableTokens().Column = %#v, want age INT", got.Column)
	}
}

func TestParseAlterTableUnsupportedForm(t *testing.T) {
	tests := []string{
		"ALTER TABLE users DROP COLUMN age",
		"ALTER TABLE users ADD age INT",
		"ALTER TABLE users ADD COLUMN",
		"ALTER TABLE users ADD COLUMN age BOOL",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			got, err := parseAlterTable(input)
			if err == nil {
				t.Fatalf("parseAlterTable() = %#v, want error", got)
			}
			if err.Error() != "parse: unsupported alter table form" {
				t.Fatalf("parseAlterTable() error = %q, want %q", err.Error(), "parse: unsupported alter table form")
			}
		})
	}
}

func TestParseAlterTableTokensUnsupportedForm(t *testing.T) {
	tests := []string{
		"ALTER TABLE users DROP COLUMN age",
		"ALTER TABLE users ADD age INT",
		"ALTER TABLE users ADD COLUMN",
		"ALTER TABLE users ADD COLUMN age BOOL",
		"ALTER TABLE users ADD COLUMN age INT extra",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			got, err := parseAlterTableTokens(input)
			if err == nil {
				t.Fatalf("parseAlterTableTokens() = %#v, want error", got)
			}
			if err.Error() != "parse: unsupported alter table form" {
				t.Fatalf("parseAlterTableTokens() error = %q, want %q", err.Error(), "parse: unsupported alter table form")
			}
		})
	}
}
