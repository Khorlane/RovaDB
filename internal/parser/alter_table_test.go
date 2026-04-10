package parser

import "testing"

func TestParseAlterTableAddColumn(t *testing.T) {
	stmt, err := parseAlterTable("ALTER TABLE users ADD COLUMN age INT")
	if err != nil {
		t.Fatalf("parseAlterTable() error = %v", err)
	}
	got, ok := stmt.(*AlterTableAddColumnStmt)
	if !ok {
		t.Fatalf("parseAlterTable() stmt type = %T, want *AlterTableAddColumnStmt", stmt)
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
	stmt, err := parseAlterTableTokens("ALTER TABLE users ADD COLUMN age INT")
	if err != nil {
		t.Fatalf("parseAlterTableTokens() error = %v", err)
	}
	got, ok := stmt.(*AlterTableAddColumnStmt)
	if !ok {
		t.Fatalf("parseAlterTableTokens() stmt type = %T, want *AlterTableAddColumnStmt", stmt)
	}
	if got.TableName != "users" {
		t.Fatalf("parseAlterTableTokens().TableName = %q, want %q", got.TableName, "users")
	}
	if got.Column != (ColumnDef{Name: "age", Type: ColumnTypeInt}) {
		t.Fatalf("parseAlterTableTokens().Column = %#v, want age INT", got.Column)
	}
}

func TestParseAlterTableAddPrimaryKey(t *testing.T) {
	stmt, err := parseAlterTable("ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id, org_id) USING INDEX idx_users_pk")
	if err != nil {
		t.Fatalf("parseAlterTable() error = %v", err)
	}
	got, ok := stmt.(*AlterTableAddPrimaryKeyStmt)
	if !ok {
		t.Fatalf("parseAlterTable() stmt type = %T, want *AlterTableAddPrimaryKeyStmt", stmt)
	}
	if got.TableName != "users" {
		t.Fatalf("TableName = %q, want users", got.TableName)
	}
	if got.PrimaryKey.Name != "pk_users" || got.PrimaryKey.IndexName != "idx_users_pk" {
		t.Fatalf("PrimaryKey = %#v, want pk_users using idx_users_pk", got.PrimaryKey)
	}
	if len(got.PrimaryKey.Columns) != 2 || got.PrimaryKey.Columns[0] != "id" || got.PrimaryKey.Columns[1] != "org_id" {
		t.Fatalf("PrimaryKey.Columns = %#v, want [id org_id]", got.PrimaryKey.Columns)
	}
}

func TestParseAlterTableAddForeignKey(t *testing.T) {
	stmt, err := parseAlterTableTokens("ALTER TABLE users ADD CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE CASCADE")
	if err != nil {
		t.Fatalf("parseAlterTableTokens() error = %v", err)
	}
	got, ok := stmt.(*AlterTableAddForeignKeyStmt)
	if !ok {
		t.Fatalf("parseAlterTableTokens() stmt type = %T, want *AlterTableAddForeignKeyStmt", stmt)
	}
	if got.TableName != "users" || got.ForeignKey.Name != "fk_users_team" || got.ForeignKey.ParentTable != "teams" {
		t.Fatalf("parsed FK = %#v, want users.fk_users_team -> teams", got)
	}
	if got.ForeignKey.IndexName != "idx_users_team" || got.ForeignKey.OnDelete != ForeignKeyDeleteActionCascade {
		t.Fatalf("ForeignKey = %#v, want idx_users_team ON DELETE CASCADE", got.ForeignKey)
	}
	if len(got.ForeignKey.Columns) != 1 || got.ForeignKey.Columns[0] != "team_id" {
		t.Fatalf("ForeignKey.Columns = %#v, want [team_id]", got.ForeignKey.Columns)
	}
	if len(got.ForeignKey.ParentColumns) != 1 || got.ForeignKey.ParentColumns[0] != "id" {
		t.Fatalf("ForeignKey.ParentColumns = %#v, want [id]", got.ForeignKey.ParentColumns)
	}
}

func TestParseAlterTableDropPrimaryKey(t *testing.T) {
	stmt, err := parseAlterTable("ALTER TABLE users DROP PRIMARY KEY")
	if err != nil {
		t.Fatalf("parseAlterTable() error = %v", err)
	}
	got, ok := stmt.(*AlterTableDropPrimaryKeyStmt)
	if !ok {
		t.Fatalf("parseAlterTable() stmt type = %T, want *AlterTableDropPrimaryKeyStmt", stmt)
	}
	if got.TableName != "users" {
		t.Fatalf("TableName = %q, want users", got.TableName)
	}
}

func TestParseAlterTableDropForeignKey(t *testing.T) {
	stmt, err := parseAlterTableTokens("ALTER TABLE users DROP FOREIGN KEY fk_users_team")
	if err != nil {
		t.Fatalf("parseAlterTableTokens() error = %v", err)
	}
	got, ok := stmt.(*AlterTableDropForeignKeyStmt)
	if !ok {
		t.Fatalf("parseAlterTableTokens() stmt type = %T, want *AlterTableDropForeignKeyStmt", stmt)
	}
	if got.TableName != "users" || got.ConstraintName != "fk_users_team" {
		t.Fatalf("parsed drop FK = %#v, want users/fk_users_team", got)
	}
}

func TestParseAlterTableUnsupportedForm(t *testing.T) {
	tests := []string{
		"ALTER TABLE users DROP COLUMN age",
		"ALTER TABLE users ADD age INT",
		"ALTER TABLE users ADD COLUMN",
		"ALTER TABLE users ADD COLUMN age BOOL",
		"ALTER TABLE users ADD PRIMARY KEY (id) USING INDEX idx_users_pk",
		"ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY () USING INDEX idx_users_pk",
		"ALTER TABLE users ADD CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team",
		"ALTER TABLE users ADD CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams USING INDEX idx_users_team ON DELETE RESTRICT",
		"ALTER TABLE users DROP FOREIGN KEY",
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
		"ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id)",
		"ALTER TABLE users ADD CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE SET NULL",
		"ALTER TABLE users DROP PRIMARY KEY extra",
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
