package parser

import "testing"

func TestParseCreateTableColumnDefinitionMetadata(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want ColumnDef
	}{
		{
			name: "plain int",
			sql:  "CREATE TABLE t (id INT)",
			want: ColumnDef{Name: "id", Type: ColumnTypeInt},
		},
		{
			name: "int default",
			sql:  "CREATE TABLE t (id INT DEFAULT 0)",
			want: ColumnDef{Name: "id", Type: ColumnTypeInt, HasDefault: true, DefaultValue: Int64Value(0)},
		},
		{
			name: "int not null",
			sql:  "CREATE TABLE t (id INT NOT NULL)",
			want: ColumnDef{Name: "id", Type: ColumnTypeInt, NotNull: true},
		},
		{
			name: "int not null default",
			sql:  "CREATE TABLE t (id INT NOT NULL DEFAULT 0)",
			want: ColumnDef{Name: "id", Type: ColumnTypeInt, NotNull: true, HasDefault: true, DefaultValue: Int64Value(0)},
		},
		{
			name: "text default",
			sql:  "CREATE TABLE t (name TEXT DEFAULT 'x')",
			want: ColumnDef{Name: "name", Type: ColumnTypeText, HasDefault: true, DefaultValue: StringValue("x")},
		},
		{
			name: "bool default",
			sql:  "CREATE TABLE t (flag BOOL NOT NULL DEFAULT TRUE)",
			want: ColumnDef{Name: "flag", Type: ColumnTypeBool, NotNull: true, HasDefault: true, DefaultValue: BoolValue(true)},
		},
		{
			name: "real default",
			sql:  "CREATE TABLE t (score REAL DEFAULT 1.5)",
			want: ColumnDef{Name: "score", Type: ColumnTypeReal, HasDefault: true, DefaultValue: RealValue(1.5)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCreateTableTokens(tc.sql)
			if err != nil {
				t.Fatalf("parseCreateTableTokens() error = %v", err)
			}
			if len(got.Columns) != 1 {
				t.Fatalf("len(Columns) = %d, want 1", len(got.Columns))
			}
			if got.Columns[0] != tc.want {
				t.Fatalf("Columns[0] = %#v, want %#v", got.Columns[0], tc.want)
			}
		})
	}
}

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

func TestParseCreateTableViaParse(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	got, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("Parse() stmt type = %T, want *CreateTableStmt", stmt)
	}
	if got.Name != "users" {
		t.Fatalf("Parse().Name = %q, want %q", got.Name, "users")
	}
	if len(got.Columns) != 2 || got.Columns[0] != (ColumnDef{Name: "id", Type: ColumnTypeInt}) || got.Columns[1] != (ColumnDef{Name: "name", Type: ColumnTypeText}) {
		t.Fatalf("Parse().Columns = %#v, want typed id/name columns", got.Columns)
	}
}

func TestParseCreateTableTokens(t *testing.T) {
	got, err := parseCreateTableTokens("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("parseCreateTableTokens() error = %v", err)
	}
	if got.Name != "users" {
		t.Fatalf("parseCreateTableTokens().Name = %q, want %q", got.Name, "users")
	}
	if len(got.Columns) != 2 || got.Columns[0] != (ColumnDef{Name: "id", Type: ColumnTypeInt}) || got.Columns[1] != (ColumnDef{Name: "name", Type: ColumnTypeText}) {
		t.Fatalf("parseCreateTableTokens().Columns = %#v, want typed id/name columns", got.Columns)
	}
}

func TestParseCreateTableNamedPrimaryKey(t *testing.T) {
	got, err := parseCreateTableTokens("CREATE TABLE users (id INT, org_id INT, CONSTRAINT pk_users PRIMARY KEY (id, org_id) USING INDEX idx_users_pk)")
	if err != nil {
		t.Fatalf("parseCreateTableTokens() error = %v", err)
	}
	if got.PrimaryKey == nil {
		t.Fatal("PrimaryKey = nil, want parsed primary key")
	}
	if got.PrimaryKey.Name != "pk_users" || got.PrimaryKey.IndexName != "idx_users_pk" {
		t.Fatalf("PrimaryKey = %#v, want pk_users using idx_users_pk", got.PrimaryKey)
	}
	if len(got.PrimaryKey.Columns) != 2 || got.PrimaryKey.Columns[0] != "id" || got.PrimaryKey.Columns[1] != "org_id" {
		t.Fatalf("PrimaryKey.Columns = %#v, want [id org_id]", got.PrimaryKey.Columns)
	}
}

func TestParseCreateTableNamedForeignKeyRestrictAndCascade(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		onDelete ForeignKeyDeleteAction
	}{
		{
			name:     "restrict",
			input:    "CREATE TABLE users (id INT, team_id INT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT)",
			onDelete: ForeignKeyDeleteActionRestrict,
		},
		{
			name:     "cascade",
			input:    "CREATE TABLE users (id INT, team_id INT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE CASCADE)",
			onDelete: ForeignKeyDeleteActionCascade,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCreateTableTokens(tc.input)
			if err != nil {
				t.Fatalf("parseCreateTableTokens() error = %v", err)
			}
			if len(got.ForeignKeys) != 1 {
				t.Fatalf("len(ForeignKeys) = %d, want 1", len(got.ForeignKeys))
			}
			fk := got.ForeignKeys[0]
			if fk.Name != "fk_users_team" || fk.ParentTable != "teams" || fk.IndexName != "idx_users_team" || fk.OnDelete != tc.onDelete {
				t.Fatalf("ForeignKeys[0] = %#v, want fk_users_team -> teams using idx_users_team with %s", fk, tc.onDelete)
			}
			if len(fk.Columns) != 1 || fk.Columns[0] != "team_id" {
				t.Fatalf("ForeignKeys[0].Columns = %#v, want [team_id]", fk.Columns)
			}
			if len(fk.ParentColumns) != 1 || fk.ParentColumns[0] != "id" {
				t.Fatalf("ForeignKeys[0].ParentColumns = %#v, want [id]", fk.ParentColumns)
			}
		})
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

func TestParseCreateTableTokensMixedRealSchema(t *testing.T) {
	got, err := parseCreateTableTokens("CREATE TABLE t (a INT, b REAL, c TEXT, d BOOL)")
	if err != nil {
		t.Fatalf("parseCreateTableTokens() error = %v", err)
	}
	want := []ColumnDef{
		{Name: "a", Type: ColumnTypeInt},
		{Name: "b", Type: ColumnTypeReal},
		{Name: "c", Type: ColumnTypeText},
		{Name: "d", Type: ColumnTypeBool},
	}
	if len(got.Columns) != len(want) {
		t.Fatalf("len(parseCreateTableTokens().Columns) = %d, want %d", len(got.Columns), len(want))
	}
	for i := range want {
		if got.Columns[i] != want[i] {
			t.Fatalf("parseCreateTableTokens().Columns[%d] = %#v, want %#v", i, got.Columns[i], want[i])
		}
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

func TestParseCreateTableTokensInvalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "missing create", input: "TABLE users (id INT, name TEXT)"},
		{name: "missing parens", input: "CREATE TABLE users id INT, name TEXT"},
		{name: "unsupported type boolean", input: "CREATE TABLE users (id BOOLEAN, name TEXT)"},
		{name: "duplicate column", input: "CREATE TABLE users (id INT, id TEXT)"},
		{name: "empty column list", input: "CREATE TABLE users ()"},
		{name: "trailing comma", input: "CREATE TABLE users (id INT,)"},
		{name: "extra trailing tokens", input: "CREATE TABLE users (id INT) extra"},
		{name: "unnamed primary key", input: "CREATE TABLE users (id INT, PRIMARY KEY (id) USING INDEX idx_users_pk)"},
		{name: "primary key missing using index", input: "CREATE TABLE users (id INT, CONSTRAINT pk_users PRIMARY KEY (id))"},
		{name: "foreign key missing using index", input: "CREATE TABLE users (team_id INT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) ON DELETE RESTRICT)"},
		{name: "foreign key missing parent columns", input: "CREATE TABLE users (team_id INT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams USING INDEX idx_users_team ON DELETE RESTRICT)"},
		{name: "foreign key missing on delete", input: "CREATE TABLE users (team_id INT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team)"},
		{name: "foreign key unsupported delete action", input: "CREATE TABLE users (team_id INT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE SET NULL)"},
		{name: "inline primary key unsupported", input: "CREATE TABLE users (id INT PRIMARY KEY)"},
		{name: "inline foreign key unsupported", input: "CREATE TABLE users (team_id INT FOREIGN KEY REFERENCES teams (id))"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCreateTableTokens(tc.input)
			if err == nil {
				t.Fatalf("parseCreateTableTokens() = %#v, want error", got)
			}
		})
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

func TestParseCreateTableRejectsInvalidColumnClauses(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "default without literal", input: "CREATE TABLE t (id INT DEFAULT)"},
		{name: "duplicate default", input: "CREATE TABLE t (id INT DEFAULT 0 DEFAULT 1)"},
		{name: "malformed not null", input: "CREATE TABLE t (id INT NOT)"},
		{name: "expression default", input: "CREATE TABLE t (id INT DEFAULT 1 + 2)"},
		{name: "function default", input: "CREATE TABLE t (id INT DEFAULT now())"},
		{name: "special form default", input: "CREATE TABLE t (id INT DEFAULT CURRENT_TIMESTAMP)"},
		{name: "placeholder default", input: "CREATE TABLE t (id INT DEFAULT ?)"},
		{name: "column reference default", input: "CREATE TABLE t (id INT DEFAULT other_col)"},
		{name: "not null default null", input: "CREATE TABLE t (id INT NOT NULL DEFAULT NULL)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCreateTableTokens(tc.input)
			if err == nil {
				t.Fatalf("parseCreateTableTokens() = %#v, want error", got)
			}
		})
	}
}
