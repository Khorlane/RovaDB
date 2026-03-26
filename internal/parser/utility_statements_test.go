package parser

import "testing"

func TestParseCreateIndexTokens(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		unique bool
		cols   []IndexColumn
	}{
		{
			name:   "basic",
			input:  "CREATE INDEX idx_users_name ON users (name)",
			unique: false,
			cols:   []IndexColumn{{Name: "name"}},
		},
		{
			name:   "unique multi column",
			input:  "CREATE UNIQUE INDEX idx_users_name_id ON users (name ASC, id DESC)",
			unique: true,
			cols:   []IndexColumn{{Name: "name"}, {Name: "id", Desc: true}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCreateIndexTokens(tc.input)
			if err != nil {
				t.Fatalf("parseCreateIndexTokens() error = %v", err)
			}
			if got.Unique != tc.unique || got.TableName != "users" {
				t.Fatalf("parseCreateIndexTokens() = %#v, want unique=%v table=users", got, tc.unique)
			}
			if len(got.Columns) != len(tc.cols) {
				t.Fatalf("len(Columns) = %d, want %d", len(got.Columns), len(tc.cols))
			}
			for i := range tc.cols {
				if got.Columns[i] != tc.cols[i] {
					t.Fatalf("Columns[%d] = %#v, want %#v", i, got.Columns[i], tc.cols[i])
				}
			}
		})
	}
}

func TestParseDropAndTxnStatements(t *testing.T) {
	tests := []struct {
		name  string
		input string
		kind  any
	}{
		{name: "drop table", input: "DROP TABLE users", kind: &DropTableStmt{}},
		{name: "drop index", input: "DROP INDEX idx_users_name", kind: &DropIndexStmt{}},
		{name: "commit", input: "COMMIT", kind: &CommitStmt{}},
		{name: "rollback", input: "ROLLBACK", kind: &RollbackStmt{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			switch tc.kind.(type) {
			case *DropTableStmt:
				if _, ok := got.(*DropTableStmt); !ok {
					t.Fatalf("Parse() type = %T, want *DropTableStmt", got)
				}
			case *DropIndexStmt:
				if _, ok := got.(*DropIndexStmt); !ok {
					t.Fatalf("Parse() type = %T, want *DropIndexStmt", got)
				}
			case *CommitStmt:
				if _, ok := got.(*CommitStmt); !ok {
					t.Fatalf("Parse() type = %T, want *CommitStmt", got)
				}
			case *RollbackStmt:
				if _, ok := got.(*RollbackStmt); !ok {
					t.Fatalf("Parse() type = %T, want *RollbackStmt", got)
				}
			}
		})
	}
}

func TestParseDropAndTxnStatementsAcceptTrailingSemicolon(t *testing.T) {
	tests := []struct {
		name  string
		input string
		kind  any
	}{
		{name: "drop table", input: "DROP TABLE users;", kind: &DropTableStmt{}},
		{name: "drop index", input: "DROP INDEX idx_users_name;", kind: &DropIndexStmt{}},
		{name: "commit", input: "COMMIT;", kind: &CommitStmt{}},
		{name: "rollback", input: "ROLLBACK;", kind: &RollbackStmt{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			switch tc.kind.(type) {
			case *DropTableStmt:
				if _, ok := got.(*DropTableStmt); !ok {
					t.Fatalf("Parse() type = %T, want *DropTableStmt", got)
				}
			case *DropIndexStmt:
				if _, ok := got.(*DropIndexStmt); !ok {
					t.Fatalf("Parse() type = %T, want *DropIndexStmt", got)
				}
			case *CommitStmt:
				if _, ok := got.(*CommitStmt); !ok {
					t.Fatalf("Parse() type = %T, want *CommitStmt", got)
				}
			case *RollbackStmt:
				if _, ok := got.(*RollbackStmt); !ok {
					t.Fatalf("Parse() type = %T, want *RollbackStmt", got)
				}
			}
		})
	}
}

func TestParseUtilityStatementsInvalid(t *testing.T) {
	tests := []string{
		"CREATE INDEX idx ON users",
		"CREATE INDEX idx ON users ()",
		"CREATE UNIQUE INDEX idx ON users (name, name)",
		"DROP TABLE",
		"DROP INDEX",
		"COMMIT now",
		"ROLLBACK now",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			got, err := Parse(input)
			if err == nil {
				t.Fatalf("Parse() = %#v, want error", got)
			}
		})
	}
}
