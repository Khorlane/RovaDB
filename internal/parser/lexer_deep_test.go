//go:build lexerdeep

package parser

import "testing"

func TestLexSQLDeepWhitespaceAndKeywordCoverage(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantKinds []tokenKind
	}{
		{
			name:  "mixed case keywords",
			input: "cReAtE tAbLe users (id int, name text, active bool, score real)",
			wantKinds: []tokenKind{
				tokenKeywordCreate,
				tokenKeywordTable,
				tokenIdentifier,
				tokenLParen,
				tokenIdentifier,
				tokenKeywordInt,
				tokenComma,
				tokenIdentifier,
				tokenKeywordText,
				tokenComma,
				tokenIdentifier,
				tokenKeywordBool,
				tokenComma,
				tokenIdentifier,
				tokenKeywordReal,
				tokenRParen,
				tokenEOF,
			},
		},
		{
			name:  "tabs and newlines",
			input: "CREATE\tTABLE\nusers\r\n(\tid\tINT,\nname\tTEXT\n)",
			wantKinds: []tokenKind{
				tokenKeywordCreate,
				tokenKeywordTable,
				tokenIdentifier,
				tokenLParen,
				tokenIdentifier,
				tokenKeywordInt,
				tokenComma,
				tokenIdentifier,
				tokenKeywordText,
				tokenRParen,
				tokenEOF,
			},
		},
		{
			name:  "alter table keywords",
			input: "AlTeR TABLE users ADD COLUMN age INT",
			wantKinds: []tokenKind{
				tokenKeywordAlter,
				tokenKeywordTable,
				tokenIdentifier,
				tokenKeywordAdd,
				tokenKeywordColumn,
				tokenIdentifier,
				tokenKeywordInt,
				tokenEOF,
			},
		},
		{
			name:  "delete keywords",
			input: "DeLeTe FROM users WHERE",
			wantKinds: []tokenKind{
				tokenKeywordDelete,
				tokenKeywordFrom,
				tokenIdentifier,
				tokenKeywordWhere,
				tokenEOF,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tokens, err := lexSQL(tc.input)
			if err != nil {
				t.Fatalf("lexSQL() error = %v", err)
			}
			if len(tokens) != len(tc.wantKinds) {
				t.Fatalf("len(tokens) = %d, want %d", len(tokens), len(tc.wantKinds))
			}
			for i, want := range tc.wantKinds {
				if tokens[i].Kind != want {
					t.Fatalf("tokens[%d].Kind = %v, want %v", i, tokens[i].Kind, want)
				}
			}
		})
	}
}

func TestLexSQLDeepIdentifierBoundaries(t *testing.T) {
	tokens, err := lexSQL("CREATE TABLE _users_1 (_id1 INT, Name_2 TEXT)")
	if err != nil {
		t.Fatalf("lexSQL() error = %v", err)
	}

	wantLexemes := []string{"CREATE", "TABLE", "_users_1", "(", "_id1", "INT", ",", "Name_2", "TEXT", ")"}
	for i, want := range wantLexemes {
		if tokens[i].Lexeme != want {
			t.Fatalf("tokens[%d].Lexeme = %q, want %q", i, tokens[i].Lexeme, want)
		}
	}
}

func TestLexSQLDeepRejectsInvalidInput(t *testing.T) {
	tests := []string{
		"CREATE TABLE users (id INT);",
		"CREATE TABLE users (id INT, name TEXT)#",
		"CREATE TABLE 9users (id INT)",
	}

	for _, input := range tests {
		if _, err := lexSQL(input); err == nil {
			t.Fatalf("lexSQL(%q) error = nil, want error", input)
		}
	}
}
