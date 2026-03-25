package parser

import "testing"

func TestLexSQLCreateTableTokens(t *testing.T) {
	tokens, err := lexSQL("CREATE TABLE users (id INT, name TEXT, active BOOL, score REAL)")
	if err != nil {
		t.Fatalf("lexSQL() error = %v", err)
	}

	wantKinds := []tokenKind{
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
	}
	if len(tokens) != len(wantKinds) {
		t.Fatalf("len(tokens) = %d, want %d (%#v)", len(tokens), len(wantKinds), tokens)
	}
	for i, want := range wantKinds {
		if tokens[i].Kind != want {
			t.Fatalf("tokens[%d].Kind = %v, want %v", i, tokens[i].Kind, want)
		}
	}
}

func TestLexSQLAlterTableTokens(t *testing.T) {
	tokens, err := lexSQL("ALTER TABLE users ADD COLUMN age INT")
	if err != nil {
		t.Fatalf("lexSQL() error = %v", err)
	}

	wantKinds := []tokenKind{
		tokenKeywordAlter,
		tokenKeywordTable,
		tokenIdentifier,
		tokenKeywordAdd,
		tokenKeywordColumn,
		tokenIdentifier,
		tokenKeywordInt,
		tokenEOF,
	}
	if len(tokens) != len(wantKinds) {
		t.Fatalf("len(tokens) = %d, want %d (%#v)", len(tokens), len(wantKinds), tokens)
	}
	for i, want := range wantKinds {
		if tokens[i].Kind != want {
			t.Fatalf("tokens[%d].Kind = %v, want %v", i, tokens[i].Kind, want)
		}
	}
}

func TestLexSQLDeleteTokensWithoutWhere(t *testing.T) {
	tokens, err := lexSQL("DELETE FROM users")
	if err != nil {
		t.Fatalf("lexSQL() error = %v", err)
	}

	wantKinds := []tokenKind{
		tokenKeywordDelete,
		tokenKeywordFrom,
		tokenIdentifier,
		tokenEOF,
	}
	if len(tokens) != len(wantKinds) {
		t.Fatalf("len(tokens) = %d, want %d (%#v)", len(tokens), len(wantKinds), tokens)
	}
	for i, want := range wantKinds {
		if tokens[i].Kind != want {
			t.Fatalf("tokens[%d].Kind = %v, want %v", i, tokens[i].Kind, want)
		}
	}
}

func TestLexSQLUpdateTokens(t *testing.T) {
	tokens, err := lexSQL("UPDATE users SET")
	if err != nil {
		t.Fatalf("lexSQL() error = %v", err)
	}

	wantKinds := []tokenKind{
		tokenKeywordUpdate,
		tokenIdentifier,
		tokenKeywordSet,
		tokenEOF,
	}
	if len(tokens) != len(wantKinds) {
		t.Fatalf("len(tokens) = %d, want %d (%#v)", len(tokens), len(wantKinds), tokens)
	}
	for i, want := range wantKinds {
		if tokens[i].Kind != want {
			t.Fatalf("tokens[%d].Kind = %v, want %v", i, tokens[i].Kind, want)
		}
	}
}

func TestLexSQLPreservesIdentifierLexemes(t *testing.T) {
	tokens, err := lexSQL("create table User_Profile (_id INT)")
	if err != nil {
		t.Fatalf("lexSQL() error = %v", err)
	}

	if tokens[2].Lexeme != "User_Profile" {
		t.Fatalf("tokens[2].Lexeme = %q, want %q", tokens[2].Lexeme, "User_Profile")
	}
	if tokens[4].Lexeme != "_id" {
		t.Fatalf("tokens[4].Lexeme = %q, want %q", tokens[4].Lexeme, "_id")
	}
}

func TestLexSQLRejectsUnexpectedCharacters(t *testing.T) {
	if _, err := lexSQL("CREATE TABLE users [id INT]"); err == nil {
		t.Fatal("lexSQL() error = nil, want unexpected character error")
	}
}
