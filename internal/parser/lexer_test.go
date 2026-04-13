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

func TestLexSQLColumnDefaultTokens(t *testing.T) {
	tokens, err := lexSQL("CREATE TABLE users (id INT NOT NULL DEFAULT NULL)")
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
		tokenKeywordNot,
		tokenKeywordNull,
		tokenKeywordDefault,
		tokenKeywordNull,
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

func TestLexSQLInsertTokens(t *testing.T) {
	tokens, err := lexSQL("INSERT INTO users VALUES")
	if err != nil {
		t.Fatalf("lexSQL() error = %v", err)
	}

	wantKinds := []tokenKind{
		tokenKeywordInsert,
		tokenKeywordInto,
		tokenIdentifier,
		tokenKeywordValues,
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

func TestLexSQLSelectTokens(t *testing.T) {
	tokens, err := lexSQL("SELECT * FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("lexSQL() error = %v", err)
	}

	wantKinds := []tokenKind{
		tokenKeywordSelect,
		tokenStar,
		tokenKeywordFrom,
		tokenIdentifier,
		tokenKeywordOrder,
		tokenKeywordBy,
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

func TestLexSQLPredicateTokens(t *testing.T) {
	tokens, err := lexSQL("WHERE NOT (id >= 10 AND name != 'bob')")
	if err != nil {
		t.Fatalf("lexSQL() error = %v", err)
	}

	wantKinds := []tokenKind{
		tokenKeywordWhere,
		tokenKeywordNot,
		tokenLParen,
		tokenIdentifier,
		tokenGTE,
		tokenNumber,
		tokenKeywordAnd,
		tokenIdentifier,
		tokenNotEq,
		tokenString,
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

func TestLexSQLPredicateTokensAlternateNotEquals(t *testing.T) {
	tokens, err := lexSQL("WHERE id <> 10")
	if err != nil {
		t.Fatalf("lexSQL() error = %v", err)
	}

	wantKinds := []tokenKind{
		tokenKeywordWhere,
		tokenIdentifier,
		tokenNotEq,
		tokenNumber,
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
	if tokens[2].Lexeme != "<>" {
		t.Fatalf("tokens[2].Lexeme = %q, want %q", tokens[2].Lexeme, "<>")
	}
}

func TestLexSQLLiteralExpressionTokens(t *testing.T) {
	tokens, err := lexSQL("SELECT 10 + -3")
	if err != nil {
		t.Fatalf("lexSQL() error = %v", err)
	}

	wantKinds := []tokenKind{
		tokenKeywordSelect,
		tokenNumber,
		tokenPlus,
		tokenNumber,
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

func TestLexSQLLiteralMinusOperatorToken(t *testing.T) {
	tokens, err := lexSQL("SELECT 5 - 3")
	if err != nil {
		t.Fatalf("lexSQL() error = %v", err)
	}

	wantKinds := []tokenKind{
		tokenKeywordSelect,
		tokenNumber,
		tokenMinus,
		tokenNumber,
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

func TestLexSQLQualifiedIdentifierTokens(t *testing.T) {
	tokens, err := lexSQL("SELECT users.id FROM users")
	if err != nil {
		t.Fatalf("lexSQL() error = %v", err)
	}

	wantKinds := []tokenKind{
		tokenKeywordSelect,
		tokenIdentifier,
		tokenDot,
		tokenIdentifier,
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

func TestLexSQLAliasTokens(t *testing.T) {
	tokens, err := lexSQL("SELECT u.id FROM users AS u")
	if err != nil {
		t.Fatalf("lexSQL() error = %v", err)
	}

	wantKinds := []tokenKind{
		tokenKeywordSelect,
		tokenIdentifier,
		tokenDot,
		tokenIdentifier,
		tokenKeywordFrom,
		tokenIdentifier,
		tokenKeywordAs,
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

func TestLexSQLJoinTokens(t *testing.T) {
	tokens, err := lexSQL("SELECT u.id FROM users u INNER JOIN accounts a ON u.id = a.id")
	if err != nil {
		t.Fatalf("lexSQL() error = %v", err)
	}

	wantKinds := []tokenKind{
		tokenKeywordSelect,
		tokenIdentifier,
		tokenDot,
		tokenIdentifier,
		tokenKeywordFrom,
		tokenIdentifier,
		tokenIdentifier,
		tokenKeywordInner,
		tokenKeywordJoin,
		tokenIdentifier,
		tokenIdentifier,
		tokenKeywordOn,
		tokenIdentifier,
		tokenDot,
		tokenIdentifier,
		tokenEq,
		tokenIdentifier,
		tokenDot,
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
