package parser

import (
	"fmt"
	"strings"
	"unicode"
)

type tokenKind int

const (
	tokenIllegal tokenKind = iota
	tokenEOF
	tokenIdentifier
	tokenKeywordCreate
	tokenKeywordTable
	tokenKeywordInt
	tokenKeywordText
	tokenKeywordBool
	tokenKeywordReal
	tokenLParen
	tokenRParen
	tokenComma
)

type token struct {
	Kind   tokenKind
	Lexeme string
	Pos    int
}

type lexer struct {
	input string
	pos   int
}

func lexSQL(input string) ([]token, error) {
	l := lexer{input: input}
	tokens := make([]token, 0, len(input)/2+1)

	for {
		tok, err := l.nextToken()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, tok)
		if tok.Kind == tokenEOF {
			return tokens, nil
		}
	}
}

func (l *lexer) nextToken() (token, error) {
	l.skipWhitespace()
	if l.pos >= len(l.input) {
		return token{Kind: tokenEOF, Pos: l.pos}, nil
	}

	start := l.pos
	ch := l.input[l.pos]

	switch ch {
	case '(':
		l.pos++
		return token{Kind: tokenLParen, Lexeme: "(", Pos: start}, nil
	case ')':
		l.pos++
		return token{Kind: tokenRParen, Lexeme: ")", Pos: start}, nil
	case ',':
		l.pos++
		return token{Kind: tokenComma, Lexeme: ",", Pos: start}, nil
	}

	r := rune(ch)
	if isIdentifierStart(r) {
		l.pos++
		for l.pos < len(l.input) {
			next := rune(l.input[l.pos])
			if !isIdentifierPart(next) {
				break
			}
			l.pos++
		}

		lexeme := l.input[start:l.pos]
		return token{
			Kind:   classifyWord(lexeme),
			Lexeme: lexeme,
			Pos:    start,
		}, nil
	}

	return token{}, newParseError(fmt.Sprintf("unexpected character %q at position %d", ch, start))
}

func (l *lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		if !unicode.IsSpace(rune(l.input[l.pos])) {
			return
		}
		l.pos++
	}
}

func classifyWord(word string) tokenKind {
	switch strings.ToUpper(word) {
	case "CREATE":
		return tokenKeywordCreate
	case "TABLE":
		return tokenKeywordTable
	case ColumnTypeInt:
		return tokenKeywordInt
	case ColumnTypeText:
		return tokenKeywordText
	case ColumnTypeBool:
		return tokenKeywordBool
	case ColumnTypeReal:
		return tokenKeywordReal
	default:
		return tokenIdentifier
	}
}

func isIdentifierStart(r rune) bool {
	return unicode.IsLetter(r) || r == '_'
}

func isIdentifierPart(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}
