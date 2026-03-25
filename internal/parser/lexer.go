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
	tokenKeywordAlter
	tokenKeywordAdd
	tokenKeywordColumn
	tokenKeywordCreate
	tokenKeywordDelete
	tokenKeywordFrom
	tokenKeywordInsert
	tokenKeywordInto
	tokenKeywordTable
	tokenKeywordSet
	tokenKeywordUpdate
	tokenKeywordValues
	tokenKeywordWhere
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
	case "ALTER":
		return tokenKeywordAlter
	case "ADD":
		return tokenKeywordAdd
	case "COLUMN":
		return tokenKeywordColumn
	case "CREATE":
		return tokenKeywordCreate
	case "DELETE":
		return tokenKeywordDelete
	case "FROM":
		return tokenKeywordFrom
	case "INSERT":
		return tokenKeywordInsert
	case "INTO":
		return tokenKeywordInto
	case "SET":
		return tokenKeywordSet
	case "TABLE":
		return tokenKeywordTable
	case "UPDATE":
		return tokenKeywordUpdate
	case "VALUES":
		return tokenKeywordValues
	case "WHERE":
		return tokenKeywordWhere
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
