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
	tokenKeywordAnd
	tokenKeywordAs
	tokenKeywordColumn
	tokenKeywordConstraint
	tokenKeywordCreate
	tokenKeywordCascade
	tokenKeywordDefault
	tokenKeywordDelete
	tokenKeywordDrop
	tokenKeywordCommit
	tokenKeywordRollback
	tokenKeywordUnique
	tokenKeywordIndex
	tokenKeywordAsc
	tokenKeywordDesc
	tokenKeywordInner
	tokenKeywordJoin
	tokenKeywordKey
	tokenKeywordNot
	tokenKeywordNull
	tokenKeywordOn
	tokenKeywordPrimary
	tokenKeywordForeign
	tokenKeywordFrom
	tokenKeywordOr
	tokenKeywordReferences
	tokenKeywordRestrict
	tokenKeywordInsert
	tokenKeywordInto
	tokenKeywordUsing
	tokenKeywordBy
	tokenKeywordOrder
	tokenKeywordSelect
	tokenKeywordTable
	tokenKeywordSet
	tokenKeywordUpdate
	tokenKeywordValues
	tokenKeywordWhere
	tokenKeywordSmallInt
	tokenKeywordInt
	tokenKeywordBigInt
	tokenKeywordText
	tokenKeywordBool
	tokenKeywordReal
	tokenKeywordDate
	tokenKeywordTime
	tokenKeywordTimestamp
	tokenNumber
	tokenString
	tokenPlaceholder
	tokenLParen
	tokenRParen
	tokenComma
	tokenDot
	tokenStar
	tokenPlus
	tokenMinus
	tokenEq
	tokenNotEq
	tokenLT
	tokenLTE
	tokenGT
	tokenGTE
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
	case '.':
		l.pos++
		return token{Kind: tokenDot, Lexeme: ".", Pos: start}, nil
	case '*':
		l.pos++
		return token{Kind: tokenStar, Lexeme: "*", Pos: start}, nil
	case '+':
		l.pos++
		return token{Kind: tokenPlus, Lexeme: "+", Pos: start}, nil
	case '?':
		l.pos++
		return token{Kind: tokenPlaceholder, Lexeme: "?", Pos: start}, nil
	case '=':
		l.pos++
		return token{Kind: tokenEq, Lexeme: "=", Pos: start}, nil
	case '!':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2
			return token{Kind: tokenNotEq, Lexeme: "!=", Pos: start}, nil
		}
	case '<':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '>' {
			l.pos += 2
			return token{Kind: tokenNotEq, Lexeme: "<>", Pos: start}, nil
		}
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2
			return token{Kind: tokenLTE, Lexeme: "<=", Pos: start}, nil
		}
		l.pos++
		return token{Kind: tokenLT, Lexeme: "<", Pos: start}, nil
	case '>':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2
			return token{Kind: tokenGTE, Lexeme: ">=", Pos: start}, nil
		}
		l.pos++
		return token{Kind: tokenGT, Lexeme: ">", Pos: start}, nil
	case '\'':
		l.pos++
		for l.pos < len(l.input) && l.input[l.pos] != '\'' {
			l.pos++
		}
		if l.pos >= len(l.input) {
			return token{}, newParseError(fmt.Sprintf("unterminated string literal at position %d", start))
		}
		l.pos++
		return token{Kind: tokenString, Lexeme: l.input[start:l.pos], Pos: start}, nil
	}

	r := rune(ch)
	if ch == '-' || (ch >= '0' && ch <= '9') {
		if tok, ok := l.scanNumber(start); ok {
			return tok, nil
		}
		if ch == '-' {
			l.pos++
			return token{Kind: tokenMinus, Lexeme: "-", Pos: start}, nil
		}
	}
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
	case "AND":
		return tokenKeywordAnd
	case "AS":
		return tokenKeywordAs
	case "COLUMN":
		return tokenKeywordColumn
	case "CONSTRAINT":
		return tokenKeywordConstraint
	case "CREATE":
		return tokenKeywordCreate
	case "CASCADE":
		return tokenKeywordCascade
	case "DEFAULT":
		return tokenKeywordDefault
	case "DELETE":
		return tokenKeywordDelete
	case "DROP":
		return tokenKeywordDrop
	case "COMMIT":
		return tokenKeywordCommit
	case "ROLLBACK":
		return tokenKeywordRollback
	case "UNIQUE":
		return tokenKeywordUnique
	case "INDEX":
		return tokenKeywordIndex
	case "ASC":
		return tokenKeywordAsc
	case "DESC":
		return tokenKeywordDesc
	case "INNER":
		return tokenKeywordInner
	case "JOIN":
		return tokenKeywordJoin
	case "KEY":
		return tokenKeywordKey
	case "NOT":
		return tokenKeywordNot
	case "NULL":
		return tokenKeywordNull
	case "ON":
		return tokenKeywordOn
	case "PRIMARY":
		return tokenKeywordPrimary
	case "FOREIGN":
		return tokenKeywordForeign
	case "BY":
		return tokenKeywordBy
	case "FROM":
		return tokenKeywordFrom
	case "INSERT":
		return tokenKeywordInsert
	case "INTO":
		return tokenKeywordInto
	case "ORDER":
		return tokenKeywordOrder
	case "OR":
		return tokenKeywordOr
	case "REFERENCES":
		return tokenKeywordReferences
	case "RESTRICT":
		return tokenKeywordRestrict
	case "SELECT":
		return tokenKeywordSelect
	case "SET":
		return tokenKeywordSet
	case "TABLE":
		return tokenKeywordTable
	case "UPDATE":
		return tokenKeywordUpdate
	case "USING":
		return tokenKeywordUsing
	case "VALUES":
		return tokenKeywordValues
	case "WHERE":
		return tokenKeywordWhere
	case ColumnTypeSmallInt:
		return tokenKeywordSmallInt
	case ColumnTypeInt:
		return tokenKeywordInt
	case ColumnTypeBigInt:
		return tokenKeywordBigInt
	case ColumnTypeText:
		return tokenKeywordText
	case ColumnTypeBool:
		return tokenKeywordBool
	case ColumnTypeReal:
		return tokenKeywordReal
	case ColumnTypeDate:
		return tokenKeywordDate
	case ColumnTypeTime:
		return tokenKeywordTime
	case ColumnTypeTimestamp:
		return tokenKeywordTimestamp
	default:
		return tokenIdentifier
	}
}

func (l *lexer) scanNumber(start int) (token, bool) {
	pos := l.pos
	if l.input[pos] == '-' {
		pos++
		if pos >= len(l.input) || l.input[pos] < '0' || l.input[pos] > '9' {
			return token{}, false
		}
	}

	hasDigit := false
	dotCount := 0
	for pos < len(l.input) {
		ch := l.input[pos]
		switch {
		case ch >= '0' && ch <= '9':
			hasDigit = true
			pos++
		case ch == '.':
			dotCount++
			if dotCount > 1 {
				return token{}, false
			}
			pos++
		default:
			l.pos = pos
			if !hasDigit {
				return token{}, false
			}
			return token{Kind: tokenNumber, Lexeme: l.input[start:pos], Pos: start}, true
		}
	}

	l.pos = pos
	if !hasDigit {
		return token{}, false
	}
	return token{Kind: tokenNumber, Lexeme: l.input[start:pos], Pos: start}, true
}

func isIdentifierStart(r rune) bool {
	return unicode.IsLetter(r) || r == '_'
}

func isIdentifierPart(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}
