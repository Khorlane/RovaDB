package parser

import "strings"

func parseValueExprFromLexer(l *lexer, stopKinds ...tokenKind) (*ValueExpr, error) {
	l.skipWhitespace()
	start := l.pos
	depth := 0
	for {
		tok, err := l.nextToken()
		if err != nil {
			return nil, newParseError("unsupported query form")
		}
		switch tok.Kind {
		case tokenLParen:
			depth++
		case tokenRParen:
			if depth == 0 && containsTokenKind(stopKinds, tokenRParen) {
				l.pos = tok.Pos
				return parseValueExprSlice(l.input[start:tok.Pos])
			}
			depth--
			if depth < 0 {
				return nil, newParseError("unsupported query form")
			}
		case tokenEOF:
			if depth != 0 {
				return nil, newParseError("unsupported query form")
			}
			if containsTokenKind(stopKinds, tokenEOF) {
				l.pos = tok.Pos
				return parseValueExprSlice(l.input[start:tok.Pos])
			}
			return nil, newParseError("unsupported query form")
		default:
			if depth == 0 && containsTokenKind(stopKinds, tok.Kind) {
				l.pos = tok.Pos
				return parseValueExprSlice(l.input[start:tok.Pos])
			}
		}
	}
}

func parseValueExprSlice(input string) (*ValueExpr, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil, newParseError("unsupported query form")
	}
	expr, ok := parseValueExpr(trimmed)
	if !ok {
		return nil, newParseError("unsupported query form")
	}
	return expr, nil
}

func containsTokenKind(kinds []tokenKind, want tokenKind) bool {
	for _, kind := range kinds {
		if kind == want {
			return true
		}
	}
	return false
}
