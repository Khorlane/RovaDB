package parser

import "strings"

type deleteTokenParser struct {
	lexer lexer
}

func parseDeleteTokens(input string) (*DeleteStmt, error) {
	p := deleteTokenParser{lexer: lexer{input: input}}
	return p.parse()
}

func (p *deleteTokenParser) parse() (*DeleteStmt, error) {
	if _, err := p.expect(tokenKeywordDelete); err != nil {
		return nil, errUnsupportedStatement
	}
	if _, err := p.expect(tokenKeywordFrom); err != nil {
		return nil, errUnsupportedStatement
	}

	tableTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(tableTok.Lexeme) {
		return nil, newParseError("unsupported query form")
	}

	p.lexer.skipWhitespace()
	if p.lexer.pos >= len(p.lexer.input) {
		return &DeleteStmt{TableName: tableTok.Lexeme}, nil
	}

	whereTok, err := p.lexer.nextToken()
	if err != nil {
		return nil, err
	}
	if whereTok.Kind != tokenKeywordWhere {
		return nil, newParseError("unsupported query form")
	}

	whereClause := strings.TrimSpace(p.lexer.input[p.lexer.pos:])
	if whereClause == "" {
		return nil, newParseError("invalid where clause")
	}

	parsedWhere, ok := parseWhereClause(whereClause)
	if !ok {
		return nil, newParseError("invalid where clause")
	}

	return &DeleteStmt{
		TableName: tableTok.Lexeme,
		Where:     parsedWhere,
	}, nil
}

func (p *deleteTokenParser) expect(kind tokenKind) (token, error) {
	tok, err := p.lexer.nextToken()
	if err != nil {
		return token{}, err
	}
	if tok.Kind != kind {
		return token{}, newParseError("unsupported query form")
	}
	return tok, nil
}
