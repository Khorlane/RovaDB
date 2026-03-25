package parser

import "strings"

type updateTokenParser struct {
	lexer lexer
}

func parseUpdateTokens(input string) (*UpdateStmt, error) {
	p := updateTokenParser{lexer: lexer{input: input}}
	return p.parse()
}

func (p *updateTokenParser) parse() (*UpdateStmt, error) {
	if _, err := p.expect(tokenKeywordUpdate); err != nil {
		return nil, errUnsupportedStatement
	}

	tableTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(tableTok.Lexeme) {
		return nil, newParseError("unsupported query form")
	}

	if _, err := p.expect(tokenKeywordSet); err != nil {
		return nil, newParseError("unsupported query form")
	}

	remainder := strings.TrimSpace(p.lexer.input[p.lexer.pos:])
	if remainder == "" {
		return nil, newParseError("unsupported query form")
	}

	var where *WhereClause
	assignmentsPart := remainder
	upperRemainder := strings.ToUpper(remainder)
	if whereIndex := strings.Index(upperRemainder, " WHERE "); whereIndex >= 0 {
		assignmentsPart = strings.TrimSpace(remainder[:whereIndex])
		whereClause := strings.TrimSpace(remainder[whereIndex+len(" WHERE "):])
		if whereClause == "" {
			return nil, newParseError("invalid where clause")
		}
		parsedWhere, ok := parseWhereClause(whereClause)
		if !ok {
			return nil, newParseError("invalid where clause")
		}
		where = parsedWhere
	}

	assignments, ok := parseAssignments(assignmentsPart)
	if !ok {
		return nil, newParseError("unsupported query form")
	}

	return &UpdateStmt{
		TableName:   tableTok.Lexeme,
		Assignments: assignments,
		Where:       where,
	}, nil
}

func (p *updateTokenParser) expect(kind tokenKind) (token, error) {
	tok, err := p.lexer.nextToken()
	if err != nil {
		return token{}, err
	}
	if tok.Kind != kind {
		return token{}, newParseError("unsupported query form")
	}
	return tok, nil
}
