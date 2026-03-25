package parser

type alterTableTokenParser struct {
	tokens []token
	pos    int
}

func parseAlterTableTokens(input string) (*AlterTableAddColumnStmt, error) {
	tokens, err := lexSQL(input)
	if err != nil {
		return nil, err
	}

	p := alterTableTokenParser{tokens: tokens}
	return p.parse()
}

func (p *alterTableTokenParser) parse() (*AlterTableAddColumnStmt, error) {
	if _, err := p.expect(tokenKeywordAlter); err != nil {
		return nil, errUnsupportedStatement
	}
	if _, err := p.expect(tokenKeywordTable); err != nil {
		return nil, errUnsupportedStatement
	}

	tableTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(tableTok.Lexeme) {
		return nil, newParseError("unsupported alter table form")
	}

	if _, err := p.expect(tokenKeywordAdd); err != nil {
		return nil, newParseError("unsupported alter table form")
	}
	if _, err := p.expect(tokenKeywordColumn); err != nil {
		return nil, newParseError("unsupported alter table form")
	}

	columnTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(columnTok.Lexeme) {
		return nil, newParseError("unsupported alter table form")
	}

	typeTok := p.current()
	switch typeTok.Kind {
	case tokenKeywordInt, tokenKeywordText:
		p.pos++
	default:
		return nil, newParseError("unsupported alter table form")
	}

	if p.current().Kind != tokenEOF {
		return nil, newParseError("unsupported alter table form")
	}

	return &AlterTableAddColumnStmt{
		TableName: tableTok.Lexeme,
		Column: ColumnDef{
			Name: columnTok.Lexeme,
			Type: normalizeColumnType(typeTok.Kind),
		},
	}, nil
}

func (p *alterTableTokenParser) current() token {
	if p.pos >= len(p.tokens) {
		return token{Kind: tokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *alterTableTokenParser) expect(kind tokenKind) (token, error) {
	tok := p.current()
	if tok.Kind != kind {
		return token{}, newParseError("unsupported alter table form")
	}
	p.pos++
	return tok, nil
}
