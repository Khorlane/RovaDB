package parser

type createTableTokenParser struct {
	tokens []token
	pos    int
}

func parseCreateTableTokens(input string) (*CreateTableStmt, error) {
	tokens, err := lexSQL(input)
	if err != nil {
		return nil, err
	}

	p := createTableTokenParser{tokens: tokens}
	return p.parse()
}

func (p *createTableTokenParser) parse() (*CreateTableStmt, error) {
	if _, err := p.expect(tokenKeywordCreate); err != nil {
		return nil, errUnsupportedStatement
	}
	if _, err := p.expect(tokenKeywordTable); err != nil {
		return nil, errUnsupportedStatement
	}

	nameTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(nameTok.Lexeme) {
		return nil, newParseError("unsupported query form")
	}

	if _, err := p.expect(tokenLParen); err != nil {
		return nil, newParseError("unsupported query form")
	}

	columns, err := p.parseColumnDefs()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, newParseError("unsupported query form")
	}

	if p.current().Kind != tokenEOF {
		return nil, newParseError("unsupported query form")
	}

	return &CreateTableStmt{Name: nameTok.Lexeme, Columns: columns}, nil
}

func (p *createTableTokenParser) parseColumnDefs() ([]ColumnDef, error) {
	if p.current().Kind == tokenRParen {
		return nil, newParseError("unsupported query form")
	}

	columns := make([]ColumnDef, 0, 4)
	seen := make(map[string]struct{})

	for {
		column, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		if _, ok := seen[column.Name]; ok {
			return nil, newParseError("unsupported query form")
		}
		seen[column.Name] = struct{}{}
		columns = append(columns, column)

		if p.current().Kind != tokenComma {
			return columns, nil
		}
		p.pos++
	}
}

func (p *createTableTokenParser) parseColumnDef() (ColumnDef, error) {
	nameTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(nameTok.Lexeme) {
		return ColumnDef{}, newParseError("unsupported query form")
	}

	typeTok := p.current()
	switch typeTok.Kind {
	case tokenKeywordInt, tokenKeywordText, tokenKeywordBool, tokenKeywordReal:
		p.pos++
	default:
		return ColumnDef{}, newParseError("unsupported query form")
	}

	return ColumnDef{Name: nameTok.Lexeme, Type: normalizeColumnType(typeTok.Kind)}, nil
}

func (p *createTableTokenParser) current() token {
	if p.pos >= len(p.tokens) {
		return token{Kind: tokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *createTableTokenParser) expect(kind tokenKind) (token, error) {
	tok := p.current()
	if tok.Kind != kind {
		return token{}, newParseError("unsupported query form")
	}
	p.pos++
	return tok, nil
}

func normalizeColumnType(kind tokenKind) string {
	switch kind {
	case tokenKeywordInt:
		return ColumnTypeInt
	case tokenKeywordText:
		return ColumnTypeText
	case tokenKeywordBool:
		return ColumnTypeBool
	case tokenKeywordReal:
		return ColumnTypeReal
	default:
		return ""
	}
}
