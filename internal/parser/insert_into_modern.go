package parser

type insertTokenParser struct {
	lexer lexer
}

func parseInsertTokens(input string) (*InsertStmt, error) {
	p := insertTokenParser{lexer: lexer{input: input}}
	return p.parse()
}

func (p *insertTokenParser) parse() (*InsertStmt, error) {
	if _, err := p.expect(tokenKeywordInsert); err != nil {
		return nil, errUnsupportedStatement
	}
	if _, err := p.expect(tokenKeywordInto); err != nil {
		return nil, errUnsupportedStatement
	}

	tableTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(tableTok.Lexeme) {
		return nil, newParseError("unsupported query form")
	}

	columns := []string(nil)
	if p.peekAfterWhitespace().Kind == tokenLParen {
		if _, err := p.expect(tokenLParen); err != nil {
			return nil, newParseError("unsupported query form")
		}
		parsedColumns, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		columns = parsedColumns
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, newParseError("unsupported query form")
		}
	}

	if _, err := p.expect(tokenKeywordValues); err != nil {
		return nil, newParseError("unsupported query form")
	}
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, newParseError("unsupported query form")
	}
	values, valueExprs, err := p.parseValuesList()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokenRParen); err != nil {
		return nil, newParseError("unsupported query form")
	}
	if _, err := p.expect(tokenEOF); err != nil {
		return nil, newParseError("unsupported query form")
	}
	if len(columns) > 0 && len(columns) != len(values) {
		return nil, newParseError("unsupported query form")
	}

	return &InsertStmt{TableName: tableTok.Lexeme, Columns: columns, Values: values, ValueExprs: valueExprs}, nil
}

func (p *insertTokenParser) expect(kind tokenKind) (token, error) {
	tok, err := p.lexer.nextToken()
	if err != nil {
		return token{}, err
	}
	if tok.Kind != kind {
		return token{}, newParseError("unsupported query form")
	}
	return tok, nil
}

func (p *insertTokenParser) peekAfterWhitespace() token {
	saved := p.lexer.pos
	p.lexer.skipWhitespace()
	tok, err := p.lexer.nextToken()
	p.lexer.pos = saved
	if err != nil {
		return token{Kind: tokenIllegal}
	}
	return tok
}

func (p *insertTokenParser) parseColumnList() ([]string, error) {
	columns := make([]string, 0, 2)
	seen := map[string]struct{}{}
	for {
		tok, err := p.expect(tokenIdentifier)
		if err != nil || !isIdentifier(tok.Lexeme) {
			return nil, newParseError("unsupported query form")
		}
		if _, ok := seen[tok.Lexeme]; ok {
			return nil, newParseError("unsupported query form")
		}
		seen[tok.Lexeme] = struct{}{}
		columns = append(columns, tok.Lexeme)

		next := p.peekAfterWhitespace()
		if next.Kind != tokenComma {
			break
		}
		if _, err := p.expect(tokenComma); err != nil {
			return nil, newParseError("unsupported query form")
		}
	}
	if len(columns) == 0 {
		return nil, newParseError("unsupported query form")
	}
	return columns, nil
}

func (p *insertTokenParser) parseValuesList() ([]Value, []*ValueExpr, error) {
	values := make([]Value, 0, 2)
	valueExprs := make([]*ValueExpr, 0, 2)
	for {
		expr, err := parseValueExprFromLexer(&p.lexer, tokenComma, tokenRParen)
		if err != nil {
			return nil, nil, newParseError("unsupported query form")
		}
		value := Value{}
		if simpleValue, _, ok := flattenSimpleValueExpr(expr); ok {
			value = simpleValue
		}
		values = append(values, value)
		valueExprs = append(valueExprs, expr)

		next := p.peekAfterWhitespace()
		if next.Kind != tokenComma {
			break
		}
		if _, err := p.expect(tokenComma); err != nil {
			return nil, nil, newParseError("unsupported query form")
		}
	}
	if len(values) == 0 {
		return nil, nil, newParseError("unsupported query form")
	}
	return values, valueExprs, nil
}
