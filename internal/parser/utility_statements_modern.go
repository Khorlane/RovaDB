package parser

type utilityTokenParser struct {
	tokens []token
	pos    int
}

func parseCreateIndexTokens(input string) (*CreateIndexStmt, error) {
	tokens, err := lexSQL(input)
	if err != nil {
		return nil, err
	}
	p := utilityTokenParser{tokens: tokens}
	return p.parseCreateIndex()
}

func parseDropTableTokens(input string) (*DropTableStmt, error) {
	tokens, err := lexSQL(input)
	if err != nil {
		return nil, err
	}
	p := utilityTokenParser{tokens: tokens}
	return p.parseDropTable()
}

func parseDropIndexTokens(input string) (*DropIndexStmt, error) {
	tokens, err := lexSQL(input)
	if err != nil {
		return nil, err
	}
	p := utilityTokenParser{tokens: tokens}
	return p.parseDropIndex()
}

func parseCommitTokens(input string) (*CommitStmt, error) {
	tokens, err := lexSQL(input)
	if err != nil {
		return nil, err
	}
	p := utilityTokenParser{tokens: tokens}
	return p.parseCommit()
}

func parseRollbackTokens(input string) (*RollbackStmt, error) {
	tokens, err := lexSQL(input)
	if err != nil {
		return nil, err
	}
	p := utilityTokenParser{tokens: tokens}
	return p.parseRollback()
}

func (p *utilityTokenParser) parseCreateIndex() (*CreateIndexStmt, error) {
	if _, err := p.expect(tokenKeywordCreate); err != nil {
		return nil, errUnsupportedStatement
	}
	unique := false
	if p.current().Kind == tokenKeywordUnique {
		p.pos++
		unique = true
	}
	if _, err := p.expect(tokenKeywordIndex); err != nil {
		return nil, errUnsupportedStatement
	}
	nameTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(nameTok.Lexeme) {
		return nil, newParseError("unsupported query form")
	}
	if _, err := p.expect(tokenKeywordOn); err != nil {
		return nil, newParseError("unsupported query form")
	}
	tableTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(tableTok.Lexeme) {
		return nil, newParseError("unsupported query form")
	}
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, newParseError("unsupported query form")
	}
	columns, err := p.parseIndexColumns()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokenRParen); err != nil {
		return nil, newParseError("unsupported query form")
	}
	if p.current().Kind != tokenEOF {
		return nil, newParseError("unsupported query form")
	}
	return &CreateIndexStmt{Name: nameTok.Lexeme, TableName: tableTok.Lexeme, Unique: unique, Columns: columns}, nil
}

func (p *utilityTokenParser) parseIndexColumns() ([]IndexColumn, error) {
	if p.current().Kind == tokenRParen {
		return nil, newParseError("unsupported query form")
	}
	columns := make([]IndexColumn, 0, 2)
	seen := map[string]struct{}{}
	for {
		nameTok, err := p.expect(tokenIdentifier)
		if err != nil || !isIdentifier(nameTok.Lexeme) {
			return nil, newParseError("unsupported query form")
		}
		if _, ok := seen[nameTok.Lexeme]; ok {
			return nil, newParseError("unsupported query form")
		}
		column := IndexColumn{Name: nameTok.Lexeme}
		switch p.current().Kind {
		case tokenKeywordAsc:
			p.pos++
		case tokenKeywordDesc:
			p.pos++
			column.Desc = true
		}
		seen[nameTok.Lexeme] = struct{}{}
		columns = append(columns, column)
		if p.current().Kind != tokenComma {
			return columns, nil
		}
		p.pos++
	}
}

func (p *utilityTokenParser) parseDropTable() (*DropTableStmt, error) {
	if _, err := p.expect(tokenKeywordDrop); err != nil {
		return nil, errUnsupportedStatement
	}
	if _, err := p.expect(tokenKeywordTable); err != nil {
		return nil, errUnsupportedStatement
	}
	nameTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(nameTok.Lexeme) {
		return nil, newParseError("unsupported query form")
	}
	if p.current().Kind != tokenEOF {
		return nil, newParseError("unsupported query form")
	}
	return &DropTableStmt{Name: nameTok.Lexeme}, nil
}

func (p *utilityTokenParser) parseDropIndex() (*DropIndexStmt, error) {
	if _, err := p.expect(tokenKeywordDrop); err != nil {
		return nil, errUnsupportedStatement
	}
	if _, err := p.expect(tokenKeywordIndex); err != nil {
		return nil, errUnsupportedStatement
	}
	nameTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(nameTok.Lexeme) {
		return nil, newParseError("unsupported query form")
	}
	if p.current().Kind != tokenEOF {
		return nil, newParseError("unsupported query form")
	}
	return &DropIndexStmt{Name: nameTok.Lexeme}, nil
}

func (p *utilityTokenParser) parseCommit() (*CommitStmt, error) {
	if _, err := p.expect(tokenKeywordCommit); err != nil {
		return nil, errUnsupportedStatement
	}
	if p.current().Kind != tokenEOF {
		return nil, newParseError("unsupported query form")
	}
	return &CommitStmt{}, nil
}

func (p *utilityTokenParser) parseRollback() (*RollbackStmt, error) {
	if _, err := p.expect(tokenKeywordRollback); err != nil {
		return nil, errUnsupportedStatement
	}
	if p.current().Kind != tokenEOF {
		return nil, newParseError("unsupported query form")
	}
	return &RollbackStmt{}, nil
}

func (p *utilityTokenParser) current() token {
	if p.pos >= len(p.tokens) {
		return token{Kind: tokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *utilityTokenParser) expect(kind tokenKind) (token, error) {
	tok := p.current()
	if tok.Kind != kind {
		return token{}, newParseError("unsupported query form")
	}
	p.pos++
	return tok, nil
}
