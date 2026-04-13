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

	columns, primaryKey, foreignKeys, err := p.parseTableElements()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(tokenRParen); err != nil {
		return nil, newParseError("unsupported query form")
	}

	if p.current().Kind != tokenEOF {
		return nil, newParseError("unsupported query form")
	}

	return &CreateTableStmt{
		Name:        nameTok.Lexeme,
		Columns:     columns,
		PrimaryKey:  primaryKey,
		ForeignKeys: foreignKeys,
	}, nil
}

func (p *createTableTokenParser) parseTableElements() ([]ColumnDef, *PrimaryKeyDef, []ForeignKeyDef, error) {
	if p.current().Kind == tokenRParen {
		return nil, nil, nil, newParseError("unsupported query form")
	}

	columns := make([]ColumnDef, 0, 4)
	var primaryKey *PrimaryKeyDef
	foreignKeys := make([]ForeignKeyDef, 0, 1)
	seen := make(map[string]struct{})

	for {
		if p.current().Kind == tokenKeywordConstraint {
			p.pos++
			constraintName, pk, fk, err := p.parseConstraintDef()
			if err != nil {
				return nil, nil, nil, err
			}
			if constraintName == "" {
				return nil, nil, nil, newParseError("unsupported query form")
			}
			if pk != nil {
				pk.Name = constraintName
				primaryKey = pk
			}
			if fk != nil {
				fk.Name = constraintName
				foreignKeys = append(foreignKeys, *fk)
			}
		} else {
			column, err := p.parseColumnDef()
			if err != nil {
				return nil, nil, nil, err
			}
			if _, ok := seen[column.Name]; ok {
				return nil, nil, nil, newParseError("unsupported query form")
			}
			seen[column.Name] = struct{}{}
			columns = append(columns, column)
		}

		if p.current().Kind != tokenComma {
			return columns, primaryKey, foreignKeys, nil
		}
		p.pos++
	}
}

func (p *createTableTokenParser) parseConstraintDef() (string, *PrimaryKeyDef, *ForeignKeyDef, error) {
	nameTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(nameTok.Lexeme) {
		return "", nil, nil, newParseError("unsupported query form")
	}

	switch p.current().Kind {
	case tokenKeywordPrimary:
		pk, err := p.parsePrimaryKeyDef()
		return nameTok.Lexeme, pk, nil, err
	case tokenKeywordForeign:
		fk, err := p.parseForeignKeyDef()
		return nameTok.Lexeme, nil, fk, err
	default:
		return "", nil, nil, newParseError("unsupported query form")
	}
}

func (p *createTableTokenParser) parsePrimaryKeyDef() (*PrimaryKeyDef, error) {
	if _, err := p.expect(tokenKeywordPrimary); err != nil {
		return nil, newParseError("unsupported query form")
	}
	if _, err := p.expect(tokenKeywordKey); err != nil {
		return nil, newParseError("unsupported query form")
	}
	columns, err := p.parseIdentifierList()
	if err != nil {
		return nil, err
	}
	indexName, err := p.parseUsingIndexClause()
	if err != nil {
		return nil, err
	}
	return &PrimaryKeyDef{
		Columns:   columns,
		IndexName: indexName,
	}, nil
}

func (p *createTableTokenParser) parseForeignKeyDef() (*ForeignKeyDef, error) {
	if _, err := p.expect(tokenKeywordForeign); err != nil {
		return nil, newParseError("unsupported query form")
	}
	if _, err := p.expect(tokenKeywordKey); err != nil {
		return nil, newParseError("unsupported query form")
	}
	childColumns, err := p.parseIdentifierList()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokenKeywordReferences); err != nil {
		return nil, newParseError("unsupported query form")
	}
	parentTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(parentTok.Lexeme) {
		return nil, newParseError("unsupported query form")
	}
	parentColumns, err := p.parseIdentifierList()
	if err != nil {
		return nil, err
	}
	indexName, err := p.parseUsingIndexClause()
	if err != nil {
		return nil, err
	}
	onDelete, err := p.parseForeignKeyOnDeleteClause()
	if err != nil {
		return nil, err
	}
	return &ForeignKeyDef{
		Columns:       childColumns,
		ParentTable:   parentTok.Lexeme,
		ParentColumns: parentColumns,
		IndexName:     indexName,
		OnDelete:      onDelete,
	}, nil
}

func (p *createTableTokenParser) parseIdentifierList() ([]string, error) {
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, newParseError("unsupported query form")
	}
	if p.current().Kind == tokenRParen {
		return nil, newParseError("unsupported query form")
	}

	values := make([]string, 0, 2)
	for {
		nameTok, err := p.expect(tokenIdentifier)
		if err != nil || !isIdentifier(nameTok.Lexeme) {
			return nil, newParseError("unsupported query form")
		}
		values = append(values, nameTok.Lexeme)
		if p.current().Kind != tokenComma {
			break
		}
		p.pos++
	}
	if _, err := p.expect(tokenRParen); err != nil {
		return nil, newParseError("unsupported query form")
	}
	return values, nil
}

func (p *createTableTokenParser) parseUsingIndexClause() (string, error) {
	if _, err := p.expect(tokenKeywordUsing); err != nil {
		return "", newParseError("unsupported query form")
	}
	if _, err := p.expect(tokenKeywordIndex); err != nil {
		return "", newParseError("unsupported query form")
	}
	nameTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(nameTok.Lexeme) {
		return "", newParseError("unsupported query form")
	}
	return nameTok.Lexeme, nil
}

func (p *createTableTokenParser) parseForeignKeyOnDeleteClause() (ForeignKeyDeleteAction, error) {
	if _, err := p.expect(tokenKeywordOn); err != nil {
		return "", newParseError("unsupported query form")
	}
	if _, err := p.expect(tokenKeywordDelete); err != nil {
		return "", newParseError("unsupported query form")
	}
	switch p.current().Kind {
	case tokenKeywordRestrict:
		p.pos++
		return ForeignKeyDeleteActionRestrict, nil
	case tokenKeywordCascade:
		p.pos++
		return ForeignKeyDeleteActionCascade, nil
	default:
		return "", newParseError("unsupported query form")
	}
}

func (p *createTableTokenParser) parseColumnDef() (ColumnDef, error) {
	nameTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(nameTok.Lexeme) {
		return ColumnDef{}, newParseError("unsupported query form")
	}

	columnType, err := p.parseColumnType()
	if err != nil {
		return ColumnDef{}, newParseError("unsupported query form")
	}

	column := ColumnDef{Name: nameTok.Lexeme, Type: columnType}

	if p.current().Kind == tokenKeywordNot {
		p.pos++
		if _, err := p.expect(tokenKeywordNull); err != nil {
			return ColumnDef{}, newParseError("unsupported query form")
		}
		column.NotNull = true
	}

	if p.current().Kind == tokenKeywordDefault {
		p.pos++
		defaultValue, err := p.parseColumnDefaultLiteral()
		if err != nil {
			return ColumnDef{}, err
		}
		if column.NotNull && defaultValue.Kind == ValueKindNull {
			return ColumnDef{}, newParseError("unsupported query form")
		}
		column.HasDefault = true
		column.DefaultValue = defaultValue
	}

	return column, nil
}

func (p *createTableTokenParser) parseColumnType() (string, error) {
	typeTok := p.current()
	switch typeTok.Kind {
	case tokenKeywordSmallInt, tokenKeywordInt, tokenKeywordBigInt, tokenKeywordText, tokenKeywordBool, tokenKeywordReal:
		p.pos++
		return normalizeColumnType(typeTok.Kind), nil
	default:
		return "", newParseError("unsupported query form")
	}
}

func (p *createTableTokenParser) parseColumnDefaultLiteral() (Value, error) {
	tok := p.current()
	value, ok := parseLiteralToken(tok)
	if !ok || value.Kind == ValueKindPlaceholder {
		return Value{}, newParseError("unsupported query form")
	}
	p.pos++
	return value, nil
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
	case tokenKeywordSmallInt:
		return ColumnTypeSmallInt
	case tokenKeywordInt:
		return ColumnTypeInt
	case tokenKeywordBigInt:
		return ColumnTypeBigInt
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
