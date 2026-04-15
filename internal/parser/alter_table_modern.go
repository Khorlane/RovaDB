package parser

type alterTableTokenParser struct {
	tokens []token
	pos    int
}

func parseAlterTableTokens(input string) (any, error) {
	tokens, err := lexSQL(input)
	if err != nil {
		return nil, err
	}

	p := alterTableTokenParser{tokens: tokens}
	return p.parse()
}

func (p *alterTableTokenParser) parse() (any, error) {
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

	switch p.current().Kind {
	case tokenKeywordAdd:
		return p.parseAdd(tableTok.Lexeme)
	case tokenKeywordDrop:
		return p.parseDrop(tableTok.Lexeme)
	default:
		return nil, newParseError("unsupported alter table form")
	}
}

func (p *alterTableTokenParser) parseAdd(tableName string) (any, error) {
	if _, err := p.expect(tokenKeywordAdd); err != nil {
		return nil, newParseError("unsupported alter table form")
	}
	switch p.current().Kind {
	case tokenKeywordColumn:
		return p.parseAddColumn(tableName)
	case tokenKeywordConstraint:
		return p.parseAddConstraint(tableName)
	default:
		return nil, newParseError("unsupported alter table form")
	}
}

func (p *alterTableTokenParser) parseAddColumn(tableName string) (*AlterTableAddColumnStmt, error) {
	if _, err := p.expect(tokenKeywordColumn); err != nil {
		return nil, newParseError("unsupported alter table form")
	}

	column, err := p.parseColumnDef()
	if err != nil {
		return nil, newParseError("unsupported alter table form")
	}

	if p.current().Kind != tokenEOF {
		return nil, newParseError("unsupported alter table form")
	}

	return &AlterTableAddColumnStmt{
		TableName: tableName,
		Column:    column,
	}, nil
}

func (p *alterTableTokenParser) parseAddConstraint(tableName string) (any, error) {
	if _, err := p.expect(tokenKeywordConstraint); err != nil {
		return nil, newParseError("unsupported alter table form")
	}
	nameTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(nameTok.Lexeme) {
		return nil, newParseError("unsupported alter table form")
	}
	switch p.current().Kind {
	case tokenKeywordPrimary:
		pk, err := p.parsePrimaryKeyDef()
		if err != nil {
			return nil, newParseError("unsupported alter table form")
		}
		pk.Name = nameTok.Lexeme
		if p.current().Kind != tokenEOF {
			return nil, newParseError("unsupported alter table form")
		}
		return &AlterTableAddPrimaryKeyStmt{TableName: tableName, PrimaryKey: *pk}, nil
	case tokenKeywordForeign:
		fk, err := p.parseForeignKeyDef()
		if err != nil {
			return nil, newParseError("unsupported alter table form")
		}
		fk.Name = nameTok.Lexeme
		if p.current().Kind != tokenEOF {
			return nil, newParseError("unsupported alter table form")
		}
		return &AlterTableAddForeignKeyStmt{TableName: tableName, ForeignKey: *fk}, nil
	default:
		return nil, newParseError("unsupported alter table form")
	}
}

func (p *alterTableTokenParser) parseDrop(tableName string) (any, error) {
	if _, err := p.expect(tokenKeywordDrop); err != nil {
		return nil, newParseError("unsupported alter table form")
	}
	switch p.current().Kind {
	case tokenKeywordPrimary:
		if _, err := p.expect(tokenKeywordPrimary); err != nil {
			return nil, newParseError("unsupported alter table form")
		}
		if _, err := p.expect(tokenKeywordKey); err != nil {
			return nil, newParseError("unsupported alter table form")
		}
		if p.current().Kind != tokenEOF {
			return nil, newParseError("unsupported alter table form")
		}
		return &AlterTableDropPrimaryKeyStmt{TableName: tableName}, nil
	case tokenKeywordForeign:
		if _, err := p.expect(tokenKeywordForeign); err != nil {
			return nil, newParseError("unsupported alter table form")
		}
		if _, err := p.expect(tokenKeywordKey); err != nil {
			return nil, newParseError("unsupported alter table form")
		}
		nameTok, err := p.expect(tokenIdentifier)
		if err != nil || !isIdentifier(nameTok.Lexeme) {
			return nil, newParseError("unsupported alter table form")
		}
		if p.current().Kind != tokenEOF {
			return nil, newParseError("unsupported alter table form")
		}
		return &AlterTableDropForeignKeyStmt{TableName: tableName, ConstraintName: nameTok.Lexeme}, nil
	default:
		return nil, newParseError("unsupported alter table form")
	}
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

func (p *alterTableTokenParser) parsePrimaryKeyDef() (*PrimaryKeyDef, error) {
	if _, err := p.expect(tokenKeywordPrimary); err != nil {
		return nil, err
	}
	if _, err := p.expect(tokenKeywordKey); err != nil {
		return nil, err
	}
	columns, err := p.parseIdentifierList()
	if err != nil {
		return nil, err
	}
	indexName, err := p.parseUsingIndexClause()
	if err != nil {
		return nil, err
	}
	return &PrimaryKeyDef{Columns: columns, IndexName: indexName}, nil
}

func (p *alterTableTokenParser) parseForeignKeyDef() (*ForeignKeyDef, error) {
	if _, err := p.expect(tokenKeywordForeign); err != nil {
		return nil, err
	}
	if _, err := p.expect(tokenKeywordKey); err != nil {
		return nil, err
	}
	columns, err := p.parseIdentifierList()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokenKeywordReferences); err != nil {
		return nil, err
	}
	parentTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(parentTok.Lexeme) {
		return nil, newParseError("unsupported alter table form")
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
		Columns:       columns,
		ParentTable:   parentTok.Lexeme,
		ParentColumns: parentColumns,
		IndexName:     indexName,
		OnDelete:      onDelete,
	}, nil
}

func (p *alterTableTokenParser) parseIdentifierList() ([]string, error) {
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}
	if p.current().Kind == tokenRParen {
		return nil, newParseError("unsupported alter table form")
	}
	values := make([]string, 0, 2)
	for {
		nameTok, err := p.expect(tokenIdentifier)
		if err != nil || !isIdentifier(nameTok.Lexeme) {
			return nil, newParseError("unsupported alter table form")
		}
		values = append(values, nameTok.Lexeme)
		if p.current().Kind != tokenComma {
			break
		}
		p.pos++
	}
	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}
	return values, nil
}

func (p *alterTableTokenParser) parseUsingIndexClause() (string, error) {
	if _, err := p.expect(tokenKeywordUsing); err != nil {
		return "", err
	}
	if _, err := p.expect(tokenKeywordIndex); err != nil {
		return "", err
	}
	nameTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(nameTok.Lexeme) {
		return "", newParseError("unsupported alter table form")
	}
	return nameTok.Lexeme, nil
}

func (p *alterTableTokenParser) parseForeignKeyOnDeleteClause() (ForeignKeyDeleteAction, error) {
	if _, err := p.expect(tokenKeywordOn); err != nil {
		return "", err
	}
	if _, err := p.expect(tokenKeywordDelete); err != nil {
		return "", err
	}
	switch p.current().Kind {
	case tokenKeywordRestrict:
		p.pos++
		return ForeignKeyDeleteActionRestrict, nil
	case tokenKeywordCascade:
		p.pos++
		return ForeignKeyDeleteActionCascade, nil
	default:
		return "", newParseError("unsupported alter table form")
	}
}

func (p *alterTableTokenParser) parseColumnDef() (ColumnDef, error) {
	nameTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(nameTok.Lexeme) {
		return ColumnDef{}, newParseError("unsupported alter table form")
	}

	columnType, err := p.parseColumnType()
	if err != nil {
		return ColumnDef{}, err
	}

	column := ColumnDef{Name: nameTok.Lexeme, Type: columnType}

	if p.current().Kind == tokenKeywordNot {
		p.pos++
		if _, err := p.expect(tokenKeywordNull); err != nil {
			return ColumnDef{}, newParseError("unsupported alter table form")
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
			return ColumnDef{}, newParseError("unsupported alter table form")
		}
		column.HasDefault = true
		column.DefaultValue = defaultValue
	}

	return column, nil
}

func (p *alterTableTokenParser) parseColumnType() (string, error) {
	typeTok := p.current()
	switch typeTok.Kind {
	case tokenKeywordSmallInt, tokenKeywordInt, tokenKeywordBigInt, tokenKeywordText, tokenKeywordBool, tokenKeywordReal:
		p.pos++
		return normalizeColumnType(typeTok.Kind), nil
	default:
		return "", newParseError("unsupported alter table form")
	}
}

func (p *alterTableTokenParser) parseColumnDefaultLiteral() (Value, error) {
	tok := p.current()
	value, err := parseLiteralToken(tok)
	if err != nil || value.Kind == ValueKindPlaceholder {
		return Value{}, newParseError("unsupported alter table form")
	}
	p.pos++
	return value, nil
}
