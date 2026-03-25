package parser

import "strings"

type selectFromTokenParser struct {
	lexer lexer
}

func parseSelectFromTokens(input string) (*SelectExpr, bool) {
	p := selectFromTokenParser{
		lexer: lexer{input: input},
	}
	return p.parse()
}

type selectLiteralTokenParser struct {
	lexer lexer
}

func parseSelectLiteralTokens(input string) (*SelectExpr, bool) {
	p := selectLiteralTokenParser{
		lexer: lexer{input: input},
	}
	return p.parse()
}

func (p *selectFromTokenParser) parse() (*SelectExpr, bool) {
	if _, ok := p.expect(tokenKeywordSelect); !ok {
		return nil, false
	}

	p.lexer.skipWhitespace()
	selectStart := p.lexer.pos
	for {
		tok, err := p.lexer.nextToken()
		if err != nil {
			return nil, false
		}
		if tok.Kind == tokenKeywordFrom {
			selectList := strings.TrimSpace(p.lexer.input[selectStart:tok.Pos])
			return p.parseAfterFrom(selectList)
		}
		if tok.Kind == tokenEOF {
			return nil, false
		}
	}
}

func (p *selectFromTokenParser) parseAfterFrom(selectList string) (*SelectExpr, bool) {
	tableTok, ok := p.expect(tokenIdentifier)
	if !ok || !isIdentifier(tableTok.Lexeme) || selectList == "" {
		return nil, false
	}

	p.lexer.skipWhitespace()
	remainder := strings.TrimSpace(p.lexer.input[p.lexer.pos:])

	var where *WhereClause
	var orderBy *OrderByClause

	if remainder != "" {
		upperRemainder := strings.ToUpper(remainder)
		if strings.HasPrefix(upperRemainder, "WHERE ") {
			wherePart := strings.TrimSpace(remainder[len("WHERE "):])
			orderByPart := ""
			if orderByIndex := strings.Index(strings.ToUpper(wherePart), " ORDER BY "); orderByIndex >= 0 {
				orderByPart = strings.TrimSpace(wherePart[orderByIndex+len(" ORDER BY "):])
				wherePart = strings.TrimSpace(wherePart[:orderByIndex])
			}

			parsedWhere, ok := parseWhereClause(wherePart)
			if !ok {
				return nil, false
			}
			where = parsedWhere

			if orderByPart != "" {
				parsedOrderBy, ok := parseOrderByClause(orderByPart)
				if !ok {
					return nil, false
				}
				orderBy = parsedOrderBy
			}
		} else if strings.HasPrefix(upperRemainder, "ORDER BY ") {
			orderByPart := strings.TrimSpace(remainder[len("ORDER BY "):])
			parsedOrderBy, ok := parseOrderByClause(orderByPart)
			if !ok {
				return nil, false
			}
			orderBy = parsedOrderBy
		} else {
			return nil, false
		}
	}

	if selectList == "*" {
		return &SelectExpr{
			TableName: tableTok.Lexeme,
			Where:     where,
			OrderBy:   orderBy,
		}, true
	}
	if strings.EqualFold(selectList, "COUNT(*)") {
		return &SelectExpr{
			TableName:   tableTok.Lexeme,
			Where:       where,
			OrderBy:     orderBy,
			IsCountStar: true,
		}, true
	}
	if strings.HasPrefix(strings.ToUpper(selectList), "COUNT(") {
		return nil, false
	}

	rawColumns := strings.Split(selectList, ",")
	columns := make([]string, 0, len(rawColumns))
	for _, raw := range rawColumns {
		column := strings.TrimSpace(raw)
		if column == "*" || !isIdentifier(column) {
			return nil, false
		}
		columns = append(columns, column)
	}
	if len(columns) == 0 {
		return nil, false
	}

	return &SelectExpr{
		TableName: tableTok.Lexeme,
		Columns:   columns,
		Where:     where,
		OrderBy:   orderBy,
	}, true
}

func (p *selectFromTokenParser) expect(kind tokenKind) (token, bool) {
	tok, err := p.lexer.nextToken()
	if err != nil {
		return token{}, false
	}
	if tok.Kind != kind {
		return token{}, false
	}
	return tok, true
}

func (p *selectLiteralTokenParser) parse() (*SelectExpr, bool) {
	if _, ok := p.expect(tokenKeywordSelect); !ok {
		return nil, false
	}

	remainder := strings.TrimSpace(p.lexer.input[p.lexer.pos:])
	if remainder == "" {
		return nil, false
	}

	if expr, ok := parseParenExpr(remainder); ok {
		return &SelectExpr{Expr: expr}, true
	}

	tokens := strings.Fields(remainder)
	if len(tokens) == 1 {
		expr, ok := parseExpr(tokens[0])
		if !ok {
			return nil, false
		}
		return &SelectExpr{Expr: expr}, true
	}
	if len(tokens) == 3 {
		return parseSpacedIntBinaryExpr(tokens[0], tokens[1], tokens[2])
	}

	return nil, false
}

func (p *selectLiteralTokenParser) expect(kind tokenKind) (token, bool) {
	tok, err := p.lexer.nextToken()
	if err != nil {
		return token{}, false
	}
	if tok.Kind != kind {
		return token{}, false
	}
	return tok, true
}
