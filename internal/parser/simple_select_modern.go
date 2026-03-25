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

	var where *WhereClause
	var predicate *PredicateExpr
	var orderBy *OrderByClause

	if !p.lexer.skipWhitespaceAndEOF() {
		nextTok, err := p.lexer.nextToken()
		if err != nil {
			return nil, false
		}
		switch nextTok.Kind {
		case tokenKeywordWhere:
			whereStart := p.lexer.pos
			orderStart := -1
			for {
				tok, err := p.lexer.nextToken()
				if err != nil {
					return nil, false
				}
				if tok.Kind == tokenEOF {
					wherePart := strings.TrimSpace(p.lexer.input[whereStart:p.lexer.pos])
					parsedWhere, parsedPredicate, ok := parseWhereBridge(wherePart)
					if !ok {
						return nil, false
					}
					where = parsedWhere
					predicate = parsedPredicate
					break
				}
				if tok.Kind == tokenKeywordOrder {
					byTok, ok := p.expect(tokenKeywordBy)
					if !ok {
						return nil, false
					}
					wherePart := strings.TrimSpace(p.lexer.input[whereStart:tok.Pos])
					parsedWhere, parsedPredicate, ok := parseWhereBridge(wherePart)
					if !ok {
						return nil, false
					}
					where = parsedWhere
					predicate = parsedPredicate
					orderStart = byTok.Pos + len(byTok.Lexeme)
					break
				}
			}
			if orderStart >= 0 {
				orderPart := strings.TrimSpace(p.lexer.input[orderStart:])
				parsedOrderBy, ok := parseOrderByClause(orderPart)
				if !ok {
					return nil, false
				}
				orderBy = parsedOrderBy
			}
		case tokenKeywordOrder:
			byTok, ok := p.expect(tokenKeywordBy)
			if !ok {
				return nil, false
			}
			orderPart := strings.TrimSpace(p.lexer.input[byTok.Pos+len(byTok.Lexeme):])
			parsedOrderBy, ok := parseOrderByClause(orderPart)
			if !ok {
				return nil, false
			}
			orderBy = parsedOrderBy
		default:
			return nil, false
		}
	}

	if selectList == "*" {
		return &SelectExpr{
			TableName: tableTok.Lexeme,
			Where:     where,
			Predicate: predicate,
			OrderBy:   orderBy,
		}, true
	}
	if strings.EqualFold(selectList, "COUNT(*)") {
		return &SelectExpr{
			TableName:   tableTok.Lexeme,
			Where:       where,
			Predicate:   predicate,
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
		Predicate: predicate,
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

func (l *lexer) skipWhitespaceAndEOF() bool {
	l.skipWhitespace()
	return l.pos >= len(l.input)
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
