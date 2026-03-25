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
	tokens []token
	pos    int
}

func parseSelectLiteralTokens(input string) (*SelectExpr, bool) {
	tokens, err := lexSQL(input)
	if err != nil {
		return nil, false
	}
	p := selectLiteralTokenParser{
		tokens: tokens,
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
	if p.current().Kind == tokenEOF {
		return nil, false
	}
	expr, ok := p.parseExprTokens(true)
	if !ok || p.current().Kind != tokenEOF {
		return nil, false
	}
	return &SelectExpr{Expr: expr}, true
}

func (p *selectLiteralTokenParser) expect(kind tokenKind) (token, bool) {
	tok := p.current()
	if tok.Kind != kind {
		return token{}, false
	}
	p.pos++
	return tok, true
}

func (p *selectLiteralTokenParser) current() token {
	if p.pos >= len(p.tokens) {
		return token{Kind: tokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *selectLiteralTokenParser) parseExprTokens(allowParen bool) (*Expr, bool) {
	if allowParen && p.current().Kind == tokenLParen {
		p.pos++
		expr, ok := p.parseExprTokens(false)
		if !ok {
			return nil, false
		}
		if _, ok := p.expect(tokenRParen); !ok {
			return nil, false
		}
		return &Expr{Kind: ExprKindParen, Inner: expr}, true
	}

	left, ok := parseExprToken(p.current())
	if !ok {
		return nil, false
	}
	p.pos++

	switch p.current().Kind {
	case tokenEOF, tokenRParen:
		return left, true
	case tokenPlus, tokenMinus:
		opTok := p.current()
		p.pos++
		leftInt, ok := exprAsIntLiteral(left)
		if !ok {
			return nil, false
		}
		right, ok := parseIntLiteralToken(p.current())
		if !ok {
			return nil, false
		}
		p.pos++

		op := BinaryOpInvalid
		switch opTok.Kind {
		case tokenPlus:
			op = BinaryOpAdd
		case tokenMinus:
			op = BinaryOpSub
		default:
			return nil, false
		}

		return &Expr{
			Kind:  ExprKindInt64Binary,
			Left:  leftInt,
			Right: right,
			Op:    op,
		}, true
	case tokenNumber:
		leftInt, ok := exprAsIntLiteral(left)
		if !ok || !strings.HasPrefix(p.current().Lexeme, "-") {
			return nil, false
		}
		right, ok := parseUnsignedIntLiteral(p.current().Lexeme[1:])
		if !ok {
			return nil, false
		}
		p.pos++
		return &Expr{
			Kind:  ExprKindInt64Binary,
			Left:  leftInt,
			Right: right,
			Op:    BinaryOpSub,
		}, true
	default:
		return nil, false
	}
}

func parseExprToken(tok token) (*Expr, bool) {
	switch tok.Kind {
	case tokenNumber:
		if i64, ok := parseIntLiteral(tok.Lexeme); ok {
			return i64, true
		}
		if value, ok := parseRealLiteral(tok.Lexeme); ok {
			return &Expr{Kind: ExprKindRealLiteral, F64: value}, true
		}
		return nil, false
	case tokenString:
		if !isSingleQuotedStringLiteral(tok.Lexeme) {
			return nil, false
		}
		if strings.Contains(tok.Lexeme[1:len(tok.Lexeme)-1], " ") {
			return nil, false
		}
		return &Expr{Kind: ExprKindStringLiteral, Str: tok.Lexeme[1 : len(tok.Lexeme)-1]}, true
	case tokenIdentifier:
		switch {
		case strings.EqualFold(tok.Lexeme, "TRUE"):
			return &Expr{Kind: ExprKindBoolLiteral, Bool: true}, true
		case strings.EqualFold(tok.Lexeme, "FALSE"):
			return &Expr{Kind: ExprKindBoolLiteral, Bool: false}, true
		default:
			return nil, false
		}
	default:
		return nil, false
	}
}

func exprAsIntLiteral(expr *Expr) (*Expr, bool) {
	if expr == nil || expr.Kind != ExprKindInt64Literal {
		return nil, false
	}
	return expr, true
}

func parseIntLiteralToken(tok token) (*Expr, bool) {
	if tok.Kind != tokenNumber {
		return nil, false
	}
	return parseIntLiteral(tok.Lexeme)
}

func parseUnsignedIntLiteral(token string) (*Expr, bool) {
	if strings.HasPrefix(token, "+") || strings.HasPrefix(token, "-") {
		return nil, false
	}
	return parseIntLiteral(token)
}
