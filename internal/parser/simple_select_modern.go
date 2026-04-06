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
	fromRefs, joins, ok := p.parseFromClause()
	if !ok || len(fromRefs) == 0 || selectList == "" {
		return nil, false
	}
	primary := fromRefs[0]

	var where *WhereClause
	var predicate *PredicateExpr
	var orderBy *OrderByClause
	var orderBys []OrderByClause

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
				parsedOrderBys, ok := parseOrderByClauses(orderPart)
				if !ok {
					return nil, false
				}
				orderBys = parsedOrderBys
				orderBy = &orderBys[0]
			}
		case tokenKeywordOrder:
			byTok, ok := p.expect(tokenKeywordBy)
			if !ok {
				return nil, false
			}
			orderPart := strings.TrimSpace(p.lexer.input[byTok.Pos+len(byTok.Lexeme):])
			parsedOrderBys, ok := parseOrderByClauses(orderPart)
			if !ok {
				return nil, false
			}
			orderBys = parsedOrderBys
			orderBy = &orderBys[0]
		default:
			return nil, false
		}
	}

	if selectList == "*" {
		return &SelectExpr{
			TableName: primary.Name,
			From:      fromRefs,
			Joins:     joins,
			Where:     where,
			Predicate: predicate,
			OrderBy:   orderBy,
			OrderBys:  orderBys,
		}, true
	}
	if strings.EqualFold(selectList, "COUNT(*)") {
		return &SelectExpr{
			TableName:   primary.Name,
			From:        fromRefs,
			Joins:       joins,
			Where:       where,
			Predicate:   predicate,
			OrderBy:     orderBy,
			OrderBys:    orderBys,
			IsCountStar: true,
		}, true
	}

	rawColumns := strings.Split(selectList, ",")
	labels := make([]string, 0, len(rawColumns))
	aliases := make([]string, 0, len(rawColumns))
	projections := make([]*ValueExpr, 0, len(rawColumns))
	columns := make([]string, 0, len(rawColumns))
	allColumns := true
	for _, raw := range rawColumns {
		item := strings.TrimSpace(raw)
		if item == "*" || item == "" {
			return nil, false
		}
		exprText, alias, ok := parseSelectProjectionItem(item)
		if !ok {
			return nil, false
		}
		expr, ok := parseValueExpr(exprText)
		if !ok {
			return nil, false
		}
		labels = append(labels, item)
		aliases = append(aliases, alias)
		projections = append(projections, expr)
		if expr.Kind == ValueExprKindColumnRef && alias == "" {
			columns = append(columns, expr.Column)
		} else {
			allColumns = false
		}
	}
	if len(projections) == 0 {
		return nil, false
	}
	if !allColumns {
		columns = nil
	}

	return &SelectExpr{
		TableName:         primary.Name,
		From:              fromRefs,
		Joins:             joins,
		Columns:           columns,
		ProjectionExprs:   projections,
		ProjectionLabels:  labels,
		ProjectionAliases: aliases,
		Where:             where,
		Predicate:         predicate,
		OrderBy:           orderBy,
		OrderBys:          orderBys,
	}, true
}

func parseSelectProjectionItem(input string) (string, string, bool) {
	tokens, err := lexSQL(input)
	if err != nil || len(tokens) < 2 {
		return "", "", false
	}
	for i := 0; i < len(tokens)-1; i++ {
		if tokens[i].Kind != tokenKeywordAs {
			continue
		}
		if len(tokens) < 4 || tokens[len(tokens)-1].Kind != tokenEOF || i != len(tokens)-3 {
			return "", "", false
		}
		aliasTok := tokens[i+1]
		if aliasTok.Kind != tokenIdentifier || !isIdentifier(aliasTok.Lexeme) {
			return "", "", false
		}
		exprText := strings.TrimSpace(input[:tokens[i].Pos])
		if exprText == "" {
			return "", "", false
		}
		return exprText, aliasTok.Lexeme, true
	}
	return input, "", true
}

func (p *selectFromTokenParser) parseFromClause() ([]TableRef, []JoinClause, bool) {
	refs := make([]TableRef, 0, 1)
	joins := make([]JoinClause, 0, 1)
	for {
		ref, ok := p.parseTableRef()
		if !ok {
			return nil, nil, false
		}
		refs = append(refs, ref)

		if p.lexer.skipWhitespaceAndEOF() {
			return refs, joins, true
		}

		nextTok, err := p.lexer.nextToken()
		if err != nil {
			return nil, nil, false
		}
		switch nextTok.Kind {
		case tokenComma:
			continue
		case tokenKeywordJoin, tokenKeywordInner:
			if len(refs) != 1 || len(joins) != 0 {
				return nil, nil, false
			}
			p.lexer.pos = nextTok.Pos
			join, ok := p.parseJoinClause()
			if !ok {
				return nil, nil, false
			}
			joins = append(joins, join)
			if p.lexer.skipWhitespaceAndEOF() {
				return refs, joins, true
			}
			boundaryTok, err := p.lexer.nextToken()
			if err != nil {
				return nil, nil, false
			}
			switch boundaryTok.Kind {
			case tokenKeywordWhere, tokenKeywordOrder:
				p.lexer.pos = boundaryTok.Pos
				return refs, joins, true
			default:
				return nil, nil, false
			}
		default:
			p.lexer.pos = nextTok.Pos
			return refs, joins, true
		}
	}
}

func (p *selectFromTokenParser) parseTableRef() (TableRef, bool) {
	tableTok, ok := p.expect(tokenIdentifier)
	if !ok || !isIdentifier(tableTok.Lexeme) {
		return TableRef{}, false
	}
	ref := TableRef{Name: tableTok.Lexeme}

	if !p.lexer.skipWhitespaceAndEOF() {
		nextTok, err := p.lexer.nextToken()
		if err != nil {
			return TableRef{}, false
		}
		switch nextTok.Kind {
		case tokenKeywordAs:
			aliasTok, ok := p.expect(tokenIdentifier)
			if !ok || !isIdentifier(aliasTok.Lexeme) {
				return TableRef{}, false
			}
			ref.Alias = aliasTok.Lexeme
		case tokenIdentifier:
			if !isIdentifier(nextTok.Lexeme) {
				return TableRef{}, false
			}
			ref.Alias = nextTok.Lexeme
		default:
			p.lexer.pos = nextTok.Pos
		}
	}

	return ref, true
}

func (p *selectFromTokenParser) parseJoinClause() (JoinClause, bool) {
	tok, err := p.lexer.nextToken()
	if err != nil {
		return JoinClause{}, false
	}
	switch tok.Kind {
	case tokenKeywordInner:
		if _, ok := p.expect(tokenKeywordJoin); !ok {
			return JoinClause{}, false
		}
	case tokenKeywordJoin:
	default:
		return JoinClause{}, false
	}

	right, ok := p.parseTableRef()
	if !ok {
		return JoinClause{}, false
	}
	if _, ok := p.expect(tokenKeywordOn); !ok {
		return JoinClause{}, false
	}

	onStart := p.lexer.pos
	for {
		tok, err := p.lexer.nextToken()
		if err != nil {
			return JoinClause{}, false
		}
		switch tok.Kind {
		case tokenEOF, tokenKeywordWhere, tokenKeywordOrder:
			onPart := strings.TrimSpace(p.lexer.input[onStart:tok.Pos])
			predicate, ok := parsePredicateExpr(onPart)
			if !ok {
				return JoinClause{}, false
			}
			p.lexer.pos = tok.Pos
			return JoinClause{Right: right, Predicate: predicate}, true
		}
	}
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
