package parser

import "strings"

type updateTokenParser struct {
	lexer lexer
}

func parseUpdateTokens(input string) (*UpdateStmt, error) {
	p := updateTokenParser{lexer: lexer{input: input}}
	return p.parse()
}

func (p *updateTokenParser) parse() (*UpdateStmt, error) {
	if _, err := p.expect(tokenKeywordUpdate); err != nil {
		return nil, errUnsupportedStatement
	}

	tableTok, err := p.expect(tokenIdentifier)
	if err != nil || !isIdentifier(tableTok.Lexeme) {
		return nil, newParseError("unsupported query form")
	}

	if _, err := p.expect(tokenKeywordSet); err != nil {
		return nil, newParseError("unsupported query form")
	}

	var where *WhereClause
	var predicate *PredicateExpr
	assignments, err := p.parseAssignmentsTokens()
	if err != nil {
		return nil, newParseError("unsupported query form")
	}
	if p.peekAfterWhitespace().Kind == tokenKeywordWhere {
		if _, err := p.expect(tokenKeywordWhere); err != nil {
			return nil, newParseError("invalid where clause")
		}
		whereClause := p.remainingTrimmed()
		if whereClause == "" {
			return nil, newParseError("invalid where clause")
		}
		parsedWhere, parsedPredicate, ok := parseWhereBridge(whereClause)
		if !ok {
			return nil, newParseError("invalid where clause")
		}
		where = parsedWhere
		predicate = parsedPredicate
		p.lexer.pos = len(p.lexer.input)
		if _, err := p.expect(tokenEOF); err != nil {
			return nil, newParseError("invalid where clause")
		}
	} else {
		if _, err := p.expect(tokenEOF); err != nil {
			return nil, newParseError("unsupported query form")
		}
	}

	return &UpdateStmt{
		TableName:   tableTok.Lexeme,
		Assignments: assignments,
		Where:       where,
		Predicate:   predicate,
	}, nil
}

func (p *updateTokenParser) expect(kind tokenKind) (token, error) {
	tok, err := p.lexer.nextToken()
	if err != nil {
		return token{}, err
	}
	if tok.Kind != kind {
		return token{}, newParseError("unsupported query form")
	}
	return tok, nil
}

func (p *updateTokenParser) peekAfterWhitespace() token {
	saved := p.lexer.pos
	p.lexer.skipWhitespace()
	tok, err := p.lexer.nextToken()
	p.lexer.pos = saved
	if err != nil {
		return token{Kind: tokenIllegal}
	}
	return tok
}

func (p *updateTokenParser) remainingTrimmed() string {
	p.lexer.skipWhitespace()
	return strings.TrimSpace(p.lexer.input[p.lexer.pos:])
}

func (p *updateTokenParser) parseAssignmentsTokens() ([]UpdateAssignment, error) {
	assignments := make([]UpdateAssignment, 0, 2)
	seen := map[string]struct{}{}
	for {
		columnTok, err := p.expect(tokenIdentifier)
		if err != nil || !isIdentifier(columnTok.Lexeme) {
			return nil, newParseError("unsupported query form")
		}
		if _, ok := seen[columnTok.Lexeme]; ok {
			return nil, newParseError("unsupported query form")
		}
		if _, err := p.expect(tokenEq); err != nil {
			return nil, newParseError("unsupported query form")
		}
		expr, err := parseValueExprFromLexer(&p.lexer, tokenComma, tokenKeywordWhere, tokenEOF)
		if err != nil {
			return nil, newParseError("unsupported query form")
		}
		value := Value{}
		var assignmentExpr *ValueExpr
		if simpleValue, _, ok := flattenSimpleValueExpr(expr); ok {
			value = simpleValue
			if expr.Kind != ValueExprKindLiteral {
				assignmentExpr = expr
			}
		} else {
			assignmentExpr = expr
		}
		assignments = append(assignments, UpdateAssignment{
			Column: columnTok.Lexeme,
			Value:  value,
			Expr:   assignmentExpr,
		})
		seen[columnTok.Lexeme] = struct{}{}

		next := p.peekAfterWhitespace()
		if next.Kind != tokenComma {
			break
		}
		if _, err := p.expect(tokenComma); err != nil {
			return nil, newParseError("unsupported query form")
		}
	}
	if len(assignments) == 0 {
		return nil, newParseError("unsupported query form")
	}
	return assignments, nil
}
