package parser

type PredicateKind int

const (
	PredicateKindInvalid PredicateKind = iota
	PredicateKindComparison
	PredicateKindAnd
	PredicateKindOr
	PredicateKindNot
)

type PredicateExpr struct {
	Kind       PredicateKind
	Comparison *Condition
	Left       *PredicateExpr
	Right      *PredicateExpr
	Inner      *PredicateExpr
}

type predicateTokenParser struct {
	tokens []token
	pos    int
}

func flattenPredicateExpr(expr *PredicateExpr) (*WhereClause, bool) {
	if expr == nil {
		return nil, true
	}

	items, ok := flattenPredicateItems(expr)
	if !ok || len(items) == 0 {
		return nil, false
	}
	return &WhereClause{Items: items}, true
}

func parsePredicateExpr(input string) (*PredicateExpr, bool) {
	tokens, err := lexSQL(input)
	if err != nil {
		return nil, false
	}

	p := predicateTokenParser{tokens: tokens}
	expr, ok := p.parseOr()
	if !ok || p.current().Kind != tokenEOF {
		return nil, false
	}
	return expr, true
}

func (p *predicateTokenParser) parseOr() (*PredicateExpr, bool) {
	left, ok := p.parseAnd()
	if !ok {
		return nil, false
	}
	for p.current().Kind == tokenKeywordOr {
		p.pos++
		right, ok := p.parseAnd()
		if !ok {
			return nil, false
		}
		left = &PredicateExpr{Kind: PredicateKindOr, Left: left, Right: right}
	}
	return left, true
}

func (p *predicateTokenParser) parseAnd() (*PredicateExpr, bool) {
	left, ok := p.parseUnary()
	if !ok {
		return nil, false
	}
	for p.current().Kind == tokenKeywordAnd {
		p.pos++
		right, ok := p.parseUnary()
		if !ok {
			return nil, false
		}
		left = &PredicateExpr{Kind: PredicateKindAnd, Left: left, Right: right}
	}
	return left, true
}

func (p *predicateTokenParser) parseUnary() (*PredicateExpr, bool) {
	if p.current().Kind == tokenKeywordNot {
		p.pos++
		inner, ok := p.parseUnary()
		if !ok {
			return nil, false
		}
		return &PredicateExpr{Kind: PredicateKindNot, Inner: inner}, true
	}
	if p.current().Kind == tokenLParen {
		p.pos++
		inner, ok := p.parseOr()
		if !ok || p.current().Kind != tokenRParen {
			return nil, false
		}
		p.pos++
		return inner, true
	}
	return p.parseComparison()
}

func (p *predicateTokenParser) parseComparison() (*PredicateExpr, bool) {
	left := p.current()
	if left.Kind != tokenIdentifier || !isIdentifier(left.Lexeme) {
		return nil, false
	}
	p.pos++

	op := p.current()
	if !isWhereOperator(op.Lexeme) {
		return nil, false
	}
	switch op.Kind {
	case tokenEq, tokenNotEq, tokenLT, tokenLTE, tokenGT, tokenGTE:
		p.pos++
	default:
		return nil, false
	}

	rightTok := p.current()
	value, ok := parseLiteralToken(rightTok)
	if !ok {
		return nil, false
	}
	p.pos++

	cond := &Condition{
		Left:     left.Lexeme,
		Operator: op.Lexeme,
		Right:    value,
	}
	return &PredicateExpr{Kind: PredicateKindComparison, Comparison: cond}, true
}

func (p *predicateTokenParser) current() token {
	if p.pos >= len(p.tokens) {
		return token{Kind: tokenEOF}
	}
	return p.tokens[p.pos]
}

func flattenPredicateItems(expr *PredicateExpr) ([]ConditionChainItem, bool) {
	if expr == nil {
		return nil, false
	}

	switch expr.Kind {
	case PredicateKindComparison:
		if expr.Comparison == nil {
			return nil, false
		}
		return []ConditionChainItem{{
			Condition: *expr.Comparison,
		}}, true
	case PredicateKindAnd:
		return flattenBinaryPredicate(expr, BooleanOpAnd)
	case PredicateKindOr:
		return flattenBinaryPredicate(expr, BooleanOpOr)
	default:
		return nil, false
	}
}

func flattenBinaryPredicate(expr *PredicateExpr, op BooleanOp) ([]ConditionChainItem, bool) {
	if expr == nil || expr.Left == nil || expr.Right == nil {
		return nil, false
	}

	leftItems, ok := flattenPredicateItems(expr.Left)
	if !ok {
		return nil, false
	}
	rightItems, ok := flattenPredicateItems(expr.Right)
	if !ok {
		return nil, false
	}
	if len(rightItems) == 0 {
		return nil, false
	}

	// The legacy flat WHERE model only preserves left-to-right chains.
	// Reject shapes where flattening would silently change grouped or
	// precedence-sensitive semantics.
	if containsDifferentPredicateOp(expr.Right, op) {
		return nil, false
	}
	if expr.Left.Kind != PredicateKindComparison && expr.Left.Kind != predicateKindForBooleanOp(op) {
		return nil, false
	}

	items := append([]ConditionChainItem{}, leftItems...)
	rightItems[0].Op = op
	items = append(items, rightItems...)
	return items, true
}

func containsDifferentPredicateOp(expr *PredicateExpr, op BooleanOp) bool {
	if expr == nil {
		return false
	}

	switch expr.Kind {
	case PredicateKindComparison:
		return false
	case PredicateKindAnd:
		if op == BooleanOpAnd {
			return containsDifferentPredicateOp(expr.Left, op) || containsDifferentPredicateOp(expr.Right, op)
		}
		return true
	case PredicateKindOr:
		if op == BooleanOpOr {
			return containsDifferentPredicateOp(expr.Left, op) || containsDifferentPredicateOp(expr.Right, op)
		}
		return true
	default:
		return true
	}
}

func predicateKindForBooleanOp(op BooleanOp) PredicateKind {
	switch op {
	case BooleanOpAnd:
		return PredicateKindAnd
	case BooleanOpOr:
		return PredicateKindOr
	default:
		return PredicateKindInvalid
	}
}
