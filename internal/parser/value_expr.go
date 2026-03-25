package parser

import "strings"

type ValueExprKind int

const (
	ValueExprKindInvalid ValueExprKind = iota
	ValueExprKindLiteral
	ValueExprKindColumnRef
	ValueExprKindFunctionCall
	ValueExprKindParen
)

type ValueExpr struct {
	Kind     ValueExprKind
	Value    Value
	Column   string
	FuncName string
	Arg      *ValueExpr
	Inner    *ValueExpr
}

type valueExprTokenParser struct {
	tokens []token
	pos    int
}

func parseValueExpr(input string) (*ValueExpr, bool) {
	tokens, err := lexSQL(input)
	if err != nil {
		return nil, false
	}
	p := valueExprTokenParser{tokens: tokens}
	expr, ok := p.parse()
	if !ok || p.current().Kind != tokenEOF {
		return nil, false
	}
	return expr, true
}

func parseValueExprTokenStream(tokens []token) (*ValueExpr, int, bool) {
	p := valueExprTokenParser{tokens: tokens}
	expr, ok := p.parse()
	if !ok {
		return nil, 0, false
	}
	return expr, p.pos, true
}

func (p *valueExprTokenParser) parse() (*ValueExpr, bool) {
	switch p.current().Kind {
	case tokenLParen:
		p.pos++
		inner, ok := p.parse()
		if !ok || p.current().Kind != tokenRParen {
			return nil, false
		}
		p.pos++
		return &ValueExpr{Kind: ValueExprKindParen, Inner: inner}, true
	case tokenIdentifier:
		tok := p.current()
		p.pos++
		if p.current().Kind == tokenLParen {
			p.pos++
			arg, ok := p.parse()
			if !ok || p.current().Kind != tokenRParen {
				return nil, false
			}
			p.pos++
			return &ValueExpr{
				Kind:     ValueExprKindFunctionCall,
				FuncName: strings.ToUpper(tok.Lexeme),
				Arg:      arg,
			}, true
		}
		switch {
		case strings.EqualFold(tok.Lexeme, "NULL"):
			return &ValueExpr{Kind: ValueExprKindLiteral, Value: NullValue()}, true
		case strings.EqualFold(tok.Lexeme, "TRUE"):
			return &ValueExpr{Kind: ValueExprKindLiteral, Value: BoolValue(true)}, true
		case strings.EqualFold(tok.Lexeme, "FALSE"):
			return &ValueExpr{Kind: ValueExprKindLiteral, Value: BoolValue(false)}, true
		default:
			return &ValueExpr{Kind: ValueExprKindColumnRef, Column: tok.Lexeme}, true
		}
	case tokenNumber, tokenString, tokenPlaceholder:
		value, ok := parseLiteralToken(p.current())
		if !ok {
			return nil, false
		}
		p.pos++
		return &ValueExpr{Kind: ValueExprKindLiteral, Value: value}, true
	default:
		return nil, false
	}
}

func (p *valueExprTokenParser) current() token {
	if p.pos >= len(p.tokens) {
		return token{Kind: tokenEOF}
	}
	return p.tokens[p.pos]
}

func flattenSimpleValueExpr(expr *ValueExpr) (Value, string, bool) {
	if expr == nil {
		return Value{}, "", false
	}

	switch expr.Kind {
	case ValueExprKindLiteral:
		return expr.Value, "", true
	case ValueExprKindColumnRef:
		return Value{}, expr.Column, true
	case ValueExprKindParen:
		return flattenSimpleValueExpr(expr.Inner)
	default:
		return Value{}, "", false
	}
}
