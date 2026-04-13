package parser

import "strings"

// InsertStmt is the tiny parsed form for INSERT INTO ... VALUES (...).
type InsertStmt struct {
	TableName  string
	Columns    []string
	Values     []Value
	ValueExprs []*ValueExpr
}

func parseInsert(input string) (*InsertStmt, error) {
	return parseInsertTokens(input)
}

func parseLiteralValue(token string) (Value, bool) {
	if strings.HasPrefix(token, "+") {
		return Value{}, false
	}
	if token == "?" {
		return PlaceholderValue(), true
	}
	if strings.EqualFold(token, "NULL") {
		return NullValue(), true
	}
	if strings.EqualFold(token, "TRUE") {
		return BoolValue(true), true
	}
	if strings.EqualFold(token, "FALSE") {
		return BoolValue(false), true
	}

	value, ok := parseUntypedIntegerLiteral(token)
	if ok {
		return Int64Value(value), true
	}
	if value, ok := parseRealLiteral(token); ok {
		return RealValue(value), true
	}

	if isSingleQuotedStringLiteral(token) {
		return StringValue(token[1 : len(token)-1]), true
	}

	return Value{}, false
}

func parseLiteralToken(tok token) (Value, bool) {
	switch tok.Kind {
	case tokenPlaceholder:
		return PlaceholderValue(), true
	case tokenNumber:
		if value, ok := parseUntypedIntegerLiteral(tok.Lexeme); ok {
			return Int64Value(value), true
		}
		if value, ok := parseRealLiteral(tok.Lexeme); ok {
			return RealValue(value), true
		}
		return Value{}, false
	case tokenString:
		if isSingleQuotedStringLiteral(tok.Lexeme) {
			return StringValue(tok.Lexeme[1 : len(tok.Lexeme)-1]), true
		}
		return Value{}, false
	case tokenIdentifier:
		switch {
		case strings.EqualFold(tok.Lexeme, "NULL"):
			return NullValue(), true
		case strings.EqualFold(tok.Lexeme, "TRUE"):
			return BoolValue(true), true
		case strings.EqualFold(tok.Lexeme, "FALSE"):
			return BoolValue(false), true
		default:
			return Value{}, false
		}
	case tokenKeywordNull:
		return NullValue(), true
	default:
		return Value{}, false
	}
}
