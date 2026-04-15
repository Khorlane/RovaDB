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
		value, err := parseStringLiteralPayload(token[1 : len(token)-1])
		if err != nil {
			return Value{}, false
		}
		return value, true
	}

	return Value{}, false
}

func parseLiteralToken(tok token) (Value, error) {
	switch tok.Kind {
	case tokenPlaceholder:
		return PlaceholderValue(), nil
	case tokenNumber:
		if value, ok := parseUntypedIntegerLiteral(tok.Lexeme); ok {
			return Int64Value(value), nil
		}
		if value, ok := parseRealLiteral(tok.Lexeme); ok {
			return RealValue(value), nil
		}
		return Value{}, newParseError("unsupported query form")
	case tokenString:
		if isSingleQuotedStringLiteral(tok.Lexeme) {
			return parseStringLiteralPayload(tok.Lexeme[1 : len(tok.Lexeme)-1])
		}
		return Value{}, newParseError("unsupported query form")
	case tokenIdentifier:
		switch {
		case strings.EqualFold(tok.Lexeme, "NULL"):
			return NullValue(), nil
		case strings.EqualFold(tok.Lexeme, "TRUE"):
			return BoolValue(true), nil
		case strings.EqualFold(tok.Lexeme, "FALSE"):
			return BoolValue(false), nil
		default:
			return Value{}, newParseError("unsupported query form")
		}
	case tokenKeywordNull:
		return NullValue(), nil
	default:
		return Value{}, newParseError("unsupported query form")
	}
}
