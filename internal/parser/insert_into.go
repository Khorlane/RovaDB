package parser

import (
	"strconv"
	"strings"
)

// InsertStmt is the tiny parsed form for INSERT INTO ... VALUES (...).
type InsertStmt struct {
	TableName string
	Columns   []string
	Values    []Value
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

	value, err := strconv.ParseInt(token, 10, 64)
	if err == nil {
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

func parseInsertColumns(input string) ([]string, bool) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil, false
	}

	rawColumns := strings.Split(trimmed, ",")
	columns := make([]string, 0, len(rawColumns))
	seen := make(map[string]struct{}, len(rawColumns))
	for _, raw := range rawColumns {
		column := strings.TrimSpace(raw)
		if !isIdentifier(column) {
			return nil, false
		}
		if _, ok := seen[column]; ok {
			return nil, false
		}
		seen[column] = struct{}{}
		columns = append(columns, column)
	}

	return columns, len(columns) > 0
}
