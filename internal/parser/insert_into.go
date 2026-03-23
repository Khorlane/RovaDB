package parser

import (
	"errors"
	"strconv"
	"strings"
)

// InsertStmt is the tiny parsed form for INSERT INTO ... VALUES (...).
type InsertStmt struct {
	TableName string
	Values    []Value
}

func parseInsert(input string) (*InsertStmt, error) {
	const prefix = "INSERT INTO"

	trimmed := strings.TrimSpace(input)
	if !strings.HasPrefix(strings.ToUpper(trimmed), prefix+" ") {
		return nil, errUnsupportedStatement
	}

	rest := strings.TrimSpace(trimmed[len(prefix):])
	split := strings.IndexAny(rest, " \t\r\n")
	if split <= 0 {
		return nil, errors.New("parser: invalid insert")
	}

	tableName := strings.TrimSpace(rest[:split])
	afterTable := strings.TrimSpace(rest[split:])
	if tableName == "" || !strings.HasPrefix(strings.ToUpper(afterTable), "VALUES ") {
		return nil, errors.New("parser: invalid insert")
	}

	valuesPart := strings.TrimSpace(afterTable[len("VALUES"):])
	if !strings.HasPrefix(valuesPart, "(") || !strings.HasSuffix(valuesPart, ")") {
		return nil, errors.New("parser: invalid insert")
	}

	inner := strings.TrimSpace(valuesPart[1 : len(valuesPart)-1])
	if inner == "" {
		return nil, errors.New("parser: invalid insert")
	}

	rawValues := strings.Split(inner, ",")
	values := make([]Value, 0, len(rawValues))
	for _, raw := range rawValues {
		token := strings.TrimSpace(raw)
		if token == "" {
			return nil, errors.New("parser: invalid insert")
		}

		value, ok := parseLiteralValue(token)
		if !ok {
			return nil, errors.New("parser: invalid insert")
		}
		values = append(values, value)
	}

	return &InsertStmt{TableName: tableName, Values: values}, nil
}

func parseLiteralValue(token string) (Value, bool) {
	if strings.HasPrefix(token, "+") {
		return Value{}, false
	}

	value, err := strconv.ParseInt(token, 10, 64)
	if err == nil {
		return Int64Value(value), true
	}

	if isSingleQuotedStringLiteral(token) {
		return StringValue(token[1 : len(token)-1]), true
	}

	return Value{}, false
}
