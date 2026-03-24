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
	const prefix = "INSERT INTO"

	trimmed := strings.TrimSpace(input)
	if !strings.HasPrefix(strings.ToUpper(trimmed), prefix+" ") {
		return nil, errUnsupportedStatement
	}

	rest := strings.TrimSpace(trimmed[len(prefix):])
	split := strings.IndexAny(rest, " \t\r\n")
	if split <= 0 {
		return nil, newParseError("unsupported query form")
	}

	tableName := strings.TrimSpace(rest[:split])
	afterTable := strings.TrimSpace(rest[split:])
	if tableName == "" {
		return nil, newParseError("unsupported query form")
	}

	columns := []string(nil)
	if strings.HasPrefix(afterTable, "(") {
		closeIdx := strings.Index(afterTable, ")")
		if closeIdx < 0 {
			return nil, newParseError("unsupported query form")
		}

		parsedColumns, ok := parseInsertColumns(afterTable[1:closeIdx])
		if !ok {
			return nil, newParseError("unsupported query form")
		}
		columns = parsedColumns
		afterTable = strings.TrimSpace(afterTable[closeIdx+1:])
	}

	if !strings.HasPrefix(strings.ToUpper(afterTable), "VALUES ") {
		return nil, newParseError("unsupported query form")
	}

	valuesPart := strings.TrimSpace(afterTable[len("VALUES"):])
	if !strings.HasPrefix(valuesPart, "(") || !strings.HasSuffix(valuesPart, ")") {
		return nil, newParseError("unsupported query form")
	}

	inner := strings.TrimSpace(valuesPart[1 : len(valuesPart)-1])
	if inner == "" {
		return nil, newParseError("unsupported query form")
	}

	rawValues := strings.Split(inner, ",")
	values := make([]Value, 0, len(rawValues))
	for _, raw := range rawValues {
		token := strings.TrimSpace(raw)
		if token == "" {
			return nil, newParseError("unsupported query form")
		}

		value, ok := parseLiteralValue(token)
		if !ok {
			return nil, newParseError("unsupported query form")
		}
		values = append(values, value)
	}
	if len(columns) > 0 && len(columns) != len(values) {
		return nil, newParseError("unsupported query form")
	}

	return &InsertStmt{TableName: tableName, Columns: columns, Values: values}, nil
}

func parseLiteralValue(token string) (Value, bool) {
	if strings.HasPrefix(token, "+") {
		return Value{}, false
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
		if column == "" {
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
