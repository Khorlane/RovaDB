package parser

import (
	"errors"
	"strings"
)

var errUnsupportedStatement = errors.New("parser: unsupported statement")

// CreateTableStmt is the tiny parsed form for CREATE TABLE.
type CreateTableStmt struct {
	Name    string
	Columns []string
}

// Parse dispatches the tiny Stage 1 statement shapes.
func Parse(input string) (any, error) {
	trimmed := strings.TrimSpace(input)
	upper := strings.ToUpper(trimmed)

	if strings.HasPrefix(upper, "CREATE TABLE ") {
		return parseCreateTable(trimmed)
	}
	if strings.HasPrefix(upper, "INSERT INTO ") {
		return parseInsert(trimmed)
	}
	if sel, ok := ParseSelectExpr(trimmed); ok {
		return sel, nil
	}

	return nil, errUnsupportedStatement
}

func parseCreateTable(input string) (*CreateTableStmt, error) {
	const prefix = "CREATE TABLE"

	trimmed := strings.TrimSpace(input)
	if !strings.HasPrefix(strings.ToUpper(trimmed), prefix+" ") {
		return nil, errUnsupportedStatement
	}

	rest := strings.TrimSpace(trimmed[len(prefix):])
	split := strings.IndexAny(rest, " \t\r\n")
	if split <= 0 {
		return nil, errors.New("parser: invalid create table")
	}

	name := strings.TrimSpace(rest[:split])
	definition := strings.TrimSpace(rest[split:])
	if name == "" || !strings.HasPrefix(definition, "(") || !strings.HasSuffix(definition, ")") {
		return nil, errors.New("parser: invalid create table")
	}

	inner := strings.TrimSpace(definition[1 : len(definition)-1])
	if inner == "" {
		return nil, errors.New("parser: invalid create table")
	}

	rawColumns := strings.Split(inner, ",")
	columns := make([]string, 0, len(rawColumns))
	seen := make(map[string]struct{}, len(rawColumns))
	for _, raw := range rawColumns {
		column := strings.TrimSpace(raw)
		if column == "" {
			return nil, errors.New("parser: invalid create table")
		}
		if _, ok := seen[column]; ok {
			return nil, errors.New("parser: invalid create table")
		}
		seen[column] = struct{}{}
		columns = append(columns, column)
	}

	return &CreateTableStmt{Name: name, Columns: columns}, nil
}
