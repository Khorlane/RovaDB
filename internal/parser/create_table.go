package parser

import (
	"strings"
)

var errUnsupportedStatement = newParseError("unsupported query form")

const (
	ColumnTypeInt  = "INT"
	ColumnTypeText = "TEXT"
)

// ColumnDef is the tiny parsed form for a typed column definition.
type ColumnDef struct {
	Name string
	Type string
}

// CreateTableStmt is the tiny parsed form for CREATE TABLE.
type CreateTableStmt struct {
	Name    string
	Columns []ColumnDef
}

// Parse dispatches the tiny Stage 1 statement shapes.
func Parse(input string) (any, error) {
	trimmed := strings.TrimSpace(input)
	upper := strings.ToUpper(trimmed)

	if strings.HasPrefix(upper, "CREATE TABLE ") {
		return parseCreateTable(trimmed)
	}
	if strings.HasPrefix(upper, "ALTER TABLE ") {
		return parseAlterTable(trimmed)
	}
	if strings.HasPrefix(upper, "INSERT INTO ") {
		return parseInsert(trimmed)
	}
	if strings.HasPrefix(upper, "DELETE FROM ") {
		return parseDelete(trimmed)
	}
	if strings.HasPrefix(upper, "UPDATE ") {
		return parseUpdate(trimmed)
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
		return nil, newParseError("unsupported query form")
	}

	name := strings.TrimSpace(rest[:split])
	definition := strings.TrimSpace(rest[split:])
	if name == "" || !strings.HasPrefix(definition, "(") || !strings.HasSuffix(definition, ")") {
		return nil, newParseError("unsupported query form")
	}

	inner := strings.TrimSpace(definition[1 : len(definition)-1])
	if inner == "" {
		return nil, newParseError("unsupported query form")
	}

	rawColumns := strings.Split(inner, ",")
	columns := make([]ColumnDef, 0, len(rawColumns))
	seen := make(map[string]struct{}, len(rawColumns))
	for _, raw := range rawColumns {
		parts := strings.Fields(strings.TrimSpace(raw))
		if len(parts) != 2 {
			return nil, newParseError("unsupported query form")
		}
		name := strings.TrimSpace(parts[0])
		typeName := strings.ToUpper(strings.TrimSpace(parts[1]))
		if name == "" {
			return nil, newParseError("unsupported query form")
		}
		if _, ok := seen[name]; ok {
			return nil, newParseError("unsupported query form")
		}
		if typeName != ColumnTypeInt && typeName != ColumnTypeText {
			return nil, newParseError("unsupported query form")
		}
		seen[name] = struct{}{}
		columns = append(columns, ColumnDef{Name: name, Type: typeName})
	}

	return &CreateTableStmt{Name: name, Columns: columns}, nil
}
