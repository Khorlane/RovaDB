package parser

import (
	"errors"
	"strings"
)

// DeleteStmt is the tiny parsed form for DELETE FROM ... [WHERE ...].
type DeleteStmt struct {
	TableName string
	Where     *WhereClause
}

func parseDelete(input string) (*DeleteStmt, error) {
	const prefix = "DELETE FROM"

	trimmed := strings.TrimSpace(input)
	if !strings.HasPrefix(strings.ToUpper(trimmed), prefix+" ") {
		return nil, errUnsupportedStatement
	}

	rest := strings.TrimSpace(trimmed[len(prefix):])
	if rest == "" {
		return nil, errors.New("parser: invalid delete")
	}

	upperRest := strings.ToUpper(rest)
	tableName := rest
	var where *WhereClause
	if whereIndex := strings.Index(upperRest, " WHERE "); whereIndex >= 0 {
		tableName = strings.TrimSpace(rest[:whereIndex])
		whereClause := strings.TrimSpace(rest[whereIndex+len(" WHERE "):])
		parsedWhere, ok := parseWhereClause(whereClause)
		if !ok {
			return nil, errors.New("parser: invalid delete")
		}
		where = parsedWhere
	}

	if tableName == "" || strings.ContainsAny(tableName, " \t\r\n,") {
		return nil, errors.New("parser: invalid delete")
	}

	return &DeleteStmt{
		TableName: tableName,
		Where:     where,
	}, nil
}
