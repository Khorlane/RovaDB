package parser

import (
	"errors"
	"strings"
)

// UpdateAssignment is one SET target/value pair.
type UpdateAssignment struct {
	Column string
	Value  Value
}

// UpdateStmt is the tiny parsed form for UPDATE ... SET ... [WHERE ...].
type UpdateStmt struct {
	TableName   string
	Assignments []UpdateAssignment
	Where       *WhereClause
}

func parseUpdate(input string) (*UpdateStmt, error) {
	const prefix = "UPDATE"

	trimmed := strings.TrimSpace(input)
	if !strings.HasPrefix(strings.ToUpper(trimmed), prefix+" ") {
		return nil, errUnsupportedStatement
	}

	rest := strings.TrimSpace(trimmed[len(prefix):])
	split := strings.Index(strings.ToUpper(rest), " SET ")
	if split <= 0 {
		return nil, errors.New("parser: invalid update")
	}

	tableName := strings.TrimSpace(rest[:split])
	setPart := strings.TrimSpace(rest[split+len(" SET "):])
	if tableName == "" {
		return nil, errors.New("parser: invalid update")
	}

	var where *WhereClause
	upperSet := strings.ToUpper(setPart)
	assignmentsPart := setPart
	if whereIndex := strings.Index(upperSet, " WHERE "); whereIndex >= 0 {
		assignmentsPart = strings.TrimSpace(setPart[:whereIndex])
		whereClause := strings.TrimSpace(setPart[whereIndex+len(" WHERE "):])
		parsedWhere, ok := parseWhereClause(whereClause)
		if !ok {
			return nil, errors.New("parser: invalid update")
		}
		where = parsedWhere
	}

	assignments, ok := parseAssignments(assignmentsPart)
	if !ok {
		return nil, errors.New("parser: invalid update")
	}

	return &UpdateStmt{
		TableName:   tableName,
		Assignments: assignments,
		Where:       where,
	}, nil
}

func parseAssignments(input string) ([]UpdateAssignment, bool) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil, false
	}

	rawAssignments := strings.Split(trimmed, ",")
	assignments := make([]UpdateAssignment, 0, len(rawAssignments))
	seen := make(map[string]struct{}, len(rawAssignments))
	for _, raw := range rawAssignments {
		parts := strings.SplitN(strings.TrimSpace(raw), "=", 2)
		if len(parts) != 2 {
			return nil, false
		}

		column := strings.TrimSpace(parts[0])
		valueToken := strings.TrimSpace(parts[1])
		if column == "" || valueToken == "" {
			return nil, false
		}
		if _, ok := seen[column]; ok {
			return nil, false
		}

		value, ok := parseLiteralValue(valueToken)
		if !ok {
			return nil, false
		}

		seen[column] = struct{}{}
		assignments = append(assignments, UpdateAssignment{
			Column: column,
			Value:  value,
		})
	}

	return assignments, len(assignments) > 0
}
