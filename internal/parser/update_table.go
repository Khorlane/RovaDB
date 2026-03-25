package parser

import "strings"

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
	Predicate   *PredicateExpr
}

func parseUpdate(input string) (*UpdateStmt, error) {
	return parseUpdateTokens(input)
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
		if !isIdentifier(column) || valueToken == "" {
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
