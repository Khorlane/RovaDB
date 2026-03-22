package parser

import (
	"strconv"
	"strings"
)

// SelectLiteral is the minimal parsed form for SELECT <literal>.
type SelectLiteral struct {
	Value Value
}

// ParseSelectLiteral recognizes the tiny Stage 1 SELECT <literal> shape.
func ParseSelectLiteral(sql string) (*SelectLiteral, bool) {
	tokens := strings.Fields(strings.TrimSpace(sql))
	if len(tokens) != 2 || !strings.EqualFold(tokens[0], "SELECT") {
		return nil, false
	}
	if strings.HasPrefix(tokens[1], "+") {
		return nil, false
	}

	value, err := strconv.ParseInt(tokens[1], 10, 64)
	if err == nil {
		return &SelectLiteral{Value: Int64Value(value)}, true
	}

	if len(tokens[1]) >= 2 && tokens[1][0] == '\'' && tokens[1][len(tokens[1])-1] == '\'' {
		return &SelectLiteral{Value: StringValue(tokens[1][1 : len(tokens[1])-1])}, true
	}

	return nil, false
}
