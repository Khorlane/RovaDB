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

	if isSingleQuotedStringLiteral(tokens[1]) {
		return &SelectLiteral{Value: StringValue(tokens[1][1 : len(tokens[1])-1])}, true
	}
	if value, ok := parseIntBinaryExpr(tokens[1]); ok {
		return &SelectLiteral{Value: Int64Value(value)}, true
	}

	return nil, false
}

func parseIntBinaryExpr(expr string) (int64, bool) {
	for i := 1; i < len(expr); i++ {
		if expr[i] != '+' && expr[i] != '-' {
			continue
		}

		left := expr[:i]
		right := expr[i+1:]
		if left == "" || right == "" {
			return 0, false
		}

		leftValue, err := strconv.ParseInt(left, 10, 64)
		if err != nil {
			return 0, false
		}
		rightValue, err := strconv.ParseInt(right, 10, 64)
		if err != nil {
			return 0, false
		}

		if expr[i] == '+' {
			return leftValue + rightValue, true
		}
		return leftValue - rightValue, true
	}

	return 0, false
}

func isSingleQuotedStringLiteral(s string) bool {
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return false
	}

	return !strings.Contains(s[1:len(s)-1], "'")
}
