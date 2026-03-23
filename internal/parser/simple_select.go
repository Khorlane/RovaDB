package parser

import (
	"strconv"
	"strings"
)

// ExprKind identifies the parsed expression shape.
type ExprKind int

const (
	ExprKindInvalid ExprKind = iota
	ExprKindInt64Literal
	ExprKindStringLiteral
	ExprKindInt64Binary
	ExprKindParen
)

// BinaryOp identifies the parsed binary operator.
type BinaryOp int

const (
	BinaryOpInvalid BinaryOp = iota
	BinaryOpAdd
	BinaryOpSub
)

// Expr is the tiny Stage 1 parsed expression model.
type Expr struct {
	Kind  ExprKind
	I64   int64
	Str   string
	Left  *Expr
	Right *Expr
	Op    BinaryOp
	Inner *Expr
}

// SelectExpr is the minimal parsed form for SELECT <expr>.
type SelectExpr struct {
	Expr        *Expr
	TableName   string
	Columns     []string
	SelectAll   bool
	WhereColumn string
	WhereValue  Value
	HasWhere    bool
}

// ParseSelectExpr recognizes the tiny Stage 1 SELECT <expr> shape.
func ParseSelectExpr(sql string) (*SelectExpr, bool) {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)
	if strings.HasPrefix(upper, "SELECT ") {
		if selectFrom, ok := parseSelectFrom(trimmed, upper); ok {
			return selectFrom, true
		}
	}

	tokens := strings.Fields(trimmed)
	if len(tokens) != 2 && len(tokens) != 4 {
		return nil, false
	}
	if !strings.EqualFold(tokens[0], "SELECT") {
		return nil, false
	}
	if expr, ok := parseParenExpr(strings.Join(tokens[1:], " ")); ok {
		return &SelectExpr{Expr: expr}, true
	}

	if len(tokens) == 2 {
		expr, ok := parseExpr(tokens[1])
		if !ok {
			return nil, false
		}

		return &SelectExpr{Expr: expr}, true
	}

	return parseSpacedIntBinaryExpr(tokens[1], tokens[2], tokens[3])
}

func parseSelectFrom(sql, upper string) (*SelectExpr, bool) {
	fromIndex := strings.Index(upper, " FROM ")
	if fromIndex < 0 {
		return nil, false
	}

	selectList := strings.TrimSpace(sql[len("SELECT "):fromIndex])
	fromPart := strings.TrimSpace(sql[fromIndex+len(" FROM "):])
	whereUpper := strings.ToUpper(fromPart)
	tableName := fromPart
	whereColumn := ""
	whereValue := Value{}
	hasWhere := false
	if whereIndex := strings.Index(whereUpper, " WHERE "); whereIndex >= 0 {
		tableName = strings.TrimSpace(fromPart[:whereIndex])
		whereClause := strings.TrimSpace(fromPart[whereIndex+len(" WHERE "):])
		column, value, ok := parseWhereClause(whereClause)
		if !ok {
			return nil, false
		}
		whereColumn = column
		whereValue = value
		hasWhere = true
	}

	if selectList == "" || tableName == "" || strings.ContainsAny(tableName, " \t\r\n,") {
		return nil, false
	}

	if selectList == "*" {
		return &SelectExpr{
			TableName:   tableName,
			SelectAll:   true,
			WhereColumn: whereColumn,
			WhereValue:  whereValue,
			HasWhere:    hasWhere,
		}, true
	}

	rawColumns := strings.Split(selectList, ",")
	columns := make([]string, 0, len(rawColumns))
	for _, raw := range rawColumns {
		column := strings.TrimSpace(raw)
		if column == "" || column == "*" || strings.ContainsAny(column, " \t\r\n()'+-*/") {
			return nil, false
		}
		columns = append(columns, column)
	}
	if len(columns) == 0 {
		return nil, false
	}

	return &SelectExpr{
		TableName:   tableName,
		Columns:     columns,
		WhereColumn: whereColumn,
		WhereValue:  whereValue,
		HasWhere:    hasWhere,
	}, true
}

func parseWhereClause(input string) (string, Value, bool) {
	tokens := strings.Fields(strings.TrimSpace(input))
	if len(tokens) != 3 || tokens[0] == "" || tokens[1] != "=" {
		return "", Value{}, false
	}

	value, ok := parseLiteralValue(tokens[2])
	if !ok {
		return "", Value{}, false
	}

	return tokens[0], value, true
}

func parseParenExpr(expr string) (*Expr, bool) {
	if len(expr) < 2 || expr[0] != '(' || expr[len(expr)-1] != ')' {
		return nil, false
	}

	inner := expr[1 : len(expr)-1]
	innerExpr, ok := parseInnerExpr(inner)
	if !ok {
		return nil, false
	}

	return &Expr{Kind: ExprKindParen, Inner: innerExpr}, true
}

func parseExpr(token string) (*Expr, bool) {
	if strings.HasPrefix(token, "+") {
		return nil, false
	}

	value, err := strconv.ParseInt(token, 10, 64)
	if err == nil {
		return &Expr{Kind: ExprKindInt64Literal, I64: value}, true
	}

	if isSingleQuotedStringLiteral(token) {
		return &Expr{Kind: ExprKindStringLiteral, Str: token[1 : len(token)-1]}, true
	}

	return parseIntBinaryExpr(token)
}

func parseInnerExpr(expr string) (*Expr, bool) {
	if strings.Contains(expr, "(") || strings.Contains(expr, ")") {
		return nil, false
	}

	innerTokens := strings.Fields(strings.TrimSpace(expr))
	switch len(innerTokens) {
	case 1:
		return parseExpr(innerTokens[0])
	case 3:
		sel, ok := parseSpacedIntBinaryExpr(innerTokens[0], innerTokens[1], innerTokens[2])
		if !ok {
			return nil, false
		}
		return sel.Expr, true
	default:
		return nil, false
	}
}

func parseIntBinaryExpr(expr string) (*Expr, bool) {
	for i := 1; i < len(expr); i++ {
		if expr[i] != '+' && expr[i] != '-' {
			continue
		}

		leftToken := expr[:i]
		rightToken := expr[i+1:]
		if leftToken == "" || rightToken == "" {
			return nil, false
		}

		left, ok := parseIntLiteral(leftToken)
		if !ok {
			return nil, false
		}
		right, ok := parseIntLiteral(rightToken)
		if !ok {
			return nil, false
		}

		op := BinaryOpAdd
		if expr[i] == '-' {
			op = BinaryOpSub
		}

		return &Expr{
			Kind:  ExprKindInt64Binary,
			Left:  left,
			Right: right,
			Op:    op,
		}, true
	}

	return nil, false
}

func parseSpacedIntBinaryExpr(leftToken, opToken, rightToken string) (*SelectExpr, bool) {
	left, ok := parseIntLiteral(leftToken)
	if !ok {
		return nil, false
	}
	right, ok := parseIntLiteral(rightToken)
	if !ok {
		return nil, false
	}

	op := BinaryOpInvalid
	switch opToken {
	case "+":
		op = BinaryOpAdd
	case "-":
		op = BinaryOpSub
	default:
		return nil, false
	}

	return &SelectExpr{
		Expr: &Expr{
			Kind:  ExprKindInt64Binary,
			Left:  left,
			Right: right,
			Op:    op,
		},
	}, true
}

func parseIntLiteral(token string) (*Expr, bool) {
	if strings.HasPrefix(token, "+") {
		return nil, false
	}

	value, err := strconv.ParseInt(token, 10, 64)
	if err != nil {
		return nil, false
	}

	return &Expr{Kind: ExprKindInt64Literal, I64: value}, true
}

func isSingleQuotedStringLiteral(s string) bool {
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return false
	}

	return !strings.Contains(s[1:len(s)-1], "'")
}
