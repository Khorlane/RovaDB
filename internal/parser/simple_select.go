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

// Condition is one parsed WHERE <column> <op> <literal> clause.
type Condition struct {
	Left     string
	Operator string
	Right    Value
}

// WhereClause is a flat AND-only WHERE shape.
type WhereClause struct {
	Conditions []Condition
}

// OrderByClause is a single-column ORDER BY clause.
type OrderByClause struct {
	Column string
	Desc   bool
}

// SelectExpr is the minimal parsed form for SELECT <expr>.
type SelectExpr struct {
	Expr      *Expr
	TableName string
	Columns   []string
	Where     *WhereClause
	OrderBy   *OrderByClause
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
	upperFromPart := strings.ToUpper(fromPart)
	orderByPart := ""
	if orderByIndex := strings.Index(upperFromPart, " ORDER BY "); orderByIndex >= 0 {
		orderByPart = strings.TrimSpace(fromPart[orderByIndex+len(" ORDER BY "):])
		fromPart = strings.TrimSpace(fromPart[:orderByIndex])
	}
	whereUpper := strings.ToUpper(fromPart)
	tableName := fromPart
	var where *WhereClause
	if whereIndex := strings.Index(whereUpper, " WHERE "); whereIndex >= 0 {
		tableName = strings.TrimSpace(fromPart[:whereIndex])
		whereClause := strings.TrimSpace(fromPart[whereIndex+len(" WHERE "):])
		parsedWhere, ok := parseWhereClause(whereClause)
		if !ok {
			return nil, false
		}
		where = parsedWhere
	}
	var orderBy *OrderByClause
	if orderByPart != "" {
		parsedOrderBy, ok := parseOrderByClause(orderByPart)
		if !ok {
			return nil, false
		}
		orderBy = parsedOrderBy
	}

	if selectList == "" || tableName == "" || strings.ContainsAny(tableName, " \t\r\n,") {
		return nil, false
	}

	if selectList == "*" {
		return &SelectExpr{
			TableName: tableName,
			Where:     where,
			OrderBy:   orderBy,
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
		TableName: tableName,
		Columns:   columns,
		Where:     where,
		OrderBy:   orderBy,
	}, true
}

func parseOrderByClause(input string) (*OrderByClause, bool) {
	tokens := strings.Fields(strings.TrimSpace(input))
	if len(tokens) < 1 || len(tokens) > 2 || tokens[0] == "" || strings.ContainsAny(tokens[0], " \t\r\n,") {
		return nil, false
	}

	orderBy := &OrderByClause{Column: tokens[0]}
	if len(tokens) == 1 {
		return orderBy, true
	}

	switch {
	case strings.EqualFold(tokens[1], "ASC"):
		return orderBy, true
	case strings.EqualFold(tokens[1], "DESC"):
		orderBy.Desc = true
		return orderBy, true
	default:
		return nil, false
	}
}

func parseWhereClause(input string) (*WhereClause, bool) {
	tokens := strings.Fields(strings.TrimSpace(input))
	if len(tokens) < 3 || len(tokens)%4 != 3 {
		return nil, false
	}

	conditions := parseWhereConditions(tokens)
	if len(conditions) == 0 {
		return nil, false
	}

	return &WhereClause{Conditions: conditions}, true
}

func parseWhereConditions(tokens []string) []Condition {
	conditions := make([]Condition, 0, len(tokens)/4+1)
	for i := 0; i < len(tokens); i += 4 {
		if i > 0 && !strings.EqualFold(tokens[i-1], "AND") {
			return nil
		}
		if tokens[i] == "" || !isWhereOperator(tokens[i+1]) {
			return nil
		}

		value, ok := parseLiteralValue(tokens[i+2])
		if !ok {
			return nil
		}

		conditions = append(conditions, Condition{
			Left:     tokens[i],
			Operator: tokens[i+1],
			Right:    value,
		})
	}

	return conditions
}

func isWhereOperator(op string) bool {
	switch op {
	case "=", "!=", "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
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
