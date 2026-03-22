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
}

// SelectExpr is the minimal parsed form for SELECT <expr>.
type SelectExpr struct {
	Expr *Expr
}

// ParseSelectExpr recognizes the tiny Stage 1 SELECT <expr> shape.
func ParseSelectExpr(sql string) (*SelectExpr, bool) {
	tokens := strings.Fields(strings.TrimSpace(sql))
	if len(tokens) != 2 || !strings.EqualFold(tokens[0], "SELECT") {
		return nil, false
	}

	expr, ok := parseExpr(tokens[1])
	if !ok {
		return nil, false
	}

	return &SelectExpr{Expr: expr}, true
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
