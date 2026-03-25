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
	ExprKindRealLiteral
	ExprKindStringLiteral
	ExprKindBoolLiteral
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
	F64   float64
	Str   string
	Bool  bool
	Left  *Expr
	Right *Expr
	Op    BinaryOp
	Inner *Expr
}

// Condition is one parsed WHERE <column> <op> <literal> clause.
type Condition struct {
	Left      string
	LeftExpr  *ValueExpr
	Operator  string
	Right     Value
	RightRef  string
	RightExpr *ValueExpr
}

// BooleanOp is the connector between WHERE conditions.
type BooleanOp string

const (
	BooleanOpAnd BooleanOp = "AND"
	BooleanOpOr  BooleanOp = "OR"
)

// ConditionChainItem preserves the parsed condition order and connector order.
type ConditionChainItem struct {
	Op        BooleanOp
	Condition Condition
}

// WhereClause is a flat left-to-right WHERE shape with no precedence rules.
type WhereClause struct {
	Items []ConditionChainItem
}

// OrderByClause is a single-column ORDER BY clause.
type OrderByClause struct {
	Column string
	Desc   bool
}

// TableRef is a FROM-clause table reference.
type TableRef struct {
	Name  string
	Alias string
}

// SelectExpr is the minimal parsed form for SELECT <expr>.
type SelectExpr struct {
	Expr             *Expr
	TableName        string
	From             []TableRef
	Columns          []string
	ProjectionExprs  []*ValueExpr
	ProjectionLabels []string
	Where            *WhereClause
	Predicate        *PredicateExpr
	OrderBy          *OrderByClause
	IsCountStar      bool
}

// ParseSelectExpr recognizes the tiny Stage 1 SELECT <expr> shape.
func ParseSelectExpr(sql string) (*SelectExpr, bool) {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)
	if strings.HasPrefix(upper, "SELECT ") {
		if selectFrom, ok := parseSelectFromTokens(trimmed); ok {
			return selectFrom, true
		}
		if exprSel, ok := parseSelectLiteralTokens(trimmed); ok {
			return exprSel, true
		}
	}

	return nil, false
}

func (s *SelectExpr) PrimaryTableRef() *TableRef {
	if s == nil {
		return nil
	}
	if len(s.From) > 0 {
		return &s.From[0]
	}
	if s.TableName == "" {
		return nil
	}
	return &TableRef{Name: s.TableName}
}

func parseOrderByClause(input string) (*OrderByClause, bool) {
	tokens, err := lexSQL(input)
	if err != nil {
		return nil, false
	}
	if len(tokens) < 2 {
		return nil, false
	}
	if tokens[0].Kind != tokenIdentifier || !isIdentifier(tokens[0].Lexeme) {
		return nil, false
	}

	column := tokens[0].Lexeme
	pos := 1
	if len(tokens) > 3 && tokens[1].Kind == tokenDot && tokens[2].Kind == tokenIdentifier && isIdentifier(tokens[2].Lexeme) {
		column = column + "." + tokens[2].Lexeme
		pos = 3
	}

	orderBy := &OrderByClause{Column: column}
	switch {
	case pos >= len(tokens):
		return nil, false
	case tokens[pos].Kind == tokenEOF:
		return orderBy, true
	case pos+1 < len(tokens) && tokens[pos+1].Kind == tokenEOF:
		switch {
		case strings.EqualFold(tokens[pos].Lexeme, "ASC"):
			return orderBy, true
		case strings.EqualFold(tokens[pos].Lexeme, "DESC"):
			orderBy.Desc = true
			return orderBy, true
		default:
			return nil, false
		}
	default:
		return nil, false
	}
}

func parseWhereClause(input string) (*WhereClause, bool) {
	tokens := strings.Fields(strings.TrimSpace(input))
	if len(tokens) < 3 || len(tokens)%4 != 3 {
		return nil, false
	}

	items := parseWhereItems(tokens)
	if len(items) == 0 {
		return nil, false
	}

	return &WhereClause{Items: items}, true
}

func parseWhereBridge(input string) (*WhereClause, *PredicateExpr, bool) {
	predicate, ok := parsePredicateExpr(input)
	if !ok {
		return nil, nil, false
	}
	where, _ := flattenPredicateExpr(predicate)
	return where, predicate, true
}

func parseWhereItems(tokens []string) []ConditionChainItem {
	items := make([]ConditionChainItem, 0, len(tokens)/4+1)
	for i := 0; i < len(tokens); i += 4 {
		var op BooleanOp
		if i > 0 {
			switch {
			case strings.EqualFold(tokens[i-1], string(BooleanOpAnd)):
				op = BooleanOpAnd
			case strings.EqualFold(tokens[i-1], string(BooleanOpOr)):
				op = BooleanOpOr
			default:
				return nil
			}
		}
		if !isIdentifier(tokens[i]) || !isWhereOperator(tokens[i+1]) {
			return nil
		}
		value, ok := parseLiteralValue(tokens[i+2])
		if !ok {
			return nil
		}

		items = append(items, ConditionChainItem{
			Op: op,
			Condition: Condition{
				Left:     tokens[i],
				Operator: tokens[i+1],
				Right:    value,
			},
		})
	}

	return items
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
	if value, ok := parseRealLiteral(token); ok {
		return &Expr{Kind: ExprKindRealLiteral, F64: value}, true
	}

	if isSingleQuotedStringLiteral(token) {
		return &Expr{Kind: ExprKindStringLiteral, Str: token[1 : len(token)-1]}, true
	}
	if strings.EqualFold(token, "TRUE") {
		return &Expr{Kind: ExprKindBoolLiteral, Bool: true}, true
	}
	if strings.EqualFold(token, "FALSE") {
		return &Expr{Kind: ExprKindBoolLiteral, Bool: false}, true
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
