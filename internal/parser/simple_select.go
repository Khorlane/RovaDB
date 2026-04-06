package parser

import "strings"

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

// WhereClause is the flattened WHERE form used when a predicate tree is not
// available. More expressive boolean precedence and grouping live in
// PredicateExpr.
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

// JoinClause is an explicit JOIN ... ON ... clause attached to SELECT.
type JoinClause struct {
	Right     TableRef
	Predicate *PredicateExpr
}

// SelectExpr is the minimal parsed form for SELECT <expr>.
type SelectExpr struct {
	Expr              *Expr
	TableName         string
	From              []TableRef
	Joins             []JoinClause
	Columns           []string
	ProjectionExprs   []*ValueExpr
	ProjectionLabels  []string
	ProjectionAliases []string
	Where             *WhereClause
	Predicate         *PredicateExpr
	OrderBy           *OrderByClause
	OrderBys          []OrderByClause
	IsCountStar       bool
}

// ParseSelectExpr recognizes the tiny Stage 1 SELECT <expr> shape.
func ParseSelectExpr(sql string) (*SelectExpr, bool) {
	trimmed := normalizeSQLInput(sql)
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

// PrimaryTableRef returns the effective primary FROM reference for SELECT
// resolution, using the single-table TableName field when no FROM list exists.
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

func parseOrderByClauses(input string) ([]OrderByClause, bool) {
	tokens, err := lexSQL(input)
	if err != nil {
		return nil, false
	}
	if len(tokens) < 2 {
		return nil, false
	}
	orderBys := make([]OrderByClause, 0, 2)
	pos := 0
	for {
		if tokens[pos].Kind != tokenIdentifier || !isIdentifier(tokens[pos].Lexeme) {
			return nil, false
		}
		column := tokens[pos].Lexeme
		pos++
		if pos+1 < len(tokens) && tokens[pos].Kind == tokenDot && tokens[pos+1].Kind == tokenIdentifier && isIdentifier(tokens[pos+1].Lexeme) {
			column = column + "." + tokens[pos+1].Lexeme
			pos += 2
		}
		item := OrderByClause{Column: column}
		if pos < len(tokens) {
			switch {
			case tokens[pos].Kind == tokenKeywordAsc:
				pos++
			case tokens[pos].Kind == tokenKeywordDesc:
				item.Desc = true
				pos++
			case tokens[pos].Kind == tokenIdentifier && strings.EqualFold(tokens[pos].Lexeme, "ASC"):
				pos++
			case tokens[pos].Kind == tokenIdentifier && strings.EqualFold(tokens[pos].Lexeme, "DESC"):
				item.Desc = true
				pos++
			}
		}
		orderBys = append(orderBys, item)
		if pos >= len(tokens) {
			return nil, false
		}
		switch tokens[pos].Kind {
		case tokenEOF:
			return orderBys, true
		case tokenComma:
			pos++
			if pos >= len(tokens) || tokens[pos].Kind == tokenEOF {
				return nil, false
			}
		default:
			return nil, false
		}
	}
}

func parseWhereBridge(input string) (*WhereClause, *PredicateExpr, bool) {
	predicate, ok := parsePredicateExpr(input)
	if !ok {
		return nil, nil, false
	}
	where, _ := flattenPredicateExpr(predicate)
	return where, predicate, true
}

func isWhereOperator(op string) bool {
	switch op {
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

func parseIntLiteral(token string) (*Expr, bool) {
	if strings.HasPrefix(token, "+") {
		return nil, false
	}

	value, ok := parsePublicIntLiteral(token)
	if !ok {
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
