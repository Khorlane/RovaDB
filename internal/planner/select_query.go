package planner

import "github.com/Khorlane/RovaDB/internal/parser"

type ValueKind int

const (
	ValueKindInvalid ValueKind = iota
	ValueKindNull
	ValueKindInt64
	ValueKindString
	ValueKindBool
	ValueKindReal
	ValueKindPlaceholder
)

type Value struct {
	Kind             ValueKind
	I64              int64
	Str              string
	Bool             bool
	F64              float64
	PlaceholderIndex int
}

func NullValue() Value {
	return Value{Kind: ValueKindNull}
}

func Int64Value(v int64) Value {
	return Value{Kind: ValueKindInt64, I64: v}
}

func StringValue(v string) Value {
	return Value{Kind: ValueKindString, Str: v}
}

func BoolValue(v bool) Value {
	return Value{Kind: ValueKindBool, Bool: v}
}

func RealValue(v float64) Value {
	return Value{Kind: ValueKindReal, F64: v}
}

func PlaceholderValue() Value {
	return Value{Kind: ValueKindPlaceholder, PlaceholderIndex: -1}
}

func (v Value) ParserValue() parser.Value {
	switch v.Kind {
	case ValueKindNull:
		return parser.NullValue()
	case ValueKindInt64:
		return parser.Int64Value(v.I64)
	case ValueKindString:
		return parser.StringValue(v.Str)
	case ValueKindBool:
		return parser.BoolValue(v.Bool)
	case ValueKindReal:
		return parser.RealValue(v.F64)
	case ValueKindPlaceholder:
		return parser.Value{Kind: parser.ValueKindPlaceholder, PlaceholderIndex: v.PlaceholderIndex}
	default:
		return parser.Value{}
	}
}

type ValueExprKind int

const (
	ValueExprKindInvalid ValueExprKind = iota
	ValueExprKindLiteral
	ValueExprKindColumnRef
	ValueExprKindBinary
	ValueExprKindFunctionCall
	ValueExprKindAggregateCall
	ValueExprKindParen
)

type ValueExprBinaryOp int

const (
	ValueExprBinaryOpInvalid ValueExprBinaryOp = iota
	ValueExprBinaryOpAdd
	ValueExprBinaryOpSub
)

type ValueExpr struct {
	Kind      ValueExprKind
	Value     Value
	Qualifier string
	Column    string
	Op        ValueExprBinaryOp
	Left      *ValueExpr
	Right     *ValueExpr
	FuncName  string
	Arg       *ValueExpr
	StarArg   bool
	Inner     *ValueExpr
}

type PredicateKind int

const (
	PredicateKindInvalid PredicateKind = iota
	PredicateKindComparison
	PredicateKindAnd
	PredicateKindOr
	PredicateKindNot
)

type Condition struct {
	Left      string
	LeftExpr  *ValueExpr
	Operator  string
	Right     Value
	RightRef  string
	RightExpr *ValueExpr
}

type BooleanOp string

const (
	BooleanOpAnd BooleanOp = "AND"
	BooleanOpOr  BooleanOp = "OR"
)

type ConditionChainItem struct {
	Op        BooleanOp
	Condition Condition
}

type WhereClause struct {
	Items []ConditionChainItem
}

type PredicateExpr struct {
	Kind       PredicateKind
	Comparison *Condition
	Left       *PredicateExpr
	Right      *PredicateExpr
	Inner      *PredicateExpr
}

type OrderByClause struct {
	Column string
	Desc   bool
}

type TableRef struct {
	Name  string
	Alias string
}

type JoinClause struct {
	Right     TableRef
	Predicate *PredicateExpr
}

type SelectQuery struct {
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

func (q *SelectQuery) PrimaryTableRef() *TableRef {
	if q == nil {
		return nil
	}
	if len(q.From) > 0 {
		return &q.From[0]
	}
	if q.TableName == "" {
		return nil
	}
	return &TableRef{Name: q.TableName}
}

func queryFromParser(stmt *parser.SelectExpr) *SelectQuery {
	if stmt == nil {
		return nil
	}
	return &SelectQuery{
		TableName:         stmt.TableName,
		From:              tableRefsFromParser(stmt.From),
		Joins:             joinsFromParser(stmt.Joins),
		Columns:           append([]string(nil), stmt.Columns...),
		ProjectionExprs:   valueExprsFromParser(stmt.ProjectionExprs),
		ProjectionLabels:  append([]string(nil), stmt.ProjectionLabels...),
		ProjectionAliases: append([]string(nil), stmt.ProjectionAliases...),
		Where:             whereFromParser(stmt.Where),
		Predicate:         predicateFromParser(stmt.Predicate),
		OrderBy:           orderByFromParser(stmt.OrderBy),
		OrderBys:          orderBysFromParser(stmt.OrderBys),
		IsCountStar:       stmt.IsCountStar,
	}
}

func tableRefsFromParser(refs []parser.TableRef) []TableRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]TableRef, 0, len(refs))
	for _, ref := range refs {
		out = append(out, TableRef{Name: ref.Name, Alias: ref.Alias})
	}
	return out
}

func joinsFromParser(joins []parser.JoinClause) []JoinClause {
	if len(joins) == 0 {
		return nil
	}
	out := make([]JoinClause, 0, len(joins))
	for _, join := range joins {
		out = append(out, JoinClause{
			Right:     TableRef{Name: join.Right.Name, Alias: join.Right.Alias},
			Predicate: predicateFromParser(join.Predicate),
		})
	}
	return out
}

func orderByFromParser(orderBy *parser.OrderByClause) *OrderByClause {
	if orderBy == nil {
		return nil
	}
	return &OrderByClause{Column: orderBy.Column, Desc: orderBy.Desc}
}

func orderBysFromParser(orderBys []parser.OrderByClause) []OrderByClause {
	if len(orderBys) == 0 {
		return nil
	}
	out := make([]OrderByClause, 0, len(orderBys))
	for _, orderBy := range orderBys {
		out = append(out, OrderByClause{Column: orderBy.Column, Desc: orderBy.Desc})
	}
	return out
}

func whereFromParser(where *parser.WhereClause) *WhereClause {
	if where == nil {
		return nil
	}
	items := make([]ConditionChainItem, 0, len(where.Items))
	for _, item := range where.Items {
		items = append(items, ConditionChainItem{
			Op:        BooleanOp(item.Op),
			Condition: conditionFromParser(item.Condition),
		})
	}
	return &WhereClause{Items: items}
}

func predicateFromParser(predicate *parser.PredicateExpr) *PredicateExpr {
	if predicate == nil {
		return nil
	}
	return &PredicateExpr{
		Kind:       PredicateKind(predicate.Kind),
		Comparison: conditionPtrFromParser(predicate.Comparison),
		Left:       predicateFromParser(predicate.Left),
		Right:      predicateFromParser(predicate.Right),
		Inner:      predicateFromParser(predicate.Inner),
	}
}

func conditionPtrFromParser(cond *parser.Condition) *Condition {
	if cond == nil {
		return nil
	}
	converted := conditionFromParser(*cond)
	return &converted
}

func conditionFromParser(cond parser.Condition) Condition {
	return Condition{
		Left:      cond.Left,
		LeftExpr:  valueExprFromParser(cond.LeftExpr),
		Operator:  cond.Operator,
		Right:     valueFromParser(cond.Right),
		RightRef:  cond.RightRef,
		RightExpr: valueExprFromParser(cond.RightExpr),
	}
}

func valueExprsFromParser(exprs []*parser.ValueExpr) []*ValueExpr {
	if len(exprs) == 0 {
		return nil
	}
	out := make([]*ValueExpr, 0, len(exprs))
	for _, expr := range exprs {
		out = append(out, valueExprFromParser(expr))
	}
	return out
}

func valueExprFromParser(expr *parser.ValueExpr) *ValueExpr {
	if expr == nil {
		return nil
	}
	return &ValueExpr{
		Kind:      ValueExprKind(expr.Kind),
		Value:     valueFromParser(expr.Value),
		Qualifier: expr.Qualifier,
		Column:    expr.Column,
		Op:        ValueExprBinaryOp(expr.Op),
		Left:      valueExprFromParser(expr.Left),
		Right:     valueExprFromParser(expr.Right),
		FuncName:  expr.FuncName,
		Arg:       valueExprFromParser(expr.Arg),
		StarArg:   expr.StarArg,
		Inner:     valueExprFromParser(expr.Inner),
	}
}

func valueFromParser(v parser.Value) Value {
	return Value{
		Kind:             ValueKind(v.Kind),
		I64:              v.I64,
		Str:              v.Str,
		Bool:             v.Bool,
		F64:              v.F64,
		PlaceholderIndex: v.PlaceholderIndex,
	}
}
