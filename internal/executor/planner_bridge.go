package executor

import (
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

type selectScanKind int

const (
	selectScanKindInvalid selectScanKind = iota
	selectScanKindTable
	selectScanKindIndex
	selectScanKindJoin
)

type runtimeValueExprKind int

const (
	runtimeValueExprKindInvalid runtimeValueExprKind = iota
	runtimeValueExprKindLiteral
	runtimeValueExprKindColumnRef
	runtimeValueExprKindBinary
	runtimeValueExprKindFunctionCall
	runtimeValueExprKindAggregateCall
	runtimeValueExprKindParen
)

type runtimeValueExprBinaryOp int

const (
	runtimeValueExprBinaryOpInvalid runtimeValueExprBinaryOp = iota
	runtimeValueExprBinaryOpAdd
	runtimeValueExprBinaryOpSub
)

type runtimeValueExpr struct {
	kind      runtimeValueExprKind
	value     parser.Value
	qualifier string
	column    string
	op        runtimeValueExprBinaryOp
	left      *runtimeValueExpr
	right     *runtimeValueExpr
	funcName  string
	arg       *runtimeValueExpr
	starArg   bool
	inner     *runtimeValueExpr
}

type runtimePredicateKind int

const (
	runtimePredicateKindInvalid runtimePredicateKind = iota
	runtimePredicateKindComparison
	runtimePredicateKindAnd
	runtimePredicateKindOr
	runtimePredicateKindNot
)

type runtimeCondition struct {
	left      string
	leftExpr  *runtimeValueExpr
	operator  string
	right     parser.Value
	rightRef  string
	rightExpr *runtimeValueExpr
}

type runtimeBooleanOp string

const (
	runtimeBooleanOpAnd runtimeBooleanOp = "AND"
	runtimeBooleanOpOr  runtimeBooleanOp = "OR"
)

type runtimeConditionChainItem struct {
	op        runtimeBooleanOp
	condition runtimeCondition
}

type runtimeWhereClause struct {
	items []runtimeConditionChainItem
}

type runtimePredicateExpr struct {
	kind       runtimePredicateKind
	comparison *runtimeCondition
	left       *runtimePredicateExpr
	right      *runtimePredicateExpr
	inner      *runtimePredicateExpr
}

type runtimeOrderByClause struct {
	column string
	desc   bool
}

type runtimeTableRef struct {
	name  string
	alias string
}

type runtimeJoinClause struct {
	right     runtimeTableRef
	predicate *runtimePredicateExpr
}

type runtimeSelectQuery struct {
	tableName         string
	from              []runtimeTableRef
	joins             []runtimeJoinClause
	columns           []string
	projectionExprs   []*runtimeValueExpr
	projectionLabels  []string
	projectionAliases []string
	where             *runtimeWhereClause
	predicate         *runtimePredicateExpr
	orderBy           *runtimeOrderByClause
	orderBys          []runtimeOrderByClause
	isCountStar       bool
}

func (q *runtimeSelectQuery) primaryTableRef() *runtimeTableRef {
	if q == nil {
		return nil
	}
	if len(q.from) > 0 {
		return &q.from[0]
	}
	if q.tableName == "" {
		return nil
	}
	return &runtimeTableRef{name: q.tableName}
}

type SelectAccessPathKind int

const (
	SelectAccessPathKindInvalid SelectAccessPathKind = iota
	SelectAccessPathKindTable
	SelectAccessPathKindIndex
	SelectAccessPathKindJoin
)

type SelectIndexLookup struct {
	TableName   string
	ColumnName  string
	LookupValue parser.Value
}

type SelectAccessPath struct {
	Kind            SelectAccessPathKind
	SingleTableName string
	JoinLeftTable   string
	JoinRightTable  string
	IndexLookup     SelectIndexLookup
}

type selectPlanBridge struct {
	query      *runtimeSelectQuery
	scanKind   selectScanKind
	accessPath SelectAccessPath
	join       selectPlanJoinScan
}

type selectPlanJoinScan struct {
	leftTableName   string
	leftTableAlias  string
	leftColumnName  string
	rightTableName  string
	rightTableAlias string
	rightColumnName string
}

// SelectExecutionHandoff is the executor-owned runtime entry shape for
// planned SELECT execution.
type SelectExecutionHandoff struct {
	bridge *selectPlanBridge
}

type IndexOnlyExecutionMode int

const (
	IndexOnlyExecutionModeInvalid IndexOnlyExecutionMode = iota
	IndexOnlyExecutionModeCountStar
	IndexOnlyExecutionModeProjection
)

// IndexOnlyExecutionHandoff centralizes the narrow index-only seam and the
// regular SELECT fallback handoff that shares the normal execution path.
type IndexOnlyExecutionHandoff struct {
	tableName      string
	columnName     string
	mode           IndexOnlyExecutionMode
	direct         bool
	fallbackSelect *SelectExecutionHandoff
}

// NewSelectExecutionHandoff adapts planner-owned SELECT output into the
// executor-owned runtime handoff once at the seam.
func NewSelectExecutionHandoff(plan *planner.SelectPlan) (*SelectExecutionHandoff, error) {
	bridge, err := bridgeSelectPlan(plan)
	if err != nil {
		return nil, err
	}
	return &SelectExecutionHandoff{bridge: bridge}, nil
}

// NewIndexOnlyExecutionHandoff adapts the planner's narrow index-only payload
// once at the seam and also prepares the regular SELECT fallback handoff.
func NewIndexOnlyExecutionHandoff(plan *planner.SelectPlan) (*IndexOnlyExecutionHandoff, error) {
	if plan == nil || plan.Query == nil || plan.ScanType != planner.ScanTypeIndexOnly || plan.IndexOnlyScan == nil {
		return nil, errInvalidSelectPlan
	}
	if plan.IndexOnlyScan.TableName == "" || len(plan.IndexOnlyScan.ColumnNames) != 1 || plan.IndexOnlyScan.ColumnNames[0] == "" {
		return nil, errInvalidSelectPlan
	}

	fallbackSelect, err := newIndexOnlyFallbackSelectHandoff(plan)
	if err != nil {
		return nil, err
	}

	mode := IndexOnlyExecutionModeProjection
	if plan.IndexOnlyScan.CountStar {
		mode = IndexOnlyExecutionModeCountStar
	}
	return &IndexOnlyExecutionHandoff{
		tableName:      plan.IndexOnlyScan.TableName,
		columnName:     plan.IndexOnlyScan.ColumnNames[0],
		mode:           mode,
		direct:         supportsDirectIndexOnlyExecution(plan),
		fallbackSelect: fallbackSelect,
	}, nil
}

// AccessPath exposes the bridge-owned access-path view without leaking planner
// shell details into executor runtime entry points.
func (h *SelectExecutionHandoff) AccessPath() SelectAccessPath {
	if h == nil || h.bridge == nil {
		return SelectAccessPath{}
	}
	return h.bridge.accessPath
}

func (h *IndexOnlyExecutionHandoff) SupportsDirectExecution() bool {
	if h == nil {
		return false
	}
	return h.direct
}

func (h *IndexOnlyExecutionHandoff) TableName() string {
	if h == nil {
		return ""
	}
	return h.tableName
}

func (h *IndexOnlyExecutionHandoff) ColumnName() string {
	if h == nil {
		return ""
	}
	return h.columnName
}

func (h *IndexOnlyExecutionHandoff) Mode() IndexOnlyExecutionMode {
	if h == nil {
		return IndexOnlyExecutionModeInvalid
	}
	return h.mode
}

func (h *IndexOnlyExecutionHandoff) FallbackSelectHandoff() *SelectExecutionHandoff {
	if h == nil {
		return nil
	}
	return h.fallbackSelect
}

// bridgeSelectPlan is the executor-facing seam for SELECT. It validates the
// current planner contract, centralizes access-path interpretation, and
// converts planner-owned query/value shapes into runtime executor structs.
// Raw planner shell input remains temporary and is a v0.41 tightening target.
func bridgeSelectPlan(plan *planner.SelectPlan) (*selectPlanBridge, error) {
	if plan == nil || plan.Query == nil {
		return nil, errUnsupportedStatement
	}

	bridge := &selectPlanBridge{
		query: runtimeSelectQueryFromPlan(plan.Query),
	}
	if bridge.query.tableName == "" {
		return bridge, nil
	}

	switch plan.ScanType {
	case planner.ScanTypeTable:
		if plan.TableScan == nil || plan.TableScan.TableName != bridge.query.tableName {
			return nil, errInvalidSelectPlan
		}
		bridge.scanKind = selectScanKindTable
		bridge.accessPath = SelectAccessPath{
			Kind:            SelectAccessPathKindTable,
			SingleTableName: plan.TableScan.TableName,
		}
	case planner.ScanTypeIndex:
		if plan.IndexScan == nil || plan.IndexScan.TableName != bridge.query.tableName || plan.IndexScan.ColumnName == "" {
			return nil, errInvalidSelectPlan
		}
		bridge.scanKind = selectScanKindIndex
		bridge.accessPath = SelectAccessPath{
			Kind:            SelectAccessPathKindIndex,
			SingleTableName: plan.IndexScan.TableName,
			IndexLookup: SelectIndexLookup{
				TableName:   plan.IndexScan.TableName,
				ColumnName:  plan.IndexScan.ColumnName,
				LookupValue: plan.IndexScan.LookupValue.ParserValue(),
			},
		}
	case planner.ScanTypeJoin:
		if plan.JoinScan == nil || plan.JoinScan.LeftTableName == "" || plan.JoinScan.RightTableName == "" || plan.JoinScan.LeftColumnName == "" || plan.JoinScan.RightColumnName == "" {
			return nil, errInvalidSelectPlan
		}
		bridge.scanKind = selectScanKindJoin
		bridge.accessPath = SelectAccessPath{
			Kind:           SelectAccessPathKindJoin,
			JoinLeftTable:  plan.JoinScan.LeftTableName,
			JoinRightTable: plan.JoinScan.RightTableName,
		}
		bridge.join = selectPlanJoinScan{
			leftTableName:   plan.JoinScan.LeftTableName,
			leftTableAlias:  plan.JoinScan.LeftTableAlias,
			leftColumnName:  plan.JoinScan.LeftColumnName,
			rightTableName:  plan.JoinScan.RightTableName,
			rightTableAlias: plan.JoinScan.RightTableAlias,
			rightColumnName: plan.JoinScan.RightColumnName,
		}
	default:
		return nil, errInvalidSelectPlan
	}
	return bridge, nil
}

// DescribeSelectAccessPath exposes the bridge-owned access-path view without
// leaking executor runtime evaluation state back into planner types.
func DescribeSelectAccessPath(plan *planner.SelectPlan) (SelectAccessPath, error) {
	handoff, err := NewSelectExecutionHandoff(plan)
	if err != nil {
		return SelectAccessPath{}, err
	}
	return handoff.AccessPath(), nil
}

func supportsDirectIndexOnlyExecution(plan *planner.SelectPlan) bool {
	if plan == nil || plan.Query == nil || plan.IndexOnlyScan == nil {
		return false
	}
	if plan.IndexOnlyScan.TableName == "" || len(plan.IndexOnlyScan.ColumnNames) != 1 || plan.IndexOnlyScan.ColumnNames[0] == "" {
		return false
	}
	if plan.IndexOnlyScan.CountStar {
		return plan.Query.IsCountStar &&
			plan.Query.Where == nil &&
			plan.Query.Predicate == nil &&
			len(plan.Query.OrderBys) == 0 &&
			plan.Query.OrderBy == nil
	}
	if plan.Query.IsCountStar ||
		plan.Query.Where != nil ||
		plan.Query.Predicate != nil ||
		len(plan.Query.OrderBys) > 0 ||
		plan.Query.OrderBy != nil ||
		len(plan.Query.ProjectionExprs) != 1 {
		return false
	}
	if len(plan.Query.ProjectionAliases) > 0 && plan.Query.ProjectionAliases[0] != "" {
		return false
	}
	expr := plan.Query.ProjectionExprs[0]
	return expr != nil && expr.Kind == planner.ValueExprKindColumnRef && expr.Column != ""
}

func newIndexOnlyFallbackSelectHandoff(plan *planner.SelectPlan) (*SelectExecutionHandoff, error) {
	downgraded := downgradeIndexOnlyPlanForSelectExecution(plan)
	if downgraded == nil {
		return nil, errInvalidSelectPlan
	}
	return NewSelectExecutionHandoff(downgraded)
}

func downgradeIndexOnlyPlanForSelectExecution(plan *planner.SelectPlan) *planner.SelectPlan {
	if plan == nil || plan.ScanType != planner.ScanTypeIndexOnly || plan.Query == nil || plan.Query.TableName == "" {
		return nil
	}
	downgraded := *plan
	downgraded.ScanType = planner.ScanTypeTable
	downgraded.TableScan = &planner.TableScan{TableName: plan.Query.TableName}
	downgraded.IndexOnlyScan = nil
	return &downgraded
}

func runtimeSelectQueryFromPlan(query *planner.SelectQuery) *runtimeSelectQuery {
	if query == nil {
		return nil
	}
	return &runtimeSelectQuery{
		tableName:         query.TableName,
		from:              runtimeTableRefsFromPlan(query.From),
		joins:             runtimeJoinClausesFromPlan(query.Joins),
		columns:           append([]string(nil), query.Columns...),
		projectionExprs:   runtimeValueExprsFromPlan(query.ProjectionExprs),
		projectionLabels:  append([]string(nil), query.ProjectionLabels...),
		projectionAliases: append([]string(nil), query.ProjectionAliases...),
		where:             runtimeWhereClauseFromPlan(query.Where),
		predicate:         runtimePredicateExprFromPlan(query.Predicate),
		orderBy:           runtimeOrderByClauseFromPlan(query.OrderBy),
		orderBys:          runtimeOrderBysFromPlan(query.OrderBys),
		isCountStar:       query.IsCountStar,
	}
}

func runtimeTableRefsFromPlan(refs []planner.TableRef) []runtimeTableRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]runtimeTableRef, 0, len(refs))
	for _, ref := range refs {
		out = append(out, runtimeTableRef{name: ref.Name, alias: ref.Alias})
	}
	return out
}

func runtimeJoinClausesFromPlan(joins []planner.JoinClause) []runtimeJoinClause {
	if len(joins) == 0 {
		return nil
	}
	out := make([]runtimeJoinClause, 0, len(joins))
	for _, join := range joins {
		out = append(out, runtimeJoinClause{
			right:     runtimeTableRef{name: join.Right.Name, alias: join.Right.Alias},
			predicate: runtimePredicateExprFromPlan(join.Predicate),
		})
	}
	return out
}

func runtimeOrderByClauseFromPlan(orderBy *planner.OrderByClause) *runtimeOrderByClause {
	if orderBy == nil {
		return nil
	}
	return &runtimeOrderByClause{column: orderBy.Column, desc: orderBy.Desc}
}

func runtimeOrderBysFromPlan(orderBys []planner.OrderByClause) []runtimeOrderByClause {
	if len(orderBys) == 0 {
		return nil
	}
	out := make([]runtimeOrderByClause, 0, len(orderBys))
	for _, orderBy := range orderBys {
		out = append(out, runtimeOrderByClause{column: orderBy.Column, desc: orderBy.Desc})
	}
	return out
}

func runtimeWhereClauseFromPlan(where *planner.WhereClause) *runtimeWhereClause {
	if where == nil {
		return nil
	}
	items := make([]runtimeConditionChainItem, 0, len(where.Items))
	for _, item := range where.Items {
		items = append(items, runtimeConditionChainItem{
			op:        runtimeBooleanOp(item.Op),
			condition: runtimeConditionFromPlan(item.Condition),
		})
	}
	return &runtimeWhereClause{items: items}
}

func runtimePredicateExprFromPlan(predicate *planner.PredicateExpr) *runtimePredicateExpr {
	if predicate == nil {
		return nil
	}
	return &runtimePredicateExpr{
		kind:       runtimePredicateKind(predicate.Kind),
		comparison: runtimeConditionPtrFromPlan(predicate.Comparison),
		left:       runtimePredicateExprFromPlan(predicate.Left),
		right:      runtimePredicateExprFromPlan(predicate.Right),
		inner:      runtimePredicateExprFromPlan(predicate.Inner),
	}
}

func runtimeConditionPtrFromPlan(cond *planner.Condition) *runtimeCondition {
	if cond == nil {
		return nil
	}
	converted := runtimeConditionFromPlan(*cond)
	return &converted
}

func runtimeConditionFromPlan(cond planner.Condition) runtimeCondition {
	return runtimeCondition{
		left:      cond.Left,
		leftExpr:  runtimeValueExprFromPlan(cond.LeftExpr),
		operator:  cond.Operator,
		right:     cond.Right.ParserValue(),
		rightRef:  cond.RightRef,
		rightExpr: runtimeValueExprFromPlan(cond.RightExpr),
	}
}

func runtimeValueExprsFromPlan(exprs []*planner.ValueExpr) []*runtimeValueExpr {
	if len(exprs) == 0 {
		return nil
	}
	out := make([]*runtimeValueExpr, 0, len(exprs))
	for _, expr := range exprs {
		out = append(out, runtimeValueExprFromPlan(expr))
	}
	return out
}

func runtimeValueExprFromPlan(expr *planner.ValueExpr) *runtimeValueExpr {
	if expr == nil {
		return nil
	}
	return &runtimeValueExpr{
		kind:      runtimeValueExprKind(expr.Kind),
		value:     expr.Value.ParserValue(),
		qualifier: expr.Qualifier,
		column:    expr.Column,
		op:        runtimeValueExprBinaryOp(expr.Op),
		left:      runtimeValueExprFromPlan(expr.Left),
		right:     runtimeValueExprFromPlan(expr.Right),
		funcName:  expr.FuncName,
		arg:       runtimeValueExprFromPlan(expr.Arg),
		starArg:   expr.StarArg,
		inner:     runtimeValueExprFromPlan(expr.Inner),
	}
}

func (b *selectPlanBridge) singleTable(tableMap map[string]*Table) (*Table, error) {
	if b == nil {
		return nil, errInvalidSelectPlan
	}

	tableName := b.accessPath.SingleTableName
	switch b.scanKind {
	case selectScanKindTable, selectScanKindIndex:
	default:
		return nil, errInvalidSelectPlan
	}

	table, ok := tableMap[tableName]
	if !ok {
		return nil, newTableNotFoundError(tableName)
	}
	return table, nil
}

func (b *selectPlanBridge) joinTables(tableMap map[string]*Table) (*Table, *Table, error) {
	if b == nil || b.scanKind != selectScanKindJoin {
		return nil, nil, errInvalidSelectPlan
	}

	leftTable := tableMap[b.accessPath.JoinLeftTable]
	if leftTable == nil {
		return nil, nil, newTableNotFoundError(b.accessPath.JoinLeftTable)
	}
	rightTable := tableMap[b.accessPath.JoinRightTable]
	if rightTable == nil {
		return nil, nil, newTableNotFoundError(b.accessPath.JoinRightTable)
	}
	return leftTable, rightTable, nil
}
