package parser

import "testing"

func TestParseSelectExprProjectionColumns(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT id, name FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil {
		t.Fatal("ParseSelectExpr() = nil, want value")
	}
	if got.TableName != "users" {
		t.Fatalf("TableName = %q, want %q", got.TableName, "users")
	}
	if len(got.Columns) != 2 || got.Columns[0] != "id" || got.Columns[1] != "name" {
		t.Fatalf("Columns = %#v, want [id name]", got.Columns)
	}
	if len(got.ProjectionExprs) != 2 || got.ProjectionExprs[0].Kind != ValueExprKindColumnRef || got.ProjectionExprs[1].Kind != ValueExprKindColumnRef {
		t.Fatalf("ProjectionExprs = %#v, want column refs", got.ProjectionExprs)
	}
}

func TestParseSelectFromTokensProjectionColumns(t *testing.T) {
	got, ok := parseSelectFromTokens("SELECT id, name FROM users")
	if !ok {
		t.Fatal("parseSelectFromTokens() ok = false, want true")
	}
	if got == nil {
		t.Fatal("parseSelectFromTokens() = nil, want value")
	}
	if got.TableName != "users" {
		t.Fatalf("TableName = %q, want %q", got.TableName, "users")
	}
	if len(got.Columns) != 2 || got.Columns[0] != "id" || got.Columns[1] != "name" {
		t.Fatalf("Columns = %#v, want [id name]", got.Columns)
	}
}

func TestParseSelectExprSingleProjectionColumn(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT id FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil {
		t.Fatal("ParseSelectExpr() = nil, want value")
	}
	if len(got.Columns) != 1 || got.Columns[0] != "id" {
		t.Fatalf("Columns = %#v, want [id]", got.Columns)
	}
}

func TestParseSelectExprProjectionFunctions(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT LOWER(name), LENGTH(name) FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil {
		t.Fatal("ParseSelectExpr() = nil, want value")
	}
	if got.TableName != "users" {
		t.Fatalf("TableName = %q, want %q", got.TableName, "users")
	}
	if got.Columns != nil {
		t.Fatalf("Columns = %#v, want nil for expression projection", got.Columns)
	}
	if len(got.ProjectionExprs) != 2 || got.ProjectionExprs[0].Kind != ValueExprKindFunctionCall || got.ProjectionExprs[1].Kind != ValueExprKindFunctionCall {
		t.Fatalf("ProjectionExprs = %#v, want function calls", got.ProjectionExprs)
	}
	if len(got.ProjectionLabels) != 2 || got.ProjectionLabels[0] != "LOWER(name)" || got.ProjectionLabels[1] != "LENGTH(name)" {
		t.Fatalf("ProjectionLabels = %#v, want original select items", got.ProjectionLabels)
	}
}

func TestParseSelectExprProjectionArithmetic(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT id + 1, score - 1.5 FROM metrics")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil || len(got.ProjectionExprs) != 2 {
		t.Fatalf("ParseSelectExpr() = %#v, want two projection exprs", got)
	}
	if got.ProjectionExprs[0].Kind != ValueExprKindBinary || got.ProjectionExprs[0].Op != ValueExprBinaryOpAdd {
		t.Fatalf("ProjectionExprs[0] = %#v, want binary add", got.ProjectionExprs[0])
	}
	if got.ProjectionExprs[1].Kind != ValueExprKindBinary || got.ProjectionExprs[1].Op != ValueExprBinaryOpSub {
		t.Fatalf("ProjectionExprs[1] = %#v, want binary sub", got.ProjectionExprs[1])
	}
}

func TestParseSelectExprQualifiedProjectionColumn(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT users.id FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil || len(got.ProjectionExprs) != 1 {
		t.Fatalf("ParseSelectExpr() = %#v, want one projection", got)
	}
	if len(got.Columns) != 1 || got.Columns[0] != "id" {
		t.Fatalf("Columns = %#v, want [id]", got.Columns)
	}
	if got.ProjectionExprs[0].Kind != ValueExprKindColumnRef || got.ProjectionExprs[0].Qualifier != "users" || got.ProjectionExprs[0].Column != "id" {
		t.Fatalf("ProjectionExprs[0] = %#v, want users.id", got.ProjectionExprs[0])
	}
}

func TestParseSelectExprSingleTableAlias(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT u.id FROM users AS u WHERE u.id = 1 ORDER BY u.id DESC")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil {
		t.Fatal("ParseSelectExpr() = nil, want value")
	}
	if got.TableName != "users" {
		t.Fatalf("TableName = %q, want %q", got.TableName, "users")
	}
	if len(got.From) != 1 || got.From[0].Name != "users" || got.From[0].Alias != "u" {
		t.Fatalf("From = %#v, want users AS u", got.From)
	}
	if len(got.ProjectionExprs) != 1 || got.ProjectionExprs[0].Qualifier != "u" || got.ProjectionExprs[0].Column != "id" {
		t.Fatalf("ProjectionExprs = %#v, want u.id", got.ProjectionExprs)
	}
	if got.Predicate == nil || got.Predicate.Comparison == nil || got.Predicate.Comparison.Left != "u.id" {
		t.Fatalf("Predicate = %#v, want alias-qualified comparison", got.Predicate)
	}
	if got.OrderBy == nil || got.OrderBy.Column != "u.id" || !got.OrderBy.Desc {
		t.Fatalf("OrderBy = %#v, want u.id DESC", got.OrderBy)
	}
}

func TestParseSelectExprMultiTableFrom(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT u.id, a.name FROM users u, accounts AS a WHERE u.id = 1")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil {
		t.Fatal("ParseSelectExpr() = nil, want value")
	}
	if got.TableName != "users" {
		t.Fatalf("TableName = %q, want %q", got.TableName, "users")
	}
	if len(got.From) != 2 {
		t.Fatalf("len(From) = %d, want 2", len(got.From))
	}
	if got.From[0] != (TableRef{Name: "users", Alias: "u"}) || got.From[1] != (TableRef{Name: "accounts", Alias: "a"}) {
		t.Fatalf("From = %#v, want users u, accounts a", got.From)
	}
	if len(got.ProjectionExprs) != 2 || got.ProjectionExprs[0].Qualifier != "u" || got.ProjectionExprs[1].Qualifier != "a" {
		t.Fatalf("ProjectionExprs = %#v, want qualified refs", got.ProjectionExprs)
	}
}

func TestParseSelectExprExplicitJoin(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT u.id, a.name FROM users u INNER JOIN accounts a ON u.id = a.id WHERE u.id = 1")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil {
		t.Fatal("ParseSelectExpr() = nil, want value")
	}
	if got.TableName != "users" {
		t.Fatalf("TableName = %q, want %q", got.TableName, "users")
	}
	if len(got.From) != 1 || got.From[0] != (TableRef{Name: "users", Alias: "u"}) {
		t.Fatalf("From = %#v, want users u", got.From)
	}
	if len(got.Joins) != 1 {
		t.Fatalf("len(Joins) = %d, want 1", len(got.Joins))
	}
	if got.Joins[0].Right != (TableRef{Name: "accounts", Alias: "a"}) {
		t.Fatalf("Joins[0].Right = %#v, want accounts a", got.Joins[0].Right)
	}
	if got.Joins[0].Predicate == nil || got.Joins[0].Predicate.Comparison == nil || got.Joins[0].Predicate.Comparison.Left != "u.id" || got.Joins[0].Predicate.Comparison.RightRef != "a.id" {
		t.Fatalf("Joins[0].Predicate = %#v, want u.id = a.id", got.Joins[0].Predicate)
	}
}

func TestParseSelectExprCountStar(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "count star", sql: "SELECT COUNT(*) FROM users"},
		{name: "count star where", sql: "SELECT COUNT(*) FROM users WHERE id > 1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseSelectExpr(tc.sql)
			if !ok {
				t.Fatal("ParseSelectExpr() ok = false, want true")
			}
			if got == nil {
				t.Fatal("ParseSelectExpr() = nil, want value")
			}
			if !got.IsCountStar || got.TableName != "users" {
				t.Fatalf("ParseSelectExpr() = %#v, want COUNT(*) select from users", got)
			}
		})
	}
}

func TestParseSelectExprAggregateFunctions(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT COUNT(name), AVG(score), SUM(score), MIN(name), MAX(score) FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil || len(got.ProjectionExprs) != 5 {
		t.Fatalf("ParseSelectExpr() = %#v, want five aggregate projections", got)
	}
	for i, name := range []string{"COUNT", "AVG", "SUM", "MIN", "MAX"} {
		if got.ProjectionExprs[i].Kind != ValueExprKindAggregateCall || got.ProjectionExprs[i].FuncName != name {
			t.Fatalf("ProjectionExprs[%d] = %#v, want aggregate %q", i, got.ProjectionExprs[i], name)
		}
	}
}

func TestParseSelectFromTokensCountStar(t *testing.T) {
	got, ok := parseSelectFromTokens("SELECT COUNT(*) FROM users WHERE id > 1")
	if !ok {
		t.Fatal("parseSelectFromTokens() ok = false, want true")
	}
	if got == nil {
		t.Fatal("parseSelectFromTokens() = nil, want value")
	}
	if !got.IsCountStar || got.TableName != "users" {
		t.Fatalf("parseSelectFromTokens() = %#v, want COUNT(*) select from users", got)
	}
}

func TestParseSelectExprSelectStarUsesNilColumns(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT * FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil {
		t.Fatal("ParseSelectExpr() = nil, want value")
	}
	if got.Columns != nil {
		t.Fatalf("Columns = %#v, want nil for SELECT *", got.Columns)
	}
}

func TestParseSelectExprInvalidCountStar(t *testing.T) {
	for _, sql := range []string{
		"SELECT COUNT(*) name FROM users",
		"SELECT COUNT() FROM users",
	} {
		if got, ok := ParseSelectExpr(sql); ok {
			t.Fatalf("ParseSelectExpr(%q) = %#v, want parse failure", sql, got)
		}
	}
}

func TestParseSelectExprWhereOperators(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		left     string
		operator string
		right    Value
	}{
		{name: "equals", sql: "SELECT id FROM users WHERE id = 1", left: "id", operator: "=", right: Int64Value(1)},
		{name: "not equals", sql: "SELECT id FROM users WHERE id != 1", left: "id", operator: "!=", right: Int64Value(1)},
		{name: "alternate not equals", sql: "SELECT id FROM users WHERE id <> 1", left: "id", operator: "!=", right: Int64Value(1)},
		{name: "less than", sql: "SELECT id FROM users WHERE id < 10", left: "id", operator: "<", right: Int64Value(10)},
		{name: "less equal", sql: "SELECT id FROM users WHERE id <= 10", left: "id", operator: "<=", right: Int64Value(10)},
		{name: "greater than", sql: "SELECT id FROM users WHERE id > 10", left: "id", operator: ">", right: Int64Value(10)},
		{name: "greater equal", sql: "SELECT id FROM users WHERE id >= 10", left: "id", operator: ">=", right: Int64Value(10)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseSelectExpr(tc.sql)
			if !ok {
				t.Fatal("ParseSelectExpr() ok = false, want true")
			}
			if got == nil || got.Where == nil {
				t.Fatalf("ParseSelectExpr() = %#v, want WHERE clause", got)
			}
			if len(got.Where.Items) != 1 {
				t.Fatalf("len(Where.Items) = %d, want 1", len(got.Where.Items))
			}
			cond := got.Where.Items[0].Condition
			if cond.Left != tc.left || cond.Operator != tc.operator || cond.Right != tc.right {
				t.Fatalf("Condition = %#v, want left=%q op=%q right=%#v", cond, tc.left, tc.operator, tc.right)
			}
		})
	}
}

func TestParseSelectExprWhereConditionChain(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []ConditionChainItem
		flat bool
	}{
		{
			name: "single condition",
			sql:  "SELECT id FROM users WHERE id > 1",
			want: []ConditionChainItem{{Condition: Condition{Left: "id", Operator: ">", Right: Int64Value(1)}}},
			flat: true,
		},
		{
			name: "and",
			sql:  "SELECT id FROM users WHERE id > 1 AND name != 'bob'",
			want: []ConditionChainItem{
				{Condition: Condition{Left: "id", Operator: ">", Right: Int64Value(1)}},
				{Op: BooleanOpAnd, Condition: Condition{Left: "name", Operator: "!=", Right: StringValue("bob")}},
			},
			flat: true,
		},
		{
			name: "or",
			sql:  "SELECT id FROM users WHERE id = 1 OR id = 2",
			want: []ConditionChainItem{
				{Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(1)}},
				{Op: BooleanOpOr, Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(2)}},
			},
			flat: true,
		},
		{
			name: "and or",
			sql:  "SELECT id FROM users WHERE id = 1 AND id = 2 OR name = 'bob'",
		},
		{
			name: "or and",
			sql:  "SELECT id FROM users WHERE id = 1 OR id = 2 AND name = 'bob'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseSelectExpr(tc.sql)
			if !ok {
				t.Fatal("ParseSelectExpr() ok = false, want true")
			}
			if got == nil || got.Predicate == nil {
				t.Fatalf("ParseSelectExpr() = %#v, want predicate tree", got)
			}
			if !tc.flat {
				if got.Where != nil {
					t.Fatalf("ParseSelectExpr().Where = %#v, want nil for non-flattenable predicate", got.Where)
				}
				return
			}
			if got.Where == nil {
				t.Fatalf("ParseSelectExpr() = %#v, want WHERE clause", got)
			}
			if len(got.Where.Items) != len(tc.want) {
				t.Fatalf("len(Where.Items) = %d, want %d", len(got.Where.Items), len(tc.want))
			}
			for i := range tc.want {
				if got.Where.Items[i] != tc.want[i] {
					t.Fatalf("Where.Items[%d] = %#v, want %#v", i, got.Where.Items[i], tc.want[i])
				}
			}
		})
	}
}

func TestParseSelectExprInvalidWhereBooleanChains(t *testing.T) {
	for _, sql := range []string{
		"SELECT id FROM users WHERE id = 1 OR",
		"SELECT id FROM users WHERE AND id = 1",
		"SELECT id FROM users WHERE id = 1 XOR id = 2",
	} {
		if got, ok := ParseSelectExpr(sql); ok {
			t.Fatalf("ParseSelectExpr(%q) = %#v, want parse failure", sql, got)
		}
	}
}

func TestParseSelectExprOrderBy(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		column string
		desc   bool
	}{
		{name: "default asc", sql: "SELECT * FROM users ORDER BY id", column: "id", desc: false},
		{name: "explicit asc", sql: "SELECT * FROM users ORDER BY id ASC", column: "id", desc: false},
		{name: "desc", sql: "SELECT * FROM users ORDER BY id DESC", column: "id", desc: true},
		{name: "with where", sql: "SELECT name FROM users WHERE id > 1 ORDER BY name DESC", column: "name", desc: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseSelectExpr(tc.sql)
			if !ok {
				t.Fatal("ParseSelectExpr() ok = false, want true")
			}
			if got == nil || got.OrderBy == nil {
				t.Fatalf("ParseSelectExpr() = %#v, want ORDER BY clause", got)
			}
			if got.OrderBy.Column != tc.column || got.OrderBy.Desc != tc.desc {
				t.Fatalf("OrderBy = %#v, want column=%q desc=%v", got.OrderBy, tc.column, tc.desc)
			}
		})
	}
}

func TestParseSelectExprOrderByMultiple(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT * FROM users ORDER BY name ASC, id DESC")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil || len(got.OrderBys) != 2 {
		t.Fatalf("ParseSelectExpr() = %#v, want two ORDER BY items", got)
	}
	if got.OrderBys[0].Column != "name" || got.OrderBys[0].Desc {
		t.Fatalf("OrderBys[0] = %#v, want name ASC", got.OrderBys[0])
	}
	if got.OrderBys[1].Column != "id" || !got.OrderBys[1].Desc {
		t.Fatalf("OrderBys[1] = %#v, want id DESC", got.OrderBys[1])
	}
	if got.OrderBy == nil || got.OrderBy.Column != "name" || got.OrderBy.Desc {
		t.Fatalf("OrderBy = %#v, want backfilled first order item", got.OrderBy)
	}
}

func TestParseSelectExprAcceptsTrailingSemicolon(t *testing.T) {
	got, ok := ParseSelectExpr("SELECT id FROM users WHERE id = 1;")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	if got == nil || got.Where == nil || len(got.Where.Items) != 1 {
		t.Fatalf("ParseSelectExpr() = %#v, want WHERE clause", got)
	}
	cond := got.Where.Items[0].Condition
	if cond.Left != "id" || cond.Operator != "=" || cond.Right != Int64Value(1) {
		t.Fatalf("Condition = %#v, want id = 1", cond)
	}
}

func TestParseSelectFromTokensOrderBy(t *testing.T) {
	got, ok := parseSelectFromTokens("SELECT name FROM users WHERE id > 1 ORDER BY name DESC")
	if !ok {
		t.Fatal("parseSelectFromTokens() ok = false, want true")
	}
	if got == nil || got.OrderBy == nil {
		t.Fatalf("parseSelectFromTokens() = %#v, want ORDER BY clause", got)
	}
	if got.OrderBy.Column != "name" || !got.OrderBy.Desc {
		t.Fatalf("OrderBy = %#v, want column=%q desc=%v", got.OrderBy, "name", true)
	}
	if got.Predicate == nil {
		t.Fatal("Predicate = nil, want populated predicate tree")
	}
}

func TestParseSelectLiteralTokens(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		kind ExprKind
	}{
		{name: "int", sql: "SELECT 1", kind: ExprKindInt64Literal},
		{name: "real", sql: "SELECT 3.14", kind: ExprKindRealLiteral},
		{name: "string", sql: "SELECT 'hi'", kind: ExprKindStringLiteral},
		{name: "bool", sql: "SELECT TRUE", kind: ExprKindBoolLiteral},
		{name: "binary", sql: "SELECT 1 + 2", kind: ExprKindInt64Binary},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseSelectLiteralTokens(tc.sql)
			if !ok {
				t.Fatal("parseSelectLiteralTokens() ok = false, want true")
			}
			if got == nil || got.Expr == nil {
				t.Fatalf("parseSelectLiteralTokens() = %#v, want Expr", got)
			}
			if got.Expr.Kind != tc.kind {
				t.Fatalf("Expr.Kind = %v, want %v", got.Expr.Kind, tc.kind)
			}
		})
	}
}
