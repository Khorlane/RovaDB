package parser

import "testing"

func TestParsePredicateExprComparison(t *testing.T) {
	got, ok := parsePredicateExpr("id = 1")
	if !ok || got == nil {
		t.Fatal("parsePredicateExpr() failed, want success")
	}
	if got.Kind != PredicateKindComparison {
		t.Fatalf("Kind = %v, want %v", got.Kind, PredicateKindComparison)
	}
	if got.Comparison == nil || got.Comparison.Left != "id" || got.Comparison.Operator != "=" || got.Comparison.Right != Int64Value(1) {
		t.Fatalf("Comparison = %#v, want id = 1", got.Comparison)
	}
}

func TestParsePredicateExprColumnComparison(t *testing.T) {
	got, ok := parsePredicateExpr("id = mirror")
	if !ok || got == nil {
		t.Fatal("parsePredicateExpr() failed, want success")
	}
	if got.Kind != PredicateKindComparison {
		t.Fatalf("Kind = %v, want %v", got.Kind, PredicateKindComparison)
	}
	if got.Comparison == nil || got.Comparison.Left != "id" || got.Comparison.Operator != "=" || got.Comparison.RightRef != "mirror" {
		t.Fatalf("Comparison = %#v, want id = mirror", got.Comparison)
	}
}

func TestParsePredicateExprFunctionComparison(t *testing.T) {
	got, ok := parsePredicateExpr("LOWER(name) = 'bob'")
	if !ok || got == nil {
		t.Fatal("parsePredicateExpr() failed, want success")
	}
	if got.Kind != PredicateKindComparison || got.Comparison == nil {
		t.Fatalf("got = %#v, want comparison", got)
	}
	if got.Comparison.LeftExpr == nil || got.Comparison.LeftExpr.Kind != ValueExprKindFunctionCall || got.Comparison.LeftExpr.FuncName != "LOWER" {
		t.Fatalf("LeftExpr = %#v, want LOWER(name)", got.Comparison.LeftExpr)
	}
	if got.Comparison.RightExpr == nil || got.Comparison.RightExpr.Kind != ValueExprKindLiteral || got.Comparison.RightExpr.Value != StringValue("bob") {
		t.Fatalf("RightExpr = %#v, want literal 'bob'", got.Comparison.RightExpr)
	}
}

func TestParsePredicateExprPrecedence(t *testing.T) {
	got, ok := parsePredicateExpr("id = 1 OR id = 2 AND name = 'bob'")
	if !ok || got == nil {
		t.Fatal("parsePredicateExpr() failed, want success")
	}
	if got.Kind != PredicateKindOr {
		t.Fatalf("root Kind = %v, want %v", got.Kind, PredicateKindOr)
	}
	if got.Right == nil || got.Right.Kind != PredicateKindAnd {
		t.Fatalf("right Kind = %#v, want AND subtree", got.Right)
	}
}

func TestParsePredicateExprParenthesizedGrouping(t *testing.T) {
	got, ok := parsePredicateExpr("(id = 1 OR id = 2) AND name = 'bob'")
	if !ok || got == nil {
		t.Fatal("parsePredicateExpr() failed, want success")
	}
	if got.Kind != PredicateKindAnd {
		t.Fatalf("root Kind = %v, want %v", got.Kind, PredicateKindAnd)
	}
	if got.Left == nil || got.Left.Kind != PredicateKindOr {
		t.Fatalf("left Kind = %#v, want OR subtree", got.Left)
	}
}

func TestParsePredicateExprNot(t *testing.T) {
	got, ok := parsePredicateExpr("NOT id = ?")
	if !ok || got == nil {
		t.Fatal("parsePredicateExpr() failed, want success")
	}
	if got.Kind != PredicateKindNot {
		t.Fatalf("root Kind = %v, want %v", got.Kind, PredicateKindNot)
	}
	if got.Inner == nil || got.Inner.Kind != PredicateKindComparison {
		t.Fatalf("inner = %#v, want comparison", got.Inner)
	}
	if got.Inner.Comparison.Right.Kind != ValueKindPlaceholder {
		t.Fatalf("placeholder kind = %v, want %v", got.Inner.Comparison.Right.Kind, ValueKindPlaceholder)
	}
}

func TestParsePredicateExprInvalid(t *testing.T) {
	tests := []string{
		"id =",
		"AND id = 1",
		"id XOR 1",
		"id = 1 OR",
		"(id = 1",
	}
	for _, input := range tests {
		if got, ok := parsePredicateExpr(input); ok {
			t.Fatalf("parsePredicateExpr(%q) = %#v, want failure", input, got)
		}
	}
}

func TestFlattenPredicateExprComparison(t *testing.T) {
	expr, ok := parsePredicateExpr("id = 1")
	if !ok {
		t.Fatal("parsePredicateExpr() failed, want success")
	}

	where, ok := flattenPredicateExpr(expr)
	if !ok || where == nil {
		t.Fatal("flattenPredicateExpr() failed, want success")
	}
	if len(where.Items) != 1 {
		t.Fatalf("len(where.Items) = %d, want 1", len(where.Items))
	}
	if where.Items[0].Condition.Left != "id" || where.Items[0].Condition.Operator != "=" || where.Items[0].Condition.Right != Int64Value(1) {
		t.Fatalf("where.Items[0] = %#v, want id = 1", where.Items[0])
	}
}

func TestFlattenPredicateExprHomogeneousChain(t *testing.T) {
	expr, ok := parsePredicateExpr("id = 1 AND name = 'bob' AND active = TRUE")
	if !ok {
		t.Fatal("parsePredicateExpr() failed, want success")
	}

	where, ok := flattenPredicateExpr(expr)
	if !ok || where == nil {
		t.Fatal("flattenPredicateExpr() failed, want success")
	}
	if len(where.Items) != 3 {
		t.Fatalf("len(where.Items) = %d, want 3", len(where.Items))
	}
	if where.Items[1].Op != BooleanOpAnd || where.Items[2].Op != BooleanOpAnd {
		t.Fatalf("ops = %#v, want AND chain", where.Items)
	}
}

func TestFlattenPredicateExprRejectsPrecedenceSensitiveShape(t *testing.T) {
	expr, ok := parsePredicateExpr("id = 1 OR id = 2 AND name = 'bob'")
	if !ok {
		t.Fatal("parsePredicateExpr() failed, want success")
	}

	if where, ok := flattenPredicateExpr(expr); ok {
		t.Fatalf("flattenPredicateExpr() = %#v, want rejection", where)
	}
}

func TestFlattenPredicateExprRejectsNot(t *testing.T) {
	expr, ok := parsePredicateExpr("NOT id = 1")
	if !ok {
		t.Fatal("parsePredicateExpr() failed, want success")
	}

	if where, ok := flattenPredicateExpr(expr); ok {
		t.Fatalf("flattenPredicateExpr() = %#v, want rejection", where)
	}
}

func TestFlattenPredicateExprRejectsColumnComparison(t *testing.T) {
	expr, ok := parsePredicateExpr("id = mirror")
	if !ok {
		t.Fatal("parsePredicateExpr() failed, want success")
	}

	if where, ok := flattenPredicateExpr(expr); ok {
		t.Fatalf("flattenPredicateExpr() = %#v, want rejection", where)
	}
}

func TestFlattenPredicateExprRejectsFunctionComparison(t *testing.T) {
	expr, ok := parsePredicateExpr("LOWER(name) = 'bob'")
	if !ok {
		t.Fatal("parsePredicateExpr() failed, want success")
	}

	if where, ok := flattenPredicateExpr(expr); ok {
		t.Fatalf("flattenPredicateExpr() = %#v, want rejection", where)
	}
}

func TestParseWhereBridgeParsesLegacyAndPredicateInParallel(t *testing.T) {
	where, predicate, ok := parseWhereBridge("id = 1 AND name = 'bob'")
	if !ok {
		t.Fatal("parseWhereBridge() failed, want success")
	}
	if where == nil || predicate == nil {
		t.Fatalf("parseWhereBridge() = %#v, %#v, want both populated", where, predicate)
	}
	if predicate.Kind != PredicateKindAnd {
		t.Fatalf("predicate.Kind = %v, want %v", predicate.Kind, PredicateKindAnd)
	}
	if len(where.Items) != 2 || where.Items[1].Op != BooleanOpAnd {
		t.Fatalf("where = %#v, want flattened AND chain", where)
	}
}

func TestParseWhereBridgeAllowsPredicateOnlyShapes(t *testing.T) {
	where, predicate, ok := parseWhereBridge("NOT id = 1")
	if !ok {
		t.Fatal("parseWhereBridge() failed, want success")
	}
	if where != nil {
		t.Fatalf("where = %#v, want nil for predicate-only shape", where)
	}
	if predicate == nil || predicate.Kind != PredicateKindNot {
		t.Fatalf("predicate = %#v, want NOT predicate", predicate)
	}
}
