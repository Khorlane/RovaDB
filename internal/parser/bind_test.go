package parser

import (
	"strings"
	"testing"
)

func TestBindPlaceholdersSelectWhere(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE id = ?")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if err := BindPlaceholders(stmt, []any{1}); err != nil {
		t.Fatalf("BindPlaceholders() error = %v", err)
	}

	sel, ok := stmt.(*SelectExpr)
	if !ok {
		t.Fatalf("stmt type = %T, want *SelectExpr", stmt)
	}
	if sel.Where == nil || len(sel.Where.Items) != 1 {
		t.Fatalf("sel.Where = %#v, want one condition", sel.Where)
	}
	if sel.Where.Items[0].Condition.Right != Int64Value(1) {
		t.Fatalf("sel.Where.Items[0].Condition.Right = %#v, want %#v", sel.Where.Items[0].Condition.Right, Int64Value(1))
	}
}

func TestBindPlaceholdersCountMismatchTooFewArgs(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE id = ?")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	err = BindPlaceholders(stmt, nil)
	if err == nil || !strings.Contains(err.Error(), "placeholder count mismatch") {
		t.Fatalf("BindPlaceholders() error = %v, want placeholder count mismatch", err)
	}
}

func TestBindPlaceholdersCountMismatchTooManyArgs(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE id = ?")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	err = BindPlaceholders(stmt, []any{1, "steve"})
	if err == nil || !strings.Contains(err.Error(), "placeholder count mismatch") {
		t.Fatalf("BindPlaceholders() error = %v, want placeholder count mismatch", err)
	}
}

func TestBindPlaceholdersSelectWhereOrdering(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE id = ? AND name = ?")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if err := BindPlaceholders(stmt, []any{1, "steve"}); err != nil {
		t.Fatalf("BindPlaceholders() error = %v", err)
	}

	sel, ok := stmt.(*SelectExpr)
	if !ok {
		t.Fatalf("stmt type = %T, want *SelectExpr", stmt)
	}
	if sel.Where == nil || len(sel.Where.Items) != 2 {
		t.Fatalf("sel.Where = %#v, want two conditions", sel.Where)
	}
	if sel.Where.Items[0].Condition.Right != Int64Value(1) {
		t.Fatalf("first bound value = %#v, want %#v", sel.Where.Items[0].Condition.Right, Int64Value(1))
	}
	if sel.Where.Items[1].Condition.Right != StringValue("steve") {
		t.Fatalf("second bound value = %#v, want %#v", sel.Where.Items[1].Condition.Right, StringValue("steve"))
	}
}
