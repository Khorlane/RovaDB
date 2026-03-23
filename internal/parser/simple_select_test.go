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
