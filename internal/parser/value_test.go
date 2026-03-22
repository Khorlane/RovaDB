package parser

import "testing"

func TestInt64Value(t *testing.T) {
	got := Int64Value(42)
	if got.Kind != ValueKindInt64 {
		t.Fatalf("Int64Value().Kind = %v, want %v", got.Kind, ValueKindInt64)
	}
	if got.I64 != 42 {
		t.Fatalf("Int64Value().I64 = %d, want 42", got.I64)
	}
}

func TestStringValue(t *testing.T) {
	got := StringValue("hello")
	if got.Kind != ValueKindString {
		t.Fatalf("StringValue().Kind = %v, want %v", got.Kind, ValueKindString)
	}
	if got.Str != "hello" {
		t.Fatalf("StringValue().Str = %q, want %q", got.Str, "hello")
	}
}

func TestParseSelectLiteralValueKinds(t *testing.T) {
	intSel, ok := ParseSelectLiteral("SELECT 1")
	if !ok {
		t.Fatal("ParseSelectLiteral(SELECT 1) ok = false, want true")
	}
	if intSel.Value.Kind != ValueKindInt64 {
		t.Fatalf("ParseSelectLiteral(SELECT 1).Value.Kind = %v, want %v", intSel.Value.Kind, ValueKindInt64)
	}
	if intSel.Value.I64 != 1 {
		t.Fatalf("ParseSelectLiteral(SELECT 1).Value.I64 = %d, want 1", intSel.Value.I64)
	}

	strSel, ok := ParseSelectLiteral("SELECT 'hi'")
	if !ok {
		t.Fatal("ParseSelectLiteral(SELECT 'hi') ok = false, want true")
	}
	if strSel.Value.Kind != ValueKindString {
		t.Fatalf("ParseSelectLiteral(SELECT 'hi').Value.Kind = %v, want %v", strSel.Value.Kind, ValueKindString)
	}
	if strSel.Value.Str != "hi" {
		t.Fatalf("ParseSelectLiteral(SELECT 'hi').Value.Str = %q, want %q", strSel.Value.Str, "hi")
	}
}
