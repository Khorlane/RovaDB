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

	if err := BindPlaceholders(stmt, []any{int32(1)}); err != nil {
		t.Fatalf("BindPlaceholders() error = %v", err)
	}

	sel, ok := stmt.(*SelectExpr)
	if !ok {
		t.Fatalf("stmt type = %T, want *SelectExpr", stmt)
	}
	if sel.Where == nil || len(sel.Where.Items) != 1 {
		t.Fatalf("sel.Where = %#v, want one condition", sel.Where)
	}
	if got, want := sel.Where.Items[0].Condition.Right, boundIntegerValue(1, BoundIntegerTypeInt32); got != want {
		t.Fatalf("sel.Where.Items[0].Condition.Right = %#v, want %#v", got, want)
	}
	if sel.Where.Items[0].Condition.Right.Kind == ValueKindIntegerLiteral {
		t.Fatalf("bound placeholder kind = %v, want typed integer kind", sel.Where.Items[0].Condition.Right.Kind)
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

	err = BindPlaceholders(stmt, []any{int32(1), "steve"})
	if err == nil || !strings.Contains(err.Error(), "placeholder count mismatch") {
		t.Fatalf("BindPlaceholders() error = %v, want placeholder count mismatch", err)
	}
}

func TestBindPlaceholdersSelectWhereOrdering(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE id = ? AND name = ?")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if err := BindPlaceholders(stmt, []any{int32(1), "steve"}); err != nil {
		t.Fatalf("BindPlaceholders() error = %v", err)
	}

	sel, ok := stmt.(*SelectExpr)
	if !ok {
		t.Fatalf("stmt type = %T, want *SelectExpr", stmt)
	}
	if sel.Where == nil || len(sel.Where.Items) != 2 {
		t.Fatalf("sel.Where = %#v, want two conditions", sel.Where)
	}
	if got, want := sel.Where.Items[0].Condition.Right, boundIntegerValue(1, BoundIntegerTypeInt32); got != want {
		t.Fatalf("first bound value = %#v, want %#v", got, want)
	}
	if sel.Where.Items[1].Condition.Right != StringValue("steve") {
		t.Fatalf("second bound value = %#v, want %#v", sel.Where.Items[1].Condition.Right, StringValue("steve"))
	}
}

func TestBindPlaceholdersUpdateOrdering(t *testing.T) {
	stmt, err := Parse("UPDATE users SET name = ?, active = ? WHERE id = ?")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if err := BindPlaceholders(stmt, []any{"sam", true, int32(1)}); err != nil {
		t.Fatalf("BindPlaceholders() error = %v", err)
	}

	update, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("stmt type = %T, want *UpdateStmt", stmt)
	}
	if len(update.Assignments) != 2 {
		t.Fatalf("len(update.Assignments) = %d, want 2", len(update.Assignments))
	}
	if update.Assignments[0].Value != StringValue("sam") {
		t.Fatalf("first assignment value = %#v, want %#v", update.Assignments[0].Value, StringValue("sam"))
	}
	if update.Assignments[1].Value != BoolValue(true) {
		t.Fatalf("second assignment value = %#v, want %#v", update.Assignments[1].Value, BoolValue(true))
	}
	if update.Where == nil || len(update.Where.Items) != 1 {
		t.Fatalf("update.Where = %#v, want one condition", update.Where)
	}
	if got, want := update.Where.Items[0].Condition.Right, boundIntegerValue(1, BoundIntegerTypeInt32); got != want {
		t.Fatalf("where value = %#v, want %#v", got, want)
	}
}

func TestBindPlaceholdersIntegerWidthMatrix(t *testing.T) {
	stmt, err := Parse("INSERT INTO numbers VALUES (?, ?, ?)")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if err := BindPlaceholders(stmt, []any{int16(7), int32(8), int64(9)}); err != nil {
		t.Fatalf("BindPlaceholders() error = %v", err)
	}

	insert, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("stmt type = %T, want *InsertStmt", stmt)
	}
	if len(insert.ValueExprs) != 3 {
		t.Fatalf("len(insert.ValueExprs) = %d, want 3", len(insert.ValueExprs))
	}
	want := []Value{
		boundIntegerValue(7, BoundIntegerTypeInt16),
		boundIntegerValue(8, BoundIntegerTypeInt32),
		boundIntegerValue(9, BoundIntegerTypeInt64),
	}
	for i := range want {
		if insert.ValueExprs[i].Kind != ValueExprKindLiteral {
			t.Fatalf("insert.ValueExprs[%d].Kind = %v, want %v", i, insert.ValueExprs[i].Kind, ValueExprKindLiteral)
		}
		if insert.ValueExprs[i].Value != want[i] {
			t.Fatalf("insert.ValueExprs[%d].Value = %#v, want %#v", i, insert.ValueExprs[i].Value, want[i])
		}
		if insert.ValueExprs[i].Value.Kind == ValueKindIntegerLiteral {
			t.Fatalf("insert.ValueExprs[%d].Value.Kind = %v, want typed integer kind", i, insert.ValueExprs[i].Value.Kind)
		}
	}
}

func TestBindPlaceholdersRejectsUnsupportedIntegerTypes(t *testing.T) {
	tests := []struct {
		name string
		arg  any
	}{
		{name: "int", arg: int(1)},
		{name: "int8", arg: int8(1)},
		{name: "uint", arg: uint(1)},
		{name: "uint32", arg: uint32(1)},
		{name: "uint64", arg: uint64(1)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse("INSERT INTO numbers VALUES (?)")
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			err = BindPlaceholders(stmt, []any{tc.arg})
			if err == nil || !strings.Contains(err.Error(), "unsupported placeholder argument type") {
				t.Fatalf("BindPlaceholders() error = %v, want unsupported placeholder argument type", err)
			}
		})
	}
}

func TestBindArgumentValueSupportedTypes(t *testing.T) {
	tests := []struct {
		name string
		arg  any
		want Value
	}{
		{name: "int16", arg: int16(1), want: boundIntegerValue(1, BoundIntegerTypeInt16)},
		{name: "int32", arg: int32(1), want: boundIntegerValue(1, BoundIntegerTypeInt32)},
		{name: "int64", arg: int64(1), want: boundIntegerValue(1, BoundIntegerTypeInt64)},
		{name: "string", arg: "steve", want: StringValue("steve")},
		{name: "bool true", arg: true, want: BoolValue(true)},
		{name: "bool false", arg: false, want: BoolValue(false)},
		{name: "float64", arg: 3.14, want: RealValue(3.14)},
		{name: "nil", arg: nil, want: NullValue()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := bindArgumentValue(tc.arg)
			if err != nil {
				t.Fatalf("bindArgumentValue() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("bindArgumentValue() = %#v, want %#v", got, tc.want)
			}
			if tc.want.IsTypedInteger() && got.Kind == ValueKindIntegerLiteral {
				t.Fatalf("bindArgumentValue() kind = %v, want typed integer kind", got.Kind)
			}
		})
	}
}

func TestBindArgumentValueUnsupportedTypes(t *testing.T) {
	tests := []struct {
		name string
		arg  any
	}{
		{name: "int", arg: int(1)},
		{name: "int8", arg: int8(1)},
		{name: "uint", arg: uint(1)},
		{name: "uint32", arg: uint32(1)},
		{name: "uint64", arg: uint64(1)},
		{name: "uintptr", arg: uintptr(1)},
		{name: "float32", arg: float32(3.14)},
		{name: "struct", arg: struct{}{}},
		{name: "slice", arg: []string{"x"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := bindArgumentValue(tc.arg)
			if err == nil || !strings.Contains(err.Error(), "unsupported placeholder argument type") {
				t.Fatalf("bindArgumentValue() error = %v, want unsupported placeholder argument type", err)
			}
		})
	}
}
