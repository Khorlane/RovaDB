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
	if got, want := sel.Where.Items[0].Condition.Right, boundIntegerValue(1, BoundIntegerTypeInt); got != want {
		t.Fatalf("sel.Where.Items[0].Condition.Right = %#v, want %#v", got, want)
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
	if got, want := sel.Where.Items[0].Condition.Right, boundIntegerValue(1, BoundIntegerTypeInt); got != want {
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

	if err := BindPlaceholders(stmt, []any{"sam", true, 1}); err != nil {
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
	if got, want := update.Where.Items[0].Condition.Right, boundIntegerValue(1, BoundIntegerTypeInt); got != want {
		t.Fatalf("where value = %#v, want %#v", got, want)
	}
}

func TestBindArgumentValueSupportedTypes(t *testing.T) {
	tests := []struct {
		name string
		arg  any
		want Value
	}{
		{name: "int", arg: 1, want: boundIntegerValue(1, BoundIntegerTypeInt)},
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
		})
	}
}

func TestBindArgumentValueUnsupportedTypes(t *testing.T) {
	tests := []struct {
		name string
		arg  any
	}{
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
