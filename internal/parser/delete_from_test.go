package parser

import "testing"

func TestParseDelete(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		tableName string
		where     *WhereClause
	}{
		{name: "delete all", input: "DELETE FROM users", tableName: "users"},
		{
			name:      "delete where int",
			input:     "DELETE FROM users WHERE id = 1",
			tableName: "users",
			where:     &WhereClause{Left: "id", Operator: "=", Right: Int64Value(1)},
		},
		{
			name:      "delete where string",
			input:     "DELETE FROM users WHERE name = 'bob'",
			tableName: "users",
			where:     &WhereClause{Left: "name", Operator: "=", Right: StringValue("bob")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseDelete(tc.input)
			if err != nil {
				t.Fatalf("parseDelete() error = %v", err)
			}
			if got.TableName != tc.tableName {
				t.Fatalf("parseDelete().TableName = %q, want %q", got.TableName, tc.tableName)
			}
			if (got.Where == nil) != (tc.where == nil) || (got.Where != nil && *got.Where != *tc.where) {
				t.Fatalf("parseDelete().Where = %#v, want %#v", got.Where, tc.where)
			}
		})
	}
}

func TestParseDeleteInvalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "missing table", input: "DELETE FROM"},
		{name: "missing rhs literal", input: "DELETE FROM users WHERE id ="},
		{name: "missing equals", input: "DELETE FROM users WHERE id 1"},
		{name: "extra condition", input: "DELETE FROM users WHERE id = 1 AND name = 'bob'"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseDelete(tc.input)
			if err == nil {
				t.Fatalf("parseDelete() = %#v, want error", got)
			}
		})
	}
}
