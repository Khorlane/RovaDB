package parser

import "testing"

func TestParseDelete(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		tableName   string
		hasWhere    bool
		whereColumn string
		whereValue  Value
	}{
		{name: "delete all", input: "DELETE FROM users", tableName: "users"},
		{
			name:        "delete where int",
			input:       "DELETE FROM users WHERE id = 1",
			tableName:   "users",
			hasWhere:    true,
			whereColumn: "id",
			whereValue:  Int64Value(1),
		},
		{
			name:        "delete where string",
			input:       "DELETE FROM users WHERE name = 'bob'",
			tableName:   "users",
			hasWhere:    true,
			whereColumn: "name",
			whereValue:  StringValue("bob"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseDelete(tc.input)
			if err != nil {
				t.Fatalf("parseDelete() error = %v", err)
			}
			if got.TableName != tc.tableName || got.HasWhere != tc.hasWhere || got.WhereColumn != tc.whereColumn || got.WhereValue != tc.whereValue {
				t.Fatalf("parseDelete() = %#v, want table=%q hasWhere=%v whereColumn=%q whereValue=%#v", got, tc.tableName, tc.hasWhere, tc.whereColumn, tc.whereValue)
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
