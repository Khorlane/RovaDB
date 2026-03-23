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
			where:     &WhereClause{Conditions: []Condition{{Left: "id", Operator: "=", Right: Int64Value(1)}}},
		},
		{
			name:      "delete where string",
			input:     "DELETE FROM users WHERE name = 'bob'",
			tableName: "users",
			where:     &WhereClause{Conditions: []Condition{{Left: "name", Operator: "=", Right: StringValue("bob")}}},
		},
		{
			name:      "delete where and",
			input:     "DELETE FROM users WHERE id > 1 AND name = 'bob'",
			tableName: "users",
			where: &WhereClause{Conditions: []Condition{
				{Left: "id", Operator: ">", Right: Int64Value(1)},
				{Left: "name", Operator: "=", Right: StringValue("bob")},
			}},
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
			if (got.Where == nil) != (tc.where == nil) {
				t.Fatalf("parseDelete().Where = %#v, want %#v", got.Where, tc.where)
			}
			if got.Where == nil {
				return
			}
			if len(got.Where.Conditions) != len(tc.where.Conditions) {
				t.Fatalf("len(parseDelete().Where.Conditions) = %d, want %d", len(got.Where.Conditions), len(tc.where.Conditions))
			}
			for i := range tc.where.Conditions {
				if got.Where.Conditions[i] != tc.where.Conditions[i] {
					t.Fatalf("parseDelete().Where.Conditions[%d] = %#v, want %#v", i, got.Where.Conditions[i], tc.where.Conditions[i])
				}
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
