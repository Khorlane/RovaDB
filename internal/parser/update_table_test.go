package parser

import "testing"

func TestParseUpdate(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		tableName   string
		assignments []UpdateAssignment
		where       *WhereClause
	}{
		{
			name:      "update all",
			input:     "UPDATE users SET name = 'bob'",
			tableName: "users",
			assignments: []UpdateAssignment{
				{Column: "name", Value: StringValue("bob")},
			},
		},
		{
			name:      "update where int",
			input:     "UPDATE users SET name = 'bob' WHERE id = 1",
			tableName: "users",
			assignments: []UpdateAssignment{
				{Column: "name", Value: StringValue("bob")},
			},
			where: &WhereClause{Left: "id", Operator: "=", Right: Int64Value(1)},
		},
		{
			name:      "update multiple assignments where string",
			input:     "UPDATE users SET name = 'bob', id = 2 WHERE name = 'alice'",
			tableName: "users",
			assignments: []UpdateAssignment{
				{Column: "name", Value: StringValue("bob")},
				{Column: "id", Value: Int64Value(2)},
			},
			where: &WhereClause{Left: "name", Operator: "=", Right: StringValue("alice")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseUpdate(tc.input)
			if err != nil {
				t.Fatalf("parseUpdate() error = %v", err)
			}
			if got.TableName != tc.tableName {
				t.Fatalf("parseUpdate().TableName = %q, want %q", got.TableName, tc.tableName)
			}
			if (got.Where == nil) != (tc.where == nil) || (got.Where != nil && *got.Where != *tc.where) {
				t.Fatalf("parseUpdate().Where = %#v, want %#v", got.Where, tc.where)
			}
			if len(got.Assignments) != len(tc.assignments) {
				t.Fatalf("parseUpdate().Assignments len = %d, want %d", len(got.Assignments), len(tc.assignments))
			}
			for i := range tc.assignments {
				if got.Assignments[i] != tc.assignments[i] {
					t.Fatalf("parseUpdate().Assignments[%d] = %#v, want %#v", i, got.Assignments[i], tc.assignments[i])
				}
			}
		})
	}
}

func TestParseUpdateInvalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "missing set", input: "UPDATE users name = 'bob'"},
		{name: "missing assignment value", input: "UPDATE users SET name ="},
		{name: "missing equals", input: "UPDATE users SET name 'bob'"},
		{name: "duplicate assignment column", input: "UPDATE users SET name = 'bob', name = 'sam'"},
		{name: "extra condition text", input: "UPDATE users SET name = 'bob' WHERE id = 1 AND name = 'alice'"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseUpdate(tc.input)
			if err == nil {
				t.Fatalf("parseUpdate() = %#v, want error", got)
			}
		})
	}
}
