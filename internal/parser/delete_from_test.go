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
			where:     &WhereClause{Items: []ConditionChainItem{{Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(1)}}}},
		},
		{
			name:      "delete where string",
			input:     "DELETE FROM users WHERE name = 'bob'",
			tableName: "users",
			where:     &WhereClause{Items: []ConditionChainItem{{Condition: Condition{Left: "name", Operator: "=", Right: StringValue("bob")}}}},
		},
		{
			name:      "delete where and",
			input:     "DELETE FROM users WHERE id > 1 AND name = 'bob'",
			tableName: "users",
			where: &WhereClause{Items: []ConditionChainItem{
				{Condition: Condition{Left: "id", Operator: ">", Right: Int64Value(1)}},
				{Op: BooleanOpAnd, Condition: Condition{Left: "name", Operator: "=", Right: StringValue("bob")}},
			}},
		},
		{
			name:      "delete where or",
			input:     "DELETE FROM users WHERE id = 1 OR name = 'bob'",
			tableName: "users",
			where: &WhereClause{Items: []ConditionChainItem{
				{Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(1)}},
				{Op: BooleanOpOr, Condition: Condition{Left: "name", Operator: "=", Right: StringValue("bob")}},
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
			if len(got.Where.Items) != len(tc.where.Items) {
				t.Fatalf("len(parseDelete().Where.Items) = %d, want %d", len(got.Where.Items), len(tc.where.Items))
			}
			for i := range tc.where.Items {
				if got.Where.Items[i] != tc.where.Items[i] {
					t.Fatalf("parseDelete().Where.Items[%d] = %#v, want %#v", i, got.Where.Items[i], tc.where.Items[i])
				}
			}
		})
	}
}

func TestParseDeleteTokens(t *testing.T) {
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
			where:     &WhereClause{Items: []ConditionChainItem{{Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(1)}}}},
		},
		{
			name:      "delete where string",
			input:     "DELETE FROM users WHERE name = 'bob'",
			tableName: "users",
			where:     &WhereClause{Items: []ConditionChainItem{{Condition: Condition{Left: "name", Operator: "=", Right: StringValue("bob")}}}},
		},
		{
			name:      "delete where and",
			input:     "DELETE FROM users WHERE id > 1 AND name = 'bob'",
			tableName: "users",
			where: &WhereClause{Items: []ConditionChainItem{
				{Condition: Condition{Left: "id", Operator: ">", Right: Int64Value(1)}},
				{Op: BooleanOpAnd, Condition: Condition{Left: "name", Operator: "=", Right: StringValue("bob")}},
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseDeleteTokens(tc.input)
			if err != nil {
				t.Fatalf("parseDeleteTokens() error = %v", err)
			}
			if got.TableName != tc.tableName {
				t.Fatalf("parseDeleteTokens().TableName = %q, want %q", got.TableName, tc.tableName)
			}
			if (got.Where == nil) != (tc.where == nil) {
				t.Fatalf("parseDeleteTokens().Where = %#v, want %#v", got.Where, tc.where)
			}
			if got.Where == nil {
				return
			}
			if len(got.Where.Items) != len(tc.where.Items) {
				t.Fatalf("len(parseDeleteTokens().Where.Items) = %d, want %d", len(got.Where.Items), len(tc.where.Items))
			}
			for i := range tc.where.Items {
				if got.Where.Items[i] != tc.where.Items[i] {
					t.Fatalf("parseDeleteTokens().Where.Items[%d] = %#v, want %#v", i, got.Where.Items[i], tc.where.Items[i])
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
		{name: "missing trailing condition", input: "DELETE FROM users WHERE id = 1 OR"},
		{name: "unsupported boolean op", input: "DELETE FROM users WHERE id = 1 XOR id = 2"},
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

func TestParseDeleteTokensInvalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "missing table", input: "DELETE FROM"},
		{name: "missing rhs literal", input: "DELETE FROM users WHERE id ="},
		{name: "missing equals", input: "DELETE FROM users WHERE id 1"},
		{name: "missing trailing condition", input: "DELETE FROM users WHERE id = 1 OR"},
		{name: "unexpected token after table", input: "DELETE FROM users extra"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseDeleteTokens(tc.input)
			if err == nil {
				t.Fatalf("parseDeleteTokens() = %#v, want error", got)
			}
		})
	}
}
