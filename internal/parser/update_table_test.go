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
			where: &WhereClause{Items: []ConditionChainItem{{Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(1)}}}},
		},
		{
			name:      "update multiple assignments where string",
			input:     "UPDATE users SET name = 'bob', id = 2 WHERE name = 'alice'",
			tableName: "users",
			assignments: []UpdateAssignment{
				{Column: "name", Value: StringValue("bob")},
				{Column: "id", Value: Int64Value(2)},
			},
			where: &WhereClause{Items: []ConditionChainItem{{Condition: Condition{Left: "name", Operator: "=", Right: StringValue("alice")}}}},
		},
		{
			name:      "update where and",
			input:     "UPDATE users SET name = 'bob' WHERE id > 1 AND name != 'sam'",
			tableName: "users",
			assignments: []UpdateAssignment{
				{Column: "name", Value: StringValue("bob")},
			},
			where: &WhereClause{Items: []ConditionChainItem{
				{Condition: Condition{Left: "id", Operator: ">", Right: Int64Value(1)}},
				{Op: BooleanOpAnd, Condition: Condition{Left: "name", Operator: "!=", Right: StringValue("sam")}},
			}},
		},
		{
			name:      "update where or",
			input:     "UPDATE users SET name = 'bob' WHERE id = 1 OR name = 'alice'",
			tableName: "users",
			assignments: []UpdateAssignment{
				{Column: "name", Value: StringValue("bob")},
			},
			where: &WhereClause{Items: []ConditionChainItem{
				{Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(1)}},
				{Op: BooleanOpOr, Condition: Condition{Left: "name", Operator: "=", Right: StringValue("alice")}},
			}},
		},
		{
			name:      "update set null",
			input:     "UPDATE users SET name = NULL WHERE id = 1",
			tableName: "users",
			assignments: []UpdateAssignment{
				{Column: "name", Value: NullValue()},
			},
			where: &WhereClause{Items: []ConditionChainItem{{Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(1)}}}},
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
			if (got.Where == nil) != (tc.where == nil) {
				t.Fatalf("parseUpdate().Where = %#v, want %#v", got.Where, tc.where)
			}
			if got.Where == nil {
				goto assignments
			}
			if len(got.Where.Items) != len(tc.where.Items) {
				t.Fatalf("len(parseUpdate().Where.Items) = %d, want %d", len(got.Where.Items), len(tc.where.Items))
			}
			for i := range tc.where.Items {
				if got.Where.Items[i] != tc.where.Items[i] {
					t.Fatalf("parseUpdate().Where.Items[%d] = %#v, want %#v", i, got.Where.Items[i], tc.where.Items[i])
				}
			}
		assignments:
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

func TestParseUpdateViaParse(t *testing.T) {
	stmt, err := Parse("UPDATE users SET name = 'bob' WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	got, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("Parse() stmt type = %T, want *UpdateStmt", stmt)
	}
	if got.TableName != "users" {
		t.Fatalf("Parse().TableName = %q, want %q", got.TableName, "users")
	}
	wantAssignments := []UpdateAssignment{{Column: "name", Value: StringValue("bob")}}
	if len(got.Assignments) != len(wantAssignments) || got.Assignments[0] != wantAssignments[0] {
		t.Fatalf("Parse().Assignments = %#v, want %#v", got.Assignments, wantAssignments)
	}
	wantWhere := &WhereClause{Items: []ConditionChainItem{{Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(1)}}}}
	if got.Where == nil {
		t.Fatal("Parse().Where = nil, want non-nil")
	}
	if len(got.Where.Items) != len(wantWhere.Items) || got.Where.Items[0] != wantWhere.Items[0] {
		t.Fatalf("Parse().Where = %#v, want %#v", got.Where, wantWhere)
	}
}

func TestParseUpdateTokens(t *testing.T) {
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
			where: &WhereClause{Items: []ConditionChainItem{{Condition: Condition{Left: "id", Operator: "=", Right: Int64Value(1)}}}},
		},
		{
			name:      "update multiple assignments where string",
			input:     "UPDATE users SET name = 'bob', id = 2 WHERE name = 'alice'",
			tableName: "users",
			assignments: []UpdateAssignment{
				{Column: "name", Value: StringValue("bob")},
				{Column: "id", Value: Int64Value(2)},
			},
			where: &WhereClause{Items: []ConditionChainItem{{Condition: Condition{Left: "name", Operator: "=", Right: StringValue("alice")}}}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseUpdateTokens(tc.input)
			if err != nil {
				t.Fatalf("parseUpdateTokens() error = %v", err)
			}
			if got.TableName != tc.tableName {
				t.Fatalf("parseUpdateTokens().TableName = %q, want %q", got.TableName, tc.tableName)
			}
			if (got.Where == nil) != (tc.where == nil) {
				t.Fatalf("parseUpdateTokens().Where = %#v, want %#v", got.Where, tc.where)
			}
			if got.Where != nil {
				if len(got.Where.Items) != len(tc.where.Items) {
					t.Fatalf("len(parseUpdateTokens().Where.Items) = %d, want %d", len(got.Where.Items), len(tc.where.Items))
				}
				for i := range tc.where.Items {
					if got.Where.Items[i] != tc.where.Items[i] {
						t.Fatalf("parseUpdateTokens().Where.Items[%d] = %#v, want %#v", i, got.Where.Items[i], tc.where.Items[i])
					}
				}
			}
			if len(got.Assignments) != len(tc.assignments) {
				t.Fatalf("parseUpdateTokens().Assignments len = %d, want %d", len(got.Assignments), len(tc.assignments))
			}
			for i := range tc.assignments {
				if got.Assignments[i] != tc.assignments[i] {
					t.Fatalf("parseUpdateTokens().Assignments[%d] = %#v, want %#v", i, got.Assignments[i], tc.assignments[i])
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
		{name: "missing trailing condition", input: "UPDATE users SET name = 'bob' WHERE id = 1 OR"},
		{name: "unsupported boolean op", input: "UPDATE users SET name = 'bob' WHERE id = 1 XOR id = 2"},
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

func TestParseUpdateTokensInvalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "missing set", input: "UPDATE users name = 'bob'"},
		{name: "missing assignment value", input: "UPDATE users SET name ="},
		{name: "missing equals", input: "UPDATE users SET name 'bob'"},
		{name: "duplicate assignment column", input: "UPDATE users SET name = 'bob', name = 'sam'"},
		{name: "missing trailing condition", input: "UPDATE users SET name = 'bob' WHERE id = 1 OR"},
		{name: "unexpected token before where", input: "UPDATE users SET name = 'bob' extra"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseUpdateTokens(tc.input)
			if err == nil {
				t.Fatalf("parseUpdateTokens() = %#v, want error", got)
			}
		})
	}
}
