package parser

import "testing"

func TestParseInsert(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		values []Value
	}{
		{
			name:   "basic",
			input:  "INSERT INTO users VALUES (1, 'steve')",
			values: []Value{Int64Value(1), StringValue("steve")},
		},
		{
			name:   "spacing",
			input:  "INSERT INTO users VALUES ( 1 , 'steve' )",
			values: []Value{Int64Value(1), StringValue("steve")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseInsert(tc.input)
			if err != nil {
				t.Fatalf("parseInsert() error = %v", err)
			}
			if got.TableName != "users" {
				t.Fatalf("parseInsert().TableName = %q, want %q", got.TableName, "users")
			}
			if len(got.Values) != len(tc.values) {
				t.Fatalf("parseInsert().Values len = %d, want %d", len(got.Values), len(tc.values))
			}
			for i := range tc.values {
				if got.Values[i] != tc.values[i] {
					t.Fatalf("parseInsert().Values[%d] = %#v, want %#v", i, got.Values[i], tc.values[i])
				}
			}
		})
	}
}

func TestParseInsertInvalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "missing values", input: "INSERT INTO users (1, 'steve')"},
		{name: "missing parens", input: "INSERT INTO users VALUES 1, 'steve'"},
		{name: "empty value slot", input: "INSERT INTO users VALUES (1, )"},
		{name: "expression value", input: "INSERT INTO users VALUES (1+2, 'steve')"},
		{name: "empty values list", input: "INSERT INTO users VALUES ()"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseInsert(tc.input)
			if err == nil {
				t.Fatalf("parseInsert() = %#v, want error", got)
			}
		})
	}
}
