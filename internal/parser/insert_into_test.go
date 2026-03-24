package parser

import "testing"

func TestParseInsert(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		cols   []string
		values []Value
	}{
		{
			name:   "basic",
			input:  "INSERT INTO users VALUES (1, 'steve')",
			cols:   nil,
			values: []Value{Int64Value(1), StringValue("steve")},
		},
		{
			name:   "spacing",
			input:  "INSERT INTO users VALUES ( 1 , 'steve' )",
			cols:   nil,
			values: []Value{Int64Value(1), StringValue("steve")},
		},
		{
			name:   "column list",
			input:  "INSERT INTO users (id, name) VALUES (1, 'steve')",
			cols:   []string{"id", "name"},
			values: []Value{Int64Value(1), StringValue("steve")},
		},
		{
			name:   "column list reordered",
			input:  "INSERT INTO users ( name , id ) VALUES ( 'steve' , 1 )",
			cols:   []string{"name", "id"},
			values: []Value{StringValue("steve"), Int64Value(1)},
		},
		{
			name:   "null literal",
			input:  "INSERT INTO users VALUES (1, NULL)",
			cols:   nil,
			values: []Value{Int64Value(1), NullValue()},
		},
		{
			name:   "bool literal true",
			input:  "INSERT INTO users VALUES (TRUE)",
			cols:   nil,
			values: []Value{BoolValue(true)},
		},
		{
			name:   "bool literal false mixed case",
			input:  "INSERT INTO users VALUES (False)",
			cols:   nil,
			values: []Value{BoolValue(false)},
		},
		{
			name:   "real literal",
			input:  "INSERT INTO users VALUES (3.14)",
			cols:   nil,
			values: []Value{RealValue(3.14)},
		},
		{
			name:   "negative real literal",
			input:  "INSERT INTO users VALUES (-2.5)",
			cols:   nil,
			values: []Value{RealValue(-2.5)},
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
			if len(got.Columns) != len(tc.cols) {
				t.Fatalf("parseInsert().Columns len = %d, want %d", len(got.Columns), len(tc.cols))
			}
			for i := range tc.cols {
				if got.Columns[i] != tc.cols[i] {
					t.Fatalf("parseInsert().Columns[%d] = %q, want %q", i, got.Columns[i], tc.cols[i])
				}
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
		{name: "duplicate column", input: "INSERT INTO users (id, id) VALUES (1, 'steve')"},
		{name: "empty column list", input: "INSERT INTO users () VALUES (1, 'steve')"},
		{name: "column value mismatch", input: "INSERT INTO users (id, name) VALUES (1)"},
		{name: "unsupported trailing decimal", input: "INSERT INTO users VALUES (1.)"},
		{name: "unsupported leading decimal", input: "INSERT INTO users VALUES (.5)"},
		{name: "unsupported exponent", input: "INSERT INTO users VALUES (1e3)"},
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
