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
		{
			name:   "date literal",
			input:  "INSERT INTO users VALUES ('2026-04-10')",
			cols:   nil,
			values: []Value{DateValue(20553)},
		},
		{
			name:   "time literal",
			input:  "INSERT INTO users VALUES ('13:45:21')",
			cols:   nil,
			values: []Value{TimeValue(49521)},
		},
		{
			name:   "timestamp literal",
			input:  "INSERT INTO users VALUES ('2026-04-10 13:45:21')",
			cols:   nil,
			values: []Value{TimestampUnresolvedValue(2026, 4, 10, 13, 45, 21)},
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

func TestParseInsertRejectsMalformedTemporalLiterals(t *testing.T) {
	tests := []string{
		"INSERT INTO users VALUES ('2026/04/10')",
		"INSERT INTO users VALUES ('2026-4-1')",
		"INSERT INTO users VALUES ('1:2:3')",
		"INSERT INTO users VALUES ('2026-04-10T13:45:21')",
		"INSERT INTO users VALUES ('2026-02-30')",
		"INSERT INTO users VALUES ('24:00:00')",
		"INSERT INTO users VALUES ('23:59:60')",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			if got, err := parseInsert(sql); err == nil {
				t.Fatalf("parseInsert() = %#v, want error", got)
			}
		})
	}
}

func TestParseInsertViaParse(t *testing.T) {
	stmt, err := Parse("INSERT INTO users VALUES (1, 'steve')")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	got, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("Parse() stmt type = %T, want *InsertStmt", stmt)
	}
	if got.TableName != "users" {
		t.Fatalf("Parse().TableName = %q, want %q", got.TableName, "users")
	}
	wantValues := []Value{Int64Value(1), StringValue("steve")}
	if len(got.Values) != len(wantValues) {
		t.Fatalf("Parse().Values len = %d, want %d", len(got.Values), len(wantValues))
	}
	for i := range wantValues {
		if got.Values[i] != wantValues[i] {
			t.Fatalf("Parse().Values[%d] = %#v, want %#v", i, got.Values[i], wantValues[i])
		}
	}
}

func TestParseInsertTokens(t *testing.T) {
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
			name:   "column list",
			input:  "INSERT INTO users (id, name) VALUES (1, 'steve')",
			cols:   []string{"id", "name"},
			values: []Value{Int64Value(1), StringValue("steve")},
		},
		{
			name:   "real literal",
			input:  "INSERT INTO users VALUES (3.14)",
			cols:   nil,
			values: []Value{RealValue(3.14)},
		},
		{
			name:   "placeholder",
			input:  "INSERT INTO users VALUES (?)",
			cols:   nil,
			values: []Value{PlaceholderValue()},
		},
		{
			name:   "function expression",
			input:  "INSERT INTO users VALUES (LOWER('STEVE'))",
			cols:   nil,
			values: []Value{{}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseInsertTokens(tc.input)
			if err != nil {
				t.Fatalf("parseInsertTokens() error = %v", err)
			}
			if got.TableName != "users" {
				t.Fatalf("parseInsertTokens().TableName = %q, want %q", got.TableName, "users")
			}
			if len(got.Columns) != len(tc.cols) {
				t.Fatalf("parseInsertTokens().Columns len = %d, want %d", len(got.Columns), len(tc.cols))
			}
			for i := range tc.cols {
				if got.Columns[i] != tc.cols[i] {
					t.Fatalf("parseInsertTokens().Columns[%d] = %q, want %q", i, got.Columns[i], tc.cols[i])
				}
			}
			if len(got.Values) != len(tc.values) {
				t.Fatalf("parseInsertTokens().Values len = %d, want %d", len(got.Values), len(tc.values))
			}
			for i := range tc.values {
				if got.Values[i] != tc.values[i] {
					t.Fatalf("parseInsertTokens().Values[%d] = %#v, want %#v", i, got.Values[i], tc.values[i])
				}
			}
			if tc.name == "function expression" {
				if len(got.ValueExprs) != 1 || got.ValueExprs[0] == nil || got.ValueExprs[0].Kind != ValueExprKindFunctionCall || got.ValueExprs[0].FuncName != "LOWER" {
					t.Fatalf("parseInsertTokens().ValueExprs = %#v, want LOWER(...) expression", got.ValueExprs)
				}
			}
		})
	}
}

func TestParseInsertTokensArithmeticValueExpr(t *testing.T) {
	got, err := parseInsertTokens("INSERT INTO users VALUES (1 + 2)")
	if err != nil {
		t.Fatalf("parseInsertTokens() error = %v", err)
	}
	if len(got.ValueExprs) != 1 || got.ValueExprs[0] == nil || got.ValueExprs[0].Kind != ValueExprKindBinary || got.ValueExprs[0].Op != ValueExprBinaryOpAdd {
		t.Fatalf("ValueExprs = %#v, want one binary add expr", got.ValueExprs)
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

func TestParseInsertTokensInvalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "missing values", input: "INSERT INTO users (1, 'steve')"},
		{name: "missing parens", input: "INSERT INTO users VALUES 1, 'steve'"},
		{name: "empty value slot", input: "INSERT INTO users VALUES (1, )"},
		{name: "empty values list", input: "INSERT INTO users VALUES ()"},
		{name: "duplicate column", input: "INSERT INTO users (id, id) VALUES (1, 'steve')"},
		{name: "empty column list", input: "INSERT INTO users () VALUES (1, 'steve')"},
		{name: "column value mismatch", input: "INSERT INTO users (id, name) VALUES (1)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseInsertTokens(tc.input)
			if err == nil {
				t.Fatalf("parseInsertTokens() = %#v, want error", got)
			}
		})
	}
}
