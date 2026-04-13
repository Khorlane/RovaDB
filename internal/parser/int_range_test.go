package parser

import (
	"strings"
	"testing"
)

func TestParseRejectsOutOfRangeUntypedIntegerLiterals(t *testing.T) {
	tests := []string{
		"SELECT 2147483648",
		"SELECT -2147483649",
		"INSERT INTO users VALUES (2147483648)",
		"INSERT INTO users VALUES (-2147483649)",
		"UPDATE users SET id = 2147483648",
		"SELECT * FROM users WHERE id = 2147483648",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			if _, err := Parse(sql); err == nil {
				t.Fatalf("Parse(%q) error = nil, want parse error", sql)
			}
		})
	}
}

func TestParseAcceptsBoundaryUntypedIntegerLiterals(t *testing.T) {
	tests := []string{
		"SELECT 2147483647",
		"SELECT -2147483648",
		"INSERT INTO users VALUES (2147483647)",
		"INSERT INTO users VALUES (-2147483648)",
		"UPDATE users SET id = -2147483648",
		"SELECT * FROM users WHERE id = 2147483647",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			if _, err := Parse(sql); err != nil {
				t.Fatalf("Parse(%q) error = %v, want nil", sql, err)
			}
		})
	}
}

func TestParseUntypedIntegerLiteralIsExplicitlyNarrow(t *testing.T) {
	tests := []struct {
		token string
		want  int64
		ok    bool
	}{
		{token: "2147483647", want: 2147483647, ok: true},
		{token: "-2147483648", want: -2147483648, ok: true},
		{token: "2147483648", ok: false},
		{token: "-2147483649", ok: false},
	}

	for _, tc := range tests {
		t.Run(tc.token, func(t *testing.T) {
			got, ok := parseUntypedIntegerLiteral(tc.token)
			if ok != tc.ok {
				t.Fatalf("parseUntypedIntegerLiteral(%q) ok = %v, want %v", tc.token, ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Fatalf("parseUntypedIntegerLiteral(%q) = %d, want %d", tc.token, got, tc.want)
			}
		})
	}
}

func TestBindArgumentValueRejectsGoInt(t *testing.T) {
	_, err := bindArgumentValue(int(1))
	if err == nil || !strings.Contains(err.Error(), "unsupported placeholder argument type") {
		t.Fatalf("bindArgumentValue(int) error = %v, want unsupported placeholder argument type", err)
	}
}
