package parser

import (
	"strings"
	"testing"
)

func TestParseRejectsOutOfRangeIntLiterals(t *testing.T) {
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

func TestParseAcceptsBoundaryIntLiterals(t *testing.T) {
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

func TestBindArgumentValueRejectsOutOfRangeInt(t *testing.T) {
	_, err := bindArgumentValue(2147483648)
	if err == nil || !strings.Contains(err.Error(), "out of range for INT") {
		t.Fatalf("bindArgumentValue(out of range int) error = %v, want out-of-range bind error", err)
	}
}
