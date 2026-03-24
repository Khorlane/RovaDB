package rovadb

import (
	"errors"
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestQuerySelectLiteral(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		value int
	}{
		{name: "select one", sql: "SELECT 1", value: 1},
		{name: "select forty two", sql: "SELECT 42", value: 42},
		{name: "select minus one", sql: "SELECT -1", value: -1},
		{name: "select minus forty two", sql: "SELECT -42", value: -42},
		{name: "select one plus two", sql: "SELECT 1+2", value: 3},
		{name: "select five minus three", sql: "SELECT 5-3", value: 2},
		{name: "select minus one plus two", sql: "SELECT -1+2", value: 1},
		{name: "select ten plus minus three", sql: "SELECT 10+-3", value: 7},
		{name: "select one plus two spaced", sql: "SELECT 1 + 2", value: 3},
		{name: "select five minus three spaced", sql: "SELECT 5 - 3", value: 2},
		{name: "select minus one plus two spaced", sql: "SELECT -1 + 2", value: 1},
		{name: "select ten plus minus three spaced", sql: "SELECT 10 + -3", value: 7},
		{name: "select parenthesized one plus two", sql: "SELECT (1+2)", value: 3},
		{name: "select parenthesized one plus two spaced", sql: "SELECT (1 + 2)", value: 3},
		{name: "select parenthesized minus one plus two", sql: "SELECT (-1+2)", value: 1},
		{name: "select trimmed mixed case", sql: " select 999 ", value: 999},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(testDBPath(t))
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			if !rows.Next() {
				t.Fatal("Next() = false, want true")
			}

			var got int
			if err := rows.Scan(&got); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			if got != tc.value {
				t.Fatalf("Scan() got %d, want %d", got, tc.value)
			}

			if rows.Next() {
				t.Fatal("Next() = true after first row, want false")
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("Err() = %v, want nil", err)
			}
		})
	}
}

func TestQuerySelectStringLiteral(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		value string
	}{
		{name: "select hello", sql: "SELECT 'hello'", value: "hello"},
		{name: "select rovadb", sql: "SELECT 'RovaDB'", value: "RovaDB"},
		{name: "select empty string", sql: "SELECT ''", value: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(testDBPath(t))
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			if !rows.Next() {
				t.Fatal("Next() = false, want true")
			}

			var got string
			if err := rows.Scan(&got); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			if got != tc.value {
				t.Fatalf("Scan() got %q, want %q", got, tc.value)
			}

			if rows.Next() {
				t.Fatal("Next() = true after first row, want false")
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("Err() = %v, want nil", err)
			}
		})
	}
}

func TestQueryUnsupportedLiteral(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "select identifier", sql: "SELECT abc"},
		{name: "select plus one", sql: "SELECT +1"},
		{name: "select chained expression", sql: "SELECT 1+2+3"},
		{name: "select chained expression spaced", sql: "SELECT 1 + 2 + 3"},
		{name: "select incomplete expression", sql: "SELECT 1 +"},
		{name: "select missing left operand", sql: "SELECT + 1"},
		{name: "select string expression", sql: "SELECT 'a'+'b'"},
		{name: "select multiply expression spaced", sql: "SELECT 1 * 2"},
		{name: "select multiply expression", sql: "SELECT 1*2"},
		{name: "select nested parenthesized expression", sql: "SELECT ((1+2))"},
		{name: "select unterminated parenthesized expression", sql: "SELECT (1+2"},
		{name: "select trailing parenthesized expression", sql: "SELECT 1+2)"},
		{name: "select missing from table", sql: "SELECT id, name users"},
		{name: "select invalid from format", sql: "SELECT id name FROM users"},
		{name: "select double quoted string", sql: `SELECT "hello"`},
		{name: "select string with spaces", sql: "SELECT 'hello world'"},
		{name: "select unterminated string", sql: "SELECT 'unterminated"},
		{name: "extra token", sql: "SELECT 1 2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(testDBPath(t))
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			if rows.Next() {
				t.Fatal("Next() = true, want false")
			}
			if rows.Err() == nil || rows.Err().Error() != "parse: unsupported query form" {
				t.Fatalf("Err() = %v, want %q", rows.Err(), "parse: unsupported query form")
			}
		})
	}
}

func TestRowsScanStringIntoAny(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 'hello'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var got any
	if err := rows.Scan(&got); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if got != "hello" {
		t.Fatalf("Scan() got %#v, want %q", got, "hello")
	}
}

func TestRowsScanStringIntoInt(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 'hello'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var got int
	err = rows.Scan(&got)
	if !errors.Is(err, ErrUnsupportedScanType) {
		t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
	}
}

func TestParseSelectExprDirect(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		ok   bool
		want *parser.Expr
	}{
		{
			name: "select integer",
			sql:  "SELECT 1",
			ok:   true,
			want: &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: 1},
		},
		{
			name: "select string",
			sql:  "SELECT 'hello'",
			ok:   true,
			want: &parser.Expr{Kind: parser.ExprKindStringLiteral, Str: "hello"},
		},
		{
			name: "select one plus two",
			sql:  "SELECT 1+2",
			ok:   true,
			want: &parser.Expr{
				Kind:  parser.ExprKindInt64Binary,
				Op:    parser.BinaryOpAdd,
				Left:  &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: 1},
				Right: &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: 2},
			},
		},
		{
			name: "select minus one plus two",
			sql:  "SELECT -1+2",
			ok:   true,
			want: &parser.Expr{
				Kind:  parser.ExprKindInt64Binary,
				Op:    parser.BinaryOpAdd,
				Left:  &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: -1},
				Right: &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: 2},
			},
		},
		{
			name: "select one plus two spaced",
			sql:  "SELECT 1 + 2",
			ok:   true,
			want: &parser.Expr{
				Kind:  parser.ExprKindInt64Binary,
				Op:    parser.BinaryOpAdd,
				Left:  &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: 1},
				Right: &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: 2},
			},
		},
		{
			name: "select minus one plus two spaced",
			sql:  "SELECT -1 + 2",
			ok:   true,
			want: &parser.Expr{
				Kind:  parser.ExprKindInt64Binary,
				Op:    parser.BinaryOpAdd,
				Left:  &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: -1},
				Right: &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: 2},
			},
		},
		{
			name: "select parenthesized one plus two",
			sql:  "SELECT (1+2)",
			ok:   true,
			want: &parser.Expr{
				Kind: parser.ExprKindParen,
				Inner: &parser.Expr{
					Kind:  parser.ExprKindInt64Binary,
					Op:    parser.BinaryOpAdd,
					Left:  &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: 1},
					Right: &parser.Expr{Kind: parser.ExprKindInt64Literal, I64: 2},
				},
			},
		},
		{
			name: "select table columns",
			sql:  "SELECT id, name FROM users",
			ok:   true,
			want: &parser.Expr{},
		},
		{
			name: "select table star",
			sql:  "SELECT * FROM users",
			ok:   true,
			want: &parser.Expr{},
		},
		{
			name: "select table columns no space after comma",
			sql:  "SELECT id,name FROM users",
			ok:   true,
			want: &parser.Expr{},
		},
		{
			name: "select table star mixed spacing",
			sql:  "SELECT  *  FROM  users",
			ok:   true,
			want: &parser.Expr{},
		},
		{
			name: "select table columns mixed spacing",
			sql:  "SELECT  id ,name  FROM  users",
			ok:   true,
			want: &parser.Expr{},
		},
		{
			name: "select table star where int",
			sql:  "SELECT * FROM users WHERE id = 1",
			ok:   true,
			want: &parser.Expr{},
		},
		{
			name: "select table column where string",
			sql:  "SELECT name FROM users WHERE name = 'bob'",
			ok:   true,
			want: &parser.Expr{},
		},
		{
			name: "select table order by desc",
			sql:  "SELECT * FROM users ORDER BY id DESC",
			ok:   true,
			want: &parser.Expr{},
		},
		{
			name: "select count star",
			sql:  "SELECT COUNT(*) FROM users",
			ok:   true,
			want: &parser.Expr{},
		},
		{name: "select identifier", sql: "SELECT abc", ok: false},
		{name: "select count column", sql: "SELECT COUNT(id) FROM users", ok: false},
		{name: "select count mixed projection", sql: "SELECT COUNT(*), name FROM users", ok: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parser.ParseSelectExpr(tc.sql)
			if ok != tc.ok {
				t.Fatalf("ParseSelectExpr() ok = %v, want %v", ok, tc.ok)
			}
			if !tc.ok {
				if got != nil {
					t.Fatalf("ParseSelectExpr() = %#v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("ParseSelectExpr() = nil, want value")
			}
			if tc.name == "select table columns" || tc.name == "select table columns no space after comma" || tc.name == "select table columns mixed spacing" {
				if got.TableName != "users" || len(got.Columns) != 2 || got.Columns[0] != "id" || got.Columns[1] != "name" {
					t.Fatalf("ParseSelectExpr() = %#v, want table users columns [id name]", got)
				}
				return
			}
			if tc.name == "select table column where string" {
				if got.TableName != "users" || len(got.Columns) != 1 || got.Columns[0] != "name" || got.Where == nil || len(got.Where.Items) != 1 || got.Where.Items[0].Condition.Left != "name" || got.Where.Items[0].Condition.Operator != "=" || got.Where.Items[0].Condition.Right != parser.StringValue("bob") {
					t.Fatalf("ParseSelectExpr() = %#v, want table users columns [name] where name='bob'", got)
				}
				return
			}
			if tc.name == "select table star" || tc.name == "select table star mixed spacing" || tc.name == "select table star where int" {
				if tc.name == "select table star where int" {
					if got.TableName != "users" || got.Columns != nil || got.Where == nil || len(got.Where.Items) != 1 || got.Where.Items[0].Condition.Left != "id" || got.Where.Items[0].Condition.Operator != "=" || got.Where.Items[0].Condition.Right != parser.Int64Value(1) {
						t.Fatalf("ParseSelectExpr() = %#v, want table users select all where id=1", got)
					}
					return
				}
				if got.TableName != "users" || got.Columns != nil {
					t.Fatalf("ParseSelectExpr() = %#v, want table users select all", got)
				}
				return
			}
			if tc.name == "select table order by desc" {
				if got.TableName != "users" || got.Columns != nil || got.OrderBy == nil || got.OrderBy.Column != "id" || !got.OrderBy.Desc {
					t.Fatalf("ParseSelectExpr() = %#v, want table users order by id desc", got)
				}
				return
			}
			if tc.name == "select count star" {
				if got.TableName != "users" || !got.IsCountStar || got.OrderBy != nil {
					t.Fatalf("ParseSelectExpr() = %#v, want table users count(*)", got)
				}
				return
			}
			if !equalExpr(got.Expr, tc.want) {
				t.Fatalf("ParseSelectExpr().Expr = %#v, want %#v", got.Expr, tc.want)
			}
		})
	}
}

func equalExpr(got, want *parser.Expr) bool {
	if got == nil || want == nil {
		return got == want
	}
	if got.Kind != want.Kind || got.I64 != want.I64 || got.Str != want.Str || got.Op != want.Op {
		return false
	}

	return equalExpr(got.Left, want.Left) && equalExpr(got.Right, want.Right) && equalExpr(got.Inner, want.Inner)
}

func TestQueryEmptySQL(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query(" ")
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Query() error = %v, want ErrInvalidArgument", err)
	}
	if rows != nil {
		t.Fatalf("Query() rows = %v, want nil", rows)
	}
}

func TestQueryClosedDB(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rows, err := db.Query("SELECT 1")
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Query() error = %v, want ErrClosed", err)
	}
	if rows != nil {
		t.Fatalf("Query() rows = %v, want nil", rows)
	}
}
