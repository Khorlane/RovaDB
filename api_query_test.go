package rovadb

import (
	"errors"
	"testing"
)

func TestQueryAPILiteralSelectReturnsRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil {
		t.Fatal("Query() rows = nil, want value")
	}
	if rows.idx != -1 {
		t.Fatalf("rows.idx = %d, want -1", rows.idx)
	}
	if len(rows.columns) != 0 {
		t.Fatalf("rows.columns = %#v, want nil/empty", rows.columns)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != 1 {
		t.Fatalf("rows.data = %#v, want [[1]]", rows.data)
	}
}

func TestQueryAPINoArgsStillWorksWithVariadicSignature(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil {
		t.Fatal("Query() rows = nil, want value")
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != 1 {
		t.Fatalf("rows.data = %#v, want [[1]]", rows.data)
	}
}

func TestQueryAPISelectFromReturnsMaterializedRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil {
		t.Fatal("Query() rows = nil, want value")
	}
	if len(rows.columns) != 2 || rows.columns[0] != "id" || rows.columns[1] != "name" {
		t.Fatalf("rows.columns = %#v, want [id name]", rows.columns)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 2 || rows.data[0][0] != 1 || rows.data[0][1] != "alice" {
		t.Fatalf("rows.data = %#v, want [[1 \"alice\"]]", rows.data)
	}
}

func TestQueryAPICountStarStillReturnsRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil {
		t.Fatal("Query() rows = nil, want value")
	}
	if len(rows.columns) != 1 || rows.columns[0] != "count" {
		t.Fatalf("rows.columns = %#v, want [count]", rows.columns)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != 1 {
		t.Fatalf("rows.data = %#v, want [[1]]", rows.data)
	}
}

func TestQueryAPINonSelectRejected(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tests := []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"UPDATE users SET name = 'bob'",
		"DELETE FROM users",
		"ALTER TABLE users ADD COLUMN age INT",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			rows, err := db.Query(sql)
			if !errors.Is(err, ErrQueryRequiresSelect) {
				t.Fatalf("Query(%q) error = %v, want ErrQueryRequiresSelect", sql, err)
			}
			if rows != nil {
				t.Fatalf("Query(%q) rows = %v, want nil", sql, rows)
			}
		})
	}
}

func TestQueryAPIPlaceholderArgsWhereClause(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT name FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 {
		t.Fatalf("rows = %#v, want one row with one column", rows)
	}
	if rows.data[0][0] != "alice" {
		t.Fatalf("rows.data = %#v, want [[\"alice\"]]", rows.data)
	}
}

func TestQueryAPIPlaceholderArgsBoolWhereClause(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, active BOOL, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, TRUE, 'alice')"); err != nil {
		t.Fatalf("Exec(insert 1) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (2, FALSE, 'bob')"); err != nil {
		t.Fatalf("Exec(insert 2) error = %v", err)
	}

	rows, err := db.Query("SELECT name FROM users WHERE active = ?", true)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 {
		t.Fatalf("rows = %#v, want one row with one column", rows)
	}
	if rows.data[0][0] != "alice" {
		t.Fatalf("rows.data = %#v, want [[\"alice\"]]", rows.data)
	}
}

func TestQueryAPILiteralAndBoundQueriesMatch(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	literalRows, err := db.Query("SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query(literal) error = %v", err)
	}
	boundRows, err := db.Query("SELECT name FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("Query(bound) error = %v", err)
	}

	if literalRows == nil || boundRows == nil {
		t.Fatalf("literalRows = %#v, boundRows = %#v, want values", literalRows, boundRows)
	}
	if len(literalRows.data) != len(boundRows.data) {
		t.Fatalf("len(literalRows.data) = %d, len(boundRows.data) = %d, want equal", len(literalRows.data), len(boundRows.data))
	}
	if len(literalRows.data) != 1 || len(literalRows.data[0]) != 1 || len(boundRows.data[0]) != 1 {
		t.Fatalf("literalRows.data = %#v, boundRows.data = %#v, want one matching row", literalRows.data, boundRows.data)
	}
	if literalRows.data[0][0] != boundRows.data[0][0] {
		t.Fatalf("literalRows.data = %#v, boundRows.data = %#v, want equal", literalRows.data, boundRows.data)
	}
}

func TestQueryAPIPlaceholderArgsRespectBooleanPrecedence(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id FROM users WHERE id = ? OR id = ? AND name = ? ORDER BY id", 1, 2, "bob")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 2 {
		t.Fatalf("rows.data = %#v, want two rows", rows.data)
	}
	if rows.data[0][0] != 1 || rows.data[1][0] != 2 {
		t.Fatalf("rows.data = %#v, want [[1] [2]]", rows.data)
	}
}

func TestQueryAPIPlaceholderArgsWithinFunctionOperand(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'ALICE')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id FROM users WHERE LOWER(name) = LOWER(?)", "BOB")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || rows.data[0][0] != 2 {
		t.Fatalf("rows.data = %#v, want [[2]]", rows.data)
	}
}

func TestQueryAPIPlaceholderArgsCountMismatchTooFew(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 1 WHERE 1 = ?")
	if err != nil {
		t.Fatalf("Query() error = %v, want nil top-level error", err)
	}
	if rows == nil || rows.err == nil {
		t.Fatalf("rows = %#v, want deferred bind error", rows)
	}
}

func TestQueryAPIPlaceholderArgsCountMismatchTooMany(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT name FROM users", 1)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil top-level error", err)
	}
	if rows == nil || rows.err == nil {
		t.Fatalf("rows = %#v, want deferred bind error", rows)
	}
}

func TestQueryAPICountStarWithPlaceholderWhereClause(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, active BOOL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, TRUE)",
		"INSERT INTO users VALUES (2, FALSE)",
		"INSERT INTO users VALUES (3, TRUE)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT COUNT(*) FROM users WHERE active = ?", true)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != 2 {
		t.Fatalf("rows.data = %#v, want [[2]]", rows.data)
	}
}

func TestQueryAPIRejectsPlaceholderOutsideValuePositionThroughPublicPath(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (? INT)"); err == nil {
		t.Fatal("Exec(CREATE TABLE t (? INT)) error = nil, want parse error")
	}
}
