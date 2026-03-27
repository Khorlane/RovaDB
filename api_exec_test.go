package rovadb

import (
	"errors"
	"testing"
)

func TestExecAPIAllowsWriteStatements(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	insertResult, err := db.Exec("INSERT INTO users VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if insertResult.RowsAffected() != 1 {
		t.Fatalf("Exec(insert).RowsAffected() = %d, want 1", insertResult.RowsAffected())
	}

	updateResult, err := db.Exec("UPDATE users SET name = 'bob' WHERE id = 1")
	if err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if updateResult.RowsAffected() != 1 {
		t.Fatalf("Exec(update).RowsAffected() = %d, want 1", updateResult.RowsAffected())
	}

	deleteResult, err := db.Exec("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	if deleteResult.RowsAffected() != 1 {
		t.Fatalf("Exec(delete).RowsAffected() = %d, want 1", deleteResult.RowsAffected())
	}
}

func TestExecAPIAllowsAlterTable(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	result, err := db.Exec("ALTER TABLE users ADD COLUMN age INT")
	if err != nil {
		t.Fatalf("Exec(alter) error = %v", err)
	}
	if result.RowsAffected() != 0 {
		t.Fatalf("Exec(alter).RowsAffected() = %d, want 0", result.RowsAffected())
	}
}

func TestExecAPIAcceptsTrailingSemicolon(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT);"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice');"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM users;")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want successful rowset", rows)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 2 || rows.data[0][0] != 1 || rows.data[0][1] != "alice" {
		t.Fatalf("rows.data = %#v, want [[1 \"alice\"]]", rows.data)
	}
}

func TestExecAPIRejectsSelect(t *testing.T) {
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

	tests := []string{
		"SELECT 1",
		"SELECT id FROM users",
		"SELECT COUNT(*) FROM users",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			result, err := db.Exec(sql)
			if !errors.Is(err, ErrExecDisallowsSelect) {
				t.Fatalf("Exec(%q) error = %v, want ErrExecDisallowsSelect", sql, err)
			}
			if result != (Result{}) {
				t.Fatalf("Exec(%q) result = %#v, want zero Result", sql, result)
			}
		})
	}
}

func TestExecAPIParserOnlyUtilityStatementsStillUnsupportedAtExecution(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}

	tests := []string{
		"CREATE UNIQUE INDEX idx_users_name ON users (name)",
		"DROP TABLE users",
		"DROP INDEX idx_users_name",
		"COMMIT",
		"ROLLBACK",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			if _, err := db.Exec(sql); err == nil {
				t.Fatalf("Exec(%q) error = nil, want unsupported query form", sql)
			}
		})
	}
}

func TestExecAPIWriteFlowStillValidatesViaQuery(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert 1) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (2, 'bob')"); err != nil {
		t.Fatalf("Exec(insert 2) error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET name = 'bobby' WHERE id = 2"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if _, err := db.Exec("DELETE FROM users WHERE id = 1"); err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil {
		t.Fatal("Query() rows = nil, want value")
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 2 {
		t.Fatalf("rows.data = %#v, want one row", rows.data)
	}
	if rows.data[0][0] != 2 || rows.data[0][1] != "bobby" {
		t.Fatalf("rows.data = %#v, want [[2 \"bobby\"]]", rows.data)
	}
}

func TestExecAPIPlaceholderArgsInsert(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (?, 'alice')", 1); err != nil {
		t.Fatalf("Exec(insert with placeholder) error = %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 2 {
		t.Fatalf("rows = %#v, want one materialized row", rows)
	}
	if rows.data[0][0] != 1 || rows.data[0][1] != "alice" {
		t.Fatalf("rows.data = %#v, want [[1 \"alice\"]]", rows.data)
	}
}

func TestExecAPIPlaceholderArgsInsertReal(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE metrics (id INT, score REAL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO metrics VALUES (?, ?)", 1, 3.14); err != nil {
		t.Fatalf("Exec(insert with placeholders) error = %v", err)
	}

	rows, err := db.Query("SELECT score FROM metrics WHERE id = 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 {
		t.Fatalf("rows = %#v, want one row with one column", rows)
	}
	if rows.data[0][0] != 3.14 {
		t.Fatalf("rows.data = %#v, want [[3.14]]", rows.data)
	}
}

func TestExecAPIPlaceholderArgsUpdate(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	result, err := db.Exec("UPDATE users SET name = ? WHERE id = ?", "sam", 1)
	if err != nil {
		t.Fatalf("Exec(update with placeholders) error = %v", err)
	}
	if result.RowsAffected() != 1 {
		t.Fatalf("Exec(update with placeholders).RowsAffected() = %d, want 1", result.RowsAffected())
	}

	rows, err := db.Query("SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 {
		t.Fatalf("rows = %#v, want one row with one column", rows)
	}
	if rows.data[0][0] != "sam" {
		t.Fatalf("rows.data = %#v, want [[\"sam\"]]", rows.data)
	}
}

func TestExecAPIPlaceholderArgsDelete(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'steve')",
		"INSERT INTO users VALUES (2, 'sam')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	result, err := db.Exec("DELETE FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("Exec(delete with placeholder) error = %v", err)
	}
	if result.RowsAffected() != 1 {
		t.Fatalf("Exec(delete with placeholder).RowsAffected() = %d, want 1", result.RowsAffected())
	}

	rows, err := db.Query("SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 2 {
		t.Fatalf("rows = %#v, want one remaining row", rows)
	}
	if rows.data[0][0] != 2 || rows.data[0][1] != "sam" {
		t.Fatalf("rows.data = %#v, want [[2 \"sam\"]]", rows.data)
	}
}

func TestExecAPIPlaceholderArgsCountMismatchInsertHasNoSideEffects(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	if _, err := db.Exec("INSERT INTO users VALUES (?, ?)", 1); err == nil {
		t.Fatal("Exec(insert with too few args) error = nil, want error")
	}

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query(count) error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != 0 {
		t.Fatalf("rows.data = %#v, want [[0]]", rows.data)
	}
}

func TestExecAPIPlaceholderArgsCountMismatchUpdateHasNoSideEffects(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	if _, err := db.Exec("UPDATE users SET name = ? WHERE id = ?", "sam"); err == nil {
		t.Fatal("Exec(update with too few args) error = nil, want error")
	}

	rows, err := db.Query("SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != "steve" {
		t.Fatalf("rows.data = %#v, want [[\"steve\"]]", rows.data)
	}
}

func TestExecAPIPlaceholderArgsCountMismatchDeleteHasNoSideEffects(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	if _, err := db.Exec("DELETE FROM users WHERE id = ?"); err == nil {
		t.Fatal("Exec(delete with too few args) error = nil, want error")
	}

	rows, err := db.Query("SELECT COUNT(*) FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query(count) error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != 1 {
		t.Fatalf("rows.data = %#v, want [[1]]", rows.data)
	}
}

func TestExecAPIPlaceholderArgsRejectsExtraArgsWhenNoPlaceholders(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)", 1); err == nil {
		t.Fatal("Exec(no placeholders with extra args) error = nil, want error")
	}
}

func TestExecAPIValueExprInsertAndUpdate(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, LOWER(?))", "STEVE"); err != nil {
		t.Fatalf("Exec(insert with value expr) error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET name = UPPER(name) WHERE id = ?", 1); err != nil {
		t.Fatalf("Exec(update with value expr) error = %v", err)
	}

	rows, err := db.Query("SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != "STEVE" {
		t.Fatalf("rows.data = %#v, want [[\"STEVE\"]]", rows.data)
	}
}

func TestExecAPIArithmeticValueExprInsertAndUpdate(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, score REAL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1 + 2, 1.5 + 2.5)"); err != nil {
		t.Fatalf("Exec(insert arithmetic) error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET score = score - 1.0 WHERE id = 3"); err != nil {
		t.Fatalf("Exec(update arithmetic) error = %v", err)
	}

	rows, err := db.Query("SELECT id, score FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 2 || rows.data[0][0] != 3 || rows.data[0][1] != 3.0 {
		t.Fatalf("rows.data = %#v, want [[3 3.0]]", rows.data)
	}
}
