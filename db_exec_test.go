package rovadb

import (
	"errors"
	"github.com/Khorlane/RovaDB/internal/parser"
	"strings"
	"testing"
)

func TestExecCreateTable(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	result, err := db.Exec("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("Exec() error = %v", err)
	}
	if result.RowsAffected() != 0 {
		t.Fatalf("Exec().RowsAffected() = %d, want 0", result.RowsAffected())
	}
	if db.tables == nil || db.tables["users"] == nil {
		t.Fatal("Exec() did not create users table")
	}
}

func TestExecCreateTableBoolSchema(t *testing.T) {
	path := testDBPath(t)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if _, err := db.Exec("CREATE TABLE flags (id INT, name TEXT, active BOOL)"); err != nil {
		t.Fatalf("Exec() error = %v", err)
	}
	if db.tables == nil || db.tables["flags"] == nil {
		t.Fatal("Exec() did not create flags table")
	}

	got := db.tables["flags"].Columns
	want := []struct {
		name string
		typ  string
	}{
		{name: "id", typ: "INT"},
		{name: "name", typ: "TEXT"},
		{name: "active", typ: "BOOL"},
	}
	if len(got) != len(want) {
		t.Fatalf("len(db.tables[\"flags\"].Columns) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Name != want[i].name || got[i].Type != want[i].typ {
			t.Fatalf("db.tables[\"flags\"].Columns[%d] = %#v, want name=%q type=%q", i, got[i], want[i].name, want[i].typ)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer reopened.Close()

	reloaded := reopened.tables["flags"].Columns
	if len(reloaded) != len(want) {
		t.Fatalf("len(reopened.tables[\"flags\"].Columns) = %d, want %d", len(reloaded), len(want))
	}
	for i := range want {
		if reloaded[i].Name != want[i].name || reloaded[i].Type != want[i].typ {
			t.Fatalf("reopened.tables[\"flags\"].Columns[%d] = %#v, want name=%q type=%q", i, reloaded[i], want[i].name, want[i].typ)
		}
	}
}

func TestExecCreateTablePersistsColumnNullabilityAndDefaults(t *testing.T) {
	path := testDBPath(t)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if _, err := db.Exec("CREATE TABLE flags (id INT, name TEXT DEFAULT 'ready', active BOOL NOT NULL, score REAL NOT NULL DEFAULT 1.25)"); err != nil {
		t.Fatalf("Exec() error = %v", err)
	}

	got := db.tables["flags"].Columns
	want := []parser.ColumnDef{
		{Name: "id", Type: parser.ColumnTypeInt},
		{Name: "name", Type: parser.ColumnTypeText, HasDefault: true, DefaultValue: parser.StringValue("ready")},
		{Name: "active", Type: parser.ColumnTypeBool, NotNull: true},
		{Name: "score", Type: parser.ColumnTypeReal, NotNull: true, HasDefault: true, DefaultValue: parser.RealValue(1.25)},
	}
	if len(got) != len(want) {
		t.Fatalf("len(db.tables[\"flags\"].Columns) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("db.tables[\"flags\"].Columns[%d] = %#v, want %#v", i, got[i], want[i])
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer reopened.Close()

	reloaded := reopened.tables["flags"].Columns
	if len(reloaded) != len(want) {
		t.Fatalf("len(reopened.tables[\"flags\"].Columns) = %d, want %d", len(reloaded), len(want))
	}
	for i := range want {
		if reloaded[i] != want[i] {
			t.Fatalf("reopened.tables[\"flags\"].Columns[%d] = %#v, want %#v", i, reloaded[i], want[i])
		}
	}
}

func TestExecCreateTableDuplicate(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("first Exec() error = %v", err)
	}

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err == nil {
		t.Fatal("second Exec() error = nil, want duplicate table error")
	}
}

func TestExecDeleteFromWhere(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert 1) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (2, 'bob')"); err != nil {
		t.Fatalf("Exec(insert 2) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (3, 'sam')"); err != nil {
		t.Fatalf("Exec(insert 3) error = %v", err)
	}

	result, err := db.Exec("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	if result.RowsAffected() != 1 {
		t.Fatalf("Exec(delete).RowsAffected() = %d, want 1", result.RowsAffected())
	}

	rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id1 int
	var name1 string
	if err := rows.Scan(&id1, &name1); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if id1 != 2 || name1 != "bob" {
		t.Fatalf("first row = (%d, %q), want (2, %q)", id1, name1, "bob")
	}

	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	var id2 int
	var name2 string
	if err := rows.Scan(&id2, &name2); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id2 != 3 || name2 != "sam" {
		t.Fatalf("second row = (%d, %q), want (3, %q)", id2, name2, "sam")
	}

	if rows.Next() {
		t.Fatal("Next() third = true, want false")
	}
}

func TestExecDeleteFromWhereOr(t *testing.T) {
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
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	result, err := db.Exec("DELETE FROM users WHERE id = 1 OR name = 'cara'")
	if err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	if result.RowsAffected() != 2 {
		t.Fatalf("Exec(delete).RowsAffected() = %d, want 2", result.RowsAffected())
	}

	rows, err := db.Query("SELECT name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsStringSequence(t, rows, "bob")
}

func TestExecDeleteFromWhereRealComparison(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE measurements (id INT, x REAL, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO measurements VALUES (1, -2.5, 'neg')",
		"INSERT INTO measurements VALUES (2, 3.14, 'pi')",
		"INSERT INTO measurements VALUES (3, 10.25, 'hi')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	result, err := db.Exec("DELETE FROM measurements WHERE x >= 10.25")
	if err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	if result.RowsAffected() != 1 {
		t.Fatalf("Exec(delete).RowsAffected() = %d, want 1", result.RowsAffected())
	}

	rows, err := db.Query("SELECT name FROM measurements ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsStringSequence(t, rows, "neg", "pi")
}

func TestExecInsertInto(t *testing.T) {
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
	rows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var id int
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if id != 1 || name != "steve" {
		t.Fatalf("row = (%d, %q), want (1, %q)", id, name, "steve")
	}
}

func TestExecInsertIntoWithColumnListReordered(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users (name, id) VALUES ('steve', 1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var id int
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if id != 1 || name != "steve" {
		t.Fatalf("row = (%d, %q), want (1, %q)", id, name, "steve")
	}
}

func TestExecInsertIntoWrongType(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES ('steve', 'bob')"); err == nil {
		t.Fatal("Exec(insert) error = nil, want type error")
	}
}

func TestExecInsertIntoBoolWrongTypeRejected(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE flags (flag BOOL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	tests := []string{
		"INSERT INTO flags VALUES (1)",
		"INSERT INTO flags VALUES (0)",
		"INSERT INTO flags VALUES ('true')",
		"INSERT INTO flags VALUES ('false')",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := db.Exec(sql)
			var dbErr *DBError
			if !errors.As(err, &dbErr) || dbErr.Kind != ErrExec {
				t.Fatalf("Exec(%q) error = %v, want exec-type mismatch error", sql, err)
			}
		})
	}
}

func TestExecInsertIntoRealWrongTypeRejected(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE measurements (x REAL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	tests := []string{
		"INSERT INTO measurements VALUES (1)",
		"INSERT INTO measurements VALUES ('1.25')",
		"INSERT INTO measurements VALUES (TRUE)",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := db.Exec(sql)
			var dbErr *DBError
			if !errors.As(err, &dbErr) || dbErr.Kind != ErrExec {
				t.Fatalf("Exec(%q) error = %v, want exec-type mismatch error", sql, err)
			}
		})
	}
}

func TestExecMutationPathsPreserveIndexedVisibilityAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT, note TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	for id := 1; id <= 18; id++ {
		name := "filler"
		if id == 1 {
			name = "alice"
		}
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", id, name, strings.Repeat("seed-", 90)); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	if _, err := db.Exec("UPDATE users SET note = ? WHERE id = 1", strings.Repeat("relocate-", 220)); err != nil {
		t.Fatalf("Exec(relocating update) error = %v", err)
	}

	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query(indexed read after update) error = %v", err)
	}
	assertRowsIntSequence(t, rows, 1)
	rows.Close()

	if _, err := db.Exec("DELETE FROM users WHERE id = 1"); err != nil {
		t.Fatalf("Exec(delete relocated row) error = %v", err)
	}

	rows, err = db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query(indexed read after delete) error = %v", err)
	}
	assertRowsIntSequence(t, rows)
	rows.Close()

	if _, err := db.Exec("INSERT INTO users VALUES (101, 'alice', ?)", strings.Repeat("fresh-", 80)); err != nil {
		t.Fatalf("Exec(reinsert reused key) error = %v", err)
	}

	rows, err = db.Query("SELECT id FROM users WHERE name = 'alice' ORDER BY id")
	if err != nil {
		t.Fatalf("Query(indexed read after key reuse) error = %v", err)
	}
	assertRowsIntSequence(t, rows, 101)
	rows.Close()

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err = db.Query("SELECT id FROM users WHERE name = 'alice' ORDER BY id")
	if err != nil {
		t.Fatalf("Query(reopen reused key) error = %v", err)
	}
	assertRowsIntSequence(t, rows, 101)
	rows.Close()
}

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

func TestExecUpdateWhere(t *testing.T) {
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
	if _, err := db.Exec("INSERT INTO users VALUES (2, 'sam')"); err != nil {
		t.Fatalf("Exec(insert 2) error = %v", err)
	}

	result, err := db.Exec("UPDATE users SET name = 'robert' WHERE id = 1")
	if err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if result.RowsAffected() != 1 {
		t.Fatalf("Exec(update).RowsAffected() = %d, want 1", result.RowsAffected())
	}

	rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id1 int
	var name1 string
	if err := rows.Scan(&id1, &name1); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if id1 != 1 || name1 != "robert" {
		t.Fatalf("first row = (%d, %q), want (1, %q)", id1, name1, "robert")
	}

	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	var id2 int
	var name2 string
	if err := rows.Scan(&id2, &name2); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id2 != 2 || name2 != "sam" {
		t.Fatalf("second row = (%d, %q), want (2, %q)", id2, name2, "sam")
	}
}

func TestExecUpdateWrongType(t *testing.T) {
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
	if _, err := db.Exec("UPDATE users SET id = 'oops' WHERE name = 'alice'"); err == nil {
		t.Fatal("Exec(update) error = nil, want type error")
	}
}

func TestExecUpdateBoolWrongTypeRejected(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE flags (id INT, flag BOOL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO flags VALUES (1, NULL)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	tests := []string{
		"UPDATE flags SET flag = 1 WHERE id = 1",
		"UPDATE flags SET flag = 0 WHERE id = 1",
		"UPDATE flags SET flag = 'true' WHERE id = 1",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := db.Exec(sql)
			var dbErr *DBError
			if !errors.As(err, &dbErr) || dbErr.Kind != ErrExec {
				t.Fatalf("Exec(%q) error = %v, want exec-type mismatch error", sql, err)
			}
		})
	}
}

func TestExecUpdateRealWrongTypeRejected(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE measurements (id INT, x REAL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO measurements VALUES (1, NULL)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	tests := []string{
		"UPDATE measurements SET x = 2 WHERE id = 1",
		"UPDATE measurements SET x = '2.5' WHERE id = 1",
		"UPDATE measurements SET x = FALSE WHERE id = 1",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := db.Exec(sql)
			var dbErr *DBError
			if !errors.As(err, &dbErr) || dbErr.Kind != ErrExec {
				t.Fatalf("Exec(%q) error = %v, want exec-type mismatch error", sql, err)
			}
		})
	}
}

func TestExecUpdateWhereOr(t *testing.T) {
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
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	result, err := db.Exec("UPDATE users SET name = 'updated' WHERE id = 1 OR id = 3")
	if err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if result.RowsAffected() != 2 {
		t.Fatalf("Exec(update).RowsAffected() = %d, want 2", result.RowsAffected())
	}

	rows, err := db.Query("SELECT name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsStringSequence(t, rows, "updated", "bob", "updated")
}

func TestExecUpdateWhereRealComparison(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE measurements (id INT, x REAL, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO measurements VALUES (1, -2.5, 'neg')",
		"INSERT INTO measurements VALUES (2, 3.14, 'pi')",
		"INSERT INTO measurements VALUES (3, 10.25, 'hi')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	result, err := db.Exec("UPDATE measurements SET name = 'small' WHERE x < 3.0")
	if err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if result.RowsAffected() != 1 {
		t.Fatalf("Exec(update).RowsAffected() = %d, want 1", result.RowsAffected())
	}

	rows, err := db.Query("SELECT name FROM measurements ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsStringSequence(t, rows, "small", "pi", "hi")
}
