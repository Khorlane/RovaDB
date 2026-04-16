package rovadb

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/temporal"
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
	var id1 int32
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
	var id2 int32
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
	var id int32
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
	var id int32
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if id != 1 || name != "steve" {
		t.Fatalf("row = (%d, %q), want (1, %q)", id, name, "steve")
	}
}

func TestExecInsertColumnOmissionUsesDefaultsAndNullability(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT NOT NULL, name TEXT DEFAULT 'ready', active BOOL NOT NULL DEFAULT TRUE, score REAL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users (id) VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT id, name, active, score FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var id int32
	var name string
	var active bool
	var score any
	if err := rows.Scan(&id, &name, &active, &score); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if id != 1 || name != "ready" || active != true || score != nil {
		t.Fatalf("row = (%d, %q, %v, %#v), want (1, %q, true, nil)", id, name, active, score, "ready")
	}
}

func TestExecInsertRejectsNotNullViolations(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT NOT NULL, active BOOL NOT NULL DEFAULT TRUE)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	tests := []struct {
		sql  string
		want string
	}{
		{sql: "INSERT INTO users VALUES (NULL, TRUE)", want: "execution: NOT NULL constraint failed: users.id"},
		{sql: "INSERT INTO users (id) VALUES (1)", want: ""},
		{sql: "INSERT INTO users VALUES (2, NULL)", want: "execution: NOT NULL constraint failed: users.active"},
		{sql: "INSERT INTO users (active) VALUES (TRUE)", want: "execution: NOT NULL constraint failed: users.id"},
	}

	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			_, err := db.Exec(tc.sql)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Exec(%q) error = %v", tc.sql, err)
				}
				return
			}
			if err == nil || err.Error() != tc.want {
				t.Fatalf("Exec(%q) error = %v, want %q", tc.sql, err, tc.want)
			}
		})
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

func TestExecInsertTypedIntegerColumnsRequireExactGoTypes(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE numbers (small_col SMALLINT, int_col INT, big_col BIGINT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	if _, err := db.Exec("INSERT INTO numbers VALUES (?, ?, ?, ?)", int16(11), int32(22), int64(33), "ok"); err != nil {
		t.Fatalf("Exec(insert exact placeholder types) error = %v", err)
	}

	tests := []struct {
		name     string
		args     []any
		wantBind bool
	}{
		{name: "smallint rejects int32", args: []any{int32(11), int32(22), int64(33), "bad-small"}},
		{name: "smallint rejects int64", args: []any{int64(11), int32(22), int64(33), "bad-small"}},
		{name: "smallint rejects int", args: []any{int(11), int32(22), int64(33), "bad-small"}, wantBind: true},
		{name: "int rejects int16", args: []any{int16(11), int16(22), int64(33), "bad-int"}},
		{name: "int rejects int64", args: []any{int16(11), int64(22), int64(33), "bad-int"}},
		{name: "int rejects int", args: []any{int16(11), int(22), int64(33), "bad-int"}, wantBind: true},
		{name: "bigint rejects int16", args: []any{int16(11), int32(22), int16(33), "bad-big"}},
		{name: "bigint rejects int32", args: []any{int16(11), int32(22), int32(33), "bad-big"}},
		{name: "bigint rejects int", args: []any{int16(11), int32(22), int(33), "bad-big"}, wantBind: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec("INSERT INTO numbers VALUES (?, ?, ?, ?)", tc.args...)
			if tc.wantBind {
				if err == nil || !strings.Contains(err.Error(), "unsupported placeholder argument type") {
					t.Fatalf("Exec(insert) error = %v, want bind-time unsupported placeholder type error", err)
				}
				return
			}
			var dbErr *DBError
			if !errors.As(err, &dbErr) || dbErr.Kind != ErrExec {
				t.Fatalf("Exec(insert) error = %v, want exec-type mismatch error", err)
			}
		})
	}

	rows, err := db.Query("SELECT name FROM numbers ORDER BY name")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, "ok")
}

func TestExecUpdateTypedIntegerColumnsRequireExactGoTypes(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE numbers (id INT, small_col SMALLINT, int_col INT, big_col BIGINT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO numbers VALUES (1, 1, 2, 3)"); err != nil {
		t.Fatalf("Exec(seed) error = %v", err)
	}

	if _, err := db.Exec(
		"UPDATE numbers SET small_col = ?, int_col = ?, big_col = ? WHERE id = 1",
		int16(11), int32(22), int64(33),
	); err != nil {
		t.Fatalf("Exec(update exact placeholder types) error = %v", err)
	}

	tests := []struct {
		name     string
		args     []any
		wantBind bool
	}{
		{name: "smallint rejects int32", args: []any{int32(11), int32(22), int64(33), int32(1)}},
		{name: "smallint rejects int", args: []any{int(11), int32(22), int64(33), int32(1)}, wantBind: true},
		{name: "int rejects int16", args: []any{int16(11), int16(22), int64(33), int32(1)}},
		{name: "int rejects int64", args: []any{int16(11), int64(22), int64(33), int32(1)}},
		{name: "bigint rejects int32", args: []any{int16(11), int32(22), int32(33), int32(1)}},
		{name: "bigint rejects int", args: []any{int16(11), int32(22), int(33), int32(1)}, wantBind: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec("UPDATE numbers SET small_col = ?, int_col = ?, big_col = ? WHERE id = ?", tc.args...)
			if tc.wantBind {
				if err == nil || !strings.Contains(err.Error(), "unsupported placeholder argument type") {
					t.Fatalf("Exec(update) error = %v, want bind-time unsupported placeholder type error", err)
				}
				return
			}
			var dbErr *DBError
			if !errors.As(err, &dbErr) || dbErr.Kind != ErrExec {
				t.Fatalf("Exec(update) error = %v, want exec-type mismatch error", err)
			}
		})
	}

	rows, err := db.Query("SELECT small_col, int_col, big_col FROM numbers WHERE id = 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var small int16
	var regular int32
	var big int64
	if err := rows.Scan(&small, &regular, &big); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if small != 11 || regular != 22 || big != 33 {
		t.Fatalf("row = (%d, %d, %d), want (11, 22, 33)", small, regular, big)
	}
}

func TestExecEngineOwnedIntegerLiteralsAndDefaultsRemainValidForTypedIntegerWrites(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE numbers (id INT, small_col SMALLINT DEFAULT 7, int_col INT DEFAULT 8, big_col BIGINT DEFAULT 9)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO numbers (id) VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert defaults) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO numbers VALUES (2, 12, 34, 56)"); err != nil {
		t.Fatalf("Exec(insert SQL literals) error = %v", err)
	}
	if _, err := db.Exec("UPDATE numbers SET small_col = 13, int_col = 35, big_col = 57 WHERE id = 2"); err != nil {
		t.Fatalf("Exec(update SQL literals) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(seed users) error = %v", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD COLUMN age SMALLINT NOT NULL DEFAULT 5"); err != nil {
		t.Fatalf("Exec(alter add column) error = %v", err)
	}

	rows, err := db.Query("SELECT small_col, int_col, big_col FROM numbers ORDER BY id")
	if err != nil {
		t.Fatalf("Query(numbers) error = %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("numbers first Next() = false, want true")
	}
	var small1 int16
	var int1 int32
	var big1 int64
	if err := rows.Scan(&small1, &int1, &big1); err != nil {
		t.Fatalf("Scan(default row) error = %v", err)
	}
	if small1 != 7 || int1 != 8 || big1 != 9 {
		t.Fatalf("default row = (%d, %d, %d), want (7, 8, 9)", small1, int1, big1)
	}
	if !rows.Next() {
		t.Fatal("numbers second Next() = false, want true")
	}
	var small2 int16
	var int2 int32
	var big2 int64
	if err := rows.Scan(&small2, &int2, &big2); err != nil {
		t.Fatalf("Scan(literal row) error = %v", err)
	}
	if small2 != 13 || int2 != 35 || big2 != 57 {
		t.Fatalf("literal row = (%d, %d, %d), want (13, 35, 57)", small2, int2, big2)
	}

	userRows, err := db.Query("SELECT age FROM users")
	if err != nil {
		t.Fatalf("Query(users) error = %v", err)
	}
	defer userRows.Close()
	assertRowsIntSequence(t, userRows, 5)
}

func TestExecTemporalWritesAcceptMatchingFamiliesAndCanonicalPlaceholderStrings(t *testing.T) {
	db, err := CreateWithOptions(freshDBPath(t), OpenOptions{DefaultTimezone: "UTC"})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE events (id INT, event_date DATE, event_time TIME, recorded_at TIMESTAMP)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(
		"INSERT INTO events VALUES (?, ?, ?, ?)",
		int32(1),
		"2026-04-10",
		"13:45:21",
		"2026-04-10 13:45:21",
	); err != nil {
		t.Fatalf("Exec(insert matching temporal placeholders) error = %v", err)
	}
	if err := db.loadRowsIntoTables(db.tables, "events"); err != nil {
		t.Fatalf("loadRowsIntoTables(after insert) error = %v", err)
	}

	row := db.tables["events"].Rows[0]
	wantInsert := []parser.Value{
		parser.IntValue(1),
		parser.DateValue(20553),
		parser.TimeValue(49521),
		parser.TimestampValue(1775828721000, 0),
	}
	if len(row) != len(wantInsert) || row[0] != wantInsert[0] || row[1] != wantInsert[1] || row[2] != wantInsert[2] || row[3] != wantInsert[3] {
		t.Fatalf("inserted row = %#v, want %#v", row, wantInsert)
	}

	if _, err := db.Exec(
		"UPDATE events SET event_date = ?, event_time = ?, recorded_at = ? WHERE id = ?",
		"2026-04-11",
		"00:00:01",
		"2026-04-11 00:00:01",
		int32(1),
	); err != nil {
		t.Fatalf("Exec(update matching temporal placeholders) error = %v", err)
	}
	if err := db.loadRowsIntoTables(db.tables, "events"); err != nil {
		t.Fatalf("loadRowsIntoTables(after update) error = %v", err)
	}

	updated := db.tables["events"].Rows[0]
	wantUpdate := []parser.Value{
		parser.IntValue(1),
		parser.DateValue(20554),
		parser.TimeValue(1),
		parser.TimestampValue(1775865601000, 0),
	}
	if len(updated) != len(wantUpdate) || updated[0] != wantUpdate[0] || updated[1] != wantUpdate[1] || updated[2] != wantUpdate[2] || updated[3] != wantUpdate[3] {
		t.Fatalf("updated row = %#v, want %#v", updated, wantUpdate)
	}
}

func TestExecTemporalWritesRejectMismatchedAndMalformedPlaceholderValues(t *testing.T) {
	db, err := CreateWithOptions(freshDBPath(t), OpenOptions{DefaultTimezone: "UTC"})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE events (id INT, event_date DATE, event_time TIME, recorded_at TIMESTAMP)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(
		"INSERT INTO events VALUES (?, ?, ?, ?)",
		int32(1),
		"2026-04-10",
		"13:45:21",
		"2026-04-10 13:45:21",
	); err != nil {
		t.Fatalf("Exec(seed) error = %v", err)
	}
	if err := db.loadRowsIntoTables(db.tables, "events"); err != nil {
		t.Fatalf("loadRowsIntoTables(seed) error = %v", err)
	}

	initialRows := cloneRows(db.tables["events"].Rows)

	tests := []struct {
		name     string
		query    string
		args     []any
		wantBind bool
	}{
		{
			name:  "insert date placeholder into time column",
			query: "INSERT INTO events VALUES (?, ?, ?, ?)",
			args:  []any{int32(2), "2026-04-10", "2026-04-10", "2026-04-10 13:45:21"},
		},
		{
			name:  "insert time placeholder into timestamp column",
			query: "INSERT INTO events VALUES (?, ?, ?, ?)",
			args:  []any{int32(2), "2026-04-10", "13:45:21", "13:45:21"},
		},
		{
			name:  "insert ordinary text into date column",
			query: "INSERT INTO events VALUES (?, ?, ?, ?)",
			args:  []any{int32(2), "not-a-date", "13:45:21", "2026-04-10 13:45:21"},
		},
		{
			name:  "insert int into date column",
			query: "INSERT INTO events VALUES (?, ?, ?, ?)",
			args:  []any{int32(2), int32(7), "13:45:21", "2026-04-10 13:45:21"},
		},
		{
			name:  "update timestamp column with date placeholder",
			query: "UPDATE events SET recorded_at = ? WHERE id = ?",
			args:  []any{"2026-04-10", int32(1)},
		},
		{
			name:  "update time column with bool placeholder",
			query: "UPDATE events SET event_time = ? WHERE id = ?",
			args:  []any{true, int32(1)},
		},
		{
			name:     "insert malformed temporal placeholder string",
			query:    "INSERT INTO events VALUES (?, ?, ?, ?)",
			args:     []any{int32(2), "2026/04/10", "13:45:21", "2026-04-10 13:45:21"},
			wantBind: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec(tc.query, tc.args...)
			if tc.wantBind {
				if err == nil || !strings.Contains(err.Error(), "invalid temporal literal") {
					t.Fatalf("Exec() error = %v, want bind-time invalid temporal literal", err)
				}
			} else {
				var dbErr *DBError
				if !errors.As(err, &dbErr) || dbErr.Kind != ErrExec {
					t.Fatalf("Exec() error = %v, want exec-type mismatch error", err)
				}
			}
			if got := db.tables["events"].Rows; len(got) != len(initialRows) {
				t.Fatalf("rows len = %d, want %d after rejected write", len(got), len(initialRows))
			} else if len(got) > 0 && (len(got[0]) != len(initialRows[0]) || got[0][0] != initialRows[0][0] || got[0][1] != initialRows[0][1] || got[0][2] != initialRows[0][2] || got[0][3] != initialRows[0][3]) {
				t.Fatalf("rows[0] = %#v, want %#v after rejected write", got[0], initialRows[0])
			}
		})
	}
}

func TestExecTimestampWritesNormalizeThroughDatabaseTimezoneContext(t *testing.T) {
	db, err := CreateWithOptions(freshDBPath(t), OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("OpenWithOptions() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE events (id INT, recorded_at TIMESTAMP)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO events VALUES (1, '2026-04-10 13:45:21')"); err != nil {
		t.Fatalf("Exec(insert literal) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO events VALUES (?, ?)", int32(2), "2026-04-10 13:45:21"); err != nil {
		t.Fatalf("Exec(insert placeholder) error = %v", err)
	}
	if err := db.loadRowsIntoTables(db.tables, "events"); err != nil {
		t.Fatalf("loadRowsIntoTables() error = %v", err)
	}

	location, err := temporal.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation() error = %v", err)
	}
	wantMillis := time.Date(2026, time.April, 10, 13, 45, 21, 0, location).UnixMilli()
	wantTimestamp := parser.TimestampValue(wantMillis, 0)

	rows := db.tables["events"].Rows
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(rows))
	}
	for i, row := range rows {
		if len(row) != 2 {
			t.Fatalf("row %d width = %d, want 2", i, len(row))
		}
		if row[1] != wantTimestamp {
			t.Fatalf("row %d recorded_at = %#v, want %#v", i, row[1], wantTimestamp)
		}
	}
}

func TestExecTimestampWritesFailWithoutDatabaseTimezoneContext(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE events (id INT, recorded_at TIMESTAMP)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	_, err = db.Exec("INSERT INTO events VALUES (1, '2026-04-10 13:45:21')")
	assertErrorContainsAll(t, err, "unresolved TIMESTAMP", "configured database timezone")
}

func TestExecTypedIntegerWritesRejectOutOfRangeSQLLiteralsByTargetWidth(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE numbers (small_col SMALLINT, int_col INT, big_col BIGINT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO numbers VALUES (1, 2, 3)"); err != nil {
		t.Fatalf("Exec(seed) error = %v", err)
	}

	tests := []string{
		"INSERT INTO numbers (small_col) VALUES (40000)",
		"UPDATE numbers SET small_col = 40000 WHERE big_col = 3",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := db.Exec(sql)
			var dbErr *DBError
			if !errors.As(err, &dbErr) || dbErr.Kind != ErrExec {
				t.Fatalf("Exec(%q) error = %v, want exec type mismatch", sql, err)
			}
		})
	}

	if _, err := db.Exec("INSERT INTO numbers (big_col) VALUES (2147483647)"); err != nil {
		t.Fatalf("Exec(bigint fitting literal) error = %v", err)
	}
}

func TestExecAlterTableTypedIntegerDefaultsResolveByTargetWidth(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(seed users) error = %v", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD COLUMN age SMALLINT NOT NULL DEFAULT 5"); err != nil {
		t.Fatalf("Exec(add smallint default) error = %v", err)
	}

	rows, err := db.Query("SELECT age FROM users")
	if err != nil {
		t.Fatalf("Query(users) error = %v", err)
	}
	defer rows.Close()
	assertRowsIntSequence(t, rows, 5)

	if _, err := db.Exec("CREATE TABLE users_bad (id INT)"); err != nil {
		t.Fatalf("Exec(create users_bad) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users_bad VALUES (1)"); err != nil {
		t.Fatalf("Exec(seed users_bad) error = %v", err)
	}
	_, err = db.Exec("ALTER TABLE users_bad ADD COLUMN age SMALLINT NOT NULL DEFAULT 40000")
	var dbErr *DBError
	if !errors.As(err, &dbErr) || dbErr.Kind != ErrExec {
		t.Fatalf("Exec(add overflowing smallint default) error = %v, want exec type mismatch", err)
	}
}

func TestTypedIntegerScanRequiresExactDestinationTypes(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE numbers (id INT, small_col SMALLINT, int_col INT, big_col BIGINT, active BOOL, score REAL, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO numbers VALUES (?, ?, ?, ?, ?, ?, ?)", int32(1), int16(11), int32(22), int64(33), true, 4.5, "ok"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT small_col, int_col, big_col FROM numbers WHERE id = 1")
	if err != nil {
		t.Fatalf("Query(rows) error = %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("rows.Next() = false, want true")
	}
	var small int16
	var regular int32
	var big int64
	if err := rows.Scan(&small, &regular, &big); err != nil {
		t.Fatalf("rows.Scan(exact types) error = %v", err)
	}
	if small != 11 || regular != 22 || big != 33 {
		t.Fatalf("rows.Scan(exact types) = (%d, %d, %d), want (11, 22, 33)", small, regular, big)
	}

	rows, err = db.Query("SELECT small_col, int_col, big_col FROM numbers WHERE id = 1")
	if err != nil {
		t.Fatalf("Query(rows mismatch) error = %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("rows.Next() mismatch = false, want true")
	}
	for _, tc := range []struct {
		name string
		dest []any
	}{
		{name: "smallint rejects int32", dest: []any{new(int32), new(int32), new(int64)}},
		{name: "smallint rejects int64", dest: []any{new(int64), new(int32), new(int64)}},
		{name: "smallint rejects int", dest: []any{new(int), new(int32), new(int64)}},
		{name: "int rejects int16", dest: []any{new(int16), new(int16), new(int64)}},
		{name: "int rejects int64", dest: []any{new(int16), new(int64), new(int64)}},
		{name: "int rejects int", dest: []any{new(int16), new(int), new(int64)}},
		{name: "bigint rejects int16", dest: []any{new(int16), new(int32), new(int16)}},
		{name: "bigint rejects int32", dest: []any{new(int16), new(int32), new(int32)}},
		{name: "bigint rejects int", dest: []any{new(int16), new(int32), new(int)}},
	} {
		t.Run("Rows.Scan "+tc.name, func(t *testing.T) {
			if err := rows.Scan(tc.dest...); !errors.Is(err, ErrUnsupportedScanType) {
				t.Fatalf("rows.Scan() error = %v, want ErrUnsupportedScanType", err)
			}
		})
	}

	row := db.QueryRow("SELECT small_col, int_col, big_col FROM numbers WHERE id = 1")
	small, regular, big = 0, 0, 0
	if err := row.Scan(&small, &regular, &big); err != nil {
		t.Fatalf("row.Scan(exact types) error = %v", err)
	}
	if small != 11 || regular != 22 || big != 33 {
		t.Fatalf("row.Scan(exact types) = (%d, %d, %d), want (11, 22, 33)", small, regular, big)
	}

	rows, err = db.Query("SELECT small_col + 1, int_col + 1, big_col + 1 FROM numbers WHERE id = 1")
	if err != nil {
		t.Fatalf("Query(typed arithmetic rows) error = %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("rows.Next() typed arithmetic = false, want true")
	}
	small, regular, big = 0, 0, 0
	if err := rows.Scan(&small, &regular, &big); err != nil {
		t.Fatalf("rows.Scan(typed arithmetic exact types) error = %v", err)
	}
	if small != 12 || regular != 23 || big != 34 {
		t.Fatalf("rows.Scan(typed arithmetic exact types) = (%d, %d, %d), want (12, 23, 34)", small, regular, big)
	}

	if err := db.QueryRow("SELECT small_col + 1, int_col + 1, big_col + 1 FROM numbers WHERE id = 1").Scan(&small, &regular, &big); err != nil {
		t.Fatalf("row.Scan(typed arithmetic exact types) error = %v", err)
	}
	if small != 12 || regular != 23 || big != 34 {
		t.Fatalf("row.Scan(typed arithmetic exact types) = (%d, %d, %d), want (12, 23, 34)", small, regular, big)
	}

	if err := db.QueryRow("SELECT small_col + 1, int_col + 1, big_col + 1 FROM numbers WHERE id = 1").Scan(new(int32), new(int32), new(int64)); !errors.Is(err, ErrUnsupportedScanType) {
		t.Fatalf("row.Scan(typed arithmetic width mismatch) error = %v, want ErrUnsupportedScanType", err)
	}

	for _, tc := range []struct {
		name string
		dest []any
	}{
		{name: "smallint rejects int32", dest: []any{new(int32), new(int32), new(int64)}},
		{name: "smallint rejects int64", dest: []any{new(int64), new(int32), new(int64)}},
		{name: "smallint rejects int", dest: []any{new(int), new(int32), new(int64)}},
		{name: "int rejects int16", dest: []any{new(int16), new(int16), new(int64)}},
		{name: "int rejects int64", dest: []any{new(int16), new(int64), new(int64)}},
		{name: "int rejects int", dest: []any{new(int16), new(int), new(int64)}},
		{name: "bigint rejects int16", dest: []any{new(int16), new(int32), new(int16)}},
		{name: "bigint rejects int32", dest: []any{new(int16), new(int32), new(int32)}},
		{name: "bigint rejects int", dest: []any{new(int16), new(int32), new(int)}},
	} {
		t.Run("Row.Scan "+tc.name, func(t *testing.T) {
			if err := db.QueryRow("SELECT small_col, int_col, big_col FROM numbers WHERE id = 1").Scan(tc.dest...); !errors.Is(err, ErrUnsupportedScanType) {
				t.Fatalf("QueryRow().Scan() error = %v, want ErrUnsupportedScanType", err)
			}
		})
	}

	var active bool
	var score float64
	var name string
	if err := db.QueryRow("SELECT active, score, name FROM numbers WHERE id = 1").Scan(&active, &score, &name); err != nil {
		t.Fatalf("QueryRow(non-integers) error = %v", err)
	}
	if !active || score != 4.5 || name != "ok" {
		t.Fatalf("QueryRow(non-integers) = (%v, %v, %q), want (true, 4.5, %q)", active, score, name, "ok")
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
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", int32(id), name, strings.Repeat("seed-", 90)); err != nil {
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

func TestExecAlterTableAddColumnAppliesExistingRowDefaultsAndNullability(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT)",
		"INSERT INTO users VALUES (1)",
		"INSERT INTO users VALUES (2)",
		"ALTER TABLE users ADD COLUMN nickname TEXT",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id, nickname FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query(nullable no default) error = %v", err)
	}
	if got := rows.data; len(got) != 2 || got[0][0] != int32(1) || got[0][1] != nil || got[1][0] != int32(2) || got[1][1] != nil {
		t.Fatalf("nullable no default rows = %#v, want [[1 nil] [2 nil]]", got)
	}
	rows.Close()

	if _, err := db.Exec("ALTER TABLE users ADD COLUMN score REAL NOT NULL DEFAULT 1.5"); err != nil {
		t.Fatalf("Exec(add score) error = %v", err)
	}
	rows, err = db.Query("SELECT id, score FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query(not null default) error = %v", err)
	}
	if got := rows.data; len(got) != 2 || got[0][0] != int32(1) || got[0][1] != 1.5 || got[1][0] != int32(2) || got[1][1] != 1.5 {
		t.Fatalf("not null default rows = %#v, want [[1 1.5] [2 1.5]]", got)
	}
	rows.Close()

	if _, err := db.Exec("INSERT INTO users (id) VALUES (3)"); err != nil {
		t.Fatalf("Exec(insert after alter) error = %v", err)
	}
	rows, err = db.Query("SELECT id, nickname, score FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query(post alter insert) error = %v", err)
	}
	if got := rows.data; len(got) != 3 || got[2][0] != int32(3) || got[2][1] != nil || got[2][2] != 1.5 {
		t.Fatalf("post alter insert rows = %#v, want row 3 to use NULL/default fill", got)
	}
	rows.Close()
}

func TestExecAlterTableAddColumnNotNullWithoutDefaultRequiresEmptyTable(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE empty_users (id INT)"); err != nil {
		t.Fatalf("Exec(create empty_users) error = %v", err)
	}
	if _, err := db.Exec("ALTER TABLE empty_users ADD COLUMN active BOOL NOT NULL"); err != nil {
		t.Fatalf("Exec(alter empty table) error = %v", err)
	}

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert users) error = %v", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD COLUMN active BOOL NOT NULL"); err == nil || err.Error() != "execution: cannot add NOT NULL column without DEFAULT to non-empty table" {
		t.Fatalf("Exec(alter populated table) error = %v, want non-empty table failure", err)
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
	if len(rows.data) != 1 || len(rows.data[0]) != 2 || rows.data[0][0] != int32(1) || rows.data[0][1] != "alice" {
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
	if rows.data[0][0] != int32(2) || rows.data[0][1] != "bobby" {
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
	if _, err := db.Exec("INSERT INTO users VALUES (?, 'alice')", int32(1)); err != nil {
		t.Fatalf("Exec(insert with placeholder) error = %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 2 {
		t.Fatalf("rows = %#v, want one materialized row", rows)
	}
	if rows.data[0][0] != int32(1) || rows.data[0][1] != "alice" {
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
	if _, err := db.Exec("INSERT INTO metrics VALUES (?, ?)", int32(1), 3.14); err != nil {
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

	result, err := db.Exec("UPDATE users SET name = ? WHERE id = ?", "sam", int32(1))
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

	result, err := db.Exec("DELETE FROM users WHERE id = ?", int32(1))
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
	if rows.data[0][0] != int32(2) || rows.data[0][1] != "sam" {
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

	if _, err := db.Exec("INSERT INTO users VALUES (?, ?)", int32(1)); err == nil {
		t.Fatal("Exec(insert with too few args) error = nil, want error")
	}

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query(count) error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != int64(0) {
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
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != int64(1) {
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
	if _, err := db.Exec("UPDATE users SET name = UPPER(name) WHERE id = ?", int32(1)); err != nil {
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

func TestExecAPIPlaceholderRejectsUnsupportedIntegerTypesAtBindTime(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	tests := []struct {
		name string
		arg  any
	}{
		{name: "int", arg: int(1)},
		{name: "int8", arg: int8(1)},
		{name: "uint", arg: uint(1)},
		{name: "uint32", arg: uint32(1)},
		{name: "uint64", arg: uint64(1)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec("INSERT INTO users VALUES (?, ?)", tc.arg, "alice")
			if err == nil || !strings.Contains(err.Error(), "unsupported placeholder argument type") {
				t.Fatalf("Exec(insert) error = %v, want unsupported placeholder argument type", err)
			}
		})
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
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 2 || rows.data[0][0] != int32(3) || rows.data[0][1] != 3.0 {
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
	var id1 int32
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
	var id2 int32
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

func TestExecUpdateNotNullEnforcementAndUntouchedDefaults(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT NOT NULL, name TEXT, active BOOL NOT NULL DEFAULT TRUE)",
		"INSERT INTO users (id, name) VALUES (1, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("UPDATE users SET name = 'bob' WHERE id = 1"); err != nil {
		t.Fatalf("Exec(update other column) error = %v", err)
	}

	rows, err := db.Query("SELECT name, active FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 2 {
		t.Fatalf("rows = %#v, want one row", rows)
	}
	if rows.data[0][0] != "bob" || rows.data[0][1] != true {
		t.Fatalf("rows.data = %#v, want [[\"bob\" true]]", rows.data)
	}

	if _, err := db.Exec("UPDATE users SET active = FALSE WHERE id = 1"); err != nil {
		t.Fatalf("Exec(valid not-null update) error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET active = NULL WHERE id = 1"); err == nil || err.Error() != "execution: NOT NULL constraint failed: users.active" {
		t.Fatalf("Exec(NULL update) error = %v, want NOT NULL constraint failure", err)
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
