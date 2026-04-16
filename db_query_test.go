package rovadb

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
	"github.com/Khorlane/RovaDB/internal/temporal"
)

// General query behavior

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
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != int64(1) {
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
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != int64(1) {
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
	if len(rows.data) != 1 || len(rows.data[0]) != 2 || rows.data[0][0] != int32(1) || rows.data[0][1] != "alice" {
		t.Fatalf("rows.data = %#v, want [[1 \"alice\"]]", rows.data)
	}
}

func TestQueryMaterializationPreservesExactIntegerWidthsAndNonIntegerShapes(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE numbers (small_col SMALLINT, int_col INT, big_col BIGINT, active BOOL, score REAL, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO numbers VALUES (?, ?, ?, ?, ?, ?)", int16(11), int32(22), int64(33), true, 4.5, "ok"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT small_col, int_col, big_col, active, score, name FROM numbers")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 6 {
		t.Fatalf("rows = %#v, want one fully materialized row", rows)
	}

	if got, ok := rows.data[0][0].(int16); !ok || got != 11 {
		t.Fatalf("rows.data[0][0] = %#v, want int16(11)", rows.data[0][0])
	}
	if got, ok := rows.data[0][1].(int32); !ok || got != 22 {
		t.Fatalf("rows.data[0][1] = %#v, want int32(22)", rows.data[0][1])
	}
	if got, ok := rows.data[0][2].(int64); !ok || got != 33 {
		t.Fatalf("rows.data[0][2] = %#v, want int64(33)", rows.data[0][2])
	}
	if got, ok := rows.data[0][3].(bool); !ok || !got {
		t.Fatalf("rows.data[0][3] = %#v, want true", rows.data[0][3])
	}
	if got, ok := rows.data[0][4].(float64); !ok || got != 4.5 {
		t.Fatalf("rows.data[0][4] = %#v, want 4.5", rows.data[0][4])
	}
	if got, ok := rows.data[0][5].(string); !ok || got != "ok" {
		t.Fatalf("rows.data[0][5] = %#v, want %q", rows.data[0][5], "ok")
	}
}

func TestQueryMaterializationKeepsTypedArithmeticSeparateFromUntypedIntegerLiterals(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE numbers (small_col SMALLINT, int_col INT, big_col BIGINT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO numbers VALUES (?, ?, ?)", int16(11), int32(22), int64(33)); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT small_col + 1, int_col + 1, big_col + 1 FROM numbers")
	if err != nil {
		t.Fatalf("Query(typed arithmetic) error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 3 {
		t.Fatalf("rows = %#v, want one typed arithmetic row", rows)
	}
	if got, ok := rows.data[0][0].(int16); !ok || got != 12 {
		t.Fatalf("rows.data[0][0] = %#v, want int16(12)", rows.data[0][0])
	}
	if got, ok := rows.data[0][1].(int32); !ok || got != 23 {
		t.Fatalf("rows.data[0][1] = %#v, want int32(23)", rows.data[0][1])
	}
	if got, ok := rows.data[0][2].(int64); !ok || got != 34 {
		t.Fatalf("rows.data[0][2] = %#v, want int64(34)", rows.data[0][2])
	}

	literalRows, err := db.Query("SELECT 1 + 2")
	if err != nil {
		t.Fatalf("Query(untyped literal arithmetic) error = %v", err)
	}
	if literalRows == nil || len(literalRows.data) != 1 || len(literalRows.data[0]) != 1 {
		t.Fatalf("literalRows = %#v, want one literal row", literalRows)
	}
	if got, ok := literalRows.data[0][0].(int64); !ok || got != 3 {
		t.Fatalf("literalRows.data[0][0] = %#v, want int64(3)", literalRows.data[0][0])
	}
}

func TestQueryAPITextComparisonsAreCaseInsensitiveAcrossWhereAndOrderBy(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'Alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'Charles')",
		"INSERT INTO users VALUES (4, 'BOB')",
		"INSERT INTO users VALUES (5, 'Bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	ordered, err := db.Query("SELECT name FROM users ORDER BY name")
	if err != nil {
		t.Fatalf("Query(order) error = %v", err)
	}
	if ordered == nil || len(ordered.data) < 3 {
		t.Fatalf("ordered rows = %#v, want materialized ordered rows", ordered)
	}
	if ordered.data[0][0] != "Alice" || ordered.data[1][0] != "bob" || ordered.data[2][0] != "BOB" {
		t.Fatalf("ordered rows = %#v, want Alice then bob-group before Charles", ordered.data)
	}

	filtered, err := db.Query("SELECT name FROM users WHERE name = 'bob' ORDER BY id")
	if err != nil {
		t.Fatalf("Query(where) error = %v", err)
	}
	if filtered == nil || len(filtered.data) != 3 {
		t.Fatalf("filtered rows = %#v, want 3 bob matches", filtered)
	}
	if filtered.data[0][0] != "bob" || filtered.data[1][0] != "BOB" || filtered.data[2][0] != "Bob" {
		t.Fatalf("filtered rows = %#v, want bob/BOB/Bob", filtered.data)
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
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != int64(1) {
		t.Fatalf("rows.data = %#v, want [[1]]", rows.data)
	}
}

func TestQueryAPIEligibleCountStarUsesIndexOnlyWithoutBaseRowFetch(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
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

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil, want value")
	}
	table.SetStorageMeta(0, table.PersistedRowCount())

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want successful count rowset", rows)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != int64(3) {
		t.Fatalf("rows.data = %#v, want [[3]]", rows.data)
	}
}

func TestQueryAPIEligibleCountStarOnEmptyIndexedTableReturnsZero(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want successful count rowset", rows)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != int64(0) {
		t.Fatalf("rows.data = %#v, want [[0]]", rows.data)
	}
}

func TestQueryAPIEligibleCountStarTracksInsertAndDeleteChanges(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
		"DELETE FROM users WHERE id = 2",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want successful count rowset", rows)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != int64(2) {
		t.Fatalf("rows.data = %#v, want [[2]]", rows.data)
	}
}

func TestQueryAPIEligibleCountStarRemainsCorrectAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
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
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want successful count rowset", rows)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != int64(3) {
		t.Fatalf("rows.data = %#v, want [[3]]", rows.data)
	}
}

func TestQueryAPIEligibleIndexedProjectionUsesIndexOnlyWithoutBaseRowFetch(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
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

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil, want value")
	}
	table.SetStorageMeta(0, table.PersistedRowCount())

	rows, err := db.Query("SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if got := rows.Columns(); len(got) != 1 || got[0] != "id" {
		t.Fatalf("Columns() = %#v, want [id]", got)
	}
	if len(rows.data) != 3 || rows.data[0][0] != int32(1) || rows.data[1][0] != int32(2) || rows.data[2][0] != int32(3) {
		t.Fatalf("rows.data = %#v, want [[1] [2] [3]]", rows.data)
	}
}

func TestQueryAPIEligibleIndexedProjectionRemainsCorrectAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
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
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query("SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if got := rows.Columns(); len(got) != 1 || got[0] != "id" {
		t.Fatalf("Columns() = %#v, want [id]", got)
	}
	if len(rows.data) != 3 || rows.data[0][0] != int32(1) || rows.data[1][0] != int32(2) || rows.data[2][0] != int32(3) {
		t.Fatalf("rows.data = %#v, want [[1] [2] [3]]", rows.data)
	}
}

func TestQueryAPIEligibleIndexedProjectionEmptyTableReturnsNoRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	rows, err := db.Query("SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if got := rows.Columns(); len(got) != 1 || got[0] != "id" {
		t.Fatalf("Columns() = %#v, want [id]", got)
	}
	if len(rows.data) != 0 {
		t.Fatalf("rows.data = %#v, want empty rowset", rows.data)
	}
}

func TestQueryAPIEligibleQualifiedIndexedProjectionWorks(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT users.id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if got := rows.Columns(); len(got) != 1 || got[0] != "users.id" {
		t.Fatalf("Columns() = %#v, want [users.id]", got)
	}
	if len(rows.data) != 2 || rows.data[0][0] != int32(1) || rows.data[1][0] != int32(2) {
		t.Fatalf("rows.data = %#v, want [[1] [2]]", rows.data)
	}
}

func TestQueryAPIEligibleQualifiedIndexedProjectionRemainsCorrectAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query("SELECT users.id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if got := rows.Columns(); len(got) != 1 || got[0] != "users.id" {
		t.Fatalf("Columns() = %#v, want [users.id]", got)
	}
	if len(rows.data) != 2 || rows.data[0][0] != int32(1) || rows.data[1][0] != int32(2) {
		t.Fatalf("rows.data = %#v, want [[1] [2]]", rows.data)
	}
}

func TestQueryAPIAliasedIndexedProjectionSurvivesLegacyRootRemoval(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil, want value")
	}
	table.SetStorageMeta(0, table.PersistedRowCount())

	rows, err := db.Query("SELECT id AS user_id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var userID int32
	if err := rows.Scan(&userID); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if userID != 1 {
		t.Fatalf("rows.Scan() = %d, want 1", userID)
	}
	if rows.Next() {
		t.Fatal("Next() second = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v", err)
	}
}

func TestQueryAPIIndexedProjectionWithOrderBySurvivesLegacyRootRemoval(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil, want value")
	}
	table.SetStorageMeta(0, table.PersistedRowCount())

	rows, err := db.Query("SELECT id FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var id int32
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if id != 1 {
		t.Fatalf("rows.Scan() = %d, want 1", id)
	}
	if rows.Next() {
		t.Fatal("Next() second = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v", err)
	}
}

func TestQueryAPIIndexedProjectionWithAliasAndOrderBySurvivesLegacyRootRemoval(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil, want value")
	}
	table.SetStorageMeta(0, table.PersistedRowCount())

	rows, err := db.Query("SELECT id AS user_id FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if got := rows.Columns(); len(got) != 1 || got[0] != "user_id" {
		t.Fatalf("Columns() = %#v, want [user_id]", got)
	}
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var userID int32
	if err := rows.Scan(&userID); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if userID != 1 {
		t.Fatalf("rows.Scan() = %d, want 1", userID)
	}
	if rows.Next() {
		t.Fatal("Next() second = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v", err)
	}
}

func TestQueryAPINonIndexedProjectionSurvivesLegacyRootRemoval(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil, want value")
	}
	table.SetStorageMeta(0, table.PersistedRowCount())

	rows, err := db.Query("SELECT name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if name != "alice" {
		t.Fatalf("rows.Scan() = %q, want %q", name, "alice")
	}
	if rows.Next() {
		t.Fatal("Next() second = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v", err)
	}
}

func TestQueryAPIEligibleIndexedProjectionTracksInsertAndDeleteChanges(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
		"DELETE FROM users WHERE id = 2",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if len(rows.data) != 2 || rows.data[0][0] != int32(1) || rows.data[1][0] != int32(3) {
		t.Fatalf("rows.data = %#v, want [[1] [3]]", rows.data)
	}
}

func TestQueryAPIEligibleIndexedProjectionTracksIndexedValueUpdate(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"UPDATE users SET id = 7 WHERE name = 'bob'",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if len(rows.data) != 2 || rows.data[0][0] != int32(1) || rows.data[1][0] != int32(7) {
		t.Fatalf("rows.data = %#v, want [[1] [7]]", rows.data)
	}
}

func TestQueryAPIFallbackProjectionRemainsCorrectAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query("SELECT name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if got := rows.Columns(); len(got) != 1 || got[0] != "name" {
		t.Fatalf("Columns() = %#v, want [name]", got)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != "alice" {
		t.Fatalf("rows.data = %#v, want [[alice]]", rows.data)
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

func TestQueryAPICommaJoinReturnsRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE customers (cust_nbr INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create customers) error = %v", err)
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX customers_ix1 ON customers (cust_nbr)"); err != nil {
		t.Fatalf("Exec(create customers index) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE orders (cust_nbr INT, order_nbr INT, total_amt INT)"); err != nil {
		t.Fatalf("Exec(create orders) error = %v", err)
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX orders_ix1 ON orders (cust_nbr, order_nbr)"); err != nil {
		t.Fatalf("Exec(create orders index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO customers VALUES (1, 'alice')",
		"INSERT INTO customers VALUES (2, 'bob')",
		"INSERT INTO orders VALUES (1, 101, 75)",
		"INSERT INTO orders VALUES (1, 102, 25)",
		"INSERT INTO orders VALUES (2, 103, 60)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT a.cust_nbr, a.name, b.order_nbr, b.total_amt FROM customers a, orders b WHERE a.cust_nbr = b.cust_nbr AND b.total_amt > 50 ORDER BY a.name")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want successful joined rowset", rows)
	}
	if len(rows.columns) != 4 || rows.columns[0] != "a.cust_nbr" || rows.columns[1] != "a.name" || rows.columns[2] != "b.order_nbr" || rows.columns[3] != "b.total_amt" {
		t.Fatalf("rows.columns = %#v, want qualified projected columns", rows.columns)
	}
	if len(rows.data) != 2 {
		t.Fatalf("rows.data = %#v, want two joined rows", rows.data)
	}
	if rows.data[0][0] != int32(1) || rows.data[0][1] != "alice" || rows.data[0][2] != int32(101) || rows.data[0][3] != int32(75) {
		t.Fatalf("rows.data[0] = %#v, want [1 alice 101 75]", rows.data[0])
	}
	if rows.data[1][0] != int32(2) || rows.data[1][1] != "bob" || rows.data[1][2] != int32(103) || rows.data[1][3] != int32(60) {
		t.Fatalf("rows.data[1] = %#v, want [2 bob 103 60]", rows.data[1])
	}
}

func TestQueryAPIExplicitJoinReturnsRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, dept_id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE accounts (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create accounts) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice', 10)",
		"INSERT INTO users VALUES (2, 'bob', 20)",
		"INSERT INTO accounts VALUES (10, 'eng')",
		"INSERT INTO accounts VALUES (20, 'ops')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT u.name, a.name FROM users u JOIN accounts a ON u.dept_id = a.id ORDER BY u.id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want successful joined rowset", rows)
	}
	if len(rows.data) != 2 || len(rows.data[0]) != 2 {
		t.Fatalf("rows.data = %#v, want two joined rows", rows.data)
	}
	if rows.data[0][0] != "alice" || rows.data[0][1] != "eng" || rows.data[1][0] != "bob" || rows.data[1][1] != "ops" {
		t.Fatalf("rows.data = %#v, want [[alice eng] [bob ops]]", rows.data)
	}
}

func TestQueryAPICommaJoinAndExplicitJoinMatch(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE customers (cust_nbr INT, name TEXT)",
		"CREATE UNIQUE INDEX customers_ix1 ON customers (cust_nbr)",
		"CREATE TABLE orders (cust_nbr INT, order_nbr INT, total_amt INT)",
		"CREATE UNIQUE INDEX orders_ix1 ON orders (cust_nbr, order_nbr)",
		"INSERT INTO customers VALUES (1, 'alice')",
		"INSERT INTO customers VALUES (2, 'bob')",
		"INSERT INTO orders VALUES (1, 101, 75)",
		"INSERT INTO orders VALUES (1, 102, 25)",
		"INSERT INTO orders VALUES (2, 103, 60)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	commaRows, err := db.Query("SELECT a.cust_nbr, a.name, b.order_nbr, b.total_amt FROM customers a, orders b WHERE a.cust_nbr = b.cust_nbr AND b.total_amt > 50 ORDER BY a.name")
	if err != nil {
		t.Fatalf("Query(comma join) error = %v", err)
	}
	explicitRows, err := db.Query("SELECT a.cust_nbr, a.name, b.order_nbr, b.total_amt FROM customers a JOIN orders b ON a.cust_nbr = b.cust_nbr WHERE b.total_amt > 50 ORDER BY a.name")
	if err != nil {
		t.Fatalf("Query(explicit join) error = %v", err)
	}
	if commaRows == nil || explicitRows == nil {
		t.Fatalf("rows = (%#v, %#v), want non-nil rowsets", commaRows, explicitRows)
	}
	if len(commaRows.columns) != len(explicitRows.columns) {
		t.Fatalf("column lengths = (%d, %d), want match", len(commaRows.columns), len(explicitRows.columns))
	}
	for i := range commaRows.columns {
		if commaRows.columns[i] != explicitRows.columns[i] {
			t.Fatalf("columns differ at %d: %q vs %q", i, commaRows.columns[i], explicitRows.columns[i])
		}
	}
	if len(commaRows.data) != len(explicitRows.data) {
		t.Fatalf("row counts = (%d, %d), want match", len(commaRows.data), len(explicitRows.data))
	}
	for i := range commaRows.data {
		if len(commaRows.data[i]) != len(explicitRows.data[i]) {
			t.Fatalf("row %d widths = (%d, %d), want match", i, len(commaRows.data[i]), len(explicitRows.data[i]))
		}
		for j := range commaRows.data[i] {
			if commaRows.data[i][j] != explicitRows.data[i][j] {
				t.Fatalf("row %d col %d differ: %v vs %v", i, j, commaRows.data[i][j], explicitRows.data[i][j])
			}
		}
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

	rows, err := db.Query("SELECT name FROM users WHERE id = ?", int32(1))
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
	boundRows, err := db.Query("SELECT name FROM users WHERE id = ?", int32(1))
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

	rows, err := db.Query("SELECT id FROM users WHERE id = ? OR id = ? AND name = ? ORDER BY id", int32(1), int32(2), "bob")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 2 {
		t.Fatalf("rows.data = %#v, want two rows", rows.data)
	}
	if rows.data[0][0] != int32(1) || rows.data[1][0] != int32(2) {
		t.Fatalf("rows.data = %#v, want [[1] [2]]", rows.data)
	}
}

func TestQueryAPIPlaceholderRejectsUnsupportedIntegerTypesAtBindTime(t *testing.T) {
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
			rows, err := db.Query("SELECT name FROM users WHERE id = ?", tc.arg)
			if err != nil {
				t.Fatalf("Query() transport error = %v", err)
			}
			if rows == nil || rows.err == nil || !strings.Contains(rows.err.Error(), "unsupported placeholder argument type") {
				t.Fatalf("rows = %#v, want unsupported placeholder argument type error", rows)
			}
		})
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
	if rows == nil || len(rows.data) != 1 || rows.data[0][0] != int32(2) {
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
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != int64(2) {
		t.Fatalf("rows.data = %#v, want [[2]]", rows.data)
	}
}

func TestQueryAPIAggregateFunctionsReturnSingleRow(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE metrics (id INT, name TEXT, score REAL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO metrics VALUES (1, 'beta', 1.5)",
		"INSERT INTO metrics VALUES (2, 'alpha', 2.5)",
		"INSERT INTO metrics VALUES (3, 'gamma', 3.0)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT COUNT(name), AVG(score), SUM(score), MIN(name), MAX(score) FROM metrics")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want aggregate rowset", rows)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 5 {
		t.Fatalf("rows.data = %#v, want one aggregate row", rows.data)
	}
	if rows.data[0][0] != int64(3) || rows.data[0][1] != (1.5+2.5+3.0)/3.0 || rows.data[0][2] != 7.0 || rows.data[0][3] != "alpha" || rows.data[0][4] != 3.0 {
		t.Fatalf("rows.data = %#v, want [[3 2.333... 7 alpha 3.0]]", rows.data)
	}
}

func TestQueryAPIArithmeticProjectionAndPredicate(t *testing.T) {
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
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id + 1 FROM users WHERE id + 1 = 3")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != int32(3) {
		t.Fatalf("rows.data = %#v, want [[3]]", rows.data)
	}
}

func TestQueryAPIAlternateNotEqualsWhereClause(t *testing.T) {
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
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id, name FROM users WHERE id <> 1 ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want successful rowset", rows)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 2 {
		t.Fatalf("rows.data = %#v, want one row with two columns", rows.data)
	}
	if rows.data[0][0] != int32(2) || rows.data[0][1] != "bob" {
		t.Fatalf("rows.data = %#v, want [[2 \"bob\"]]", rows.data)
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

// Select behavior

func TestQuerySelectFromTable(t *testing.T) {
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
	if _, err := db.Exec("INSERT INTO users VALUES (2, 'sam')"); err != nil {
		t.Fatalf("Exec(insert 2) error = %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM users")
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
	if id1 != 1 || name1 != "steve" {
		t.Fatalf("first row = (%d, %q), want (1, %q)", id1, name1, "steve")
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

	if rows.Next() {
		t.Fatal("Next() third = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func TestQuerySelectAllFromTable(t *testing.T) {
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

	rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if got := rows.Columns(); len(got) != 2 || got[0] != "id" || got[1] != "name" {
		t.Fatalf("Columns() = %#v, want [id name]", got)
	}

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

func TestQuerySelectInvalidColumn(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	rows, err := db.Query("SELECT email FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: column not found: email" {
		t.Fatalf("Err() = %v, want %q", rows.Err(), "execution: column not found: email")
	}
}

func TestQuerySelectMissingWhereColumnIncludesName(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	rows, err := db.Query("SELECT id FROM users WHERE email = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: column not found: email" {
		t.Fatalf("Err() = %v, want %q", rows.Err(), "execution: column not found: email")
	}
}

func TestQuerySelectFromEmptyTable(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() != nil {
		t.Fatalf("Err() = %v, want nil", rows.Err())
	}
}

func TestQuerySelectSubsetOrder(t *testing.T) {
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

	rows, err := db.Query("SELECT name, id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if got := rows.Columns(); len(got) != 2 || got[0] != "name" || got[1] != "id" {
		t.Fatalf("Columns() = %#v, want [name id]", got)
	}

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var name string
	var id int32
	if err := rows.Scan(&name, &id); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if name != "steve" || id != 1 {
		t.Fatalf("row = (%q, %d), want (%q, %d)", name, id, "steve", 1)
	}
}

func TestQuerySelectAliasUsesAliasAsOutputColumnName(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE customers (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO customers VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT id AS cust_nbr FROM customers")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if got := rows.Columns(); len(got) != 1 || got[0] != "cust_nbr" {
		t.Fatalf("Columns() = %#v, want [cust_nbr]", got)
	}
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var custNbr int32
	if err := rows.Scan(&custNbr); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if custNbr != 1 {
		t.Fatalf("cust_nbr = %d, want 1", custNbr)
	}
}

func TestQuerySelectOrderByAlias(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE customers (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO customers VALUES (2, 'bob')",
		"INSERT INTO customers VALUES (1, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id AS cust_nbr FROM customers ORDER BY cust_nbr")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	assertRowsIntSequence(t, rows, 1, 2)
}

func TestQuerySelectWhereDoesNotResolveAlias(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE customers (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	rows, err := db.Query("SELECT id AS cust_nbr FROM customers WHERE cust_nbr = 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: column not found: cust_nbr" {
		t.Fatalf("Err() = %v, want %q", rows.Err(), "execution: column not found: cust_nbr")
	}
}

func TestQuerySelectSingleProjectedColumn(t *testing.T) {
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

	rows, err := db.Query("SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if got := rows.Columns(); len(got) != 1 || got[0] != "id" {
		t.Fatalf("Columns() = %#v, want [id]", got)
	}
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var id int32
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if id != 1 {
		t.Fatalf("id = %d, want 1", id)
	}
}

func TestQuerySelectExpressionProjection(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'SteVe')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT LOWER(name), LENGTH(name) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if got := rows.Columns(); len(got) != 2 || got[0] != "LOWER(name)" || got[1] != "LENGTH(name)" {
		t.Fatalf("Columns() = %#v, want [LOWER(name) LENGTH(name)]", got)
	}
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var lower string
	var length int64
	if err := rows.Scan(&lower, &length); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if lower != "steve" || length != 5 {
		t.Fatalf("row = (%q, %d), want (%q, %d)", lower, length, "steve", 5)
	}
}

func TestQuerySelectQualifiedProjectionAndPredicate(t *testing.T) {
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

	rows, err := db.Query("SELECT users.id FROM users WHERE users.name = 'bob'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if got := rows.Columns(); len(got) != 1 || got[0] != "users.id" {
		t.Fatalf("Columns() = %#v, want [users.id]", got)
	}
	assertRowsIntSequence(t, rows, 2)
}

func TestQuerySelectMissingTable(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil {
		t.Fatal("Err() = nil, want missing table error")
	}
}

func TestQuerySelectWrongScanShape(t *testing.T) {
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
	err = rows.Scan(&id)
	if !errors.Is(err, ErrScanMismatch) {
		t.Fatalf("Scan() error = %v, want ErrScanMismatch", err)
	}
}

func TestQuerySelectWhereIntEquality(t *testing.T) {
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

	rows, err := db.Query("SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if name != "steve" {
		t.Fatalf("Scan() got %q, want %q", name, "steve")
	}
	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
}

func TestQuerySelectWhereIndexedEquality(t *testing.T) {
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
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 1, 3)
}

func TestQuerySelectWhereIndexedEqualityUsesPageBackedLookup(t *testing.T) {
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
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 1, 3)
}

func TestQuerySelectWhereIndexedEqualityNoMatch(t *testing.T) {
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
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	rows, err := db.Query("SELECT * FROM users WHERE name = 'bob'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() != nil {
		t.Fatalf("Err() = %v, want nil", rows.Err())
	}
}

func TestQuerySelectWhereIndexedEqualityNoMatchUsesPageBackedLookup(t *testing.T) {
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
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	rows, err := db.Query("SELECT * FROM users WHERE name = 'bob'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() != nil {
		t.Fatalf("Err() = %v, want nil", rows.Err())
	}
}

func TestQuerySelectWhereIndexedEqualityWithProjectionAndOrderBy(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (3, 'alice')",
		"INSERT INTO users VALUES (1, 'bob')",
		"INSERT INTO users VALUES (2, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice' ORDER BY id DESC")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 3, 2)
}

func TestQuerySelectWhereIndexedEqualityAfterReopenUsesPageBackedLookup(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (3, 'alice')",
		"INSERT INTO users VALUES (1, 'bob')",
		"INSERT INTO users VALUES (2, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice' ORDER BY id DESC")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 3, 2)
}

func TestQuerySelectCountStarWithIndexedEquality(t *testing.T) {
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
		"INSERT INTO users VALUES (2, 'alice')",
		"INSERT INTO users VALUES (3, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	rows, err := db.Query("SELECT COUNT(*) FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 2)
}

func TestQuerySelectCountStarWithIndexedEqualitySingleMatch(t *testing.T) {
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
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	rows, err := db.Query("SELECT COUNT(*) FROM users WHERE name = 'bob'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 1)
}

func TestQuerySelectCountStarWithIndexedEqualityDuplicateMatchesThroughBTree(t *testing.T) {
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
		"INSERT INTO users VALUES (2, 'alice')",
		"INSERT INTO users VALUES (3, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	rows, err := db.Query("SELECT COUNT(*) FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 2)
}

func TestQuerySelectCountStarWithIndexedEqualityNoMatchesThroughBTree(t *testing.T) {
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
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	rows, err := db.Query("SELECT COUNT(*) FROM users WHERE name = 'zoe'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 0)
}

func TestQuerySelectWhereNumericComparisons(t *testing.T) {
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

	rows, err := db.Query("SELECT id FROM users WHERE id >= 2")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id int32
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if id != 2 {
		t.Fatalf("first id = %d, want 2", id)
	}
	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id != 3 {
		t.Fatalf("second id = %d, want 3", id)
	}
	if rows.Next() {
		t.Fatal("Next() third = true, want false")
	}
}

func TestQuerySelectWhereStringEquality(t *testing.T) {
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

	rows, err := db.Query("SELECT name FROM users WHERE name = 'bob'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if name != "bob" {
		t.Fatalf("Scan() got %q, want %q", name, "bob")
	}
	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
}

func TestQuerySelectWhereStringNotEqual(t *testing.T) {
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

	rows, err := db.Query("SELECT name FROM users WHERE name != 'bob'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if name != "alice" {
		t.Fatalf("Scan() got %q, want %q", name, "alice")
	}
	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
}

func TestQuerySelectWhereTypeMismatch(t *testing.T) {
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

	rows, err := db.Query("SELECT * FROM users WHERE id = 'abc'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil {
		t.Fatal("Err() = nil, want type mismatch error")
	}
}

func TestQuerySelectWhereAndConditions(t *testing.T) {
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
		"INSERT INTO users VALUES (4, 'dina')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id FROM users WHERE id > 1 AND id < 4")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id int32
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if id != 2 {
		t.Fatalf("first id = %d, want 2", id)
	}
	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id != 3 {
		t.Fatalf("second id = %d, want 3", id)
	}
	if rows.Next() {
		t.Fatal("Next() third = true, want false")
	}
}

func TestQuerySelectWhereOrConditions(t *testing.T) {
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

	rows, err := db.Query("SELECT id FROM users WHERE id = 1 OR id = 3")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 1, 3)
}

func TestQuerySelectWhereOrNoMatches(t *testing.T) {
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

	rows, err := db.Query("SELECT * FROM users WHERE id = 2 OR name = 'bob'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() != nil {
		t.Fatalf("Err() = %v, want nil", rows.Err())
	}
}

func TestQuerySelectWhereUsesBooleanPrecedence(t *testing.T) {
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
		"INSERT INTO users VALUES (1, 'bob')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT name FROM users WHERE id = 1 OR id = 2 AND name = 'bob'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsStringSequence(t, rows, "alice", "bob", "bob")
}

func TestQuerySelectWhereSupportsNotAndGrouping(t *testing.T) {
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

	rows, err := db.Query("SELECT id FROM users WHERE NOT (id = 1 OR name = 'cara')")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 2)
}

func TestQuerySelectWhereSupportsColumnComparison(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE pairs (id INT, mirror INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO pairs VALUES (1, 1)",
		"INSERT INTO pairs VALUES (2, 3)",
		"INSERT INTO pairs VALUES (4, 4)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id FROM pairs WHERE id = mirror ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 1, 4)
}

func TestQuerySelectWhereSupportsFunctionOperands(t *testing.T) {
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
		"INSERT INTO users VALUES (3, 'Cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id FROM users WHERE LOWER(name) = 'bob'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 2)
}

func TestQuerySelectCountStarEmptyTable(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if got := rows.Columns(); len(got) != 1 || got[0] != "count" {
		t.Fatalf("Columns() = %#v, want [count]", got)
	}
	assertRowsIntSequence(t, rows, 0)
}

func TestQuerySelectCountStarPopulatedTable(t *testing.T) {
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

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 3)
}

func TestQuerySelectCountStarWithWhere(t *testing.T) {
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

	rows, err := db.Query("SELECT COUNT(*) FROM users WHERE id > 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 2)
}

func TestQuerySelectCountStarNonIndexPathRemainsUnchanged(t *testing.T) {
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

	rows, err := db.Query("SELECT COUNT(*) FROM users WHERE id > 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 2)
}

func TestQuerySelectCountStarWithWhereNoMatches(t *testing.T) {
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

	rows, err := db.Query("SELECT COUNT(*) FROM users WHERE id > 10")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 0)
}

func TestQuerySelectCountStarOrderByUnsupported(t *testing.T) {
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

	rows, err := db.Query("SELECT COUNT(*) FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil {
		t.Fatal("Err() = nil, want aggregate order by error")
	}
}

func TestQuerySelectCountColumnUnsupported(t *testing.T) {
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
		"INSERT INTO users VALUES (2, NULL)",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT COUNT(id) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 3)
}

func TestQuerySelectCountExprSkipsNulls(t *testing.T) {
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
		"INSERT INTO users VALUES (2, NULL)",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT COUNT(name) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 2)
}

func TestQuerySelectCountMixedProjectionUnsupported(t *testing.T) {
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

	rows, err := db.Query("SELECT COUNT(*), name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: unsupported query form" {
		t.Fatalf("Err() = %v, want %q", rows.Err(), "execution: unsupported query form")
	}
}

func TestQuerySelectCountStarAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 2)
}

func TestQuerySelectMalformedWhereBooleanChain(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT * FROM users WHERE id = 1 OR")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "parse: invalid where clause" {
		t.Fatalf("Err() = %v, want %q", rows.Err(), "parse: invalid where clause")
	}
}

func TestQuerySelectOrderByIntAsc(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (3, 'cara')",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 1, 2, 3)
}

func TestQuerySelectOrderByIntDesc(t *testing.T) {
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
		"INSERT INTO users VALUES (3, 'cara')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id FROM users ORDER BY id DESC")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 3, 2, 1)
}

func TestQuerySelectOrderByStringAsc(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT name FROM users ORDER BY name ASC")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsStringSequence(t, rows, "alice", "bob", "cara")
}

func TestQuerySelectOrderByStringDesc(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT name FROM users ORDER BY name DESC")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsStringSequence(t, rows, "cara", "bob", "alice")
}

func TestQuerySelectOrderByTemporalFamilies(t *testing.T) {
	db, err := CreateWithOptions(freshDBPath(t), OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE events (id INT, event_date DATE, event_time TIME, recorded_at TIMESTAMP)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO events VALUES (1, '2026-04-11', '13:45:21', '2026-04-10 13:45:21')",
		"INSERT INTO events VALUES (2, '2026-04-10', '13:45:20', '2026-04-10 13:45:20')",
		"INSERT INTO events VALUES (3, '2026-04-12', '13:45:22', '2026-04-10 13:46:21')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	tests := []struct {
		name string
		sql  string
		want []int
	}{
		{name: "date asc", sql: "SELECT id FROM events ORDER BY event_date ASC", want: []int{2, 1, 3}},
		{name: "date desc", sql: "SELECT id FROM events ORDER BY event_date DESC", want: []int{3, 1, 2}},
		{name: "time asc", sql: "SELECT id FROM events ORDER BY event_time ASC", want: []int{2, 1, 3}},
		{name: "time desc", sql: "SELECT id FROM events ORDER BY event_time DESC", want: []int{3, 1, 2}},
		{name: "timestamp asc", sql: "SELECT id FROM events ORDER BY recorded_at ASC", want: []int{2, 1, 3}},
		{name: "timestamp desc", sql: "SELECT id FROM events ORDER BY recorded_at DESC", want: []int{3, 1, 2}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			assertRowsIntSequence(t, rows, tc.want...)
		})
	}
}

func TestQuerySelectOrderByWithWhere(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (3, 'cara')",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT name FROM users WHERE id > 1 ORDER BY id DESC")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsStringSequence(t, rows, "cara", "bob")
}

func TestQuerySelectOrderByMultipleColumns(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (2, 'alice')",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (3, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT name FROM users ORDER BY name ASC, id DESC")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsStringSequence(t, rows, "alice", "alice", "bob")
}

func TestQuerySelectOrderByUnknownColumn(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	rows, err := db.Query("SELECT * FROM users ORDER BY age")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: column not found: age" {
		t.Fatalf("Err() = %v, want %q", rows.Err(), "execution: column not found: age")
	}
}

func assertRowsIntSequence(t *testing.T, rows *Rows, want ...int) {
	t.Helper()

	for i, wantValue := range want {
		if !rows.Next() {
			t.Fatalf("Next() row %d = false, want true", i)
		}
		var got any
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan() row %d error = %v", i, err)
		}
		gotValue := numericValueToInt(t, got)
		if gotValue != wantValue {
			t.Fatalf("row %d = %d, want %d", i, gotValue, wantValue)
		}
	}
	if rows.Next() {
		t.Fatal("Next() after expected rows = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func assertRowsStringSequence(t *testing.T, rows *Rows, want ...string) {
	t.Helper()

	for i, wantValue := range want {
		if !rows.Next() {
			t.Fatalf("Next() row %d = false, want true", i)
		}
		var got string
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan() row %d error = %v", i, err)
		}
		if got != wantValue {
			t.Fatalf("row %d = %q, want %q", i, got, wantValue)
		}
	}
	if rows.Next() {
		t.Fatal("Next() after expected rows = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func TestQuerySelectWhereNoMatches(t *testing.T) {
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

	rows, err := db.Query("SELECT * FROM users WHERE name = 'bob'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() != nil {
		t.Fatalf("Err() = %v, want nil", rows.Err())
	}
}

func TestQuerySelectWhereBoolComparisons(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE flags (id INT, active BOOL, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO flags VALUES (1, TRUE, 'alpha')",
		"INSERT INTO flags VALUES (2, FALSE, 'beta')",
		"INSERT INTO flags VALUES (3, NULL, 'gamma')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	tests := []struct {
		name string
		sql  string
		want []int
	}{
		{name: "equals true", sql: "SELECT id FROM flags WHERE active = TRUE", want: []int{1}},
		{name: "equals false", sql: "SELECT id FROM flags WHERE active = FALSE", want: []int{2}},
		{name: "not equals true", sql: "SELECT id FROM flags WHERE active != TRUE", want: []int{2, 3}},
		{name: "not equals false", sql: "SELECT id FROM flags WHERE active != FALSE", want: []int{1, 3}},
		{name: "zero match", sql: "SELECT id FROM flags WHERE active = TRUE AND id = 2", want: nil},
		{name: "null equals null", sql: "SELECT id FROM flags WHERE active = NULL", want: []int{3}},
		{name: "null not equals true", sql: "SELECT id FROM flags WHERE active != TRUE AND id = 3", want: []int{3}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			assertRowsIntSequence(t, rows, tc.want...)
		})
	}
}

func TestQuerySelectWhereBoolTypeMismatch(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE flags (id INT, active BOOL, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO flags VALUES (1, TRUE, 'alpha')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	tests := []string{
		"SELECT id FROM flags WHERE active = 1",
		"SELECT id FROM flags WHERE active = 'true'",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			rows, err := db.Query(sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			if rows.Next() {
				t.Fatal("Next() = true, want false")
			}
			if rows.Err() == nil {
				t.Fatal("Err() = nil, want type mismatch error")
			}
		})
	}
}

func TestQuerySelectWhereRealComparisons(t *testing.T) {
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

	tests := []struct {
		name string
		sql  string
		want []int
	}{
		{name: "equals", sql: "SELECT id FROM measurements WHERE x = 3.14", want: []int{2}},
		{name: "not equals", sql: "SELECT id FROM measurements WHERE x != 3.14", want: []int{1, 3}},
		{name: "less than", sql: "SELECT id FROM measurements WHERE x < 3.0", want: []int{1}},
		{name: "less equal", sql: "SELECT id FROM measurements WHERE x <= 3.14", want: []int{1, 2}},
		{name: "greater than", sql: "SELECT id FROM measurements WHERE x > -1.0", want: []int{2, 3}},
		{name: "greater equal", sql: "SELECT id FROM measurements WHERE x >= 10.25", want: []int{3}},
		{name: "zero match", sql: "SELECT id FROM measurements WHERE x < -10.0", want: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			assertRowsIntSequence(t, rows, tc.want...)
		})
	}
}

func TestQuerySelectWhereRealTypeMismatch(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE measurements (id INT, x REAL, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO measurements VALUES (1, 3.14, 'pi')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	tests := []string{
		"SELECT id FROM measurements WHERE x = 3",
		"SELECT id FROM measurements WHERE x = '3.14'",
		"SELECT id FROM measurements WHERE x = TRUE",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			rows, err := db.Query(sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			if rows.Next() {
				t.Fatal("Next() = true, want false")
			}
			if rows.Err() == nil {
				t.Fatal("Err() = nil, want type mismatch error")
			}
		})
	}
}

func TestQuerySelectWhereTemporalComparisons(t *testing.T) {
	db, err := CreateWithOptions(freshDBPath(t), OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE events (id INT, event_date DATE, event_time TIME, recorded_at TIMESTAMP)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO events VALUES (1, '2026-04-10', '13:45:21', '2026-04-10 13:45:21')",
		"INSERT INTO events VALUES (2, '2026-04-11', '13:45:20', '2026-04-10 13:45:21')",
		"INSERT INTO events VALUES (3, '2026-04-12', '13:45:22', '2026-04-10 13:46:21')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	tests := []struct {
		name string
		sql  string
		want []int
	}{
		{name: "date equality", sql: "SELECT id FROM events WHERE event_date = '2026-04-10' ORDER BY id", want: []int{1}},
		{name: "time range", sql: "SELECT id FROM events WHERE event_time >= '13:45:21' ORDER BY id", want: []int{1, 3}},
		{name: "timestamp equality", sql: "SELECT id FROM events WHERE recorded_at = '2026-04-10 13:45:21' ORDER BY id", want: []int{1, 2}},
		{name: "timestamp range", sql: "SELECT id FROM events WHERE recorded_at > '2026-04-10 13:45:21' ORDER BY id", want: []int{3}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			assertRowsIntSequence(t, rows, tc.want...)
		})
	}
}

func TestQuerySelectWhereTemporalTypeMismatch(t *testing.T) {
	db, err := CreateWithOptions(freshDBPath(t), OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE events (id INT, event_date DATE, event_time TIME, recorded_at TIMESTAMP)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO events VALUES (1, '2026-04-10', '13:45:21', '2026-04-10 13:45:21')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	tests := []string{
		"SELECT id FROM events WHERE event_date = '13:45:21'",
		"SELECT id FROM events WHERE event_time = '2026-04-10 13:45:21'",
		"SELECT id FROM events WHERE recorded_at = '2026-04-10'",
		"SELECT id FROM events WHERE event_date = TRUE",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			rows, err := db.Query(sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			defer rows.Close()

			if rows.Next() {
				t.Fatal("Next() = true, want false")
			}
			if rows.Err() == nil {
				t.Fatal("Err() = nil, want type mismatch error")
			}
		})
	}
}

func TestQueryTemporalMaterializationAndScan(t *testing.T) {
	db, err := CreateWithOptions(freshDBPath(t), OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE events (id INT, event_date DATE, event_time TIME, recorded_at TIMESTAMP)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO events VALUES (1, '2026-04-15', '12:34:56', '2026-04-15 16:17:18')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	wantDate := time.Date(2026, time.April, 15, 0, 0, 0, 0, time.UTC)
	location, err := temporal.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation() error = %v", err)
	}
	wantTimestamp := time.Date(2026, time.April, 15, 16, 17, 18, 0, location)
	wantTime, err := NewTime(12, 34, 56)
	if err != nil {
		t.Fatalf("NewTime() error = %v", err)
	}

	rows, err := db.Query("SELECT event_date, event_time, recorded_at FROM events WHERE id = 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var gotDate time.Time
	var gotTime Time
	var gotTimestamp time.Time
	if err := rows.Scan(&gotDate, &gotTime, &gotTimestamp); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if !gotDate.Equal(wantDate) {
		t.Fatalf("rows.Scan(DATE) = %v, want %v", gotDate, wantDate)
	}
	if gotTime != wantTime {
		t.Fatalf("rows.Scan(TIME) = %#v, want %#v", gotTime, wantTime)
	}
	if !gotTimestamp.Equal(wantTimestamp) {
		t.Fatalf("rows.Scan(TIMESTAMP) = %v, want %v", gotTimestamp, wantTimestamp)
	}
	if gotTimestamp.Location().String() != "America/New_York" {
		t.Fatalf("rows.Scan(TIMESTAMP) location = %q, want %q", gotTimestamp.Location().String(), "America/New_York")
	}

	var rowDate time.Time
	var rowTime Time
	var rowTimestamp time.Time
	if err := db.QueryRow("SELECT event_date, event_time, recorded_at FROM events WHERE id = 1").Scan(&rowDate, &rowTime, &rowTimestamp); err != nil {
		t.Fatalf("QueryRow().Scan() error = %v", err)
	}
	if !rowDate.Equal(wantDate) {
		t.Fatalf("QueryRow().Scan(DATE) = %v, want %v", rowDate, wantDate)
	}
	if rowTime != wantTime {
		t.Fatalf("QueryRow().Scan(TIME) = %#v, want %#v", rowTime, wantTime)
	}
	if !rowTimestamp.Equal(wantTimestamp) {
		t.Fatalf("QueryRow().Scan(TIMESTAMP) = %v, want %v", rowTimestamp, wantTimestamp)
	}
	if rowTimestamp.Location().String() != "America/New_York" {
		t.Fatalf("QueryRow().Scan(TIMESTAMP) location = %q, want %q", rowTimestamp.Location().String(), "America/New_York")
	}
}

func TestQueryTemporalMaterializationAndScanAfterReopen(t *testing.T) {
	path := freshDBPath(t)

	db, err := CreateWithOptions(path, OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE events (id INT, recorded_at TIMESTAMP)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO events VALUES (1, '2026-04-15 16:17:18')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	location, err := temporal.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation() error = %v", err)
	}
	wantTimestamp := time.Date(2026, time.April, 15, 16, 17, 18, 0, location)

	var got time.Time
	if err := db.QueryRow("SELECT recorded_at FROM events WHERE id = 1").Scan(&got); err != nil {
		t.Fatalf("QueryRow().Scan() after reopen error = %v", err)
	}
	if !got.Equal(wantTimestamp) {
		t.Fatalf("QueryRow().Scan() after reopen = %v, want %v", got, wantTimestamp)
	}
	if got.Location().String() != "America/New_York" {
		t.Fatalf("QueryRow().Scan() after reopen location = %q, want %q", got.Location().String(), "America/New_York")
	}
}

func TestQueryTimestampMaterializationFailsForOutOfRangeZoneID(t *testing.T) {
	path := freshDBPath(t)

	db, err := CreateWithOptions(path, OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("OpenWithOptions() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE events (id INT, recorded_at TIMESTAMP)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	location, err := temporal.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation() error = %v", err)
	}
	millis := time.Date(2026, time.April, 10, 13, 45, 21, 0, location).UnixMilli()

	_, err = db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		if err := db.loadRowsIntoTables(stagedTables, "events"); err != nil {
			return err
		}
		stagedTables["events"].Rows = [][]parser.Value{
			{parser.IntValue(1), parser.TimestampValue(millis, 9)},
		}
		return db.applyStagedTableRewrite(stagedTables, "events")
	})
	if err != nil {
		t.Fatalf("execMutatingStatement() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	err = db.QueryRow("SELECT recorded_at FROM events WHERE id = 1").Scan(new(time.Time))
	assertErrorContainsAll(t, err, "timestamp materialization", "zone_id 9", "out of range", "corrupted catalog page")
}

func TestOpenRejectsPersistedInvalidTimezoneDictionaryEntry(t *testing.T) {
	path := freshDBPath(t)

	db, err := CreateWithOptions(path, OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("OpenWithOptions() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE events (id INT, recorded_at TIMESTAMP)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	location, err := temporal.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation() error = %v", err)
	}
	millis := time.Date(2026, time.April, 10, 13, 45, 21, 0, location).UnixMilli()

	_, err = db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		if err := db.loadRowsIntoTables(stagedTables, "events"); err != nil {
			return err
		}
		stagedTables["events"].Rows = [][]parser.Value{
			{parser.IntValue(1), parser.TimestampValue(millis, 1)},
		}
		return db.applyStagedTableRewrite(stagedTables, "events")
	})
	if err != nil {
		t.Fatalf("execMutatingStatement() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rewriteCatalogTemporalMetadataForTest(t, path, "America/New_York", temporal.CurrentTimezoneBasisVersion, []string{"America/New_York", "Mars/Olympus"})

	reopened, err := Open(path)
	if err == nil {
		_ = reopened.Close()
		t.Fatal("Open() error = nil, want invalid persisted timezone dictionary entry failure")
	}
	assertErrorContainsAll(t, err, "open", `zone_id 1 ("Mars/Olympus")`, `invalid timezone "Mars/Olympus"`, "corrupted catalog page")
}

func TestQueryTemporalScanTypeMismatch(t *testing.T) {
	db, err := CreateWithOptions(freshDBPath(t), OpenOptions{DefaultTimezone: "America/New_York"})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE events (id INT, event_date DATE, event_time TIME, recorded_at TIMESTAMP)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO events VALUES (1, '2026-04-15', '12:34:56', '2026-04-15 16:17:18')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	tests := []struct {
		name string
		sql  string
		dest any
	}{
		{name: "DATE rejects *Time", sql: "SELECT event_date FROM events WHERE id = 1", dest: new(Time)},
		{name: "TIME rejects *time.Time", sql: "SELECT event_time FROM events WHERE id = 1", dest: new(time.Time)},
		{name: "TIMESTAMP rejects *Time", sql: "SELECT recorded_at FROM events WHERE id = 1", dest: new(Time)},
		{name: "TIMESTAMP rejects *string", sql: "SELECT recorded_at FROM events WHERE id = 1", dest: new(string)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := db.QueryRow(tc.sql).Scan(tc.dest); !errors.Is(err, ErrUnsupportedScanType) {
				t.Fatalf("QueryRow().Scan() error = %v, want ErrUnsupportedScanType", err)
			}
		})
	}
}

func TestQuerySelectNullRoundTrip(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, NULL)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT name FROM users")
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
	if got != nil {
		t.Fatalf("Scan() got %#v, want nil", got)
	}
}

func TestQuerySelectWhereEqualsNull(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, NULL)",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id FROM users WHERE name = NULL")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 1)
}

func TestQuerySelectWhereNotEqualsNull(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, NULL)",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id FROM users WHERE name != NULL")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 2)
}

func TestQuerySelectWhereLessThanNullErrors(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, NULL)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT * FROM users WHERE name < NULL")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil {
		t.Fatal("Err() = nil, want comparison error")
	}
}

func TestQueryUpdateSetsNull(t *testing.T) {
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
	if _, err := db.Exec("UPDATE users SET name = NULL WHERE id = 1"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}

	rows, err := db.Query("SELECT name FROM users WHERE id = 1")
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
	if got != nil {
		t.Fatalf("Scan() got %#v, want nil", got)
	}
}

func TestQueryNullPersistsAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, NULL)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query("SELECT name FROM users WHERE id = 1")
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
	if got != nil {
		t.Fatalf("Scan() got %#v, want nil", got)
	}
}

// QueryRow behavior

func TestRowScanSuccessSingleRow(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	row := db.QueryRow("SELECT 1")

	var i int64
	if err := row.Scan(&i); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if i != 1 {
		t.Fatalf("Scan() got %d, want 1", i)
	}
	if row.rows == nil || !row.rows.closed {
		t.Fatalf("row.rows.closed = %v, want true", row.rows != nil && row.rows.closed)
	}
}

func TestRowScanSuccessSingleRowMultipleColumns(t *testing.T) {
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

	row := db.QueryRow("SELECT id, name FROM users WHERE id = 1")
	var i int32
	var s string
	if err := row.Scan(&i, &s); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if i != 1 || s != "alice" {
		t.Fatalf("Scan() = (%d, %q), want (1, %q)", i, s, "alice")
	}
}

func TestQueryRowReturnsWrapperForLiteralSelect(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	row := db.QueryRow("SELECT 1")
	if row == nil {
		t.Fatal("QueryRow() = nil, want value")
	}
	if row.rows == nil {
		t.Fatal("QueryRow().rows = nil, want value")
	}
	if row.rows.err != nil {
		t.Fatalf("QueryRow().rows.err = %v, want nil", row.rows.err)
	}
	if row.rows.idx != -1 {
		t.Fatalf("QueryRow().rows.idx = %d, want -1", row.rows.idx)
	}
	if len(row.rows.data) != 1 || len(row.rows.data[0]) != 1 || row.rows.data[0][0] != int64(1) {
		t.Fatalf("QueryRow().rows.data = %#v, want [[1]]", row.rows.data)
	}
}

func TestQueryRowDefersNonSelectQueryError(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	row := db.QueryRow("CREATE TABLE users (id INT)")
	if row == nil {
		t.Fatal("QueryRow() = nil, want value")
	}
	if row.rows == nil {
		t.Fatal("QueryRow().rows = nil, want value")
	}
	if !errors.Is(row.rows.err, ErrQueryRequiresSelect) {
		t.Fatalf("QueryRow().rows.err = %v, want ErrQueryRequiresSelect", row.rows.err)
	}
	if row.rows.idx != -1 {
		t.Fatalf("QueryRow().rows.idx = %d, want -1", row.rows.idx)
	}
}

func TestQueryRowDefersMalformedQueryError(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	row := db.QueryRow("SELECT * FROM users WHERE id =")
	if row == nil {
		t.Fatal("QueryRow() = nil, want value")
	}
	if row.rows == nil {
		t.Fatal("QueryRow().rows = nil, want value")
	}
	if row.rows.err == nil || row.rows.err.Error() != "parse: invalid where clause" {
		t.Fatalf("QueryRow().rows.err = %v, want %q", row.rows.err, "parse: invalid where clause")
	}
}

func TestQueryRowDefersClosedDBError(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	row := db.QueryRow("SELECT 1")
	if row == nil {
		t.Fatal("QueryRow() = nil, want value")
	}
	if row.rows == nil {
		t.Fatal("QueryRow().rows = nil, want value")
	}
	if !errors.Is(row.rows.err, ErrClosed) {
		t.Fatalf("QueryRow().rows.err = %v, want ErrClosed", row.rows.err)
	}
}

func TestQueryRowDefersNilDBError(t *testing.T) {
	var db *DB

	row := db.QueryRow("SELECT 1")
	if row == nil {
		t.Fatal("QueryRow() = nil, want value")
	}
	if row.rows == nil {
		t.Fatal("QueryRow().rows = nil, want value")
	}
	if !errors.Is(row.rows.err, ErrInvalidArgument) {
		t.Fatalf("QueryRow().rows.err = %v, want ErrInvalidArgument", row.rows.err)
	}
}

func TestRowScanNoRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	row := db.QueryRow("SELECT id FROM users WHERE id = 999")
	var i int32
	if err := row.Scan(&i); !errors.Is(err, ErrNoRows) {
		t.Fatalf("Scan() error = %v, want ErrNoRows", err)
	}
	if row.rows == nil || !row.rows.closed {
		t.Fatalf("row.rows.closed = %v, want true", row.rows != nil && row.rows.closed)
	}
}

func TestRowScanMultipleRows(t *testing.T) {
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
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	row := db.QueryRow("SELECT id FROM users ORDER BY id")
	var i int32
	if err := row.Scan(&i); !errors.Is(err, ErrMultipleRows) {
		t.Fatalf("Scan() error = %v, want ErrMultipleRows", err)
	}
	if row.rows == nil || !row.rows.closed {
		t.Fatalf("row.rows.closed = %v, want true", row.rows != nil && row.rows.closed)
	}
}

func TestRowScanDeferredQueryErrorPassthrough(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	row := db.QueryRow("CREATE TABLE users (id INT)")
	var i int32
	if err := row.Scan(&i); !errors.Is(err, ErrQueryRequiresSelect) {
		t.Fatalf("Scan() error = %v, want ErrQueryRequiresSelect", err)
	}

	row = db.QueryRow("SELECT * FROM users WHERE id =")
	if err := row.Scan(&i); err == nil || err.Error() != "parse: invalid where clause" {
		t.Fatalf("Scan() error = %v, want %q", err, "parse: invalid where clause")
	}
}

func TestRowScanClosedAndNilDBDeferredErrors(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	var i int32
	row := db.QueryRow("SELECT 1")
	if err := row.Scan(&i); !errors.Is(err, ErrClosed) {
		t.Fatalf("Scan() error = %v, want ErrClosed", err)
	}

	var nilDB *DB
	row = nilDB.QueryRow("SELECT 1")
	if err := row.Scan(&i); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Scan() error = %v, want ErrInvalidArgument", err)
	}
}

func TestRowScanMismatchAndTypeMismatchPassthrough(t *testing.T) {
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

	row := db.QueryRow("SELECT id, name FROM users")
	var i int32
	if err := row.Scan(&i); !errors.Is(err, ErrScanMismatch) {
		t.Fatalf("Scan() error = %v, want ErrScanMismatch", err)
	}

	row = db.QueryRow("SELECT name FROM users")
	if err := row.Scan(&i); !errors.Is(err, ErrUnsupportedScanType) {
		t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
	}
}

func TestRowScanNilReceiver(t *testing.T) {
	var row *Row
	var i int32

	if err := row.Scan(&i); !errors.Is(err, ErrNoRows) {
		t.Fatalf("Scan() error = %v, want ErrNoRows", err)
	}
}

func TestQueryRowPlaceholderArgsWhereClause(t *testing.T) {
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

	row := db.QueryRow("SELECT name FROM users WHERE id = ?", int32(1))
	var name string
	if err := row.Scan(&name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if name != "alice" {
		t.Fatalf("name = %q, want %q", name, "alice")
	}
}

func TestQueryRowPlaceholderArgsReflectsUpdatedRow(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (?, ?)", int32(1), "steve"); err != nil {
		t.Fatalf("Exec(insert with placeholders) error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET name = ? WHERE id = ?", "sam", int32(1)); err != nil {
		t.Fatalf("Exec(update with placeholders) error = %v", err)
	}

	row := db.QueryRow("SELECT name FROM users WHERE id = ?", int32(1))
	var name string
	if err := row.Scan(&name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if name != "sam" {
		t.Fatalf("name = %q, want %q", name, "sam")
	}
}

func TestQueryRowIndexedEqualityUsesDurableLookupPath(t *testing.T) {
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
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	row := db.QueryRow("SELECT id FROM users WHERE name = 'bob'")
	var id int32
	if err := row.Scan(&id); err != nil {
		t.Fatalf("QueryRow(indexed equality).Scan() error = %v", err)
	}
	if id != 2 {
		t.Fatalf("QueryRow(indexed equality).Scan() got %d, want 2", id)
	}
}

func TestQueryRowIndexedEqualityDuplicateMatchesRemainMultipleRows(t *testing.T) {
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
		"INSERT INTO users VALUES (2, 'alice')",
		"INSERT INTO users VALUES (3, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	row := db.QueryRow("SELECT id FROM users WHERE name = 'alice'")
	var id int32
	if err := row.Scan(&id); !errors.Is(err, ErrMultipleRows) {
		t.Fatalf("QueryRow(duplicate indexed equality).Scan() = %v, want ErrMultipleRows", err)
	}
}

func TestQueryRowIndexedEqualityNoMatchRemainsNoRows(t *testing.T) {
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
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	row := db.QueryRow("SELECT id FROM users WHERE name = 'zoe'")
	var id int32
	if err := row.Scan(&id); !errors.Is(err, ErrNoRows) {
		t.Fatalf("QueryRow(no-match indexed equality).Scan() = %v, want ErrNoRows", err)
	}
}

func TestQueryRowIndexedEqualityUsesLogicalIndexMetadataWhenRuntimeShellIsAbsent(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	row := db.QueryRow("SELECT id FROM users WHERE name = 'bob'")
	var id int32
	if err := row.Scan(&id); err != nil {
		t.Fatalf("QueryRow(indexed logical metadata).Scan() error = %v", err)
	}
	if id != 2 {
		t.Fatalf("QueryRow(indexed logical metadata).Scan() got %d, want 2", id)
	}
}

func TestQueryRowNonIndexPathRemainsUnchanged(t *testing.T) {
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

	row := db.QueryRow("SELECT name FROM users WHERE id > 1")
	var name string
	if err := row.Scan(&name); !errors.Is(err, ErrMultipleRows) {
		t.Fatalf("QueryRow(non-index path).Scan() = %v, want ErrMultipleRows", err)
	}
}

func TestQueryRowPlaceholderArgsCountMismatchIsDeferred(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	row := db.QueryRow("SELECT 1 WHERE 1 = ?")
	if row == nil || row.rows == nil || row.rows.err == nil {
		t.Fatalf("row = %#v, want deferred bind error", row)
	}
}

// Literal handling

func TestQuerySelectLiteral(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		value int64
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

			var got int64
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

func TestQuerySelectBoolLiteral(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		value bool
	}{
		{name: "select true", sql: "SELECT TRUE", value: true},
		{name: "select false", sql: "SELECT FALSE", value: false},
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

			var got bool
			if err := rows.Scan(&got); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			if got != tc.value {
				t.Fatalf("Scan() got %v, want %v", got, tc.value)
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

func TestQuerySelectRealLiteral(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		value float64
	}{
		{name: "select pi-ish", sql: "SELECT 3.14", value: 3.14},
		{name: "select negative real", sql: "SELECT -2.5", value: -2.5},
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

			var got float64
			if err := rows.Scan(&got); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			if got != tc.value {
				t.Fatalf("Scan() got %v, want %v", got, tc.value)
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

func TestRowsScanBoolIntoAny(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT TRUE")
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
	if got != true {
		t.Fatalf("Scan() got %#v, want true", got)
	}
}

func TestRowsScanRealIntoAny(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 3.14")
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
	if got != 3.14 {
		t.Fatalf("Scan() got %#v, want 3.14", got)
	}
}

func TestQueryRowSelectRealLiteral(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	var got float64
	if err := db.QueryRow("SELECT 3.14").Scan(&got); err != nil {
		t.Fatalf("QueryRow().Scan() error = %v", err)
	}
	if got != 3.14 {
		t.Fatalf("QueryRow().Scan() got %v, want 3.14", got)
	}
}

func TestRowsScanRealIntoWrongDestination(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 3.14")
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

func TestUntypedIntegerResultsScanOnlyIntoInt64(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1)",
		"INSERT INTO users VALUES (2)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	tests := []struct {
		name string
		sql  string
		want int64
	}{
		{name: "literal", sql: "SELECT 1", want: 1},
		{name: "literal arithmetic", sql: "SELECT 1 + 2", want: 3},
		{name: "count star", sql: "SELECT COUNT(*) FROM users", want: 2},
	}

	for _, tc := range tests {
		t.Run("Rows.Scan "+tc.name, func(t *testing.T) {
			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("Query(%q) error = %v", tc.sql, err)
			}
			defer rows.Close()

			if !rows.Next() {
				t.Fatalf("Next(%q) = false, want true", tc.sql)
			}

			var got int64
			if err := rows.Scan(&got); err != nil {
				t.Fatalf("Scan(%q, *int64) error = %v", tc.sql, err)
			}
			if got != tc.want {
				t.Fatalf("Scan(%q, *int64) = %d, want %d", tc.sql, got, tc.want)
			}
		})

		t.Run("Rows.Scan rejects int "+tc.name, func(t *testing.T) {
			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("Query(%q) error = %v", tc.sql, err)
			}
			defer rows.Close()

			if !rows.Next() {
				t.Fatalf("Next(%q) = false, want true", tc.sql)
			}

			var got int
			if err := rows.Scan(&got); !errors.Is(err, ErrUnsupportedScanType) {
				t.Fatalf("Scan(%q, *int) error = %v, want ErrUnsupportedScanType", tc.sql, err)
			}
		})

		t.Run("Row.Scan "+tc.name, func(t *testing.T) {
			var got int64
			if err := db.QueryRow(tc.sql).Scan(&got); err != nil {
				t.Fatalf("QueryRow(%q).Scan(*int64) error = %v", tc.sql, err)
			}
			if got != tc.want {
				t.Fatalf("QueryRow(%q).Scan(*int64) = %d, want %d", tc.sql, got, tc.want)
			}
		})

		t.Run("Row.Scan rejects int "+tc.name, func(t *testing.T) {
			var got int
			if err := db.QueryRow(tc.sql).Scan(&got); !errors.Is(err, ErrUnsupportedScanType) {
				t.Fatalf("QueryRow(%q).Scan(*int) error = %v, want ErrUnsupportedScanType", tc.sql, err)
			}
		})
	}
}

func TestQuerySelectNullLiteralRegression(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT NULL")
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

// Query trace behavior

func TestExplainQueryPathReportsTableScan(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	trace, err := db.ExplainQueryPath("SELECT name FROM users WHERE id > 1")
	if err != nil {
		t.Fatalf("ExplainQueryPath() error = %v", err)
	}
	if trace.ScanType != "table" {
		t.Fatalf("ExplainQueryPath().ScanType = %q, want %q", trace.ScanType, "table")
	}
	if trace.TableName != "users" {
		t.Fatalf("ExplainQueryPath().TableName = %q, want %q", trace.TableName, "users")
	}
	if trace.IndexName != "" {
		t.Fatalf("ExplainQueryPath().IndexName = %q, want empty", trace.IndexName)
	}
	if trace.UsesBTree {
		t.Fatal("ExplainQueryPath().UsesBTree = true, want false")
	}
}

func TestExplainQueryPathReportsPageBackedIndexScan(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	trace, err := db.ExplainQueryPath("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("ExplainQueryPath() error = %v", err)
	}
	if trace.ScanType != "index" {
		t.Fatalf("ExplainQueryPath().ScanType = %q, want %q", trace.ScanType, "index")
	}
	if trace.TableName != "users" {
		t.Fatalf("ExplainQueryPath().TableName = %q, want %q", trace.TableName, "users")
	}
	if trace.IndexName != "idx_users_name" {
		t.Fatalf("ExplainQueryPath().IndexName = %q, want %q", trace.IndexName, "idx_users_name")
	}
	if !trace.UsesBTree {
		t.Fatal("ExplainQueryPath().UsesBTree = false, want true")
	}
}

func TestExplainQueryPathSurvivesReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	trace, err := db.ExplainQueryPath("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("ExplainQueryPath() after reopen error = %v", err)
	}
	if trace.ScanType != "index" || trace.TableName != "users" || trace.IndexName != "idx_users_name" || !trace.UsesBTree {
		t.Fatalf("ExplainQueryPath() after reopen = %#v, want index/users/idx_users_name/true", trace)
	}
}

func TestExplainQueryPathUsesLogicalIndexMetadataWhenLegacyEntriesAreCleared(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	trace, err := db.ExplainQueryPath("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("ExplainQueryPath() error = %v", err)
	}
	if trace.ScanType != "index" || !trace.UsesBTree {
		t.Fatalf("ExplainQueryPath() = %#v, want index/B+Tree trace", trace)
	}
}

func TestExplainQueryPathUsesLogicalIndexMetadataWhenRuntimeShellIsAbsent(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	trace, err := db.ExplainQueryPath("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("ExplainQueryPath() error = %v", err)
	}
	if trace.ScanType != "index" || trace.IndexName != "idx_users_name" || !trace.UsesBTree {
		t.Fatalf("ExplainQueryPath() = %#v, want index/users/idx_users_name/true", trace)
	}
}

func TestExplainQueryPathRejectsIndexScanWithNonIndexRoot(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	table := db.tables["users"]
	indexDef := table.IndexDefinition("idx_users_name")
	if table == nil || indexDef == nil {
		t.Fatalf("index setup failed: table=%v indexDef=%v", table, indexDef)
	}
	indexDef.RootPageID = uint32(table.RootPageID())

	_, err = db.ExplainQueryPath("SELECT id FROM users WHERE name = 'alice'")
	if err == nil || err.Error() != "storage: corrupted index page" {
		t.Fatalf("ExplainQueryPath() error = %v, want %q", err, "storage: corrupted index page")
	}
}

func TestExplainQueryPathOnClosedDBReturnsErrClosed(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = db.ExplainQueryPath("SELECT 1")
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("ExplainQueryPath() error = %v, want ErrClosed", err)
	}
}

func TestExplainQueryPathReturnsParseErrorForMalformedQuery(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	_, err = db.ExplainQueryPath("SELECT * FROM users WHERE id =")
	if err == nil || err.Error() != "parse: invalid where clause" {
		t.Fatalf("ExplainQueryPath() error = %v, want %q", err, "parse: invalid where clause")
	}
}

// System catalog query behavior

func TestQuerySelectFromSystemCatalogTables(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD COLUMN active INT"); err != nil {
		t.Fatalf("Exec(alter table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	rows, err := db.Query("SELECT table_name FROM sys_tables ORDER BY table_name")
	if err != nil {
		t.Fatalf("Query(sys_tables) error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, "users")

	rows, err = db.Query("SELECT column_name FROM sys_tb_columns ORDER BY table_id, ordinal_position")
	if err != nil {
		t.Fatalf("Query(sys_tb_columns) error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, "id", "name", "active")

	rows, err = db.Query("SELECT index_name FROM sys_indexes ORDER BY index_name")
	if err != nil {
		t.Fatalf("Query(sys_indexes) error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, "idx_users_name")

	rows, err = db.Query("SELECT column_name FROM sys_ix_columns ORDER BY index_id, ordinal_position")
	if err != nil {
		t.Fatalf("Query(sys_ix_columns) error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, "name")
}

func TestQuerySystemCatalogReflectsSchemaChanges(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	rows, err := db.Query("SELECT COUNT(*) FROM sys_tables")
	if err != nil {
		t.Fatalf("Query(COUNT sys_tables) error = %v", err)
	}
	defer rows.Close()
	assertRowsIntSequence(t, rows, 2)

	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	if _, err := db.Exec("DROP TABLE teams"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}

	rows, err = db.Query("SELECT table_name FROM sys_tables ORDER BY table_name")
	if err != nil {
		t.Fatalf("Query(sys_tables after drops) error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, "users")

	rows, err = db.Query("SELECT COUNT(*) FROM sys_indexes")
	if err != nil {
		t.Fatalf("Query(COUNT sys_indexes) error = %v", err)
	}
	defer rows.Close()
	assertRowsIntSequence(t, rows, 0)
}

func TestSystemCatalogTablesRemainReadOnly(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tests := []string{
		"CREATE TABLE sys_tables (id INT)",
		"CREATE INDEX idx_sys_tables_name ON sys_tables (table_name)",
		"INSERT INTO sys_tables VALUES (1, 'users')",
		"UPDATE sys_tables SET table_name = 'users'",
		"DELETE FROM sys_tables",
		"ALTER TABLE sys_tables ADD COLUMN extra INT",
		"DROP TABLE sys_tables",
	}
	for _, sql := range tests {
		if _, err := db.Exec(sql); err == nil || err.Error() != "execution: system tables are read-only" {
			t.Fatalf("Exec(%q) error = %v, want %q", sql, err, "execution: system tables are read-only")
		}
	}
}

func TestOldSystemCatalogTableNamesAreRejected(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tests := []struct {
		sql  string
		want string
	}{
		{sql: "SELECT table_name FROM __sys_tables", want: "execution: table not found: __sys_tables"},
		{sql: "SELECT column_name FROM __sys_columns", want: "execution: table not found: __sys_columns"},
		{sql: "SELECT index_name FROM __sys_indexes", want: "execution: table not found: __sys_indexes"},
		{sql: "SELECT column_name FROM __sys_index_columns", want: "execution: table not found: __sys_index_columns"},
	}

	for _, tc := range tests {
		rows, err := db.Query(tc.sql)
		if err != nil {
			t.Fatalf("Query(%q) error = %v", tc.sql, err)
		}
		if rows == nil {
			t.Fatalf("Query(%q) rows = nil, want deferred error", tc.sql)
		}
		if rows.Next() {
			t.Fatalf("Query(%q) Next() = true, want false", tc.sql)
		}
		if rows.Err() == nil || rows.Err().Error() != tc.want {
			t.Fatalf("Query(%q) Err() = %v, want %q", tc.sql, rows.Err(), tc.want)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}
}

func TestQuerySystemCatalogTablesAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD COLUMN active INT"); err != nil {
		t.Fatalf("Exec(alter users) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if _, err := db.Exec("DROP TABLE teams"); err != nil {
		t.Fatalf("Exec(drop teams) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	assertSystemCatalogQuerySnapshot(t, db, []string{"users"}, []string{"id", "name", "active"}, []string{"idx_users_name"}, []string{"name"})

	rows, err := db.Query("SELECT COUNT(*) FROM sys_tb_columns")
	if err != nil {
		t.Fatalf("Query(COUNT sys_tb_columns) error = %v", err)
	}
	defer rows.Close()
	assertRowsIntSequence(t, rows, 3)
}

func TestQuerySystemCatalogTablesImmediatelyAfterUpgradeOpen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	catalog = catalogWithDirectoryRootsForSave(t, rawDB.File(), catalog)
	filtered := make([]storage.CatalogTable, 0, len(catalog.Tables))
	for _, table := range catalog.Tables {
		if isSystemCatalogTableName(table.Name) {
			continue
		}
		filtered = append(filtered, table)
	}
	catalog.Tables = filtered
	if err := storage.SaveCatalog(pager, catalog); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	rewriteDirectoryRootMappingsForCatalogTables(t, rawDB.File(), catalog)
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("upgrade Open() error = nil, want corrupted header page")
	}
	if !strings.Contains(err.Error(), "storage: corrupted header page:") || !strings.Contains(err.Error(), "orphan table-header page") {
		t.Fatalf("upgrade Open() error = %v, want orphan table-header detail", err)
	}
}

func TestQuerySystemCatalogRepeatedReopenDoesNotDuplicateRows(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("first reopen Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first reopen Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second reopen Open() error = %v", err)
	}
	defer db.Close()

	assertSystemCatalogQuerySnapshot(t, db, []string{"users"}, []string{"id", "name"}, []string{"idx_users_name"}, []string{"name"})

	rows, err := db.Query("SELECT COUNT(*) FROM sys_tables")
	if err != nil {
		t.Fatalf("Query(COUNT sys_tables) error = %v", err)
	}
	defer rows.Close()
	assertRowsIntSequence(t, rows, 1)

	rows, err = db.Query("SELECT COUNT(*) FROM sys_tb_columns")
	if err != nil {
		t.Fatalf("Query(COUNT sys_tb_columns) error = %v", err)
	}
	defer rows.Close()
	assertRowsIntSequence(t, rows, 2)

	rows, err = db.Query("SELECT COUNT(*) FROM sys_indexes")
	if err != nil {
		t.Fatalf("Query(COUNT sys_indexes) error = %v", err)
	}
	defer rows.Close()
	assertRowsIntSequence(t, rows, 1)

	rows, err = db.Query("SELECT COUNT(*) FROM sys_ix_columns")
	if err != nil {
		t.Fatalf("Query(COUNT sys_ix_columns) error = %v", err)
	}
	defer rows.Close()
	assertRowsIntSequence(t, rows, 1)
}

func assertSystemCatalogQuerySnapshot(t *testing.T, db *DB, wantTables, wantColumns, wantIndexes, wantIndexColumns []string) {
	t.Helper()

	rows, err := db.Query("SELECT table_name FROM sys_tables ORDER BY table_name")
	if err != nil {
		t.Fatalf("Query(sys_tables) error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, wantTables...)

	rows, err = db.Query("SELECT column_name FROM sys_tb_columns ORDER BY table_id, ordinal_position")
	if err != nil {
		t.Fatalf("Query(sys_tb_columns) error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, wantColumns...)

	rows, err = db.Query("SELECT index_name FROM sys_indexes ORDER BY index_name")
	if err != nil {
		t.Fatalf("Query(sys_indexes) error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, wantIndexes...)

	rows, err = db.Query("SELECT column_name FROM sys_ix_columns ORDER BY index_id, ordinal_position")
	if err != nil {
		t.Fatalf("Query(sys_ix_columns) error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, wantIndexColumns...)
}

// Index-only boundary behavior

func TestQueryAPIIndexOnlyProjectionRejectsCorruptedIndexRootThroughStorageBoundary(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX users_ix1 ON users (id)",
		"INSERT INTO users VALUES (1, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := table.IndexDefinition("users_ix1")
	if indexDef == nil {
		t.Fatal("IndexDefinition(users_ix1) = nil")
	}
	indexDef.RootPageID = uint32(table.RootPageID())

	rows, err := db.Query("SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query() transport error = %v", err)
	}
	if rows == nil || rows.err == nil || rows.err.Error() != "storage: corrupted index page" {
		t.Fatalf("rows.err = %v, want %q", rows.err, "storage: corrupted index page")
	}
}
