package rovadb

import (
	"os"
	"strings"
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestExecAPICreateIndexSingleColumnPersistsAndSupportsQueryPath(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
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
	result, err := db.Exec("CREATE INDEX idx_users_name ON users (name)")
	if err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if result.RowsAffected() != 0 {
		t.Fatalf("RowsAffected() = %d, want 0", result.RowsAffected())
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	if table.IndexDefinition("idx_users_name") == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil, want non-nil (defs=%#v)", table.IndexDefs)
	}
	if table.Indexes["name"] == nil {
		t.Fatal("table.Indexes[name] = nil, want active BasicIndex")
	}

	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice' ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if got := collectIntRowsFromRows(t, rows); len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("alice ids = %#v, want []int{1, 3}", got)
	}
}

func TestExecAPICreateIndexRejectsExistingOversizedIndexedText(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tooLarge := strings.Repeat("a", 513)
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("INSERT INTO users VALUES (?, ?)", 1, tooLarge); err != nil {
		t.Fatalf("Exec(insert oversized plain text before indexing) error = %v", err)
	}

	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err == nil || err.Error() != "execution: indexed TEXT column value exceeds 512-byte limit" {
		t.Fatalf("Exec(create index oversized text) error = %v, want %q", err, "execution: indexed TEXT column value exceeds 512-byte limit")
	}
}

func TestExecAPICreateUniqueIndexRejectsExistingOversizedIndexedText(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tooLarge := strings.Repeat("b", 513)
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (?, ?)", 1, tooLarge); err != nil {
		t.Fatalf("Exec(insert oversized plain text before indexing) error = %v", err)
	}

	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_name ON users (name)"); err == nil || err.Error() != "execution: indexed TEXT column value exceeds 512-byte limit" {
		t.Fatalf("Exec(create unique index oversized text) error = %v, want %q", err, "execution: indexed TEXT column value exceeds 512-byte limit")
	}
}

func TestExecAPICreateIndexRejectsExistingOversizedMultiColumnIndexedText(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tooLarge := strings.Repeat("c", 513)
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, bio TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", 1, "alice", tooLarge); err != nil {
		t.Fatalf("Exec(insert oversized plain text before indexing) error = %v", err)
	}

	if _, err := db.Exec("CREATE INDEX idx_users_name_bio ON users (name, bio DESC)"); err == nil || err.Error() != "execution: indexed TEXT column value exceeds 512-byte limit" {
		t.Fatalf("Exec(create multi-column index oversized text) error = %v, want %q", err, "execution: indexed TEXT column value exceeds 512-byte limit")
	}
}

func TestExecAPICreateIndexAllowsIndexedTextAt512Bytes(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	atLimit := strings.Repeat("d", 512)
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (?, ?)", 1, atLimit); err != nil {
		t.Fatalf("Exec(insert at limit) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index at limit) error = %v", err)
	}
}

func TestExecAPICreateIndexPersistsRichDefinitionWithoutActivatingLegacyIndex(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, score INT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	result, err := db.Exec("CREATE INDEX idx_users_name_score ON users (name ASC, score DESC)")
	if err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if result.RowsAffected() != 0 {
		t.Fatalf("RowsAffected() = %d, want 0", result.RowsAffected())
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := table.IndexDefinition("idx_users_name_score")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(idx_users_name_score) = nil, defs=%#v", table.IndexDefs)
	}
	if len(indexDef.Columns) != 2 || indexDef.Columns[0].Name != "name" || indexDef.Columns[1].Name != "score" || !indexDef.Columns[1].Desc {
		t.Fatalf("indexDef.Columns = %#v, want [name score DESC]", indexDef.Columns)
	}
	if len(table.Indexes) != 0 {
		t.Fatalf("table.Indexes = %#v, want no active legacy index", table.Indexes)
	}
}

func TestExecAPICreateIndexRejectsDuplicateNameAcrossDatabase(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE teams (id INT, name TEXT)",
		"CREATE INDEX idx_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("CREATE INDEX idx_name ON teams (name)"); err == nil || err.Error() != "execution: index already exists" {
		t.Fatalf("Exec(duplicate name) error = %v, want %q", err, "execution: index already exists")
	}
}

func TestExecAPICreateIndexRejectsEquivalentDefinition(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_a ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("CREATE INDEX idx_b ON users (name ASC)"); err == nil || err.Error() != "execution: equivalent index already exists" {
		t.Fatalf("Exec(equivalent index) error = %v, want %q", err, "execution: equivalent index already exists")
	}
}

func TestExecAPICreateIndexRejectsMissingTableOrColumn(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}

	if _, err := db.Exec("CREATE INDEX idx_missing_table ON teams (name)"); err == nil || err.Error() != "execution: table not found" {
		t.Fatalf("Exec(missing table) error = %v, want %q", err, "execution: table not found")
	}
	if _, err := db.Exec("CREATE INDEX idx_missing_column ON users (email)"); err == nil || err.Error() != "execution: column not found" {
		t.Fatalf("Exec(missing column) error = %v, want %q", err, "execution: column not found")
	}
}

func TestExecAPICreateUniqueIndexRejectsExistingDuplicateKeys(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_name ON users (name)"); err == nil || err.Error() != "execution: duplicate indexed key values already exist" {
		t.Fatalf("Exec(create unique duplicate) error = %v, want %q", err, "execution: duplicate indexed key values already exist")
	}
}

func TestExecAPICreateUniqueIndexRejectsExistingNulls(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, NULL)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_name ON users (name)"); err == nil || err.Error() != "execution: NULL exists in unique indexed key" {
		t.Fatalf("Exec(create unique null) error = %v, want %q", err, "execution: NULL exists in unique indexed key")
	}
}

func TestExecAPICreateUniqueIndexEnforcesLaterWrites(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create unique) error = %v", err)
	}

	if _, err := db.Exec("INSERT INTO users VALUES (3, 'alice')"); err == nil || err.Error() != "execution: duplicate indexed key values already exist" {
		t.Fatalf("Exec(insert duplicate) error = %v, want %q", err, "execution: duplicate indexed key values already exist")
	}
	if _, err := db.Exec("INSERT INTO users VALUES (3, NULL)"); err == nil || err.Error() != "execution: NULL exists in unique indexed key" {
		t.Fatalf("Exec(insert null) error = %v, want %q", err, "execution: NULL exists in unique indexed key")
	}
	if _, err := db.Exec("UPDATE users SET name = 'alice' WHERE id = 2"); err == nil || err.Error() != "execution: duplicate indexed key values already exist" {
		t.Fatalf("Exec(update duplicate) error = %v, want %q", err, "execution: duplicate indexed key values already exist")
	}
	if _, err := db.Exec("UPDATE users SET name = NULL WHERE id = 2"); err == nil || err.Error() != "execution: NULL exists in unique indexed key" {
		t.Fatalf("Exec(update null) error = %v, want %q", err, "execution: NULL exists in unique indexed key")
	}
}

func TestExecAPIMultiColumnUniqueIndexPersistsAndEnforcesAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, first TEXT, last TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'Ada', 'Lovelace')",
		"INSERT INTO users VALUES (2, 'Grace', 'Hopper')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_full_name ON users (first ASC, last DESC)"); err != nil {
		t.Fatalf("Exec(create unique multi-column) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	if table == nil || table.IndexDefinition("idx_users_full_name") == nil {
		t.Fatalf("IndexDefinition(idx_users_full_name) missing after reopen, table=%#v", table)
	}
	if len(table.Indexes) != 0 {
		t.Fatalf("table.Indexes = %#v, want no legacy planner index for multi-column definition", table.Indexes)
	}

	if _, err := db.Exec("INSERT INTO users VALUES (3, 'Ada', 'Lovelace')"); err == nil || err.Error() != "execution: duplicate indexed key values already exist" {
		t.Fatalf("Exec(insert duplicate tuple) error = %v, want %q", err, "execution: duplicate indexed key values already exist")
	}
	if _, err := db.Exec("INSERT INTO users VALUES (3, 'Ada', NULL)"); err == nil || err.Error() != "execution: NULL exists in unique indexed key" {
		t.Fatalf("Exec(insert null tuple) error = %v, want %q", err, "execution: NULL exists in unique indexed key")
	}
}

func TestCreateIndexRecoveryDoesNotExposePartialIndex(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	db.afterJournalWriteHook = func() error {
		return os.ErrInvalid
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err == nil {
		t.Fatal("Exec(create index) error = nil, want failure")
	}
	if _, err := os.Stat(storage.JournalPath(path)); err != nil {
		t.Fatalf("journal stat error = %v, want surviving journal", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	if table.IndexDefinition("idx_users_name") != nil {
		t.Fatalf("IndexDefinition(idx_users_name) = %#v, want nil after recovery", table.IndexDefinition("idx_users_name"))
	}
	if len(table.Indexes) != 0 {
		t.Fatalf("table.Indexes = %#v, want empty after recovery", table.Indexes)
	}
	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if got := collectIntRowsFromRows(t, rows); len(got) != 1 || got[0] != 1 {
		t.Fatalf("query rows = %#v, want []int{1}", got)
	}
}

func collectIntRowsFromRows(t *testing.T, rows *Rows) []int {
	t.Helper()

	got := []int{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err() = %v", err)
	}
	return got
}
