package rovadb

import (
	"strings"
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

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
