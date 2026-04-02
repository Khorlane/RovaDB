package rovadb

import "testing"

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
	rows, err := db.Query("SELECT table_name FROM __sys_tables ORDER BY table_name")
	if err != nil {
		t.Fatalf("Query(__sys_tables) error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, "users")

	rows, err = db.Query("SELECT column_name FROM __sys_columns ORDER BY table_id, ordinal_position")
	if err != nil {
		t.Fatalf("Query(__sys_columns) error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, "id", "name", "active")

	rows, err = db.Query("SELECT index_name FROM __sys_indexes ORDER BY index_name")
	if err != nil {
		t.Fatalf("Query(__sys_indexes) error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, "idx_users_name")

	rows, err = db.Query("SELECT column_name FROM __sys_index_columns ORDER BY index_id, ordinal_position")
	if err != nil {
		t.Fatalf("Query(__sys_index_columns) error = %v", err)
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

	rows, err := db.Query("SELECT COUNT(*) FROM __sys_tables")
	if err != nil {
		t.Fatalf("Query(COUNT __sys_tables) error = %v", err)
	}
	defer rows.Close()
	assertRowsIntSequence(t, rows, 2)

	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	if _, err := db.Exec("DROP TABLE teams"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}

	rows, err = db.Query("SELECT table_name FROM __sys_tables ORDER BY table_name")
	if err != nil {
		t.Fatalf("Query(__sys_tables after drops) error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, "users")

	rows, err = db.Query("SELECT COUNT(*) FROM __sys_indexes")
	if err != nil {
		t.Fatalf("Query(COUNT __sys_indexes) error = %v", err)
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
		"CREATE TABLE __sys_tables (id INT)",
		"CREATE INDEX idx_sys_tables_name ON __sys_tables (table_name)",
		"INSERT INTO __sys_tables VALUES (1, 'users')",
		"UPDATE __sys_tables SET table_name = 'users'",
		"DELETE FROM __sys_tables",
		"ALTER TABLE __sys_tables ADD COLUMN extra INT",
		"DROP TABLE __sys_tables",
	}
	for _, sql := range tests {
		if _, err := db.Exec(sql); err == nil || err.Error() != "execution: system tables are read-only" {
			t.Fatalf("Exec(%q) error = %v, want %q", sql, err, "execution: system tables are read-only")
		}
	}
}
