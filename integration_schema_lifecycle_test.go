package rovadb

import "testing"

func TestSchemaLifecycleRoundTripConfidence(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE teams (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO teams VALUES (1, 'ops')",
		"CREATE INDEX idx_users_name ON users (name)",
		"CREATE UNIQUE INDEX idx_users_id ON users (id)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	tables, err := db.ListTables()
	if err != nil {
		t.Fatalf("ListTables() after first reopen error = %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("len(ListTables()) after first reopen = %d, want 2", len(tables))
	}
	if _, err := db.GetTableSchema("users"); err != nil {
		t.Fatalf("GetTableSchema(users) after first reopen error = %v", err)
	}
	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query(users by indexable predicate) error = %v", err)
	}
	if got := collectIntRowsFromRows(t, rows); len(got) != 1 || got[0] != 1 {
		t.Fatalf("users query rows after first reopen = %#v, want []int{1}", got)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close() error = %v", err)
	}
	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() after drop index error = %v", err)
	}

	db = reopenDB(t, path)
	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil after second reopen")
	}
	if table.IndexDefinition("idx_users_name") != nil {
		t.Fatalf("IndexDefinition(idx_users_name) = %#v, want nil after reopen", table.IndexDefinition("idx_users_name"))
	}
	if table.IndexDefinition("idx_users_id") == nil {
		t.Fatalf("IndexDefinition(idx_users_id) = nil, want surviving unique definition (defs=%#v)", table.IndexDefs)
	}
	rows, err = db.Query("SELECT id, name FROM teams")
	if err != nil {
		t.Fatalf("Query(teams) after second reopen error = %v", err)
	}
	var teamID int
	var teamName string
	if !rows.Next() {
		t.Fatal("teams rows.Next() = false, want true")
	}
	if err := rows.Scan(&teamID, &teamName); err != nil {
		t.Fatalf("teams rows.Scan() error = %v", err)
	}
	if teamID != 1 || teamName != "ops" {
		t.Fatalf("teams row = (%d,%q), want (1,\"ops\")", teamID, teamName)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("teams rows.Err() = %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("teams rows.Close() error = %v", err)
	}
	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() after drop table error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	tables, err = db.ListTables()
	if err != nil {
		t.Fatalf("ListTables() after final reopen error = %v", err)
	}
	if len(tables) != 1 || tables[0].Name != "teams" {
		t.Fatalf("ListTables() after final reopen = %#v, want only teams", tables)
	}
	if _, err := db.GetTableSchema("users"); err == nil || err.Error() != "table not found: users" {
		t.Fatalf("GetTableSchema(users) error = %v, want %q", err, "table not found: users")
	}
	rows, err = db.Query("SELECT id, name FROM teams")
	if err != nil {
		t.Fatalf("Query(teams) after final reopen error = %v", err)
	}
	if !rows.Next() {
		t.Fatal("final teams rows.Next() = false, want true")
	}
	if err := rows.Scan(&teamID, &teamName); err != nil {
		t.Fatalf("final teams rows.Scan() error = %v", err)
	}
	if teamID != 1 || teamName != "ops" {
		t.Fatalf("final teams row = (%d,%q), want (1,\"ops\")", teamID, teamName)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("final teams rows.Err() = %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("final teams rows.Close() error = %v", err)
	}
}
