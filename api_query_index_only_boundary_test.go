package rovadb

import "testing"

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
