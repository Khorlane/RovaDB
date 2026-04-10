package rovadb

import "testing"

func TestExecCreateTableWithNamedPrimaryKeyPersistsMetadata(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, org_id INT, name TEXT, CONSTRAINT pk_users PRIMARY KEY (id, org_id) USING INDEX idx_users_pk)"); err != nil {
		t.Fatalf("Exec(create table with pk) error = %v", err)
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("users table = nil, want created table")
	}
	if table.PrimaryKeyDef == nil || table.PrimaryKeyDef.Name != "pk_users" {
		t.Fatalf("PrimaryKeyDef = %#v, want pk_users", table.PrimaryKeyDef)
	}
	indexDef := table.IndexDefinition("idx_users_pk")
	if indexDef == nil || !indexDef.Unique {
		t.Fatalf("IndexDefinition(idx_users_pk) = %#v, want unique supporting index", indexDef)
	}
	if table.PrimaryKeyDef.IndexID != indexDef.IndexID {
		t.Fatalf("PrimaryKeyDef.IndexID = %d, want %d", table.PrimaryKeyDef.IndexID, indexDef.IndexID)
	}
}

func TestExecCreateTableWithNamedForeignKeyIsRecognizedButNotImplemented(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, team_id INT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT)"); err == nil {
		t.Fatal("Exec(create table with fk) error = nil, want not implemented")
	}
}
