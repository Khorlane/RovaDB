package rovadb

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestForeignKeyMetadataPersistsAcrossReopenViaDDL(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, team_id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_team ON users (team_id)"); err != nil {
		t.Fatalf("Exec(create users team index) error = %v", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE CASCADE"); err != nil {
		t.Fatalf("Exec(add foreign key) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	users := db.tables["users"]
	if users == nil {
		t.Fatal("users table = nil")
	}
	if len(users.ForeignKeyDefs) != 1 || users.ForeignKeyDefs[0].Name != "fk_users_team" {
		t.Fatalf("users.ForeignKeyDefs = %#v, want fk_users_team", users.ForeignKeyDefs)
	}
	if users.ForeignKeyDefs[0].OnDeleteAction != storage.CatalogForeignKeyDeleteActionCascade {
		t.Fatalf("users.ForeignKeyDefs[0].OnDeleteAction = %d, want %d", users.ForeignKeyDefs[0].OnDeleteAction, storage.CatalogForeignKeyDeleteActionCascade)
	}
}

func TestInvalidAlterAddPrimaryAndForeignKeyRollbackAtomically(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, team_id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_team ON users (team_id)"); err != nil {
		t.Fatalf("Exec(create fk index) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 99)"); err != nil {
		t.Fatalf("Exec(insert orphan child row) error = %v", err)
	}

	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id) USING INDEX idx_users_pk_missing"); err == nil {
		t.Fatal("Exec(add invalid pk) error = nil, want failure")
	}
	if db.tables["users"].PrimaryKeyDef != nil {
		t.Fatalf("users.PrimaryKeyDef = %#v, want nil after failed add pk", db.tables["users"].PrimaryKeyDef)
	}

	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT"); err == nil {
		t.Fatal("Exec(add invalid fk) error = nil, want failure")
	}
	if len(db.tables["users"].ForeignKeyDefs) != 0 {
		t.Fatalf("users.ForeignKeyDefs = %#v, want unchanged empty after failed add fk", db.tables["users"].ForeignKeyDefs)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if db.tables["users"].PrimaryKeyDef != nil {
		t.Fatalf("reopened users.PrimaryKeyDef = %#v, want nil", db.tables["users"].PrimaryKeyDef)
	}
	if len(db.tables["users"].ForeignKeyDefs) != 0 {
		t.Fatalf("reopened users.ForeignKeyDefs = %#v, want empty", db.tables["users"].ForeignKeyDefs)
	}
}
