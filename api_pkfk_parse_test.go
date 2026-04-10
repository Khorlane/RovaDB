package rovadb

import (
	"strings"
	"testing"
)

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

func TestExecCreateTableWithNamedForeignKeyPersistsMetadata(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)"); err != nil {
		t.Fatalf("Exec(create parent table) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, team_id INT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT)"); err != nil {
		t.Fatalf("Exec(create table with fk) error = %v", err)
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("users table = nil, want created table")
	}
	if len(table.ForeignKeyDefs) != 1 || table.ForeignKeyDefs[0].Name != "fk_users_team" {
		t.Fatalf("ForeignKeyDefs = %#v, want fk_users_team", table.ForeignKeyDefs)
	}
	indexDef := table.IndexDefinition("idx_users_team")
	if indexDef == nil {
		t.Fatal("IndexDefinition(idx_users_team) = nil, want supporting index")
	}
	if table.ForeignKeyDefs[0].ChildIndexID != indexDef.IndexID {
		t.Fatalf("ForeignKeyDefs[0].ChildIndexID = %d, want %d", table.ForeignKeyDefs[0].ChildIndexID, indexDef.IndexID)
	}
}

func TestCreateAndAlterPrimaryForeignKeyDDLValidation(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE teams (id INT, code TEXT, CONSTRAINT pk_root PRIMARY KEY (id) USING INDEX idx_teams_pk)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, team_id INT, alt_team_id INT, note TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_pk ON users (id)"); err != nil {
		t.Fatalf("Exec(create users pk index) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_team ON users (team_id, note)"); err != nil {
		t.Fatalf("Exec(create users team index) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO teams VALUES (1, 'alpha')"); err != nil {
		t.Fatalf("Exec(insert teams) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (10, 1, 1, 'ready')"); err != nil {
		t.Fatalf("Exec(insert users) error = %v", err)
	}

	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id) USING INDEX idx_missing"); err == nil || !strings.Contains(err.Error(), "supporting index not found") {
		t.Fatalf("Exec(add pk missing index) error = %v, want supporting index not found", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id) USING INDEX idx_users_team"); err == nil || !strings.Contains(err.Error(), "must be unique") {
		t.Fatalf("Exec(add pk non-unique index) error = %v, want unique index rejection", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (team_id) USING INDEX idx_users_pk"); err == nil || !strings.Contains(err.Error(), "shape mismatch") {
		t.Fatalf("Exec(add pk wrong shape) error = %v, want supporting index shape mismatch", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id) USING INDEX idx_users_pk"); err != nil {
		t.Fatalf("Exec(add valid pk) error = %v", err)
	}
	if db.tables["users"].PrimaryKeyDef == nil || db.tables["users"].PrimaryKeyDef.Name != "pk_users" {
		t.Fatalf("users PrimaryKeyDef = %#v, want pk_users", db.tables["users"].PrimaryKeyDef)
	}

	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT fk_users_missing FOREIGN KEY (team_id) REFERENCES missing (id) USING INDEX idx_users_team ON DELETE RESTRICT"); err == nil || !strings.Contains(err.Error(), "parent table not found") {
		t.Fatalf("Exec(add fk missing parent) error = %v, want parent table not found", err)
	}
	if _, err := db.Exec("CREATE TABLE nopk (id INT)"); err != nil {
		t.Fatalf("Exec(create nopk) error = %v", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT fk_users_nopk FOREIGN KEY (team_id) REFERENCES nopk (id) USING INDEX idx_users_team ON DELETE RESTRICT"); err == nil || !strings.Contains(err.Error(), "parent primary key not found") {
		t.Fatalf("Exec(add fk missing parent pk) error = %v, want parent primary key not found", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT fk_users_count FOREIGN KEY (team_id, alt_team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT"); err == nil || !strings.Contains(err.Error(), "column count mismatch") {
		t.Fatalf("Exec(add fk count mismatch) error = %v, want column count mismatch", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT fk_users_type FOREIGN KEY (note) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT"); err == nil || !strings.Contains(err.Error(), "type mismatch") {
		t.Fatalf("Exec(add fk type mismatch) error = %v, want type mismatch", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT fk_users_parent_cols FOREIGN KEY (team_id) REFERENCES teams (code) USING INDEX idx_users_team ON DELETE RESTRICT"); err == nil || !strings.Contains(err.Error(), "match parent primary key exactly") {
		t.Fatalf("Exec(add fk parent column mismatch) error = %v, want parent primary key exact match", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT fk_users_missing_idx FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_missing ON DELETE RESTRICT"); err == nil || !strings.Contains(err.Error(), "supporting index not found") {
		t.Fatalf("Exec(add fk missing index) error = %v, want supporting index not found", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_alt_team_note ON users (alt_team_id, note)"); err != nil {
		t.Fatalf("Exec(create wrong prefix index) error = %v", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT fk_users_prefix FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_alt_team_note ON DELETE RESTRICT"); err == nil || !strings.Contains(err.Error(), "supporting index shape mismatch") {
		t.Fatalf("Exec(add fk wrong prefix) error = %v, want supporting index shape mismatch", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT pk_users_again PRIMARY KEY (id) USING INDEX idx_users_pk"); err == nil || !strings.Contains(err.Error(), "multiple primary keys not allowed") {
		t.Fatalf("Exec(add second pk) error = %v, want multiple primary keys not allowed", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT"); err != nil {
		t.Fatalf("Exec(add valid fk) error = %v", err)
	}
	if len(db.tables["users"].ForeignKeyDefs) != 1 || db.tables["users"].ForeignKeyDefs[0].Name != "fk_users_team" {
		t.Fatalf("users ForeignKeyDefs = %#v, want fk_users_team", db.tables["users"].ForeignKeyDefs)
	}
	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT fk_users_team FOREIGN KEY (alt_team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT"); err == nil || !strings.Contains(err.Error(), "duplicate constraint name on table") {
		t.Fatalf("Exec(add duplicate fk name) error = %v, want duplicate constraint name on table", err)
	}
	if _, err := db.Exec("CREATE TABLE projects (id INT, team_id INT, CONSTRAINT pk_root PRIMARY KEY (id) USING INDEX idx_projects_pk, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_projects_team ON DELETE RESTRICT)"); err != nil {
		t.Fatalf("Exec(create projects with reused names on different table) error = %v", err)
	}
}
