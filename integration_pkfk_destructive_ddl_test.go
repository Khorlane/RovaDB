package rovadb

import "testing"

func TestFailedConstraintTeardownStatementsLeaveSchemaUnchangedAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE nopk (id INT)",
		"CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)",
		"CREATE TABLE parents (id INT, CONSTRAINT pk_parents PRIMARY KEY (id) USING INDEX idx_parents_pk)",
		"CREATE TABLE users (id INT, team_id INT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("ALTER TABLE nopk DROP PRIMARY KEY"); err == nil || err.Error() != "execution: primary key not found: table=nopk" {
		t.Fatalf("Exec(drop missing primary key) error = %v, want deterministic missing primary key error", err)
	}
	if _, err := db.Exec("ALTER TABLE users DROP FOREIGN KEY fk_missing"); err == nil || err.Error() != "execution: foreign key not found: table=users constraint=fk_missing" {
		t.Fatalf("Exec(drop missing foreign key) error = %v, want deterministic missing foreign key error", err)
	}
	if _, err := db.Exec("DROP INDEX idx_users_team"); err == nil {
		t.Fatal("Exec(drop supporting fk index) error = nil, want blocked drop")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	if db.tables["nopk"] == nil || db.tables["nopk"].PrimaryKeyDef != nil {
		t.Fatalf("reopened nopk.PrimaryKeyDef = %#v, want unchanged nil primary key", db.tables["nopk"].PrimaryKeyDef)
	}
	if db.tables["teams"] == nil || db.tables["teams"].PrimaryKeyDef == nil || db.tables["teams"].PrimaryKeyDef.Name != "pk_teams" {
		t.Fatalf("reopened teams.PrimaryKeyDef = %#v, want referenced primary key intact after failed drops", db.tables["teams"].PrimaryKeyDef)
	}
	if db.tables["parents"] == nil || db.tables["parents"].PrimaryKeyDef == nil || db.tables["parents"].PrimaryKeyDef.Name != "pk_parents" {
		t.Fatalf("reopened parents.PrimaryKeyDef = %#v, want unrelated primary key intact", db.tables["parents"].PrimaryKeyDef)
	}
	if db.tables["users"] == nil || len(db.tables["users"].ForeignKeyDefs) != 1 || db.tables["users"].ForeignKeyDefs[0].Name != "fk_users_team" {
		t.Fatalf("reopened users.ForeignKeyDefs = %#v, want unchanged fk after failed fk/index drops", db.tables["users"].ForeignKeyDefs)
	}
	if db.tables["users"].IndexDefinition("idx_users_team") == nil {
		t.Fatalf("reopened IndexDefinition(idx_users_team) = nil, want supporting index retained after failed drop")
	}
}

func TestDropPrimaryKeyAndDependentForeignKeysPersistAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)",
		"CREATE TABLE users (id INT, team_id INT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT)",
		"INSERT INTO teams VALUES (1)",
		"INSERT INTO users VALUES (10, 1)",
		"ALTER TABLE teams DROP PRIMARY KEY",
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
	if db.tables["teams"] == nil || db.tables["teams"].PrimaryKeyDef != nil {
		t.Fatalf("reopened teams.PrimaryKeyDef = %#v, want nil", db.tables["teams"].PrimaryKeyDef)
	}
	if db.tables["teams"].IndexDefinition("idx_teams_pk") == nil {
		t.Fatalf("reopened IndexDefinition(idx_teams_pk) = nil, want retained supporting index")
	}
	if db.tables["users"] == nil || len(db.tables["users"].ForeignKeyDefs) != 0 {
		t.Fatalf("reopened users.ForeignKeyDefs = %#v, want dependent foreign keys removed", db.tables["users"].ForeignKeyDefs)
	}

	rows, err := db.Query("SELECT id, team_id FROM users")
	if err != nil {
		t.Fatalf("Query(users) error = %v", err)
	}
	defer rows.Close()
	var id, teamID int
	if !rows.Next() {
		t.Fatal("rows.Next() = false, want true")
	}
	if err := rows.Scan(&id, &teamID); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if id != 10 || teamID != 1 {
		t.Fatalf("users row = (%d,%d), want (10,1)", id, teamID)
	}
}

func TestDropTableDependencyTeardownPersistsAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)",
		"CREATE TABLE users (id INT, team_id INT, note TEXT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT)",
		"CREATE INDEX idx_users_note ON users (note)",
		"INSERT INTO teams VALUES (1)",
		"INSERT INTO users VALUES (10, 1, 'ready')",
		"DROP TABLE teams",
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
	if db.tables["teams"] != nil {
		t.Fatalf("reopened db.tables[teams] = %#v, want nil", db.tables["teams"])
	}
	if db.tables["users"] == nil || len(db.tables["users"].ForeignKeyDefs) != 0 {
		t.Fatalf("reopened users.ForeignKeyDefs = %#v, want dependent foreign keys removed", db.tables["users"].ForeignKeyDefs)
	}
	if db.tables["users"].IndexDefinition("idx_users_team") == nil || db.tables["users"].IndexDefinition("idx_users_note") == nil {
		t.Fatalf("reopened users indexes = %#v, want surviving indexes retained", db.tables["users"].IndexDefs)
	}

	rows, err := db.Query("SELECT id, team_id, note FROM users")
	if err != nil {
		t.Fatalf("Query(users) error = %v", err)
	}
	defer rows.Close()
	var id, teamID int
	var note string
	if !rows.Next() {
		t.Fatal("rows.Next() = false, want true")
	}
	if err := rows.Scan(&id, &teamID, &note); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if id != 10 || teamID != 1 || note != "ready" {
		t.Fatalf("users row = (%d,%d,%q), want (10,1,\"ready\")", id, teamID, note)
	}
}
