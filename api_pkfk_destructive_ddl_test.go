package rovadb

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestExecAPIDropForeignKeyRemovesConstraintOnly(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)",
		"CREATE TABLE users (id INT, team_id INT, note TEXT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT)",
		"INSERT INTO teams VALUES (1)",
		"INSERT INTO users VALUES (10, 1, 'ready')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("ALTER TABLE users DROP FOREIGN KEY fk_users_team"); err != nil {
		t.Fatalf("Exec(drop foreign key) error = %v", err)
	}

	users := db.tables["users"]
	if users == nil {
		t.Fatal("db.tables[users] = nil")
	}
	if len(users.ForeignKeyDefs) != 0 {
		t.Fatalf("users.ForeignKeyDefs = %#v, want empty after drop", users.ForeignKeyDefs)
	}
	if users.IndexDefinition("idx_users_team") == nil {
		t.Fatalf("IndexDefinition(idx_users_team) = nil, want supporting index retained")
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
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v", err)
	}
}

func TestExecAPIDropForeignKeyMissingFailsAtomically(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)",
		"CREATE TABLE users (id INT, team_id INT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT)",
		"INSERT INTO teams VALUES (1)",
		"INSERT INTO users VALUES (10, 1)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("ALTER TABLE users DROP FOREIGN KEY fk_missing"); err == nil || err.Error() != "execution: foreign key not found: table=users constraint=fk_missing" {
		t.Fatalf("Exec(drop missing foreign key) error = %v, want deterministic missing foreign key error", err)
	}

	users := db.tables["users"]
	if users == nil || len(users.ForeignKeyDefs) != 1 || users.ForeignKeyDefs[0].Name != "fk_users_team" {
		t.Fatalf("users.ForeignKeyDefs = %#v, want unchanged fk_users_team after failed drop", users.ForeignKeyDefs)
	}
	if users.IndexDefinition("idx_users_team") == nil {
		t.Fatalf("IndexDefinition(idx_users_team) = nil, want unchanged supporting index")
	}
}

func TestExecAPIDropPrimaryKeyRemovesDependentForeignKeysAndKeepsIndexes(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE teams (id INT, name TEXT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)",
		"CREATE TABLE users (id INT, team_id INT, team_shadow INT, CONSTRAINT pk_users PRIMARY KEY (id) USING INDEX idx_users_pk, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT, CONSTRAINT fk_users_team_shadow FOREIGN KEY (team_shadow) REFERENCES teams (id) USING INDEX idx_users_team_shadow ON DELETE CASCADE)",
		"INSERT INTO teams VALUES (1, 'ops')",
		"INSERT INTO users VALUES (10, 1, 1)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("ALTER TABLE teams DROP PRIMARY KEY"); err != nil {
		t.Fatalf("Exec(drop primary key) error = %v", err)
	}

	teams := db.tables["teams"]
	if teams == nil {
		t.Fatal("db.tables[teams] = nil")
	}
	if teams.PrimaryKeyDef != nil {
		t.Fatalf("teams.PrimaryKeyDef = %#v, want nil after drop", teams.PrimaryKeyDef)
	}
	if teams.IndexDefinition("idx_teams_pk") == nil {
		t.Fatalf("IndexDefinition(idx_teams_pk) = nil, want supporting index retained")
	}

	users := db.tables["users"]
	if users == nil {
		t.Fatal("db.tables[users] = nil")
	}
	if len(users.ForeignKeyDefs) != 0 {
		t.Fatalf("users.ForeignKeyDefs = %#v, want dependent foreign keys removed", users.ForeignKeyDefs)
	}
	if users.IndexDefinition("idx_users_team") == nil || users.IndexDefinition("idx_users_team_shadow") == nil {
		t.Fatalf("users indexes = %#v, want supporting child indexes retained", users.IndexDefs)
	}

	rows, err := db.Query("SELECT id, team_id, team_shadow FROM users")
	if err != nil {
		t.Fatalf("Query(users) error = %v", err)
	}
	defer rows.Close()
	var id, teamID, teamShadow int
	if !rows.Next() {
		t.Fatal("rows.Next() = false, want true")
	}
	if err := rows.Scan(&id, &teamID, &teamShadow); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if id != 10 || teamID != 1 || teamShadow != 1 {
		t.Fatalf("users row = (%d,%d,%d), want (10,1,1)", id, teamID, teamShadow)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v", err)
	}
}

func TestExecAPIDropPrimaryKeyWithoutPrimaryKeyFailsAtomically(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}

	if _, err := db.Exec("ALTER TABLE teams DROP PRIMARY KEY"); err == nil || err.Error() != "execution: primary key not found: table=teams" {
		t.Fatalf("Exec(drop missing primary key) error = %v, want deterministic missing primary key error", err)
	}
	if db.tables["teams"] == nil || db.tables["teams"].PrimaryKeyDef != nil {
		t.Fatalf("teams.PrimaryKeyDef = %#v, want unchanged nil primary key", db.tables["teams"].PrimaryKeyDef)
	}
}

func TestExecAPIDropTableTeardownRemovesDependentForeignKeysOnly(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE teams (id INT, name TEXT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)",
		"CREATE TABLE users (id INT, team_id INT, note TEXT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT)",
		"CREATE TABLE audit (id INT, user_id INT, CONSTRAINT pk_audit PRIMARY KEY (id) USING INDEX idx_audit_pk)",
		"CREATE INDEX idx_users_note ON users (note)",
		"INSERT INTO teams VALUES (1, 'ops')",
		"INSERT INTO users VALUES (10, 1, 'ready')",
		"INSERT INTO audit VALUES (100, 10)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	droppedTableIndexRoot := db.tables["teams"].IndexDefinition("idx_teams_pk").RootPageID

	if _, err := db.Exec("DROP TABLE teams"); err != nil {
		t.Fatalf("Exec(drop teams) error = %v", err)
	}

	if db.tables["teams"] != nil {
		t.Fatalf("db.tables[teams] = %#v, want nil after drop", db.tables["teams"])
	}
	users := db.tables["users"]
	if users == nil {
		t.Fatal("db.tables[users] = nil")
	}
	if len(users.ForeignKeyDefs) != 0 {
		t.Fatalf("users.ForeignKeyDefs = %#v, want dependent foreign keys removed", users.ForeignKeyDefs)
	}
	if users.IndexDefinition("idx_users_team") == nil || users.IndexDefinition("idx_users_note") == nil {
		t.Fatalf("users indexes = %#v, want surviving-table indexes retained", users.IndexDefs)
	}
	if db.tables["audit"] == nil || db.tables["audit"].PrimaryKeyDef == nil {
		t.Fatalf("audit table = %#v, want unrelated constraints intact", db.tables["audit"])
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
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v", err)
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
		t.Fatalf("reopened users.ForeignKeyDefs = %#v, want no dependent foreign keys", db.tables["users"].ForeignKeyDefs)
	}
	if db.tables["users"].IndexDefinition("idx_users_team") == nil || db.tables["users"].IndexDefinition("idx_users_note") == nil {
		t.Fatalf("reopened users indexes = %#v, want surviving indexes retained", db.tables["users"].IndexDefs)
	}
	if containsPageID(freeListChainForTest(t, db.pager, storage.PageID(db.freeListHead)), storage.PageID(droppedTableIndexRoot)) == false {
		t.Fatalf("free list should contain dropped table index root %d", droppedTableIndexRoot)
	}
}

func TestExecAPIDropIndexConstraintBlockingRules(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)",
		"CREATE TABLE users (id INT, team_id INT, note TEXT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT)",
		"CREATE INDEX idx_users_note ON users (note)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("DROP INDEX idx_teams_pk"); err == nil || err.Error() != "execution: index required by constraint: table=teams index=idx_teams_pk constraint=pk_teams type=primary_key" {
		t.Fatalf("Exec(drop pk supporting index) error = %v, want deterministic constraint-blocking error", err)
	}
	if _, err := db.Exec("DROP INDEX idx_users_team"); err == nil || err.Error() != "execution: index required by constraint: table=users index=idx_users_team constraint=fk_users_team type=foreign_key" {
		t.Fatalf("Exec(drop fk supporting index) error = %v, want deterministic constraint-blocking error", err)
	}
	if _, err := db.Exec("DROP INDEX idx_users_note"); err != nil {
		t.Fatalf("Exec(drop non-supporting index) error = %v", err)
	}
	if db.tables["users"].IndexDefinition("idx_users_note") != nil {
		t.Fatalf("IndexDefinition(idx_users_note) = %#v, want nil after successful drop", db.tables["users"].IndexDefinition("idx_users_note"))
	}

	if _, err := db.Exec("ALTER TABLE users DROP FOREIGN KEY fk_users_team"); err != nil {
		t.Fatalf("Exec(drop fk before index drop) error = %v", err)
	}
	if _, err := db.Exec("DROP INDEX idx_users_team"); err != nil {
		t.Fatalf("Exec(drop former fk supporting index) error = %v", err)
	}

	if _, err := db.Exec("ALTER TABLE teams DROP PRIMARY KEY"); err != nil {
		t.Fatalf("Exec(drop pk before index drop) error = %v", err)
	}
	if _, err := db.Exec("DROP INDEX idx_teams_pk"); err != nil {
		t.Fatalf("Exec(drop former pk supporting index) error = %v", err)
	}
}
