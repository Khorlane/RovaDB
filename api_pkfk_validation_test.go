package rovadb

import (
	"strings"
	"testing"
)

func TestAlterTableAddForeignKeyRejectsExistingNullAndOrphanRows(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO teams VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert teams) error = %v", err)
	}

	if _, err := db.Exec("CREATE TABLE users_null (id INT, team_id INT)"); err != nil {
		t.Fatalf("Exec(create users_null) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_null_team ON users_null (team_id)"); err != nil {
		t.Fatalf("Exec(create users_null index) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users_null VALUES (1, NULL)"); err != nil {
		t.Fatalf("Exec(insert users_null) error = %v", err)
	}
	if _, err := db.Exec("ALTER TABLE users_null ADD CONSTRAINT fk_users_null FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_null_team ON DELETE RESTRICT"); err == nil || !strings.Contains(err.Error(), "NULL child row violation") {
		t.Fatalf("Exec(add fk with null child) error = %v, want NULL child row violation", err)
	}

	if _, err := db.Exec("CREATE TABLE users_orphan (id INT, team_id INT)"); err != nil {
		t.Fatalf("Exec(create users_orphan) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_orphan_team ON users_orphan (team_id)"); err != nil {
		t.Fatalf("Exec(create users_orphan index) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users_orphan VALUES (1, 99)"); err != nil {
		t.Fatalf("Exec(insert users_orphan) error = %v", err)
	}
	if _, err := db.Exec("ALTER TABLE users_orphan ADD CONSTRAINT fk_users_orphan FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_orphan_team ON DELETE RESTRICT"); err == nil || !strings.Contains(err.Error(), "existing row violation") {
		t.Fatalf("Exec(add fk with orphan child) error = %v, want existing row violation", err)
	}
}
