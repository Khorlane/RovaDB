package rovadb

import (
	"strings"
	"testing"
)

func TestPrimaryKeyRuntimeEnforcement(t *testing.T) {
	t.Run("insert and update on single-column primary key", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		mustExec(t, db, "CREATE TABLE users (id INT, name TEXT, CONSTRAINT pk_users PRIMARY KEY (id) USING INDEX idx_users_pk)")
		mustExec(t, db, "INSERT INTO users VALUES (1, 'alice')")

		if _, err := db.Exec("INSERT INTO users VALUES (NULL, 'null-id')"); err == nil || !isConstraintError(err, "table=users", "constraint=pk_users", "type=primary_key_null") {
			t.Fatalf("Exec(insert null pk) error = %v, want primary key null violation", err)
		}
		if _, err := db.Exec("INSERT INTO users VALUES (1, 'dupe')"); err == nil || !isConstraintError(err, "table=users", "constraint=pk_users", "type=primary_key_duplicate") {
			t.Fatalf("Exec(insert duplicate pk) error = %v, want primary key duplicate violation", err)
		}

		mustExec(t, db, "UPDATE users SET name = 'alice-updated' WHERE id = 1")
		if got := mustQueryString(t, db, "SELECT name FROM users WHERE id = 1"); got != "alice-updated" {
			t.Fatalf("updated name = %q, want %q", got, "alice-updated")
		}

		if _, err := db.Exec("UPDATE users SET id = 2 WHERE id = 1"); err == nil || !isConstraintError(err, "table=users", "constraint=pk_users", "type=primary_key_update_forbidden") {
			t.Fatalf("Exec(update pk) error = %v, want primary key update forbidden violation", err)
		}
		if got := mustQueryInt(t, db, "SELECT id FROM users WHERE name = 'alice-updated'"); got != 1 {
			t.Fatalf("persisted id after failed pk update = %d, want 1", got)
		}
	})

	t.Run("composite primary key duplicate and update", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		mustExec(t, db, "CREATE TABLE memberships (org_id INT, user_id INT, label TEXT, CONSTRAINT pk_memberships PRIMARY KEY (org_id, user_id) USING INDEX idx_memberships_pk)")
		mustExec(t, db, "INSERT INTO memberships VALUES (1, 10, 'owner')")

		if _, err := db.Exec("INSERT INTO memberships VALUES (1, 10, 'duplicate')"); err == nil || !isConstraintError(err, "table=memberships", "constraint=pk_memberships", "type=primary_key_duplicate") {
			t.Fatalf("Exec(insert duplicate composite pk) error = %v, want composite pk duplicate violation", err)
		}
		if _, err := db.Exec("UPDATE memberships SET user_id = 11 WHERE org_id = 1 AND user_id = 10"); err == nil || !isConstraintError(err, "table=memberships", "constraint=pk_memberships", "type=primary_key_update_forbidden") {
			t.Fatalf("Exec(update composite pk component) error = %v, want composite pk update forbidden", err)
		}
	})
}

func TestForeignKeyRuntimeEnforcement(t *testing.T) {
	t.Run("single-column foreign key insert and update", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		mustExec(t, db, "CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)")
		mustExec(t, db, "INSERT INTO teams VALUES (1)")
		mustExec(t, db, "INSERT INTO teams VALUES (2)")
		mustExec(t, db, "CREATE TABLE users (id INT, team_id INT, name TEXT, CONSTRAINT pk_users PRIMARY KEY (id) USING INDEX idx_users_pk, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT)")

		mustExec(t, db, "INSERT INTO users VALUES (1, 1, 'alice')")
		if _, err := db.Exec("INSERT INTO users VALUES (2, NULL, 'null-team')"); err == nil || !isConstraintError(err, "table=users", "constraint=fk_users_team", "type=foreign_key_null") {
			t.Fatalf("Exec(insert null fk) error = %v, want foreign key null violation", err)
		}
		if _, err := db.Exec("INSERT INTO users VALUES (2, 99, 'orphan')"); err == nil || !isConstraintError(err, "table=users", "constraint=fk_users_team", "type=foreign_key_missing_parent") {
			t.Fatalf("Exec(insert missing parent) error = %v, want foreign key missing parent violation", err)
		}

		mustExec(t, db, "UPDATE users SET name = 'alice-updated' WHERE id = 1")
		mustExec(t, db, "UPDATE users SET team_id = 2 WHERE id = 1")
		if got := mustQueryInt(t, db, "SELECT team_id FROM users WHERE id = 1"); got != 2 {
			t.Fatalf("updated team_id = %d, want 2", got)
		}
		if _, err := db.Exec("UPDATE users SET team_id = NULL WHERE id = 1"); err == nil || !isConstraintError(err, "table=users", "constraint=fk_users_team", "type=foreign_key_null") {
			t.Fatalf("Exec(update fk to null) error = %v, want foreign key null violation", err)
		}
		if _, err := db.Exec("UPDATE users SET team_id = 77 WHERE id = 1"); err == nil || !isConstraintError(err, "table=users", "constraint=fk_users_team", "type=foreign_key_missing_parent") {
			t.Fatalf("Exec(update fk to missing parent) error = %v, want foreign key missing parent violation", err)
		}
	})

	t.Run("composite foreign key insert", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		mustExec(t, db, "CREATE TABLE parents (id INT, region_id INT, CONSTRAINT pk_parents PRIMARY KEY (id, region_id) USING INDEX idx_parents_pk)")
		mustExec(t, db, "INSERT INTO parents VALUES (1, 100)")
		mustExec(t, db, "CREATE TABLE children (id INT, parent_id INT, region_id INT, CONSTRAINT pk_children PRIMARY KEY (id) USING INDEX idx_children_pk, CONSTRAINT fk_children_parent FOREIGN KEY (parent_id, region_id) REFERENCES parents (id, region_id) USING INDEX idx_children_parent ON DELETE RESTRICT)")

		mustExec(t, db, "INSERT INTO children VALUES (1, 1, 100)")
		if _, err := db.Exec("INSERT INTO children VALUES (2, 1, 999)"); err == nil || !isConstraintError(err, "table=children", "constraint=fk_children_parent", "type=foreign_key_missing_parent") {
			t.Fatalf("Exec(insert missing composite parent) error = %v, want foreign key missing parent violation", err)
		}
	})
}

func TestDeleteRestrictAndCascadeBehavior(t *testing.T) {
	t.Run("restrict deletes are enforced atomically", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		mustExec(t, db, "CREATE TABLE parents (id INT, CONSTRAINT pk_parents PRIMARY KEY (id) USING INDEX idx_parents_pk)")
		mustExec(t, db, "CREATE TABLE children (id INT, parent_id INT, CONSTRAINT pk_children PRIMARY KEY (id) USING INDEX idx_children_pk, CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id) USING INDEX idx_children_parent ON DELETE RESTRICT)")
		mustExec(t, db, "INSERT INTO parents VALUES (1)")
		mustExec(t, db, "INSERT INTO parents VALUES (2)")
		mustExec(t, db, "INSERT INTO parents VALUES (3)")
		mustExec(t, db, "INSERT INTO children VALUES (10, 2)")

		mustExec(t, db, "DELETE FROM parents WHERE id = 1")
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parents"); got != 2 {
			t.Fatalf("parent count after unreferenced delete = %d, want 2", got)
		}

		if _, err := db.Exec("DELETE FROM parents WHERE id = 2"); err == nil || !isConstraintError(err, "table=children", "constraint=fk_children_parent", "type=foreign_key_restrict") {
			t.Fatalf("Exec(delete referenced parent) error = %v, want foreign key restrict violation", err)
		}
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parents WHERE id = 2"); got != 1 {
			t.Fatalf("parent row after failed restrict delete count = %d, want 1", got)
		}

		if _, err := db.Exec("DELETE FROM parents WHERE id = 2 OR id = 3"); err == nil || !isConstraintError(err, "table=children", "constraint=fk_children_parent", "type=foreign_key_restrict") {
			t.Fatalf("Exec(delete mixed parent set) error = %v, want foreign key restrict violation", err)
		}
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parents"); got != 2 {
			t.Fatalf("parent count after failed multi-delete = %d, want 2", got)
		}
	})

	t.Run("cascade delete removes direct child rows", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		mustExec(t, db, "CREATE TABLE parents (id INT, CONSTRAINT pk_parents PRIMARY KEY (id) USING INDEX idx_parents_pk)")
		mustExec(t, db, "CREATE TABLE children (id INT, parent_id INT, CONSTRAINT pk_children PRIMARY KEY (id) USING INDEX idx_children_pk, CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id) USING INDEX idx_children_parent ON DELETE CASCADE)")
		mustExec(t, db, "INSERT INTO parents VALUES (1)")
		mustExec(t, db, "INSERT INTO children VALUES (10, 1)")

		if _, err := db.Exec("DELETE FROM parents WHERE id = 1"); err != nil {
			t.Fatalf("Exec(delete cascade parent) error = %v, want nil", err)
		}
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parents WHERE id = 1"); got != 0 {
			t.Fatalf("parent count after cascade delete = %d, want 0", got)
		}
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM children WHERE parent_id = 1"); got != 0 {
			t.Fatalf("child count after cascade delete = %d, want 0", got)
		}
	})

	t.Run("cascade delete removes full grandchild chain and multi-row target set", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		mustExec(t, db, "CREATE TABLE parents (id INT, CONSTRAINT pk_parents PRIMARY KEY (id) USING INDEX idx_parents_pk)")
		mustExec(t, db, "CREATE TABLE children (id INT, parent_id INT, CONSTRAINT pk_children PRIMARY KEY (id) USING INDEX idx_children_pk, CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id) USING INDEX idx_children_parent ON DELETE CASCADE)")
		mustExec(t, db, "CREATE TABLE grandchildren (id INT, child_id INT, CONSTRAINT pk_grandchildren PRIMARY KEY (id) USING INDEX idx_grandchildren_pk, CONSTRAINT fk_grandchildren_child FOREIGN KEY (child_id) REFERENCES children (id) USING INDEX idx_grandchildren_child ON DELETE CASCADE)")
		for _, sql := range []string{
			"INSERT INTO parents VALUES (1)",
			"INSERT INTO parents VALUES (2)",
			"INSERT INTO parents VALUES (3)",
			"INSERT INTO children VALUES (10, 1)",
			"INSERT INTO children VALUES (11, 1)",
			"INSERT INTO children VALUES (20, 2)",
			"INSERT INTO grandchildren VALUES (100, 10)",
			"INSERT INTO grandchildren VALUES (101, 11)",
			"INSERT INTO grandchildren VALUES (200, 20)",
		} {
			mustExec(t, db, sql)
		}

		if _, err := db.Exec("DELETE FROM parents WHERE id = 1 OR id = 2"); err != nil {
			t.Fatalf("Exec(delete parent cascade chain) error = %v, want nil", err)
		}
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parents"); got != 1 {
			t.Fatalf("parent count after cascading multi-delete = %d, want 1", got)
		}
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM children"); got != 0 {
			t.Fatalf("child count after cascading multi-delete = %d, want 0", got)
		}
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM grandchildren"); got != 0 {
			t.Fatalf("grandchild count after cascading multi-delete = %d, want 0", got)
		}
	})

	t.Run("mixed restrict and cascade validate against final state", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		mustExec(t, db, "CREATE TABLE parents (id INT, CONSTRAINT pk_parents PRIMARY KEY (id) USING INDEX idx_parents_pk)")
		mustExec(t, db, "CREATE TABLE children (id INT, parent_id INT, CONSTRAINT pk_children PRIMARY KEY (id) USING INDEX idx_children_pk, CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id) USING INDEX idx_children_parent ON DELETE CASCADE)")
		mustExec(t, db, "CREATE TABLE audits (id INT, child_id INT, parent_id INT, CONSTRAINT pk_audits PRIMARY KEY (id) USING INDEX idx_audits_pk, CONSTRAINT fk_audits_child FOREIGN KEY (child_id) REFERENCES children (id) USING INDEX idx_audits_child ON DELETE RESTRICT, CONSTRAINT fk_audits_parent FOREIGN KEY (parent_id) REFERENCES parents (id) USING INDEX idx_audits_parent ON DELETE RESTRICT)")

		for _, sql := range []string{
			"INSERT INTO parents VALUES (1)",
			"INSERT INTO parents VALUES (2)",
			"INSERT INTO children VALUES (10, 1)",
			"INSERT INTO children VALUES (20, 2)",
			"INSERT INTO audits VALUES (100, 10, 1)",
		} {
			mustExec(t, db, sql)
		}

		if _, err := db.Exec("DELETE FROM parents WHERE id = 1"); err == nil || !isConstraintError(err, "table=audits", "constraint=fk_audits_child", "type=foreign_key_restrict") {
			t.Fatalf("Exec(delete mixed restrict/cascade) error = %v, want final-state restrict violation", err)
		}
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parents WHERE id = 1"); got != 1 {
			t.Fatalf("parent count after failed mixed delete = %d, want 1", got)
		}
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM children WHERE id = 10"); got != 1 {
			t.Fatalf("child count after failed mixed delete = %d, want 1", got)
		}

		mustExec(t, db, "DELETE FROM audits WHERE id = 100")
		if _, err := db.Exec("DELETE FROM parents WHERE id = 1"); err != nil {
			t.Fatalf("Exec(delete final-state-clean mixed graph) error = %v, want nil", err)
		}
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parents WHERE id = 1"); got != 0 {
			t.Fatalf("parent count after successful mixed delete = %d, want 0", got)
		}
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM children WHERE id = 10"); got != 0 {
			t.Fatalf("child count after successful mixed delete = %d, want 0", got)
		}
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parents WHERE id = 2"); got != 1 {
			t.Fatalf("unrelated parent count after mixed delete = %d, want 1", got)
		}
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM children WHERE id = 20"); got != 1 {
			t.Fatalf("unrelated child count after mixed delete = %d, want 1", got)
		}
	})

	t.Run("self-referencing cascade and repeated reachability delete correctly", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		mustExec(t, db, "CREATE TABLE nodes (id INT, parent_id INT, alt_parent_id INT, CONSTRAINT pk_nodes PRIMARY KEY (id) USING INDEX idx_nodes_pk)")
		mustExec(t, db, "CREATE INDEX idx_nodes_parent ON nodes(parent_id)")
		mustExec(t, db, "CREATE INDEX idx_nodes_alt_parent ON nodes(alt_parent_id)")
		mustExec(t, db, "ALTER TABLE nodes ADD CONSTRAINT fk_nodes_parent FOREIGN KEY (parent_id) REFERENCES nodes (id) USING INDEX idx_nodes_parent ON DELETE CASCADE")
		mustExec(t, db, "ALTER TABLE nodes ADD CONSTRAINT fk_nodes_alt_parent FOREIGN KEY (alt_parent_id) REFERENCES nodes (id) USING INDEX idx_nodes_alt_parent ON DELETE CASCADE")
		for _, sql := range []string{
			"INSERT INTO nodes VALUES (1, 1, 1)",
			"INSERT INTO nodes VALUES (2, 1, 1)",
			"INSERT INTO nodes VALUES (3, 2, 1)",
			"INSERT INTO nodes VALUES (4, 3, 2)",
		} {
			mustExec(t, db, sql)
		}

		if _, err := db.Exec("DELETE FROM nodes WHERE id = 1"); err != nil {
			t.Fatalf("Exec(delete self-referencing root) error = %v, want nil", err)
		}
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM nodes"); got != 0 {
			t.Fatalf("node count after self-referencing cascade delete = %d, want 0", got)
		}
	})

	t.Run("self-referencing surviving restrict row still blocks delete", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		mustExec(t, db, "CREATE TABLE nodes (id INT, parent_id INT, owner_id INT, CONSTRAINT pk_nodes PRIMARY KEY (id) USING INDEX idx_nodes_pk)")
		mustExec(t, db, "CREATE INDEX idx_nodes_parent ON nodes(parent_id)")
		mustExec(t, db, "CREATE INDEX idx_nodes_owner ON nodes(owner_id)")
		mustExec(t, db, "ALTER TABLE nodes ADD CONSTRAINT fk_nodes_parent FOREIGN KEY (parent_id) REFERENCES nodes (id) USING INDEX idx_nodes_parent ON DELETE CASCADE")
		mustExec(t, db, "ALTER TABLE nodes ADD CONSTRAINT fk_nodes_owner FOREIGN KEY (owner_id) REFERENCES nodes (id) USING INDEX idx_nodes_owner ON DELETE RESTRICT")
		for _, sql := range []string{
			"INSERT INTO nodes VALUES (1, 1, 1)",
			"INSERT INTO nodes VALUES (2, 1, 1)",
			"INSERT INTO nodes VALUES (3, 2, 1)",
			"INSERT INTO nodes VALUES (4, 4, 1)",
		} {
			mustExec(t, db, sql)
		}

		if _, err := db.Exec("DELETE FROM nodes WHERE id = 1"); err == nil || !isConstraintError(err, "table=nodes", "constraint=fk_nodes_owner", "type=foreign_key_restrict") {
			t.Fatalf("Exec(delete self-referencing root with surviving restrict row) error = %v, want restrict violation", err)
		}
		if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM nodes"); got != 4 {
			t.Fatalf("node count after failed self-referencing mixed delete = %d, want 4", got)
		}
	})
}

func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return db
}

func mustExec(t *testing.T, db *DB, sql string) {
	t.Helper()
	if _, err := db.Exec(sql); err != nil {
		t.Fatalf("Exec(%q) error = %v", sql, err)
	}
}

func mustQueryInt(t *testing.T, db *DB, sql string) int {
	t.Helper()
	var got int
	if err := db.QueryRow(sql).Scan(&got); err != nil {
		t.Fatalf("QueryRow(%q).Scan() error = %v", sql, err)
	}
	return got
}

func mustQueryString(t *testing.T, db *DB, sql string) string {
	t.Helper()
	var got string
	if err := db.QueryRow(sql).Scan(&got); err != nil {
		t.Fatalf("QueryRow(%q).Scan() error = %v", sql, err)
	}
	return got
}

func isConstraintError(err error, parts ...string) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, part := range parts {
		if !strings.Contains(msg, part) {
			return false
		}
	}
	return true
}
