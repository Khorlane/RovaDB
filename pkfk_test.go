package rovadb

import (
	"strings"
	"testing"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
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

func TestWriteValidationLoadTargetsDeleteIncludesRootAndDescendantClosureWithoutDuplicates(t *testing.T) {
	tables := map[string]*executor.Table{
		"parents": {
			Name:    "parents",
			TableID: 1,
			PrimaryKeyDef: &storage.CatalogPrimaryKey{
				Name:    "pk_parents",
				Columns: []string{"id"},
			},
		},
		"children_a": {
			Name:    "children_a",
			TableID: 2,
			PrimaryKeyDef: &storage.CatalogPrimaryKey{
				Name:    "pk_children_a",
				Columns: []string{"id"},
			},
			ForeignKeyDefs: []storage.CatalogForeignKey{
				{
					Name:                 "fk_children_a_parent",
					ParentTableID:        1,
					ParentPrimaryKeyName: "pk_parents",
				},
			},
		},
		"children_b": {
			Name:    "children_b",
			TableID: 3,
			PrimaryKeyDef: &storage.CatalogPrimaryKey{
				Name:    "pk_children_b",
				Columns: []string{"id"},
			},
			ForeignKeyDefs: []storage.CatalogForeignKey{
				{
					Name:                 "fk_children_b_parent",
					ParentTableID:        1,
					ParentPrimaryKeyName: "pk_parents",
				},
			},
		},
		"grandchildren": {
			Name:    "grandchildren",
			TableID: 4,
			PrimaryKeyDef: &storage.CatalogPrimaryKey{
				Name:    "pk_grandchildren",
				Columns: []string{"id"},
			},
			ForeignKeyDefs: []storage.CatalogForeignKey{
				{
					Name:                 "fk_grandchildren_child_a",
					ParentTableID:        2,
					ParentPrimaryKeyName: "pk_children_a",
				},
				{
					Name:                 "fk_grandchildren_child_b",
					ParentTableID:        3,
					ParentPrimaryKeyName: "pk_children_b",
				},
			},
		},
	}

	got := writeValidationLoadTargets(tables, &parser.DeleteStmt{TableName: "parents"})
	want := []string{"parents", "children_a", "grandchildren", "children_b"}
	if len(got) != len(want) {
		t.Fatalf("writeValidationLoadTargets(delete) len = %d, want %d; got = %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("writeValidationLoadTargets(delete)[%d] = %q, want %q; got = %#v", i, got[i], want[i], got)
		}
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
}

func TestForeignKeyCascadeGraphValidationAtDDLTime(t *testing.T) {
	t.Run("create table rejects self referencing cascade cycle atomically", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		if _, err := db.Exec("CREATE TABLE nodes (id INT, parent_id INT, CONSTRAINT pk_nodes PRIMARY KEY (id) USING INDEX idx_nodes_pk, CONSTRAINT fk_nodes_parent FOREIGN KEY (parent_id) REFERENCES nodes (id) USING INDEX idx_nodes_parent ON DELETE CASCADE)"); err == nil || !strings.Contains(err.Error(), "cascade cycle detected") || !strings.Contains(err.Error(), "nodes.fk_nodes_parent") {
			t.Fatalf("Exec(create illegal self-cycle) error = %v, want cascade cycle rejection naming nodes.fk_nodes_parent", err)
		}
		if db.tables["nodes"] != nil {
			t.Fatalf("db.tables[nodes] = %#v, want nil after failed create", db.tables["nodes"])
		}
	})

	t.Run("create table rejects multiple cascade paths atomically", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		mustExec(t, db, "CREATE TABLE a (id INT, CONSTRAINT pk_a PRIMARY KEY (id) USING INDEX idx_a_pk)")
		mustExec(t, db, "CREATE TABLE b (id INT, a_id INT, CONSTRAINT pk_b PRIMARY KEY (id) USING INDEX idx_b_pk, CONSTRAINT fk_b_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_b_a ON DELETE CASCADE)")
		mustExec(t, db, "CREATE TABLE c (id INT, a_id INT, CONSTRAINT pk_c PRIMARY KEY (id) USING INDEX idx_c_pk, CONSTRAINT fk_c_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_c_a ON DELETE CASCADE)")

		if _, err := db.Exec("CREATE TABLE d (id INT, b_id INT, c_id INT, CONSTRAINT pk_d PRIMARY KEY (id) USING INDEX idx_d_pk, CONSTRAINT fk_d_b FOREIGN KEY (b_id) REFERENCES b (id) USING INDEX idx_d_b ON DELETE CASCADE, CONSTRAINT fk_d_c FOREIGN KEY (c_id) REFERENCES c (id) USING INDEX idx_d_c ON DELETE CASCADE)"); err == nil || !strings.Contains(err.Error(), "multiple cascade paths detected") || !strings.Contains(err.Error(), "d.fk_d_c") {
			t.Fatalf("Exec(create illegal diamond) error = %v, want multiple cascade path rejection naming d.fk_d_c", err)
		}
		if db.tables["d"] != nil {
			t.Fatalf("db.tables[d] = %#v, want nil after failed create", db.tables["d"])
		}
	})

	t.Run("create table with one cascade path and separate restrict path succeeds", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		mustExec(t, db, "CREATE TABLE a (id INT, CONSTRAINT pk_a PRIMARY KEY (id) USING INDEX idx_a_pk)")
		mustExec(t, db, "CREATE TABLE b (id INT, a_id INT, CONSTRAINT pk_b PRIMARY KEY (id) USING INDEX idx_b_pk, CONSTRAINT fk_b_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_b_a ON DELETE CASCADE)")
		mustExec(t, db, "CREATE TABLE c (id INT, a_id INT, CONSTRAINT pk_c PRIMARY KEY (id) USING INDEX idx_c_pk, CONSTRAINT fk_c_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_c_a ON DELETE RESTRICT)")
		mustExec(t, db, "CREATE TABLE d (id INT, b_id INT, c_id INT, CONSTRAINT pk_d PRIMARY KEY (id) USING INDEX idx_d_pk, CONSTRAINT fk_d_b FOREIGN KEY (b_id) REFERENCES b (id) USING INDEX idx_d_b ON DELETE CASCADE, CONSTRAINT fk_d_c FOREIGN KEY (c_id) REFERENCES c (id) USING INDEX idx_d_c ON DELETE RESTRICT)")

		if db.tables["d"] == nil || len(db.tables["d"].ForeignKeyDefs) != 2 {
			t.Fatalf("db.tables[d] = %#v, want legal two-fk table", db.tables["d"])
		}
	})

	t.Run("alter table rejects final foreign key that closes cascade cycle", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		mustExec(t, db, "CREATE TABLE a (id INT, b_id INT, CONSTRAINT pk_a PRIMARY KEY (id) USING INDEX idx_a_pk)")
		mustExec(t, db, "CREATE INDEX idx_a_b ON a(b_id)")
		mustExec(t, db, "CREATE TABLE b (id INT, a_id INT, CONSTRAINT pk_b PRIMARY KEY (id) USING INDEX idx_b_pk, CONSTRAINT fk_b_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_b_a ON DELETE CASCADE)")

		if _, err := db.Exec("ALTER TABLE a ADD CONSTRAINT fk_a_b FOREIGN KEY (b_id) REFERENCES b (id) USING INDEX idx_a_b ON DELETE CASCADE"); err == nil || !strings.Contains(err.Error(), "cascade cycle detected") || !strings.Contains(err.Error(), "a.fk_a_b") {
			t.Fatalf("Exec(add cycle-closing fk) error = %v, want cascade cycle rejection naming a.fk_a_b", err)
		}
		if len(db.tables["a"].ForeignKeyDefs) != 0 {
			t.Fatalf("a.ForeignKeyDefs = %#v, want unchanged after failed alter", db.tables["a"].ForeignKeyDefs)
		}
	})

	t.Run("alter table rejects final foreign key that creates multiple cascade paths", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		mustExec(t, db, "CREATE TABLE a (id INT, CONSTRAINT pk_a PRIMARY KEY (id) USING INDEX idx_a_pk)")
		mustExec(t, db, "CREATE TABLE b (id INT, a_id INT, CONSTRAINT pk_b PRIMARY KEY (id) USING INDEX idx_b_pk, CONSTRAINT fk_b_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_b_a ON DELETE CASCADE)")
		mustExec(t, db, "CREATE TABLE c (id INT, a_id INT, CONSTRAINT pk_c PRIMARY KEY (id) USING INDEX idx_c_pk, CONSTRAINT fk_c_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_c_a ON DELETE CASCADE)")
		mustExec(t, db, "CREATE TABLE d (id INT, b_id INT, c_id INT, CONSTRAINT pk_d PRIMARY KEY (id) USING INDEX idx_d_pk, CONSTRAINT fk_d_b FOREIGN KEY (b_id) REFERENCES b (id) USING INDEX idx_d_b ON DELETE CASCADE)")
		mustExec(t, db, "CREATE INDEX idx_d_c ON d(c_id)")

		if _, err := db.Exec("ALTER TABLE d ADD CONSTRAINT fk_d_c FOREIGN KEY (c_id) REFERENCES c (id) USING INDEX idx_d_c ON DELETE CASCADE"); err == nil || !strings.Contains(err.Error(), "multiple cascade paths detected") || !strings.Contains(err.Error(), "d.fk_d_c") {
			t.Fatalf("Exec(add diamond-closing fk) error = %v, want multiple cascade path rejection naming d.fk_d_c", err)
		}
		if len(db.tables["d"].ForeignKeyDefs) != 1 || db.tables["d"].ForeignKeyDefs[0].Name != "fk_d_b" {
			t.Fatalf("d.ForeignKeyDefs = %#v, want unchanged single fk_d_b after failed alter", db.tables["d"].ForeignKeyDefs)
		}
	})

	t.Run("restrict broken cycle and self referencing restrict stay legal", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		mustExec(t, db, "CREATE TABLE a (id INT, b_id INT, CONSTRAINT pk_a PRIMARY KEY (id) USING INDEX idx_a_pk)")
		mustExec(t, db, "CREATE INDEX idx_a_b ON a(b_id)")
		mustExec(t, db, "CREATE TABLE b (id INT, a_id INT, CONSTRAINT pk_b PRIMARY KEY (id) USING INDEX idx_b_pk, CONSTRAINT fk_b_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_b_a ON DELETE CASCADE)")
		mustExec(t, db, "ALTER TABLE a ADD CONSTRAINT fk_a_b FOREIGN KEY (b_id) REFERENCES b (id) USING INDEX idx_a_b ON DELETE RESTRICT")

		mustExec(t, db, "CREATE TABLE nodes (id INT, parent_id INT, CONSTRAINT pk_nodes PRIMARY KEY (id) USING INDEX idx_nodes_pk, CONSTRAINT fk_nodes_parent FOREIGN KEY (parent_id) REFERENCES nodes (id) USING INDEX idx_nodes_parent ON DELETE RESTRICT)")
		if len(db.tables["nodes"].ForeignKeyDefs) != 1 || db.tables["nodes"].ForeignKeyDefs[0].Name != "fk_nodes_parent" {
			t.Fatalf("nodes.ForeignKeyDefs = %#v, want persisted self-referencing restrict fk", db.tables["nodes"].ForeignKeyDefs)
		}
	})
}

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
	var id, teamID int32
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
	var id, teamID, teamShadow int32
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
	var id, teamID int32
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

func TestPrimaryAndForeignKeyMetadataPersistAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, team_id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX idx_teams_pk ON teams (id)"); err != nil {
		t.Fatalf("Exec(create teams pk index) error = %v", err)
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_pk ON users (id)"); err != nil {
		t.Fatalf("Exec(create users pk index) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_team ON users (team_id)"); err != nil {
		t.Fatalf("Exec(create users fk index) error = %v", err)
	}

	staged := cloneTables(db.tables)
	applyPrimaryKeyMetadata(t, staged["teams"], "pk_teams", "idx_teams_pk", "id")
	applyPrimaryKeyMetadata(t, staged["users"], "pk_users", "idx_users_pk", "id")
	applyForeignKeyMetadata(t, staged["users"], staged["teams"], "fk_users_team", "idx_users_team", storage.CatalogForeignKeyDeleteActionCascade, []string{"team_id"}, []string{"id"})
	if err := db.applyStagedCatalogOnly(staged); err != nil {
		t.Fatalf("applyStagedCatalogOnly() error = %v", err)
	}
	db.tables = staged
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	teamsPK := db.tables["teams"].PrimaryKeyDefinition()
	if teamsPK == nil || teamsPK.Name != "pk_teams" || teamsPK.IndexID == 0 || len(teamsPK.Columns) != 1 || teamsPK.Columns[0] != "id" {
		t.Fatalf("teams PrimaryKeyDefinition() = %#v, want pk_teams on id", teamsPK)
	}
	usersPK := db.tables["users"].PrimaryKeyDefinition()
	if usersPK == nil || usersPK.Name != "pk_users" {
		t.Fatalf("users PrimaryKeyDefinition() = %#v, want pk_users", usersPK)
	}
	usersFKs := db.tables["users"].ForeignKeyDefinitions()
	if len(usersFKs) != 1 {
		t.Fatalf("len(users ForeignKeyDefinitions()) = %d, want 1", len(usersFKs))
	}
	if usersFKs[0].OnDeleteAction != storage.CatalogForeignKeyDeleteActionCascade {
		t.Fatalf("users FK OnDeleteAction = %d, want %d", usersFKs[0].OnDeleteAction, storage.CatalogForeignKeyDeleteActionCascade)
	}
	if got := executor.ForeignKeysReferencingTable(db.tables, db.tables["teams"].TableID); len(got) != 1 || got[0].Name != "fk_users_team" {
		t.Fatalf("ForeignKeysReferencingTable() = %#v, want fk_users_team", got)
	}
	if !executor.IndexReferencedByConstraint(db.tables, db.tables["users"].IndexDefinition("idx_users_team").IndexID) {
		t.Fatal("IndexReferencedByConstraint(idx_users_team) = false, want true")
	}
}

func TestOpenRejectsForeignKeyDependingOnMissingParentPrimaryKey(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, team_id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX idx_teams_pk ON teams (id)"); err != nil {
		t.Fatalf("Exec(create teams pk index) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_team ON users (team_id)"); err != nil {
		t.Fatalf("Exec(create users fk index) error = %v", err)
	}

	staged := cloneTables(db.tables)
	applyPrimaryKeyMetadata(t, staged["teams"], "pk_teams", "idx_teams_pk", "id")
	applyForeignKeyMetadata(t, staged["users"], staged["teams"], "fk_users_team", "idx_users_team", storage.CatalogForeignKeyDeleteActionRestrict, []string{"team_id"}, []string{"id"})
	if err := db.applyStagedCatalogOnly(staged); err != nil {
		t.Fatalf("applyStagedCatalogOnly(valid) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	catalog = catalogWithDirectoryRootsForSave(t, rawDB.File(), catalog)
	for i := range catalog.Tables {
		if catalog.Tables[i].Name != "users" || len(catalog.Tables[i].ForeignKeys) == 0 {
			continue
		}
		catalog.Tables[i].ForeignKeys[0].ParentPrimaryKeyName = "missing_pk"
	}
	if err := storage.SaveCatalog(pager, catalog); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	rewriteDirectoryRootMappingsForCatalogTables(t, rawDB.File(), catalog)
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("reopen Open() error = nil, want missing parent PK dependency rejection")
	}
	if !strings.Contains(err.Error(), "constraint/table mismatch") {
		t.Fatalf("reopen Open() error = %v, want constraint/table mismatch", err)
	}
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
	var got any
	if err := db.QueryRow(sql).Scan(&got); err != nil {
		t.Fatalf("QueryRow(%q).Scan() error = %v", sql, err)
	}
	return numericValueToInt(t, got)
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

func applyPrimaryKeyMetadata(t *testing.T, table *executor.Table, constraintName, indexName string, columns ...string) {
	t.Helper()
	if table == nil {
		t.Fatal("applyPrimaryKeyMetadata() requires table")
	}
	indexDef := table.IndexDefinition(indexName)
	if indexDef == nil {
		t.Fatalf("IndexDefinition(%q) = nil", indexName)
	}
	table.PrimaryKeyDef = &storage.CatalogPrimaryKey{
		Name:       constraintName,
		TableID:    table.TableID,
		Columns:    append([]string(nil), columns...),
		IndexID:    indexDef.IndexID,
		ImplicitNN: true,
	}
}

func applyForeignKeyMetadata(t *testing.T, child *executor.Table, parent *executor.Table, constraintName, childIndexName string, onDelete uint8, childColumns, parentColumns []string) {
	t.Helper()
	if child == nil || parent == nil || parent.PrimaryKeyDef == nil {
		t.Fatal("applyForeignKeyMetadata() requires child, parent, and parent PK")
	}
	indexDef := child.IndexDefinition(childIndexName)
	if indexDef == nil {
		t.Fatalf("IndexDefinition(%q) = nil", childIndexName)
	}
	child.ForeignKeyDefs = append(child.ForeignKeyDefs, storage.CatalogForeignKey{
		Name:                 constraintName,
		ChildTableID:         child.TableID,
		ChildColumns:         append([]string(nil), childColumns...),
		ParentTableID:        parent.TableID,
		ParentColumns:        append([]string(nil), parentColumns...),
		ParentPrimaryKeyName: parent.PrimaryKeyDef.Name,
		ChildIndexID:         indexDef.IndexID,
		OnDeleteAction:       onDelete,
	})
}
