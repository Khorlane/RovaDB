package rovadb

import "testing"

func TestPKFKMilestoneEndToEndContract(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	for _, sql := range []string{
		"CREATE TABLE parent_create (id INT, name TEXT, CONSTRAINT pk_parent_create PRIMARY KEY (id) USING INDEX idx_parent_create_pk)",
		"CREATE TABLE child_create (id INT, parent_id INT, note TEXT, CONSTRAINT pk_child_create PRIMARY KEY (id) USING INDEX idx_child_create_pk, CONSTRAINT fk_child_create_parent FOREIGN KEY (parent_id) REFERENCES parent_create (id) USING INDEX idx_child_create_parent ON DELETE RESTRICT)",
		"CREATE TABLE parent_alter (id INT, region_id INT, label TEXT)",
		"CREATE UNIQUE INDEX idx_parent_alter_pk ON parent_alter (id, region_id)",
		"ALTER TABLE parent_alter ADD CONSTRAINT pk_parent_alter PRIMARY KEY (id, region_id) USING INDEX idx_parent_alter_pk",
		"CREATE TABLE child_alter (id INT, parent_id INT, region_id INT, note TEXT)",
		"CREATE INDEX idx_child_alter_parent ON child_alter (parent_id, region_id, note)",
		"ALTER TABLE child_alter ADD CONSTRAINT fk_child_alter_parent FOREIGN KEY (parent_id, region_id) REFERENCES parent_alter (id, region_id) USING INDEX idx_child_alter_parent ON DELETE CASCADE",
		"INSERT INTO parent_create VALUES (1, 'alpha')",
		"INSERT INTO child_create VALUES (10, 1, 'restrict-row')",
		"INSERT INTO parent_alter VALUES (7, 70, 'west')",
		"INSERT INTO child_alter VALUES (20, 7, 70, 'cascade-row')",
	} {
		mustExec(t, db, sql)
	}

	if _, err := db.Exec("INSERT INTO parent_create VALUES (1, 'duplicate')"); err == nil || !isConstraintError(err, "table=parent_create", "constraint=pk_parent_create", "type=primary_key_duplicate") {
		t.Fatalf("Exec(insert duplicate pk row) error = %v, want primary key duplicate violation", err)
	}
	if _, err := db.Exec("UPDATE parent_create SET id = 2 WHERE id = 1"); err == nil || !isConstraintError(err, "table=parent_create", "constraint=pk_parent_create", "type=primary_key_update_forbidden") {
		t.Fatalf("Exec(update pk column) error = %v, want primary key update forbidden violation", err)
	}
	if got := mustQueryInt(t, db, "SELECT id FROM parent_create WHERE name = 'alpha'"); got != 1 {
		t.Fatalf("parent_create id after failed pk update = %d, want 1", got)
	}

	if _, err := db.Exec("INSERT INTO child_create VALUES (11, 99, 'orphan')"); err == nil || !isConstraintError(err, "table=child_create", "constraint=fk_child_create_parent", "type=foreign_key_missing_parent") {
		t.Fatalf("Exec(insert orphan child) error = %v, want foreign key missing parent violation", err)
	}
	if _, err := db.Exec("UPDATE child_create SET parent_id = 99 WHERE id = 10"); err == nil || !isConstraintError(err, "table=child_create", "constraint=fk_child_create_parent", "type=foreign_key_missing_parent") {
		t.Fatalf("Exec(update child to missing parent) error = %v, want foreign key missing parent violation", err)
	}
	if got := mustQueryInt(t, db, "SELECT parent_id FROM child_create WHERE id = 10"); got != 1 {
		t.Fatalf("child_create parent_id after failed fk update = %d, want 1", got)
	}

	if _, err := db.Exec("DELETE FROM parent_create WHERE id = 1"); err == nil || !isConstraintError(err, "table=child_create", "constraint=fk_child_create_parent", "type=foreign_key_restrict") {
		t.Fatalf("Exec(delete restricted parent) error = %v, want foreign key restrict violation", err)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parent_create WHERE id = 1"); got != 1 {
		t.Fatalf("parent_create count after failed restrict delete = %d, want 1", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM child_create WHERE id = 10"); got != 1 {
		t.Fatalf("child_create count after failed restrict delete = %d, want 1", got)
	}

	if _, err := db.Exec("DELETE FROM parent_alter WHERE id = 7 AND region_id = 70"); err != nil {
		t.Fatalf("Exec(delete cascading parent) error = %v, want nil", err)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parent_alter"); got != 0 {
		t.Fatalf("parent_alter count after cascade delete = %d, want 0", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM child_alter"); got != 0 {
		t.Fatalf("child_alter count after cascade delete = %d, want 0", got)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if db.tables["parent_create"] == nil || db.tables["parent_create"].PrimaryKeyDef == nil || db.tables["parent_create"].PrimaryKeyDef.Name != "pk_parent_create" {
		t.Fatalf("reopened parent_create.PrimaryKeyDef = %#v, want pk_parent_create", db.tables["parent_create"].PrimaryKeyDef)
	}
	if db.tables["child_create"] == nil || len(db.tables["child_create"].ForeignKeyDefs) != 1 || db.tables["child_create"].ForeignKeyDefs[0].Name != "fk_child_create_parent" {
		t.Fatalf("reopened child_create.ForeignKeyDefs = %#v, want fk_child_create_parent", db.tables["child_create"].ForeignKeyDefs)
	}
	if db.tables["parent_alter"] == nil || db.tables["parent_alter"].PrimaryKeyDef == nil || db.tables["parent_alter"].PrimaryKeyDef.Name != "pk_parent_alter" {
		t.Fatalf("reopened parent_alter.PrimaryKeyDef = %#v, want pk_parent_alter", db.tables["parent_alter"].PrimaryKeyDef)
	}
	if db.tables["child_alter"] == nil || len(db.tables["child_alter"].ForeignKeyDefs) != 1 || db.tables["child_alter"].ForeignKeyDefs[0].Name != "fk_child_alter_parent" {
		t.Fatalf("reopened child_alter.ForeignKeyDefs = %#v, want fk_child_alter_parent", db.tables["child_alter"].ForeignKeyDefs)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM child_alter"); got != 0 {
		t.Fatalf("reopened child_alter count = %d, want 0 after persisted cascade delete", got)
	}
	if _, err := db.Exec("INSERT INTO child_create VALUES (12, 1, 'post-reopen')"); err != nil {
		t.Fatalf("Exec(insert valid child after reopen) error = %v", err)
	}
}

func TestPKFKMilestoneDestructiveDDLContract(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	for _, sql := range []string{
		"CREATE TABLE drop_parent (id INT, CONSTRAINT pk_drop_parent PRIMARY KEY (id) USING INDEX idx_drop_parent_pk)",
		"CREATE TABLE drop_child (id INT, parent_id INT, CONSTRAINT pk_drop_child PRIMARY KEY (id) USING INDEX idx_drop_child_pk, CONSTRAINT fk_drop_child_parent FOREIGN KEY (parent_id) REFERENCES drop_parent (id) USING INDEX idx_drop_child_parent ON DELETE RESTRICT)",
		"INSERT INTO drop_parent VALUES (1)",
		"INSERT INTO drop_child VALUES (10, 1)",
		"CREATE TABLE gone_parent (id INT, CONSTRAINT pk_gone_parent PRIMARY KEY (id) USING INDEX idx_gone_parent_pk)",
		"CREATE TABLE survive_child (id INT, parent_id INT, note TEXT, CONSTRAINT fk_survive_child_parent FOREIGN KEY (parent_id) REFERENCES gone_parent (id) USING INDEX idx_survive_child_parent ON DELETE CASCADE)",
		"CREATE INDEX idx_survive_child_note ON survive_child (note)",
		"INSERT INTO gone_parent VALUES (5)",
		"INSERT INTO survive_child VALUES (50, 5, 'kept-index')",
	} {
		mustExec(t, db, sql)
	}

	if _, err := db.Exec("DROP INDEX idx_drop_parent_pk"); err == nil || err.Error() != "execution: index required by constraint: table=drop_parent index=idx_drop_parent_pk constraint=pk_drop_parent type=primary_key" {
		t.Fatalf("Exec(drop pk index) error = %v, want deterministic blocking error", err)
	}
	if _, err := db.Exec("DROP INDEX idx_drop_child_parent"); err == nil || err.Error() != "execution: index required by constraint: table=drop_child index=idx_drop_child_parent constraint=fk_drop_child_parent type=foreign_key" {
		t.Fatalf("Exec(drop fk index) error = %v, want deterministic blocking error", err)
	}

	mustExec(t, db, "ALTER TABLE drop_parent DROP PRIMARY KEY")
	if db.tables["drop_parent"].PrimaryKeyDef != nil {
		t.Fatalf("drop_parent.PrimaryKeyDef = %#v, want nil after drop", db.tables["drop_parent"].PrimaryKeyDef)
	}
	if len(db.tables["drop_child"].ForeignKeyDefs) != 0 {
		t.Fatalf("drop_child.ForeignKeyDefs = %#v, want dependent fks removed", db.tables["drop_child"].ForeignKeyDefs)
	}
	if db.tables["drop_parent"].IndexDefinition("idx_drop_parent_pk") == nil || db.tables["drop_child"].IndexDefinition("idx_drop_child_parent") == nil {
		t.Fatalf("supporting indexes should remain after drop primary key")
	}

	mustExec(t, db, "DROP TABLE gone_parent")
	if db.tables["gone_parent"] != nil {
		t.Fatalf("db.tables[gone_parent] = %#v, want nil", db.tables["gone_parent"])
	}
	if len(db.tables["survive_child"].ForeignKeyDefs) != 0 {
		t.Fatalf("survive_child.ForeignKeyDefs = %#v, want dependent fks removed after drop table", db.tables["survive_child"].ForeignKeyDefs)
	}
	if db.tables["survive_child"].IndexDefinition("idx_survive_child_parent") == nil || db.tables["survive_child"].IndexDefinition("idx_survive_child_note") == nil {
		t.Fatalf("surviving child indexes should remain after drop table")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if db.tables["drop_parent"] == nil || db.tables["drop_parent"].PrimaryKeyDef != nil {
		t.Fatalf("reopened drop_parent.PrimaryKeyDef = %#v, want nil", db.tables["drop_parent"].PrimaryKeyDef)
	}
	if db.tables["drop_child"] == nil || len(db.tables["drop_child"].ForeignKeyDefs) != 0 {
		t.Fatalf("reopened drop_child.ForeignKeyDefs = %#v, want empty", db.tables["drop_child"].ForeignKeyDefs)
	}
	if db.tables["gone_parent"] != nil {
		t.Fatalf("reopened db.tables[gone_parent] = %#v, want nil", db.tables["gone_parent"])
	}
	if db.tables["survive_child"] == nil || len(db.tables["survive_child"].ForeignKeyDefs) != 0 {
		t.Fatalf("reopened survive_child.ForeignKeyDefs = %#v, want empty", db.tables["survive_child"].ForeignKeyDefs)
	}
	if db.tables["survive_child"].IndexDefinition("idx_survive_child_parent") == nil || db.tables["survive_child"].IndexDefinition("idx_survive_child_note") == nil {
		t.Fatalf("reopened surviving child indexes should remain after drop table")
	}
}
