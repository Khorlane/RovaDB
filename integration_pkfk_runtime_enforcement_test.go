package rovadb

import (
	"strings"
	"testing"
)

func TestPKFKRuntimeEnforcementPersistsAcrossReopenAndStaysAtomic(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	mustExec(t, db, "CREATE TABLE parents (id INT, CONSTRAINT pk_parents PRIMARY KEY (id) USING INDEX idx_parents_pk)")
	mustExec(t, db, "CREATE TABLE children (id INT, parent_id INT, note TEXT, CONSTRAINT pk_children PRIMARY KEY (id) USING INDEX idx_children_pk, CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id) USING INDEX idx_children_parent ON DELETE RESTRICT)")
	mustExec(t, db, "INSERT INTO parents VALUES (1)")
	mustExec(t, db, "INSERT INTO children VALUES (10, 1, 'seed')")
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if _, err := db.Exec("INSERT INTO parents VALUES (1)"); err == nil || !strings.Contains(err.Error(), "table=parents") || !strings.Contains(err.Error(), "constraint=pk_parents") || !strings.Contains(err.Error(), "type=primary_key_duplicate") {
		t.Fatalf("Exec(insert duplicate parent after reopen) error = %v, want persisted pk enforcement", err)
	}
	if _, err := db.Exec("INSERT INTO children VALUES (11, 99, 'orphan')"); err == nil || !strings.Contains(err.Error(), "table=children") || !strings.Contains(err.Error(), "constraint=fk_children_parent") || !strings.Contains(err.Error(), "type=foreign_key_missing_parent") {
		t.Fatalf("Exec(insert orphan child after reopen) error = %v, want persisted fk enforcement", err)
	}
	if _, err := db.Exec("DELETE FROM parents WHERE id = 1"); err == nil || !strings.Contains(err.Error(), "table=children") || !strings.Contains(err.Error(), "constraint=fk_children_parent") || !strings.Contains(err.Error(), "type=foreign_key_restrict") {
		t.Fatalf("Exec(delete referenced parent after reopen) error = %v, want restrict violation", err)
	}

	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parents WHERE id = 1"); got != 1 {
		t.Fatalf("persisted parent count after failed writes = %d, want 1", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM children WHERE id = 10"); got != 1 {
		t.Fatalf("persisted child count after failed writes = %d, want 1", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM children WHERE id = 11"); got != 0 {
		t.Fatalf("orphan child count after failed insert = %d, want 0", got)
	}
}

func TestCascadeDeletePersistsAcrossReopenAndMixedFailuresStayAtomic(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	mustExec(t, db, "CREATE TABLE parents (id INT, CONSTRAINT pk_parents PRIMARY KEY (id) USING INDEX idx_parents_pk)")
	mustExec(t, db, "CREATE TABLE children (id INT, parent_id INT, CONSTRAINT pk_children PRIMARY KEY (id) USING INDEX idx_children_pk, CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id) USING INDEX idx_children_parent ON DELETE CASCADE)")
	mustExec(t, db, "CREATE TABLE audits (id INT, child_id INT, CONSTRAINT pk_audits PRIMARY KEY (id) USING INDEX idx_audits_pk, CONSTRAINT fk_audits_child FOREIGN KEY (child_id) REFERENCES children (id) USING INDEX idx_audits_child ON DELETE RESTRICT)")
	for _, sql := range []string{
		"INSERT INTO parents VALUES (1)",
		"INSERT INTO parents VALUES (2)",
		"INSERT INTO children VALUES (10, 1)",
		"INSERT INTO children VALUES (20, 2)",
		"INSERT INTO audits VALUES (100, 10)",
	} {
		mustExec(t, db, sql)
	}

	if _, err := db.Exec("DELETE FROM parents WHERE id = 1"); err == nil || !strings.Contains(err.Error(), "table=audits") || !strings.Contains(err.Error(), "constraint=fk_audits_child") || !strings.Contains(err.Error(), "type=foreign_key_restrict") {
		t.Fatalf("Exec(delete mixed restrict/cascade before reopen) error = %v, want restrict violation", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parents WHERE id = 1"); got != 1 {
		t.Fatalf("parent count after failed mixed delete and reopen = %d, want 1", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM children WHERE id = 10"); got != 1 {
		t.Fatalf("child count after failed mixed delete and reopen = %d, want 1", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM audits WHERE id = 100"); got != 1 {
		t.Fatalf("audit count after failed mixed delete and reopen = %d, want 1", got)
	}

	mustExec(t, db, "DELETE FROM audits WHERE id = 100")
	if _, err := db.Exec("DELETE FROM parents WHERE id = 1 OR id = 2"); err != nil {
		t.Fatalf("Exec(delete cascade after clearing restrict rows) error = %v, want nil", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parents"); got != 0 {
		t.Fatalf("parent count after successful cascade and reopen = %d, want 0", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM children"); got != 0 {
		t.Fatalf("child count after successful cascade and reopen = %d, want 0", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM audits"); got != 0 {
		t.Fatalf("audit count after successful cascade and reopen = %d, want 0", got)
	}
}
