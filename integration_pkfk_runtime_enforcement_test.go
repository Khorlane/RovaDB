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
