package rovadb

import "testing"

func TestIllegalCascadeGraphDDLLeavesSchemaUnchangedAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	mustExec(t, db, "CREATE TABLE a (id INT, CONSTRAINT pk_a PRIMARY KEY (id) USING INDEX idx_a_pk)")
	mustExec(t, db, "CREATE TABLE b (id INT, a_id INT, CONSTRAINT pk_b PRIMARY KEY (id) USING INDEX idx_b_pk, CONSTRAINT fk_b_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_b_a ON DELETE CASCADE)")
	mustExec(t, db, "CREATE TABLE c (id INT, a_id INT, CONSTRAINT pk_c PRIMARY KEY (id) USING INDEX idx_c_pk, CONSTRAINT fk_c_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_c_a ON DELETE CASCADE)")
	mustExec(t, db, "CREATE TABLE d (id INT, b_id INT, c_id INT, CONSTRAINT pk_d PRIMARY KEY (id) USING INDEX idx_d_pk, CONSTRAINT fk_d_b FOREIGN KEY (b_id) REFERENCES b (id) USING INDEX idx_d_b ON DELETE CASCADE)")
	mustExec(t, db, "CREATE INDEX idx_d_c ON d(c_id)")

	if _, err := db.Exec("ALTER TABLE d ADD CONSTRAINT fk_d_c FOREIGN KEY (c_id) REFERENCES c (id) USING INDEX idx_d_c ON DELETE CASCADE"); err == nil {
		t.Fatal("Exec(add illegal multiple path fk) error = nil, want failure")
	}
	if len(db.tables["d"].ForeignKeyDefs) != 1 || db.tables["d"].ForeignKeyDefs[0].Name != "fk_d_b" {
		t.Fatalf("d.ForeignKeyDefs = %#v, want unchanged single fk after failed alter", db.tables["d"].ForeignKeyDefs)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if len(db.tables["d"].ForeignKeyDefs) != 1 || db.tables["d"].ForeignKeyDefs[0].Name != "fk_d_b" {
		t.Fatalf("reopened d.ForeignKeyDefs = %#v, want unchanged single fk after failed alter", db.tables["d"].ForeignKeyDefs)
	}
}

func TestLegalCascadeGraphSchemaPersistsAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	mustExec(t, db, "CREATE TABLE a (id INT, CONSTRAINT pk_a PRIMARY KEY (id) USING INDEX idx_a_pk)")
	mustExec(t, db, "CREATE TABLE b (id INT, a_id INT, CONSTRAINT pk_b PRIMARY KEY (id) USING INDEX idx_b_pk, CONSTRAINT fk_b_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_b_a ON DELETE CASCADE)")
	mustExec(t, db, "CREATE TABLE c (id INT, a_id INT, CONSTRAINT pk_c PRIMARY KEY (id) USING INDEX idx_c_pk, CONSTRAINT fk_c_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_c_a ON DELETE RESTRICT)")
	mustExec(t, db, "CREATE TABLE d (id INT, b_id INT, c_id INT, CONSTRAINT pk_d PRIMARY KEY (id) USING INDEX idx_d_pk, CONSTRAINT fk_d_b FOREIGN KEY (b_id) REFERENCES b (id) USING INDEX idx_d_b ON DELETE CASCADE, CONSTRAINT fk_d_c FOREIGN KEY (c_id) REFERENCES c (id) USING INDEX idx_d_c ON DELETE RESTRICT)")
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if db.tables["d"] == nil || len(db.tables["d"].ForeignKeyDefs) != 2 {
		t.Fatalf("reopened d table = %#v, want persisted legal cascade graph", db.tables["d"])
	}
}
