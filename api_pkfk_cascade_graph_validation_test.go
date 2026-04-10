package rovadb

import (
	"strings"
	"testing"
)

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
