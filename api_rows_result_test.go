package rovadb

import "testing"

func TestRowsColumnsReturnsCopyForLiteralSelect(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	first := rows.Columns()
	if first != nil {
		t.Fatalf("Columns() = %#v, want nil for literal select", first)
	}
}

func TestRowsColumnsReturnsCopyForTableSelect(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	first := rows.Columns()
	if len(first) != 2 || first[0] != "id" || first[1] != "name" {
		t.Fatalf("Columns() = %#v, want [id name]", first)
	}

	first[0] = "mutated"
	second := rows.Columns()
	if len(second) != 2 || second[0] != "id" || second[1] != "name" {
		t.Fatalf("Columns() after mutation = %#v, want [id name]", second)
	}
}

func TestRowsColumnsEmptyResultAfterCloseAndExhaustion(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	rows, err := db.Query("SELECT id FROM users WHERE id = 999")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if got := rows.Columns(); len(got) != 1 || got[0] != "id" {
		t.Fatalf("Columns() on empty result = %#v, want [id]", got)
	}
	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if got := rows.Columns(); len(got) != 1 || got[0] != "id" {
		t.Fatalf("Columns() after exhaustion = %#v, want [id]", got)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := rows.Columns(); len(got) != 1 || got[0] != "id" {
		t.Fatalf("Columns() after Close = %#v, want [id]", got)
	}
}

func TestRowsColumnsNilReceiver(t *testing.T) {
	var rows *Rows
	if got := rows.Columns(); got != nil {
		t.Fatalf("nil Rows Columns() = %#v, want nil", got)
	}
}

func TestResultRowsAffectedHelpers(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	createResult, err := db.Exec("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if got := createResult.RowsAffected(); got != 0 {
		t.Fatalf("create RowsAffected() = %d, want 0", got)
	}

	insertResult, err := db.Exec("INSERT INTO users VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if got := insertResult.RowsAffected(); got != 1 {
		t.Fatalf("insert RowsAffected() = %d, want 1", got)
	}

	updateResult, err := db.Exec("UPDATE users SET name = 'bob' WHERE id = 1")
	if err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if got := updateResult.RowsAffected(); got != 1 {
		t.Fatalf("update RowsAffected() = %d, want 1", got)
	}

	deleteResult, err := db.Exec("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	if got := deleteResult.RowsAffected(); got != 1 {
		t.Fatalf("delete RowsAffected() = %d, want 1", got)
	}

	alterResult, err := db.Exec("ALTER TABLE users ADD COLUMN age INT")
	if err != nil {
		t.Fatalf("Exec(alter) error = %v", err)
	}
	if got := alterResult.RowsAffected(); got != 0 {
		t.Fatalf("alter RowsAffected() = %d, want 0", got)
	}
}
