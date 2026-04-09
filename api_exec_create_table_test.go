package rovadb

import (
	"testing"
)

func TestExecCreateTable(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	result, err := db.Exec("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("Exec() error = %v", err)
	}
	if result.RowsAffected() != 0 {
		t.Fatalf("Exec().RowsAffected() = %d, want 0", result.RowsAffected())
	}
	if db.tables == nil || db.tables["users"] == nil {
		t.Fatal("Exec() did not create users table")
	}
}

func TestExecCreateTableBoolSchema(t *testing.T) {
	path := testDBPath(t)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if _, err := db.Exec("CREATE TABLE flags (id INT, name TEXT, active BOOL)"); err != nil {
		t.Fatalf("Exec() error = %v", err)
	}
	if db.tables == nil || db.tables["flags"] == nil {
		t.Fatal("Exec() did not create flags table")
	}

	got := db.tables["flags"].Columns
	want := []struct {
		name string
		typ  string
	}{
		{name: "id", typ: "INT"},
		{name: "name", typ: "TEXT"},
		{name: "active", typ: "BOOL"},
	}
	if len(got) != len(want) {
		t.Fatalf("len(db.tables[\"flags\"].Columns) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Name != want[i].name || got[i].Type != want[i].typ {
			t.Fatalf("db.tables[\"flags\"].Columns[%d] = %#v, want name=%q type=%q", i, got[i], want[i].name, want[i].typ)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer reopened.Close()

	reloaded := reopened.tables["flags"].Columns
	if len(reloaded) != len(want) {
		t.Fatalf("len(reopened.tables[\"flags\"].Columns) = %d, want %d", len(reloaded), len(want))
	}
	for i := range want {
		if reloaded[i].Name != want[i].name || reloaded[i].Type != want[i].typ {
			t.Fatalf("reopened.tables[\"flags\"].Columns[%d] = %#v, want name=%q type=%q", i, reloaded[i], want[i].name, want[i].typ)
		}
	}
}

func TestExecCreateTableDuplicate(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("first Exec() error = %v", err)
	}

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err == nil {
		t.Fatal("second Exec() error = nil, want duplicate table error")
	}
}
