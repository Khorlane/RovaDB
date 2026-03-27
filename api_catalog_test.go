package rovadb

import (
	"errors"
	"testing"
)

func TestListTablesReturnsExpectedTableNames(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE teams (id INT, active BOOL)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	tables, err := db.ListTables()
	if err != nil {
		t.Fatalf("ListTables() error = %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("len(ListTables()) = %d, want 2", len(tables))
	}
	if tables[0].Name != "teams" || tables[1].Name != "users" {
		t.Fatalf("ListTables() names = [%q %q], want [\"teams\" \"users\"]", tables[0].Name, tables[1].Name)
	}
}

func TestGetTableSchemaReturnsColumnDefinitions(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, active BOOL, score REAL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	table, err := db.GetTableSchema("users")
	if err != nil {
		t.Fatalf("GetTableSchema() error = %v", err)
	}
	if table.Name != "users" {
		t.Fatalf("GetTableSchema().Name = %q, want %q", table.Name, "users")
	}
	if len(table.Columns) != 4 {
		t.Fatalf("len(GetTableSchema().Columns) = %d, want 4", len(table.Columns))
	}

	want := []ColumnInfo{
		{Name: "id", Type: "INT"},
		{Name: "name", Type: "TEXT"},
		{Name: "active", Type: "BOOL"},
		{Name: "score", Type: "REAL"},
	}
	for i := range want {
		if table.Columns[i] != want[i] {
			t.Fatalf("GetTableSchema().Columns[%d] = %#v, want %#v", i, table.Columns[i], want[i])
		}
	}
}

func TestGetTableSchemaUnknownTableReturnsError(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	_, err = db.GetTableSchema("missing")
	if err == nil {
		t.Fatal("GetTableSchema(missing) error = nil, want error")
	}
	if err.Error() != "table not found: missing" {
		t.Fatalf("GetTableSchema(missing) error = %v, want %q", err, "table not found: missing")
	}
}

func TestCatalogIntrospectionWorksAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE flags (id INT, active BOOL)"); err != nil {
		t.Fatalf("Exec(create flags) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	tables, err := db.ListTables()
	if err != nil {
		t.Fatalf("ListTables() after reopen error = %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("len(ListTables() after reopen) = %d, want 2", len(tables))
	}

	table, err := db.GetTableSchema("FLAGS")
	if err != nil {
		t.Fatalf("GetTableSchema() after reopen error = %v", err)
	}
	if table.Name != "flags" {
		t.Fatalf("GetTableSchema().Name after reopen = %q, want %q", table.Name, "flags")
	}
	if len(table.Columns) != 2 {
		t.Fatalf("len(GetTableSchema().Columns after reopen) = %d, want 2", len(table.Columns))
	}
	if table.Columns[1] != (ColumnInfo{Name: "active", Type: "BOOL"}) {
		t.Fatalf("GetTableSchema().Columns[1] after reopen = %#v, want %#v", table.Columns[1], ColumnInfo{Name: "active", Type: "BOOL"})
	}
}

func TestCatalogIntrospectionOnClosedDBReturnsErrClosed(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := db.ListTables(); !errors.Is(err, ErrClosed) {
		t.Fatalf("ListTables() error = %v, want ErrClosed", err)
	}
	if _, err := db.GetTableSchema("users"); !errors.Is(err, ErrClosed) {
		t.Fatalf("GetTableSchema() error = %v, want ErrClosed", err)
	}
}
