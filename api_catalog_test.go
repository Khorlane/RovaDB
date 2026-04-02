package rovadb

import (
	"errors"
	"testing"

	"github.com/Khorlane/RovaDB/internal/executor"
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
	if _, err := db.SchemaDigest(); !errors.Is(err, ErrClosed) {
		t.Fatalf("SchemaDigest() error = %v, want ErrClosed", err)
	}
}

func TestSchemaDigestTracksLogicalSchemaChanges(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	emptyDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest(empty) error = %v", err)
	}
	repeatedEmptyDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest(repeat empty) error = %v", err)
	}
	if emptyDigest != repeatedEmptyDigest {
		t.Fatalf("empty digest mismatch: %q vs %q", emptyDigest, repeatedEmptyDigest)
	}

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	tableDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest(after create table) error = %v", err)
	}
	assertDigestChanged(t, emptyDigest, tableDigest)

	if _, err := db.Exec("ALTER TABLE users ADD COLUMN active INT"); err != nil {
		t.Fatalf("Exec(alter users) error = %v", err)
	}
	columnDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest(after add column) error = %v", err)
	}
	assertDigestChanged(t, tableDigest, columnDigest)

	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	indexDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest(after create index) error = %v", err)
	}
	assertDigestChanged(t, columnDigest, indexDigest)

	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	dropIndexDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest(after drop index) error = %v", err)
	}
	assertDigestChanged(t, indexDigest, dropIndexDigest)
	if dropIndexDigest != columnDigest {
		t.Fatalf("digest after drop index = %q, want %q", dropIndexDigest, columnDigest)
	}

	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop users) error = %v", err)
	}
	dropTableDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest(after drop table) error = %v", err)
	}
	if dropTableDigest != emptyDigest {
		t.Fatalf("digest after drop table = %q, want %q", dropTableDigest, emptyDigest)
	}
}

func TestSchemaDigestPreservedAcrossReopenAndIgnoresSystemTables(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	beforeReopen, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest(before reopen) error = %v", err)
	}
	if got, want := string(schemaDigestPayload(db.tables)), string(schemaDigestPayload(userOnlyTablesForDigestTest(db.tables))); got != want {
		t.Fatalf("schemaDigestPayload() included system tables:\ngot  %q\nwant %q", got, want)
	}
	if _, ok := findPublicTableInfo(db.tables, systemTableTables); ok {
		t.Fatal("findPublicTableInfo(system table) = true, want false")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	afterReopen, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest(after reopen) error = %v", err)
	}
	if beforeReopen != afterReopen {
		t.Fatalf("reopen digest mismatch: %q vs %q", beforeReopen, afterReopen)
	}
	if got, want := string(schemaDigestPayload(db.tables)), string(schemaDigestPayload(userOnlyTablesForDigestTest(db.tables))); got != want {
		t.Fatalf("schemaDigestPayload() after reopen included system tables:\ngot  %q\nwant %q", got, want)
	}

	if _, ok := findPublicTableInfo(db.tables, systemTableTables); ok {
		t.Fatal("findPublicTableInfo(system table after reopen) = true, want false")
	}
}

func assertDigestChanged(t *testing.T, before, after string) {
	t.Helper()
	if before == after {
		t.Fatalf("digest did not change: %q", before)
	}
}

func userOnlyTablesForDigestTest(tables map[string]*executor.Table) map[string]*executor.Table {
	filtered := make(map[string]*executor.Table)
	for name, table := range tables {
		if table == nil || table.IsSystem {
			continue
		}
		filtered[name] = table
	}
	return filtered
}
