package rovadb

import (
	"encoding/binary"
	"errors"
	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
	"github.com/Khorlane/RovaDB/internal/storage"
	"os"
	"strings"
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

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT DEFAULT 'ready', active BOOL NOT NULL, score REAL NOT NULL DEFAULT 1.25)"); err != nil {
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
		{Name: "name", Type: "TEXT", HasDefault: true, DefaultValue: "ready"},
		{Name: "active", Type: "BOOL", NotNull: true},
		{Name: "score", Type: "REAL", NotNull: true, HasDefault: true, DefaultValue: 1.25},
	}
	for i := range want {
		if table.Columns[i] != want[i] {
			t.Fatalf("GetTableSchema().Columns[%d] = %#v, want %#v", i, table.Columns[i], want[i])
		}
	}
}

func TestGetTableSchemaPreservesDeclaredIntegerWidths(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE numbers (small_col SMALLINT, int_col INT, big_col BIGINT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	table, err := db.GetTableSchema("numbers")
	if err != nil {
		t.Fatalf("GetTableSchema() error = %v", err)
	}

	want := []ColumnInfo{
		{Name: "small_col", Type: "SMALLINT"},
		{Name: "int_col", Type: "INT"},
		{Name: "big_col", Type: "BIGINT"},
	}
	if len(table.Columns) != len(want) {
		t.Fatalf("len(GetTableSchema().Columns) = %d, want %d", len(table.Columns), len(want))
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
	if _, err := db.Exec("CREATE TABLE flags (id INT, active BOOL NOT NULL DEFAULT TRUE)"); err != nil {
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
	if table.Columns[1] != (ColumnInfo{Name: "active", Type: "BOOL", NotNull: true, HasDefault: true, DefaultValue: true}) {
		t.Fatalf("GetTableSchema().Columns[1] after reopen = %#v, want %#v", table.Columns[1], ColumnInfo{Name: "active", Type: "BOOL", NotNull: true, HasDefault: true, DefaultValue: true})
	}
}

func TestCatalogIntrospectionPreservesDeclaredIntegerWidthsAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if _, err := db.Exec("CREATE TABLE numbers (small_col SMALLINT, int_col INT, big_col BIGINT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create numbers) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	table, err := db.GetTableSchema("numbers")
	if err != nil {
		t.Fatalf("GetTableSchema() after reopen error = %v", err)
	}
	want := []ColumnInfo{
		{Name: "small_col", Type: "SMALLINT"},
		{Name: "int_col", Type: "INT"},
		{Name: "big_col", Type: "BIGINT"},
		{Name: "name", Type: "TEXT"},
	}
	if len(table.Columns) != len(want) {
		t.Fatalf("len(GetTableSchema().Columns after reopen) = %d, want %d", len(table.Columns), len(want))
	}
	for i := range want {
		if table.Columns[i] != want[i] {
			t.Fatalf("GetTableSchema().Columns[%d] after reopen = %#v, want %#v", i, table.Columns[i], want[i])
		}
	}

	rows, err := db.Query("SELECT table_id FROM sys_tables WHERE table_name = 'numbers'")
	if err != nil {
		t.Fatalf("Query(sys_tables) error = %v", err)
	}
	if !rows.Next() {
		t.Fatal("sys_tables lookup for numbers returned no rows")
	}
	var tableID int32
	if err := rows.Scan(&tableID); err != nil {
		t.Fatalf("Scan(sys_tables) error = %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Rows.Close(sys_tables) = %v", err)
	}

	rows, err = db.Query("SELECT column_name, column_type FROM sys_tb_columns WHERE table_id = ? ORDER BY ordinal_position", tableID)
	if err != nil {
		t.Fatalf("Query(sys_tb_columns) error = %v", err)
	}
	defer rows.Close()

	var gotTypes []string
	for rows.Next() {
		var columnName string
		var columnType string
		if err := rows.Scan(&columnName, &columnType); err != nil {
			t.Fatalf("Scan(sys_tb_columns) error = %v", err)
		}
		gotTypes = append(gotTypes, columnName+":"+columnType)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err() = %v", err)
	}
	wantTypes := []string{
		"small_col:SMALLINT",
		"int_col:INT",
		"big_col:BIGINT",
		"name:TEXT",
	}
	if strings.Join(gotTypes, ",") != strings.Join(wantTypes, ",") {
		t.Fatalf("sys_tb_columns declared types = %v, want %v", gotTypes, wantTypes)
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
	if _, err := db.SchemaDigestFromSystemCatalog(); !errors.Is(err, ErrClosed) {
		t.Fatalf("SchemaDigestFromSystemCatalog() error = %v, want ErrClosed", err)
	}
	if err := db.VerifySystemCatalogDigest(); !errors.Is(err, ErrClosed) {
		t.Fatalf("VerifySystemCatalogDigest() error = %v, want ErrClosed", err)
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
	assertSchemaDigestMethodsMatch(t, db, emptyDigest)
	assertSystemCatalogDigestVerificationOK(t, db)
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
	assertSchemaDigestMethodsMatch(t, db, tableDigest)
	assertSystemCatalogDigestVerificationOK(t, db)
	assertDigestChanged(t, emptyDigest, tableDigest)

	if _, err := db.Exec("ALTER TABLE users ADD COLUMN active INT"); err != nil {
		t.Fatalf("Exec(alter users) error = %v", err)
	}
	columnDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest(after add column) error = %v", err)
	}
	assertSchemaDigestMethodsMatch(t, db, columnDigest)
	assertSystemCatalogDigestVerificationOK(t, db)
	assertDigestChanged(t, tableDigest, columnDigest)

	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	indexDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest(after create index) error = %v", err)
	}
	assertSchemaDigestMethodsMatch(t, db, indexDigest)
	assertSystemCatalogDigestVerificationOK(t, db)
	assertDigestChanged(t, columnDigest, indexDigest)

	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	dropIndexDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest(after drop index) error = %v", err)
	}
	assertSchemaDigestMethodsMatch(t, db, dropIndexDigest)
	assertSystemCatalogDigestVerificationOK(t, db)
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
	assertSchemaDigestMethodsMatch(t, db, dropTableDigest)
	assertSystemCatalogDigestVerificationOK(t, db)
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
	assertSchemaDigestMethodsMatch(t, db, beforeReopen)
	assertSystemCatalogDigestVerificationOK(t, db)
	if got, want := string(schemaDigestPayloadFromTables(db.tables)), string(schemaDigestPayloadFromTables(userOnlyTablesForDigestTest(db.tables))); got != want {
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
	assertSchemaDigestMethodsMatch(t, db, afterReopen)
	assertSystemCatalogDigestVerificationOK(t, db)
	if beforeReopen != afterReopen {
		t.Fatalf("reopen digest mismatch: %q vs %q", beforeReopen, afterReopen)
	}
	if got, want := string(schemaDigestPayloadFromTables(db.tables)), string(schemaDigestPayloadFromTables(userOnlyTablesForDigestTest(db.tables))); got != want {
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

func assertSchemaDigestMethodsMatch(t *testing.T, db *DB, want string) {
	t.Helper()
	got, err := db.SchemaDigestFromSystemCatalog()
	if err != nil {
		t.Fatalf("SchemaDigestFromSystemCatalog() error = %v", err)
	}
	if got != want {
		t.Fatalf("SchemaDigestFromSystemCatalog() = %q, want %q", got, want)
	}
}

func assertSystemCatalogDigestVerificationOK(t *testing.T, db *DB) {
	t.Helper()
	if err := db.VerifySystemCatalogDigest(); err != nil {
		t.Fatalf("VerifySystemCatalogDigest() error = %v", err)
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

func TestExecAPICreateIndexSingleColumnPersistsAndSupportsQueryPath(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	result, err := db.Exec("CREATE INDEX idx_users_name ON users (name)")
	if err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if result.RowsAffected() != 0 {
		t.Fatalf("RowsAffected() = %d, want 0", result.RowsAffected())
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := table.IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil, want non-nil (defs=%#v)", table.IndexDefs)
	}
	if indexDef.RootPageID == 0 {
		t.Fatal("IndexDefinition(idx_users_name).RootPageID = 0, want nonzero")
	}
	if indexDef.IndexID == 0 {
		t.Fatal("IndexDefinition(idx_users_name).IndexID = 0, want nonzero")
	}
	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice' ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if got := collectIntRowsFromRows(t, rows); len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("alice ids = %#v, want []int{1, 3}", got)
	}
}

func TestExecAPICreateIndexRejectsExistingOversizedIndexedText(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tooLarge := strings.Repeat("a", 513)
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("INSERT INTO users VALUES (?, ?)", int32(1), tooLarge); err != nil {
		t.Fatalf("Exec(insert oversized plain text before indexing) error = %v", err)
	}

	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err == nil || err.Error() != "execution: indexed TEXT column value exceeds 512-byte limit" {
		t.Fatalf("Exec(create index oversized text) error = %v, want %q", err, "execution: indexed TEXT column value exceeds 512-byte limit")
	}
}

func TestExecAPICreateUniqueIndexRejectsExistingOversizedIndexedText(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tooLarge := strings.Repeat("b", 513)
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (?, ?)", int32(1), tooLarge); err != nil {
		t.Fatalf("Exec(insert oversized plain text before indexing) error = %v", err)
	}

	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_name ON users (name)"); err == nil || err.Error() != "execution: indexed TEXT column value exceeds 512-byte limit" {
		t.Fatalf("Exec(create unique index oversized text) error = %v, want %q", err, "execution: indexed TEXT column value exceeds 512-byte limit")
	}
}

func TestExecAPICreateIndexRejectsExistingOversizedMultiColumnIndexedText(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tooLarge := strings.Repeat("c", 513)
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, bio TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", int32(1), "alice", tooLarge); err != nil {
		t.Fatalf("Exec(insert oversized plain text before indexing) error = %v", err)
	}

	if _, err := db.Exec("CREATE INDEX idx_users_name_bio ON users (name, bio DESC)"); err == nil || err.Error() != "execution: indexed TEXT column value exceeds 512-byte limit" {
		t.Fatalf("Exec(create multi-column index oversized text) error = %v, want %q", err, "execution: indexed TEXT column value exceeds 512-byte limit")
	}
}

func TestExecAPICreateIndexAllowsIndexedTextAt512Bytes(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	atLimit := strings.Repeat("d", 512)
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (?, ?)", int32(1), atLimit); err != nil {
		t.Fatalf("Exec(insert at limit) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index at limit) error = %v", err)
	}
}

func TestExecAPICreateIndexPersistsRichDefinitionWithoutActivatingLegacyIndex(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, score INT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	result, err := db.Exec("CREATE INDEX idx_users_name_score ON users (name ASC, score DESC)")
	if err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if result.RowsAffected() != 0 {
		t.Fatalf("RowsAffected() = %d, want 0", result.RowsAffected())
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := table.IndexDefinition("idx_users_name_score")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(idx_users_name_score) = nil, defs=%#v", table.IndexDefs)
	}
	if indexDef.RootPageID == 0 {
		t.Fatal("IndexDefinition(idx_users_name_score).RootPageID = 0, want nonzero")
	}
	if len(indexDef.Columns) != 2 || indexDef.Columns[0].Name != "name" || indexDef.Columns[1].Name != "score" || !indexDef.Columns[1].Desc {
		t.Fatalf("indexDef.Columns = %#v, want [name score DESC]", indexDef.Columns)
	}
}

func TestExecAPICreateIndexRejectsDuplicateNameAcrossDatabase(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE teams (id INT, name TEXT)",
		"CREATE INDEX idx_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("CREATE INDEX idx_name ON teams (name)"); err == nil || err.Error() != "execution: index already exists" {
		t.Fatalf("Exec(duplicate name) error = %v, want %q", err, "execution: index already exists")
	}
}

func TestExecAPICreateIndexAllocatesDistinctRootPages(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, email TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create first index) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_email ON users (email)"); err != nil {
		t.Fatalf("Exec(create second index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	nameIndex := table.IndexDefinition("idx_users_name")
	if nameIndex == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil, defs=%#v", table.IndexDefs)
	}
	emailIndex := table.IndexDefinition("idx_users_email")
	if emailIndex == nil {
		t.Fatalf("IndexDefinition(idx_users_email) = nil, defs=%#v", table.IndexDefs)
	}
	if nameIndex.RootPageID == 0 || emailIndex.RootPageID == 0 {
		t.Fatalf("root pages = (%d, %d), want both nonzero", nameIndex.RootPageID, emailIndex.RootPageID)
	}
	if nameIndex.RootPageID == emailIndex.RootPageID {
		t.Fatalf("root pages = (%d, %d), want distinct values", nameIndex.RootPageID, emailIndex.RootPageID)
	}
}

func TestExecAPICreateIndexRejectsEquivalentDefinition(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_a ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("CREATE INDEX idx_b ON users (name ASC)"); err == nil || err.Error() != "execution: equivalent index already exists" {
		t.Fatalf("Exec(equivalent index) error = %v, want %q", err, "execution: equivalent index already exists")
	}
}

func TestExecAPICreateIndexRejectsMissingTableOrColumn(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}

	if _, err := db.Exec("CREATE INDEX idx_missing_table ON teams (name)"); err == nil || err.Error() != "execution: table not found: teams" {
		t.Fatalf("Exec(missing table) error = %v, want %q", err, "execution: table not found: teams")
	}
	if _, err := db.Exec("CREATE INDEX idx_missing_column ON users (email)"); err == nil || err.Error() != "execution: column not found" {
		t.Fatalf("Exec(missing column) error = %v, want %q", err, "execution: column not found")
	}
}

func TestExecAPICreateUniqueIndexRejectsExistingDuplicateKeys(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_name ON users (name)"); err == nil || err.Error() != "execution: duplicate indexed key values already exist" {
		t.Fatalf("Exec(create unique duplicate) error = %v, want %q", err, "execution: duplicate indexed key values already exist")
	}
}

func TestExecAPICreateUniqueIndexRejectsExistingNulls(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, NULL)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_name ON users (name)"); err == nil || err.Error() != "execution: NULL exists in unique indexed key" {
		t.Fatalf("Exec(create unique null) error = %v, want %q", err, "execution: NULL exists in unique indexed key")
	}
}

func TestExecAPICreateUniqueIndexEnforcesLaterWrites(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create unique) error = %v", err)
	}

	if _, err := db.Exec("INSERT INTO users VALUES (3, 'alice')"); err == nil || err.Error() != "execution: duplicate indexed key values already exist" {
		t.Fatalf("Exec(insert duplicate) error = %v, want %q", err, "execution: duplicate indexed key values already exist")
	}
	if _, err := db.Exec("INSERT INTO users VALUES (3, NULL)"); err == nil || err.Error() != "execution: NULL exists in unique indexed key" {
		t.Fatalf("Exec(insert null) error = %v, want %q", err, "execution: NULL exists in unique indexed key")
	}
	if _, err := db.Exec("UPDATE users SET name = 'alice' WHERE id = 2"); err == nil || err.Error() != "execution: duplicate indexed key values already exist" {
		t.Fatalf("Exec(update duplicate) error = %v, want %q", err, "execution: duplicate indexed key values already exist")
	}
	if _, err := db.Exec("UPDATE users SET name = NULL WHERE id = 2"); err == nil || err.Error() != "execution: NULL exists in unique indexed key" {
		t.Fatalf("Exec(update null) error = %v, want %q", err, "execution: NULL exists in unique indexed key")
	}
}

func TestExecAPIMultiColumnUniqueIndexPersistsAndEnforcesAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, first TEXT, last TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'Ada', 'Lovelace')",
		"INSERT INTO users VALUES (2, 'Grace', 'Hopper')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_full_name ON users (first ASC, last DESC)"); err != nil {
		t.Fatalf("Exec(create unique multi-column) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	if table == nil || table.IndexDefinition("idx_users_full_name") == nil {
		t.Fatalf("IndexDefinition(idx_users_full_name) missing after reopen, table=%#v", table)
	}
	if table.IndexDefinition("idx_users_full_name").RootPageID == 0 {
		t.Fatal("IndexDefinition(idx_users_full_name).RootPageID = 0, want nonzero")
	}
	if _, err := db.Exec("INSERT INTO users VALUES (3, 'Ada', 'Lovelace')"); err == nil || err.Error() != "execution: duplicate indexed key values already exist" {
		t.Fatalf("Exec(insert duplicate tuple) error = %v, want %q", err, "execution: duplicate indexed key values already exist")
	}
	if _, err := db.Exec("INSERT INTO users VALUES (3, 'Ada', NULL)"); err == nil || err.Error() != "execution: NULL exists in unique indexed key" {
		t.Fatalf("Exec(insert null tuple) error = %v, want %q", err, "execution: NULL exists in unique indexed key")
	}
}

func TestCreateIndexRecoveryDoesNotExposePartialIndex(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	db.afterJournalWriteHook = func() error {
		return os.ErrInvalid
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err == nil {
		t.Fatal("Exec(create index) error = nil, want failure")
	}
	if _, err := os.Stat(storage.JournalPath(path)); err != nil {
		t.Fatalf("journal stat error = %v, want surviving journal", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	if table.IndexDefinition("idx_users_name") == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil, want persisted index after WAL recovery")
	}
	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if got := collectIntRowsFromRows(t, rows); len(got) != 1 || got[0] != 1 {
		t.Fatalf("query rows = %#v, want []int{1}", got)
	}
}

func collectIntRowsFromRows(t *testing.T, rows *Rows) []int {
	t.Helper()

	got := []int{}
	for rows.Next() {
		var v any
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		got = append(got, numericValueToInt(t, v))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err() = %v", err)
	}
	return got
}

func TestExecAPIDropIndexRemovesDefinitionAndPlannerUse(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	stmt, ok := parser.ParseSelectExpr("SELECT id FROM users WHERE name = 'alice'")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}
	plan, err := planner.PlanSelect(stmt, plannerTableMetadata(db.tables))
	if err != nil {
		t.Fatalf("PlanSelect(before drop) error = %v", err)
	}
	if plan.ScanType != planner.ScanTypeIndex || plan.IndexScan == nil {
		t.Fatalf("plan before drop = %#v, want index scan", plan)
	}

	result, err := db.Exec("DROP INDEX idx_users_name")
	if err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	if result.RowsAffected() != 0 {
		t.Fatalf("RowsAffected() = %d, want 0", result.RowsAffected())
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	if table.IndexDefinition("idx_users_name") != nil {
		t.Fatalf("IndexDefinition(idx_users_name) = %#v, want nil", table.IndexDefinition("idx_users_name"))
	}
	plan, err = planner.PlanSelect(stmt, plannerTableMetadata(db.tables))
	if err != nil {
		t.Fatalf("PlanSelect(after drop) error = %v", err)
	}
	if plan.ScanType != planner.ScanTypeTable || plan.IndexScan != nil {
		t.Fatalf("plan after drop = %#v, want table scan fallback", plan)
	}
}

func TestExecAPIDropIndexMissingFails(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}

	if _, err := db.Exec("DROP INDEX idx_users_name"); err == nil || err.Error() != "execution: index not found" {
		t.Fatalf("Exec(drop missing index) error = %v, want %q", err, "execution: index not found")
	}
}

func TestExecAPIDropIndexLeavesOtherIndexesIntactAndPersistsAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT, score INT)",
		"CREATE INDEX idx_users_name ON users (name)",
		"CREATE INDEX idx_users_name_score ON users (name, score DESC)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	if table.IndexDefinition("idx_users_name") != nil {
		t.Fatalf("IndexDefinition(idx_users_name) = %#v, want nil", table.IndexDefinition("idx_users_name"))
	}
	if table.IndexDefinition("idx_users_name_score") == nil {
		t.Fatalf("IndexDefinition(idx_users_name_score) = nil, want non-nil (defs=%#v)", table.IndexDefs)
	}
}

func TestExecAPIDropIndexFreesRootPageAndReusesIt(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	droppedRootPageID := storage.PageID(db.tables["users"].IndexDefinition("idx_users_name").RootPageID)

	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	if db.freeListHead == 0 {
		t.Fatal("db.freeListHead = 0, want nonzero after drop")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	freePage, err := pager.Get(droppedRootPageID)
	if err != nil {
		t.Fatalf("pager.Get(%d) error = %v", droppedRootPageID, err)
	}
	if _, err := storage.FreePageNext(freePage.Data()); err != nil {
		t.Fatalf("FreePageNext() error = %v", err)
	}
	head, err := storage.ReadDirectoryFreeListHead(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	if head != db.freeListHead {
		t.Fatalf("ReadDirectoryFreeListHead() = %d, want %d", head, db.freeListHead)
	}
	chain := freeListChainForTest(t, pager, storage.PageID(head))
	if !containsPageID(chain, droppedRootPageID) {
		t.Fatalf("free list chain = %#v, want dropped index root %d present", chain, droppedRootPageID)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	headBeforeCreate := db.freeListHead
	if _, err := db.Exec("CREATE INDEX idx_users_name_again ON users (name)"); err != nil {
		t.Fatalf("Exec(create replacement index) error = %v", err)
	}
	reusedRootPageID := db.tables["users"].IndexDefinition("idx_users_name_again").RootPageID
	if reusedRootPageID != headBeforeCreate {
		t.Fatalf("replacement index RootPageID = %d, want free-list head %d", reusedRootPageID, headBeforeCreate)
	}
}

func TestExecAPIDropTableRemovesTableAndDependentIndexes(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
		"CREATE UNIQUE INDEX idx_users_id ON users (id)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	result, err := db.Exec("DROP TABLE users")
	if err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}
	if result.RowsAffected() != 0 {
		t.Fatalf("RowsAffected() = %d, want 0", result.RowsAffected())
	}
	if _, ok := db.tables["users"]; ok {
		t.Fatalf("db.tables[users] still present: %#v", db.tables["users"])
	}

	rows, err := db.Query("SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query(dropped table) direct error = %v, want deferred row error", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("rows.Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: table not found: users" {
		t.Fatalf("rows.Err() = %v, want %q", rows.Err(), "execution: table not found: users")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	if _, ok := db.tables["users"]; ok {
		t.Fatalf("reopened db.tables[users] still present: %#v", db.tables["users"])
	}
	rows, err = db.Query("SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query(dropped table after reopen) direct error = %v, want deferred row error", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("rows.Next() after reopen = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: table not found: users" {
		t.Fatalf("rows.Err() after reopen = %v, want %q", rows.Err(), "execution: table not found: users")
	}
}

func TestExecAPIDropTableMissingFails(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("DROP TABLE users"); err == nil || err.Error() != "execution: table not found: users" {
		t.Fatalf("Exec(drop missing table) error = %v, want %q", err, "execution: table not found: users")
	}
}

func TestExecAPIDropTableLeavesUnrelatedTablesIntact(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE teams (id INT, name TEXT)",
		"INSERT INTO teams VALUES (1, 'ops')",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM teams")
	if err != nil {
		t.Fatalf("Query(teams) error = %v", err)
	}
	defer rows.Close()
	var id int32
	var name string
	if !rows.Next() {
		t.Fatal("rows.Next() = false, want true")
	}
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if id != 1 || name != "ops" {
		t.Fatalf("teams row = (%d,%q), want (1,\"ops\")", id, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v", err)
	}
}

func TestExecAPIDropTableFreesTableAndIndexRootsIntoFreeList(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
		"CREATE UNIQUE INDEX idx_users_id ON users (id)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	table := db.tables["users"]
	tableRootPageID := table.RootPageID()
	spaceMapPageIDs, dataPageIDs, err := committedTablePhysicalStorageInventory(db.pool, table)
	if err != nil {
		t.Fatalf("committedTablePhysicalStorageInventory() error = %v", err)
	}
	indexNames := []string{"idx_users_id", "idx_users_name"}
	indexRootPageIDs := make([]storage.PageID, 0, len(indexNames))
	for _, indexName := range indexNames {
		indexRootPageIDs = append(indexRootPageIDs, storage.PageID(table.IndexDefinition(indexName).RootPageID))
	}

	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}
	if db.freeListHead == 0 {
		t.Fatal("db.freeListHead = 0, want nonzero after drop")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	defer rawDB.Close()

	head, err := storage.ReadDirectoryFreeListHead(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	if head == 0 {
		t.Fatal("ReadDirectoryFreeListHead() = 0, want nonzero after drop")
	}
	chain := freeListChainForTest(t, pager, storage.PageID(head))
	wantPages := []storage.PageID{tableRootPageID, indexRootPageIDs[1], indexRootPageIDs[0], table.TableHeaderPageID()}
	wantPages = append(wantPages, spaceMapPageIDs...)
	wantPages = append(wantPages, dataPageIDs...)
	for _, pageID := range wantPages {
		if !containsPageID(chain, pageID) {
			t.Fatalf("free list chain = %#v, want page %d present", chain, pageID)
		}
	}
}

func TestExecAPIDropTableFreedRootIsReusableAfterReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop users) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	headBeforeCreate := db.freeListHead
	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if got := db.tables["teams"].RootPageID(); got != storage.PageID(headBeforeCreate) {
		t.Fatalf("teams.RootPageID() = %d, want free-list head %d", got, headBeforeCreate)
	}
}

func TestExecAPIDropTableReopenLeavesNoGhostPhysicalOwnership(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	droppedTableID := db.tables["users"].TableID
	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop users) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	if _, ok := db.tables["users"]; ok {
		t.Fatalf("reopened db.tables[users] still present: %#v", db.tables["users"])
	}
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}
	if ghost := findPhysicalOwnershipPageForTableID(t, db, droppedTableID); ghost != 0 {
		t.Fatalf("found ghost physical ownership for dropped table id %d on page %d", droppedTableID, ghost)
	}
}

func freeListChainForTest(t *testing.T, pager *storage.Pager, head storage.PageID) []storage.PageID {
	t.Helper()
	chain := make([]storage.PageID, 0)
	seen := make(map[storage.PageID]struct{})
	for head != 0 {
		if _, exists := seen[head]; exists {
			t.Fatalf("free list cycle at %d", head)
		}
		seen[head] = struct{}{}
		chain = append(chain, head)
		page, err := pager.Get(head)
		if err != nil {
			t.Fatalf("pager.Get(%d) error = %v", head, err)
		}
		next, err := storage.FreePageNext(page.Data())
		if err != nil {
			t.Fatalf("FreePageNext(%d) error = %v", head, err)
		}
		head = storage.PageID(next)
	}
	return chain
}

func containsPageID(ids []storage.PageID, want storage.PageID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func findPhysicalOwnershipPageForTableID(t *testing.T, db *DB, tableID uint32) storage.PageID {
	t.Helper()
	if db == nil || db.pool == nil || db.pager == nil {
		t.Fatal("findPhysicalOwnershipPageForTableID() requires open db")
	}
	for pageID := storage.PageID(0); pageID < db.pager.NextPageID(); pageID++ {
		pageData, err := readCommittedPageData(db.pool, pageID)
		if err != nil {
			t.Fatalf("readCommittedPageData(%d) error = %v", pageID, err)
		}
		if err := storage.ValidatePageImage(pageData); err != nil {
			t.Fatalf("ValidatePageImage(%d) error = %v", pageID, err)
		}
		switch storage.PageType(binary.LittleEndian.Uint16(pageData[4:6])) {
		case storage.PageTypeHeader:
			role, err := storage.HeaderPageRoleValue(pageData)
			if err != nil {
				t.Fatalf("HeaderPageRoleValue(%d) error = %v", pageID, err)
			}
			if role != storage.HeaderPageRoleTable {
				continue
			}
			owner, err := storage.TableHeaderTableID(pageData)
			if err != nil {
				t.Fatalf("TableHeaderTableID(%d) error = %v", pageID, err)
			}
			if owner == tableID {
				return pageID
			}
		case storage.PageTypeSpaceMap:
			owner, err := storage.SpaceMapOwningTableID(pageData)
			if err != nil {
				t.Fatalf("SpaceMapOwningTableID(%d) error = %v", pageID, err)
			}
			if owner == tableID {
				return pageID
			}
		case storage.PageTypeTable:
			owner, err := storage.TablePageOwningTableID(pageData)
			if err != nil {
				t.Fatalf("TablePageOwningTableID(%d) error = %v", pageID, err)
			}
			if owner == tableID {
				return pageID
			}
		}
	}
	return 0
}

func TestAPIErrorContractQueryAndExecRouting(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("CREATE TABLE users (id INT)")
	if !errors.Is(err, ErrQueryRequiresSelect) {
		t.Fatalf("Query(non-select) error = %v, want ErrQueryRequiresSelect", err)
	}
	if rows != nil {
		t.Fatalf("Query(non-select) rows = %v, want nil", rows)
	}

	result, err := db.Exec("SELECT 1")
	if !errors.Is(err, ErrExecDisallowsSelect) {
		t.Fatalf("Exec(select) error = %v, want ErrExecDisallowsSelect", err)
	}
	if result != (Result{}) {
		t.Fatalf("Exec(select) result = %#v, want zero Result", result)
	}
}

func TestAPIErrorContractRowsStateErrors(t *testing.T) {
	rows := newRows(nil, [][]any{{1}})

	var i int32
	if err := rows.Scan(&i); !errors.Is(err, ErrScanBeforeNext) {
		t.Fatalf("Scan() before Next = %v, want ErrScanBeforeNext", err)
	}

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	if err := rows.Scan(); !errors.Is(err, ErrScanMismatch) {
		t.Fatalf("Scan() count mismatch = %v, want ErrScanMismatch", err)
	}

	var s string
	if err := rows.Scan(&s); !errors.Is(err, ErrUnsupportedScanType) {
		t.Fatalf("Scan() unsupported type = %v, want ErrUnsupportedScanType", err)
	}

	if err := rows.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := rows.Scan(&i); !errors.Is(err, ErrRowsClosed) {
		t.Fatalf("Scan() after Close = %v, want ErrRowsClosed", err)
	}
}

func TestAPIErrorContractRowStrictness(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	row := db.QueryRow("SELECT id FROM users WHERE id = 999")
	var i int32
	if err := row.Scan(&i); !errors.Is(err, ErrNoRows) {
		t.Fatalf("QueryRow(no rows).Scan() = %v, want ErrNoRows", err)
	}

	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	row = db.QueryRow("SELECT id FROM users ORDER BY id")
	if err := row.Scan(&i); !errors.Is(err, ErrMultipleRows) {
		t.Fatalf("QueryRow(multi-row).Scan() = %v, want ErrMultipleRows", err)
	}
}

func TestAPIErrorContractDeferredPassthrough(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	row := db.QueryRow("SELECT * FROM users WHERE id =")
	var i int32
	err = row.Scan(&i)
	if err == nil || err.Error() != "parse: invalid where clause" {
		t.Fatalf("QueryRow(malformed).Scan() = %v, want %q", err, "parse: invalid where clause")
	}
	if errors.Is(err, ErrNoRows) {
		t.Fatal("QueryRow(malformed).Scan() matched ErrNoRows, want passthrough parse error")
	}

	rows, err := db.Query("SELEC 1")
	if err == nil || err.Error() != "parse: unsupported query form" {
		t.Fatalf("Query(malformed) error = %v, want %q", err, "parse: unsupported query form")
	}
	if rows != nil {
		t.Fatalf("Query(malformed) rows = %v, want nil", rows)
	}

	row = db.QueryRow("SELECT * FROM users")
	err = row.Scan(&i)
	if err == nil || err.Error() != "execution: table not found: users" {
		t.Fatalf("QueryRow(exec error).Scan() = %v, want %q", err, "execution: table not found: users")
	}
	if errors.Is(err, ErrNoRows) {
		t.Fatal("QueryRow(exec error).Scan() matched ErrNoRows, want passthrough execution error")
	}
}

func TestExecAPIIndexedTextLimitRejectsInsertIntoIndexedTextColumn(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT, bio TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	tooLarge := strings.Repeat("a", 513)
	if _, err := db.Exec("INSERT INTO users VALUES (1, ?, ?)", tooLarge, "ok"); err == nil || err.Error() != "execution: indexed TEXT column value exceeds 512-byte limit" {
		t.Fatalf("Exec(insert oversized indexed text) error = %v, want %q", err, "execution: indexed TEXT column value exceeds 512-byte limit")
	}
}

func TestExecAPIIndexedTextLimitRejectsUpdateIntoIndexedTextColumn(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	tooLarge := strings.Repeat("b", 513)
	if _, err := db.Exec("UPDATE users SET name = ? WHERE id = 1", tooLarge); err == nil || err.Error() != "execution: indexed TEXT column value exceeds 512-byte limit" {
		t.Fatalf("Exec(update oversized indexed text) error = %v, want %q", err, "execution: indexed TEXT column value exceeds 512-byte limit")
	}
}

func TestExecAPIIndexedTextLimitAllowsOversizedNonIndexedText(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT, bio TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	tooLarge := strings.Repeat("c", 513)
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice', ?)", tooLarge); err != nil {
		t.Fatalf("Exec(insert oversized non-indexed text) error = %v", err)
	}

	rows, err := db.Query("SELECT bio FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	var got string
	if !rows.Next() {
		t.Fatal("rows.Next() = false, want true")
	}
	if err := rows.Scan(&got); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if got != tooLarge {
		t.Fatalf("bio length = %d, want %d", len(got), len(tooLarge))
	}
}

func TestExecRejectsOutOfRangeIntPlaceholderInsert(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	if _, err := db.Exec("INSERT INTO users VALUES (?)", 2147483648); err == nil {
		t.Fatal("Exec(insert out-of-range int placeholder) error = nil, want error")
	}
}

func TestExecRejectsOutOfRangeIntPlaceholderUpdate(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	if _, err := db.Exec("UPDATE users SET id = ? WHERE id = 1", -2147483649); err == nil {
		t.Fatal("Exec(update out-of-range int placeholder) error = nil, want error")
	}
}

func TestQueryRejectsOutOfRangeIntLiteral(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 2147483648")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "parse: unsupported query form" {
		t.Fatalf("Err() = %v, want %q", rows.Err(), "parse: unsupported query form")
	}
}

func TestQueryAcceptsBoundaryIntLiteral(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	var got int
	if err := db.QueryRow("SELECT 2147483647").Scan(&got); err != nil {
		t.Fatalf("QueryRow().Scan() error = %v", err)
	}
	if got != 2147483647 {
		t.Fatalf("QueryRow().Scan() got %d, want 2147483647", got)
	}
}

func TestQueryRowReturnsBoundaryIntResult(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	var got int
	if err := db.QueryRow("SELECT 2147483647").Scan(&got); err != nil {
		t.Fatalf("QueryRow().Scan() error = %v", err)
	}
	if got != 2147483647 {
		t.Fatalf("QueryRow().Scan() got %d, want 2147483647", got)
	}
}

func TestQueryIntArithmeticOverflowIsDeferred(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 2147483647 + 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: integer out of range" {
		t.Fatalf("Err() = %v, want %q", rows.Err(), "execution: integer out of range")
	}
}

func TestQueryAbsOverflowIsDeferred(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (-2147483648)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT ABS(id) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: integer out of range" {
		t.Fatalf("Err() = %v, want %q", rows.Err(), "execution: integer out of range")
	}
}

func TestUpdateIntArithmeticOverflowDoesNotChangeState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (2147483647)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	if _, err := db.Exec("UPDATE users SET id = id + 1 WHERE id = 2147483647"); err == nil || !strings.Contains(err.Error(), "integer out of range") {
		t.Fatalf("Exec(update overflow) error = %v, want integer out of range", err)
	}

	assertSelectIntRows(t, db, "SELECT id FROM users", 2147483647)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT id FROM users", 2147483647)
}

func TestEngineStatusOnFreshDB(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	status, err := db.EngineStatus()
	if err != nil {
		t.Fatalf("EngineStatus() error = %v", err)
	}
	if status.DBFormatVersion != storage.CurrentDBFormatVersion {
		t.Fatalf("EngineStatus().DBFormatVersion = %d, want %d", status.DBFormatVersion, storage.CurrentDBFormatVersion)
	}
	if status.WALVersion != storage.CurrentWALVersion {
		t.Fatalf("EngineStatus().WALVersion = %d, want %d", status.WALVersion, storage.CurrentWALVersion)
	}
	if status.TableCount != 0 {
		t.Fatalf("EngineStatus().TableCount = %d, want 0", status.TableCount)
	}
	if status.IndexCount != 0 {
		t.Fatalf("EngineStatus().IndexCount = %d, want 0", status.IndexCount)
	}
	if status.FreeListHead != 0 {
		t.Fatalf("EngineStatus().FreeListHead = %d, want 0", status.FreeListHead)
	}
}

func TestEngineStatusTracksUserTableAndIndexCounts(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE teams (id INT)",
		"CREATE INDEX idx_users_name ON users (name)",
		"CREATE UNIQUE INDEX idx_users_id ON users (id)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	status, err := db.EngineStatus()
	if err != nil {
		t.Fatalf("EngineStatus() error = %v", err)
	}
	if status.TableCount != 2 {
		t.Fatalf("EngineStatus().TableCount = %d, want 2", status.TableCount)
	}
	if status.IndexCount != 2 {
		t.Fatalf("EngineStatus().IndexCount = %d, want 2", status.IndexCount)
	}
}

func TestEngineStatusSurfacesCheckpointAndFreeListState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}

	status, err := db.EngineStatus()
	if err != nil {
		t.Fatalf("EngineStatus() error = %v", err)
	}
	if status.LastCheckpointLSN == 0 {
		t.Fatal("EngineStatus().LastCheckpointLSN = 0, want nonzero")
	}
	if status.LastCheckpointPageCount == 0 {
		t.Fatal("EngineStatus().LastCheckpointPageCount = 0, want nonzero")
	}
	if status.FreeListHead == 0 {
		t.Fatal("EngineStatus().FreeListHead = 0, want nonzero after drop")
	}
	if status.TableCount != 1 {
		t.Fatalf("EngineStatus().TableCount = %d, want 1", status.TableCount)
	}
	if status.IndexCount != 0 {
		t.Fatalf("EngineStatus().IndexCount = %d, want 0", status.IndexCount)
	}
}

func TestEngineStatusOnClosedDBReturnsErrClosed(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = db.EngineStatus()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("EngineStatus() error = %v, want ErrClosed", err)
	}
}

func TestCheckEngineConsistencyOnFreshDB(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	result, err := db.CheckEngineConsistency()
	if err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}
	if !result.OK {
		t.Fatal("CheckEngineConsistency().OK = false, want true")
	}
	if result.CheckedTableRoots != 0 {
		t.Fatalf("CheckEngineConsistency().CheckedTableRoots = %d, want 0", result.CheckedTableRoots)
	}
	if result.CheckedTableHeaders != 0 {
		t.Fatalf("CheckEngineConsistency().CheckedTableHeaders = %d, want 0", result.CheckedTableHeaders)
	}
	if result.CheckedIndexRoots != 0 {
		t.Fatalf("CheckEngineConsistency().CheckedIndexRoots = %d, want 0", result.CheckedIndexRoots)
	}
	if result.CheckedSpaceMapPages != 0 {
		t.Fatalf("CheckEngineConsistency().CheckedSpaceMapPages = %d, want 0", result.CheckedSpaceMapPages)
	}
	if result.CheckedDataPages != 0 {
		t.Fatalf("CheckEngineConsistency().CheckedDataPages = %d, want 0", result.CheckedDataPages)
	}
	if result.FreeListHead != 0 {
		t.Fatalf("CheckEngineConsistency().FreeListHead = %d, want 0", result.FreeListHead)
	}
}

func TestCheckEngineConsistencyTracksUserRootsOnly(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	result, err := db.CheckEngineConsistency()
	if err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}
	if !result.OK {
		t.Fatal("CheckEngineConsistency().OK = false, want true")
	}
	if result.CheckedTableRoots != 1 {
		t.Fatalf("CheckEngineConsistency().CheckedTableRoots = %d, want 1", result.CheckedTableRoots)
	}
	if result.CheckedTableHeaders != 1 {
		t.Fatalf("CheckEngineConsistency().CheckedTableHeaders = %d, want 1", result.CheckedTableHeaders)
	}
	if result.CheckedIndexRoots != 1 {
		t.Fatalf("CheckEngineConsistency().CheckedIndexRoots = %d, want 1", result.CheckedIndexRoots)
	}
	if result.CheckedSpaceMapPages != 0 {
		t.Fatalf("CheckEngineConsistency().CheckedSpaceMapPages = %d, want 0", result.CheckedSpaceMapPages)
	}
	if result.CheckedDataPages != 0 {
		t.Fatalf("CheckEngineConsistency().CheckedDataPages = %d, want 0", result.CheckedDataPages)
	}
}

func TestCheckEngineConsistencyTracksOwnedPhysicalPages(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, note TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	for id := 1; id <= 24; id++ {
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?)", int32(id), strings.Repeat("payload-", 110)); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	table := db.tables["users"]
	spaceMapPageIDs, dataPageIDs, err := committedTablePhysicalStorageInventory(db.pool, table)
	if err != nil {
		t.Fatalf("committedTablePhysicalStorageInventory() error = %v", err)
	}

	result, err := db.CheckEngineConsistency()
	if err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}
	if !result.OK {
		t.Fatal("CheckEngineConsistency().OK = false, want true")
	}
	if result.CheckedTableRoots != 1 {
		t.Fatalf("CheckEngineConsistency().CheckedTableRoots = %d, want 1", result.CheckedTableRoots)
	}
	if result.CheckedTableHeaders != 1 {
		t.Fatalf("CheckEngineConsistency().CheckedTableHeaders = %d, want 1", result.CheckedTableHeaders)
	}
	if result.CheckedSpaceMapPages != len(spaceMapPageIDs) {
		t.Fatalf("CheckEngineConsistency().CheckedSpaceMapPages = %d, want %d", result.CheckedSpaceMapPages, len(spaceMapPageIDs))
	}
	if result.CheckedDataPages != len(dataPageIDs) {
		t.Fatalf("CheckEngineConsistency().CheckedDataPages = %d, want %d", result.CheckedDataPages, len(dataPageIDs))
	}
}

func TestCheckEngineConsistencySurfacesFreeListHead(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}

	result, err := db.CheckEngineConsistency()
	if err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}
	if !result.OK {
		t.Fatal("CheckEngineConsistency().OK = false, want true")
	}
	if result.FreeListHead == 0 {
		t.Fatal("CheckEngineConsistency().FreeListHead = 0, want nonzero after drop")
	}
}

func TestCheckEngineConsistencyRejectsMalformedFreeListHead(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}

	db.freeListHead = uint32(db.tables["users"].RootPageID())
	if _, err := db.CheckEngineConsistency(); err == nil {
		t.Fatal("CheckEngineConsistency() error = nil, want malformed free-list-head failure")
	}
}

func TestCheckEngineConsistencyRejectsMalformedTableRootPageType(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	db.tables["users"].SetStorageMeta(storage.PageID(db.tables["users"].IndexDefinition("idx_users_name").RootPageID), db.tables["users"].PersistedRowCount())
	if _, err := db.CheckEngineConsistency(); err == nil {
		t.Fatal("CheckEngineConsistency() error = nil, want malformed table-root failure")
	}
}

func TestCheckEngineConsistencyRejectsMalformedIndexRootPageType(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	db.tables["users"].IndexDefinition("idx_users_name").RootPageID = uint32(db.tables["users"].RootPageID())
	if _, err := db.CheckEngineConsistency(); err == nil {
		t.Fatal("CheckEngineConsistency() error = nil, want malformed index-root failure")
	}
}

func TestCheckEngineConsistencyOnClosedDBReturnsErrClosed(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = db.CheckEngineConsistency()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("CheckEngineConsistency() error = %v, want ErrClosed", err)
	}
}

func TestPageUsageOnFreshDB(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	usage, err := db.PageUsage()
	if err != nil {
		t.Fatalf("PageUsage() error = %v", err)
	}
	if usage.TotalPages != 9 {
		t.Fatalf("PageUsage().TotalPages = %d, want 9", usage.TotalPages)
	}
	if usage.DirectoryPages != 1 {
		t.Fatalf("PageUsage().DirectoryPages = %d, want 1", usage.DirectoryPages)
	}
	if usage.HeaderPages != 4 {
		t.Fatalf("PageUsage().HeaderPages = %d, want 4 system table header pages", usage.HeaderPages)
	}
	if usage.TablePages != 4 {
		t.Fatalf("PageUsage().TablePages = %d, want 4 system table pages", usage.TablePages)
	}
	if usage.SpaceMapPages != 0 || usage.IndexLeafPages != 0 || usage.IndexInternalPages != 0 || usage.FreePages != 0 {
		t.Fatalf("PageUsage() = %#v, want only directory + system table/header pages", usage)
	}
}

func TestPageUsageTracksTableIndexAndFreePages(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	before, err := db.PageUsage()
	if err != nil {
		t.Fatalf("PageUsage(before) error = %v", err)
	}

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	afterTable, err := db.PageUsage()
	if err != nil {
		t.Fatalf("PageUsage(after table) error = %v", err)
	}
	if afterTable.TablePages < before.TablePages+1 {
		t.Fatalf("PageUsage().TablePages after table = %d, want at least %d", afterTable.TablePages, before.TablePages+1)
	}
	if afterTable.DirectoryPages != 1 {
		t.Fatalf("PageUsage().DirectoryPages after table = %d, want 1", afterTable.DirectoryPages)
	}

	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	afterIndex, err := db.PageUsage()
	if err != nil {
		t.Fatalf("PageUsage(after index) error = %v", err)
	}
	if afterIndex.IndexLeafPages < afterTable.IndexLeafPages+1 {
		t.Fatalf("PageUsage().IndexLeafPages after index = %d, want at least %d", afterIndex.IndexLeafPages, afterTable.IndexLeafPages+1)
	}

	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	afterDrop, err := db.PageUsage()
	if err != nil {
		t.Fatalf("PageUsage(after drop) error = %v", err)
	}
	if afterDrop.FreePages < afterIndex.FreePages+1 {
		t.Fatalf("PageUsage().FreePages after drop = %d, want at least %d", afterDrop.FreePages, afterIndex.FreePages+1)
	}
}

func TestPageUsageFailsOnMalformedPage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
		"CREATE UNIQUE INDEX idx_users_id ON users (id)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	tailFreePageID := storage.PageID(db.tables["users"].IndexDefinition("idx_users_id").RootPageID)
	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	page, err := pager.Get(tailFreePageID)
	if err != nil {
		t.Fatalf("pager.Get(%d) error = %v", tailFreePageID, err)
	}
	clear(page.Data())
	copy(page.Data(), []byte("not-a-valid-page"))
	pager.MarkDirty(page)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		defer db.Close()
		t.Fatal("reopen Open() error = nil, want malformed page failure")
	}
	if err.Error() != "storage: corrupted page header" {
		t.Fatalf("reopen Open() error = %v, want %q", err, "storage: corrupted page header")
	}
}

func TestPageUsageOnClosedDBReturnsErrClosed(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = db.PageUsage()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("PageUsage() error = %v, want ErrClosed", err)
	}
}

func TestSchemaInventoryOnFreshDB(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	inventory, err := db.SchemaInventory()
	if err != nil {
		t.Fatalf("SchemaInventory() error = %v", err)
	}
	if len(inventory.Tables) != 0 {
		t.Fatalf("len(SchemaInventory().Tables) = %d, want 0", len(inventory.Tables))
	}
	if len(inventory.Indexes) != 0 {
		t.Fatalf("len(SchemaInventory().Indexes) = %d, want 0", len(inventory.Indexes))
	}
}

func TestSchemaInventoryIncludesUserTablesAndIndexes(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE UNIQUE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	users := db.tables["users"]
	indexDef := users.IndexDefinition("idx_users_name")
	if users == nil || indexDef == nil {
		t.Fatalf("schema setup failed: users=%v index=%v", users, indexDef)
	}

	inventory, err := db.SchemaInventory()
	if err != nil {
		t.Fatalf("SchemaInventory() error = %v", err)
	}
	if len(inventory.Tables) != 1 {
		t.Fatalf("len(SchemaInventory().Tables) = %d, want 1", len(inventory.Tables))
	}
	if len(inventory.Indexes) != 1 {
		t.Fatalf("len(SchemaInventory().Indexes) = %d, want 1", len(inventory.Indexes))
	}

	tableInfo := inventory.Tables[0]
	if tableInfo.TableID != users.TableID ||
		tableInfo.TableName != "users" ||
		tableInfo.RootPageID != uint32(users.RootPageID()) ||
		tableInfo.TableHeaderPageID != uint32(users.TableHeaderPageID()) ||
		tableInfo.FirstSpaceMapPageID != 0 ||
		tableInfo.OwnedSpaceMapPages != 0 ||
		tableInfo.EnumeratedSpaceMapPages != 0 ||
		tableInfo.OwnedDataPages != 0 ||
		tableInfo.EnumeratedDataPages != 0 ||
		!tableInfo.PhysicalMetaPresent ||
		!tableInfo.PhysicalMetaValid ||
		!tableInfo.PhysicalInventoryMatch ||
		tableInfo.IndexCount != 1 {
		t.Fatalf("SchemaInventory().Tables[0] = %#v, want users metadata", tableInfo)
	}

	indexInfo := inventory.Indexes[0]
	if indexInfo.IndexID != indexDef.IndexID || indexInfo.TableName != "users" || indexInfo.IndexName != "idx_users_name" || indexInfo.RootPageID != indexDef.RootPageID || !indexInfo.IsUnique {
		t.Fatalf("SchemaInventory().Indexes[0] = %#v, want idx_users_name metadata", indexInfo)
	}
}

func TestSchemaInventoryIncludesPhysicalStorageCountsForOwnedPages(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, note TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	for id := 1; id <= 24; id++ {
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?)", int32(id), strings.Repeat("payload-", 110)); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	users := db.tables["users"]
	spaceMapPageIDs, dataPageIDs, err := committedTablePhysicalStorageInventory(db.pool, users)
	if err != nil {
		t.Fatalf("committedTablePhysicalStorageInventory() error = %v", err)
	}

	inventory, err := db.SchemaInventory()
	if err != nil {
		t.Fatalf("SchemaInventory() error = %v", err)
	}
	if len(inventory.Tables) != 1 {
		t.Fatalf("len(SchemaInventory().Tables) = %d, want 1", len(inventory.Tables))
	}

	tableInfo := inventory.Tables[0]
	if tableInfo.TableHeaderPageID != uint32(users.TableHeaderPageID()) ||
		tableInfo.FirstSpaceMapPageID != uint32(users.FirstSpaceMapPageID()) ||
		tableInfo.OwnedSpaceMapPages != users.OwnedSpaceMapPageCount() ||
		tableInfo.EnumeratedSpaceMapPages != uint32(len(spaceMapPageIDs)) ||
		tableInfo.OwnedDataPages != users.OwnedDataPageCount() ||
		tableInfo.EnumeratedDataPages != uint32(len(dataPageIDs)) ||
		!tableInfo.PhysicalMetaPresent ||
		!tableInfo.PhysicalMetaValid ||
		!tableInfo.PhysicalInventoryMatch {
		t.Fatalf("SchemaInventory().Tables[0] = %#v, want physical storage counts from users table", tableInfo)
	}
}

func TestSchemaInventoryOrderingIsDeterministic(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE accounts (id INT, email TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
		"CREATE UNIQUE INDEX idx_accounts_email ON accounts (email)",
		"CREATE INDEX idx_accounts_id ON accounts (id)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	inventory, err := db.SchemaInventory()
	if err != nil {
		t.Fatalf("SchemaInventory() error = %v", err)
	}

	if got := []string{inventory.Tables[0].TableName, inventory.Tables[1].TableName}; got[0] != "accounts" || got[1] != "users" {
		t.Fatalf("SchemaInventory().Tables order = %#v, want [accounts users]", got)
	}
	if got := []string{
		inventory.Indexes[0].TableName + "." + inventory.Indexes[0].IndexName,
		inventory.Indexes[1].TableName + "." + inventory.Indexes[1].IndexName,
		inventory.Indexes[2].TableName + "." + inventory.Indexes[2].IndexName,
	}; got[0] != "accounts.idx_accounts_email" || got[1] != "accounts.idx_accounts_id" || got[2] != "users.idx_users_name" {
		t.Fatalf("SchemaInventory().Indexes order = %#v, want [accounts.idx_accounts_email accounts.idx_accounts_id users.idx_users_name]", got)
	}
}

func TestSchemaInventoryExcludesSystemTables(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	inventory, err := db.SchemaInventory()
	if err != nil {
		t.Fatalf("SchemaInventory() error = %v", err)
	}
	for _, tableInfo := range inventory.Tables {
		if isSystemCatalogTableName(tableInfo.TableName) {
			t.Fatalf("SchemaInventory() included system table %#v", tableInfo)
		}
	}
	for _, indexInfo := range inventory.Indexes {
		if isSystemCatalogTableName(indexInfo.TableName) {
			t.Fatalf("SchemaInventory() included system-table index %#v", indexInfo)
		}
	}
}

func TestSchemaInventoryOnClosedDBReturnsErrClosed(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = db.SchemaInventory()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("SchemaInventory() error = %v, want ErrClosed", err)
	}
}

func TestEngineSnapshotOnFreshDBMatchesHelpers(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	status, err := db.EngineStatus()
	if err != nil {
		t.Fatalf("EngineStatus() error = %v", err)
	}
	check, err := db.CheckEngineConsistency()
	if err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}
	pageUsage, err := db.PageUsage()
	if err != nil {
		t.Fatalf("PageUsage() error = %v", err)
	}
	inventory, err := db.SchemaInventory()
	if err != nil {
		t.Fatalf("SchemaInventory() error = %v", err)
	}

	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() error = %v", err)
	}
	if snapshot.Status != status {
		t.Fatalf("EngineSnapshot().Status = %#v, want %#v", snapshot.Status, status)
	}
	if snapshot.Check != check {
		t.Fatalf("EngineSnapshot().Check = %#v, want %#v", snapshot.Check, check)
	}
	if snapshot.PageUsage != pageUsage {
		t.Fatalf("EngineSnapshot().PageUsage = %#v, want %#v", snapshot.PageUsage, pageUsage)
	}
	if len(snapshot.Inventory.Tables) != len(inventory.Tables) || len(snapshot.Inventory.Indexes) != len(inventory.Indexes) {
		t.Fatalf("EngineSnapshot().Inventory = %#v, want %#v", snapshot.Inventory, inventory)
	}
	for i := range inventory.Tables {
		if snapshot.Inventory.Tables[i] != inventory.Tables[i] {
			t.Fatalf("EngineSnapshot().Inventory.Tables[%d] = %#v, want %#v", i, snapshot.Inventory.Tables[i], inventory.Tables[i])
		}
	}
	for i := range inventory.Indexes {
		if snapshot.Inventory.Indexes[i] != inventory.Indexes[i] {
			t.Fatalf("EngineSnapshot().Inventory.Indexes[%d] = %#v, want %#v", i, snapshot.Inventory.Indexes[i], inventory.Indexes[i])
		}
	}
}

func TestEngineSnapshotReflectsSchemaAndDropChanges(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() after create error = %v", err)
	}
	if snapshot.Status.TableCount != 1 || snapshot.Status.IndexCount != 1 {
		t.Fatalf("EngineSnapshot().Status after create = %#v, want 1 table and 1 index", snapshot.Status)
	}
	if snapshot.Check.CheckedTableRoots != 1 || snapshot.Check.CheckedIndexRoots != 1 {
		t.Fatalf("EngineSnapshot().Check after create = %#v, want 1 checked table root and 1 checked index root", snapshot.Check)
	}
	if len(snapshot.Inventory.Tables) != 1 || len(snapshot.Inventory.Indexes) != 1 {
		t.Fatalf("EngineSnapshot().Inventory after create = %#v, want one table and one index", snapshot.Inventory)
	}

	indexRootPageID := db.tables["users"].IndexDefinition("idx_users_name").RootPageID
	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}

	snapshot, err = db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() after drop error = %v", err)
	}
	if snapshot.Status.TableCount != 0 || snapshot.Status.IndexCount != 0 {
		t.Fatalf("EngineSnapshot().Status after drop = %#v, want empty user schema", snapshot.Status)
	}
	if snapshot.Check.CheckedTableRoots != 0 || snapshot.Check.CheckedIndexRoots != 0 {
		t.Fatalf("EngineSnapshot().Check after drop = %#v, want zero checked user roots", snapshot.Check)
	}
	if len(snapshot.Inventory.Tables) != 0 || len(snapshot.Inventory.Indexes) != 0 {
		t.Fatalf("EngineSnapshot().Inventory after drop = %#v, want empty inventory", snapshot.Inventory)
	}
	if snapshot.PageUsage.FreePages == 0 {
		t.Fatalf("EngineSnapshot().PageUsage = %#v, want freed pages after drop", snapshot.PageUsage)
	}
	if snapshot.Check.FreeListHead != indexRootPageID && snapshot.Check.FreeListHead == 0 {
		t.Fatalf("EngineSnapshot().Check.FreeListHead = %d, want nonzero free-list head after drop", snapshot.Check.FreeListHead)
	}
}

func TestEngineSnapshotOnClosedDBReturnsErrClosed(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = db.EngineSnapshot()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("EngineSnapshot() error = %v, want ErrClosed", err)
	}
}

func TestEngineSnapshotStringOnFreshDBIsStable(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() error = %v", err)
	}

	got := snapshot.String()
	want := "Engine Status\n" +
		"DB format: 1\n" +
		"WAL version: 1\n" +
		"Checkpoint: LSN=0 pages=0\n" +
		"Free list head: 0\n" +
		"Tables: 0\n" +
		"Indexes: 0\n\n" +
		"Consistency\n" +
		"OK: true\n" +
		"Checked table roots: 0\n" +
		"Checked table headers: 0\n" +
		"Checked index roots: 0\n" +
		"Checked space map pages: 0\n" +
		"Checked data pages: 0\n\n" +
		"Page Usage\n" +
		"Total: 9\n" +
		"Header: 4\n" +
		"Space map: 0\n" +
		"Table: 4\n" +
		"Index leaf: 0\n" +
		"Index internal: 0\n" +
		"Free: 0\n" +
		"Directory: 1\n\n" +
		"Schema Inventory\n" +
		"Tables:\n" +
		"Indexes:\n"
	if got != want {
		t.Fatalf("EngineSnapshot().String() = %q, want %q", got, want)
	}
}

func TestEngineSnapshotStringIncludesSchemaDetails(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() error = %v", err)
	}
	formatted := snapshot.String()

	for _, want := range []string{
		"Engine Status\n",
		"\nConsistency\n",
		"\nPage Usage\n",
		"\nSchema Inventory\n",
		"Tables:\n- users (id=",
		"header=",
		"first_space_map=",
		"space_maps=",
		"data_pages=",
		"physical=ok",
		"Indexes:\n- users.idx_users_name (id=",
	} {
		if !strings.Contains(formatted, want) {
			t.Fatalf("EngineSnapshot().String() missing %q in %q", want, formatted)
		}
	}
}

func TestEngineSnapshotStringOrderingIsDeterministic(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE accounts (id INT, email TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
		"CREATE UNIQUE INDEX idx_accounts_email ON accounts (email)",
		"CREATE INDEX idx_accounts_id ON accounts (id)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() error = %v", err)
	}
	formatted := snapshot.String()

	accountsTablePos := strings.Index(formatted, "- accounts (id=")
	usersTablePos := strings.Index(formatted, "- users (id=")
	if accountsTablePos == -1 || usersTablePos == -1 || accountsTablePos >= usersTablePos {
		t.Fatalf("EngineSnapshot().String() table order incorrect: %q", formatted)
	}

	accountsEmailPos := strings.Index(formatted, "- accounts.idx_accounts_email (id=")
	accountsIDPos := strings.Index(formatted, "- accounts.idx_accounts_id (id=")
	usersIndexPos := strings.Index(formatted, "- users.idx_users_name (id=")
	if accountsEmailPos == -1 || accountsIDPos == -1 || usersIndexPos == -1 {
		t.Fatalf("EngineSnapshot().String() missing expected index lines: %q", formatted)
	}
	if !(accountsEmailPos < accountsIDPos && accountsIDPos < usersIndexPos) {
		t.Fatalf("EngineSnapshot().String() index order incorrect: %q", formatted)
	}
}

func TestEngineReportMatchesSnapshotStringOnFreshDB(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() error = %v", err)
	}
	report, err := db.EngineReport()
	if err != nil {
		t.Fatalf("EngineReport() error = %v", err)
	}
	if report != snapshot.String() {
		t.Fatalf("EngineReport() = %q, want %q", report, snapshot.String())
	}
}

func TestEngineReportReflectsUpdatedState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	report, err := db.EngineReport()
	if err != nil {
		t.Fatalf("EngineReport() after create error = %v", err)
	}
	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() after create error = %v", err)
	}
	if report != snapshot.String() {
		t.Fatalf("EngineReport() after create = %q, want %q", report, snapshot.String())
	}

	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}
	report, err = db.EngineReport()
	if err != nil {
		t.Fatalf("EngineReport() after drop error = %v", err)
	}
	snapshot, err = db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() after drop error = %v", err)
	}
	if report != snapshot.String() {
		t.Fatalf("EngineReport() after drop = %q, want %q", report, snapshot.String())
	}
}

func TestEngineReportOnClosedDBReturnsErrClosed(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = db.EngineReport()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("EngineReport() error = %v, want ErrClosed", err)
	}
}
