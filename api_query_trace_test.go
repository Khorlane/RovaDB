package rovadb

import (
	"errors"
	"testing"
)

func TestExplainQueryPathReportsTableScan(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	trace, err := db.ExplainQueryPath("SELECT name FROM users WHERE id > 1")
	if err != nil {
		t.Fatalf("ExplainQueryPath() error = %v", err)
	}
	if trace.ScanType != "table" {
		t.Fatalf("ExplainQueryPath().ScanType = %q, want %q", trace.ScanType, "table")
	}
	if trace.TableName != "users" {
		t.Fatalf("ExplainQueryPath().TableName = %q, want %q", trace.TableName, "users")
	}
	if trace.IndexName != "" {
		t.Fatalf("ExplainQueryPath().IndexName = %q, want empty", trace.IndexName)
	}
	if trace.UsesBTree {
		t.Fatal("ExplainQueryPath().UsesBTree = true, want false")
	}
}

func TestExplainQueryPathReportsPageBackedIndexScan(t *testing.T) {
	db, err := Open(testDBPath(t))
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

	trace, err := db.ExplainQueryPath("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("ExplainQueryPath() error = %v", err)
	}
	if trace.ScanType != "index" {
		t.Fatalf("ExplainQueryPath().ScanType = %q, want %q", trace.ScanType, "index")
	}
	if trace.TableName != "users" {
		t.Fatalf("ExplainQueryPath().TableName = %q, want %q", trace.TableName, "users")
	}
	if trace.IndexName != "idx_users_name" {
		t.Fatalf("ExplainQueryPath().IndexName = %q, want %q", trace.IndexName, "idx_users_name")
	}
	if !trace.UsesBTree {
		t.Fatal("ExplainQueryPath().UsesBTree = false, want true")
	}
}

func TestExplainQueryPathSurvivesReopen(t *testing.T) {
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
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	trace, err := db.ExplainQueryPath("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("ExplainQueryPath() after reopen error = %v", err)
	}
	if trace.ScanType != "index" || trace.TableName != "users" || trace.IndexName != "idx_users_name" || !trace.UsesBTree {
		t.Fatalf("ExplainQueryPath() after reopen = %#v, want index/users/idx_users_name/true", trace)
	}
}

func TestExplainQueryPathUsesLogicalIndexMetadataWhenLegacyEntriesAreCleared(t *testing.T) {
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
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	delete(db.tables["users"].Indexes, "name")

	trace, err := db.ExplainQueryPath("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("ExplainQueryPath() error = %v", err)
	}
	if trace.ScanType != "index" || !trace.UsesBTree {
		t.Fatalf("ExplainQueryPath() = %#v, want index/B+Tree trace", trace)
	}
}

func TestExplainQueryPathUsesLogicalIndexMetadataWhenRuntimeShellIsAbsent(t *testing.T) {
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
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	delete(db.tables["users"].Indexes, "name")

	trace, err := db.ExplainQueryPath("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("ExplainQueryPath() error = %v", err)
	}
	if trace.ScanType != "index" || trace.IndexName != "idx_users_name" || !trace.UsesBTree {
		t.Fatalf("ExplainQueryPath() = %#v, want index/users/idx_users_name/true", trace)
	}
}

func TestExplainQueryPathRejectsIndexScanWithNonIndexRoot(t *testing.T) {
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

	table := db.tables["users"]
	index := table.Indexes["name"]
	indexDef := table.IndexDefinition("idx_users_name")
	if table == nil || index == nil || indexDef == nil {
		t.Fatalf("index setup failed: table=%v index=%v indexDef=%v", table, index, indexDef)
	}
	index.RootPageID = uint32(table.RootPageID())
	indexDef.RootPageID = uint32(table.RootPageID())

	_, err = db.ExplainQueryPath("SELECT id FROM users WHERE name = 'alice'")
	if err == nil || err.Error() != "storage: corrupted index page" {
		t.Fatalf("ExplainQueryPath() error = %v, want %q", err, "storage: corrupted index page")
	}
}

func TestExplainQueryPathOnClosedDBReturnsErrClosed(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = db.ExplainQueryPath("SELECT 1")
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("ExplainQueryPath() error = %v, want ErrClosed", err)
	}
}

func TestExplainQueryPathReturnsParseErrorForMalformedQuery(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	_, err = db.ExplainQueryPath("SELECT * FROM users WHERE id =")
	if err == nil || err.Error() != "parse: invalid where clause" {
		t.Fatalf("ExplainQueryPath() error = %v, want %q", err, "parse: invalid where clause")
	}
}
