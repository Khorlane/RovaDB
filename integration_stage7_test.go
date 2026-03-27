package rovadb

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestStage7IndexLifecycleAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
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
	if err := db.defineLegacyBasicIndex("users", "name"); err != nil {
		t.Fatalf("defineLegacyBasicIndex() error = %v", err)
	}
	assertQueryIntRows(t, db, "SELECT id FROM users WHERE name = 'alice' ORDER BY id", 1, 3)
	assertIndexConsistency(t, db.tables["users"])
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertQueryIntRows(t, db, "SELECT id FROM users WHERE name = 'alice' ORDER BY id", 1, 3)
	assertIndexConsistency(t, db.tables["users"])
}

func TestStage7MutationAndIndexCorrectness(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.defineLegacyBasicIndex("users", "name"); err != nil {
		t.Fatalf("defineLegacyBasicIndex() error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET name = 'alice' WHERE id = 2"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if _, err := db.Exec("DELETE FROM users WHERE id = 1"); err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}

	assertQueryIntRows(t, db, "SELECT id FROM users WHERE name = 'alice' ORDER BY id", 2)
	assertQueryIntRows(t, db, "SELECT id FROM users WHERE name = 'cara'", 3)
	assertIndexConsistency(t, db.tables["users"])
}

func TestStage7IndexedAndNonIndexedQueriesStayAligned(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'alice')",
		"INSERT INTO users VALUES (4, 'dina')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	indexableBaseline := collectIntRows(t, db, "SELECT id FROM users WHERE name = 'alice' ORDER BY id")
	nonIndexedBaseline := collectIntRows(t, db, "SELECT id FROM users WHERE id > 2 ORDER BY id")
	fullBaseline := collectIntRows(t, db, "SELECT id FROM users ORDER BY id")

	if err := db.defineLegacyBasicIndex("users", "name"); err != nil {
		t.Fatalf("defineLegacyBasicIndex() error = %v", err)
	}

	if got := collectIntRows(t, db, "SELECT id FROM users WHERE name = 'alice' ORDER BY id"); !reflect.DeepEqual(got, indexableBaseline) {
		t.Fatalf("indexed equality rows = %#v, want %#v", got, indexableBaseline)
	}
	if got := collectIntRows(t, db, "SELECT id FROM users WHERE id > 2 ORDER BY id"); !reflect.DeepEqual(got, nonIndexedBaseline) {
		t.Fatalf("non-indexed where rows = %#v, want %#v", got, nonIndexedBaseline)
	}
	if got := collectIntRows(t, db, "SELECT id FROM users ORDER BY id"); !reflect.DeepEqual(got, fullBaseline) {
		t.Fatalf("full scan rows = %#v, want %#v", got, fullBaseline)
	}
	assertIndexConsistency(t, db.tables["users"])
}

func TestStage7IndexEdgeCases(t *testing.T) {
	t.Run("null equality", func(t *testing.T) {
		db, err := Open(testDBPath(t))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer db.Close()

		for _, sql := range []string{
			"CREATE TABLE users (id INT, name TEXT)",
			"INSERT INTO users VALUES (1, NULL)",
			"INSERT INTO users VALUES (2, 'bob')",
			"INSERT INTO users VALUES (3, NULL)",
		} {
			if _, err := db.Exec(sql); err != nil {
				t.Fatalf("Exec(%q) error = %v", sql, err)
			}
		}
		if err := db.defineLegacyBasicIndex("users", "name"); err != nil {
			t.Fatalf("defineLegacyBasicIndex() error = %v", err)
		}

		assertQueryIntRows(t, db, "SELECT id FROM users WHERE name = NULL ORDER BY id", 1, 3)
		assertIndexConsistency(t, db.tables["users"])
	})

	t.Run("empty table with index", func(t *testing.T) {
		db, err := Open(testDBPath(t))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer db.Close()

		if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
			t.Fatalf("Exec(create) error = %v", err)
		}
		if err := db.defineLegacyBasicIndex("users", "name"); err != nil {
			t.Fatalf("defineLegacyBasicIndex() error = %v", err)
		}

		assertQueryIntRows(t, db, "SELECT COUNT(*) FROM users WHERE name = 'alice'", 0)
		assertIndexConsistency(t, db.tables["users"])
	})

	t.Run("single row", func(t *testing.T) {
		db, err := Open(testDBPath(t))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer db.Close()

		if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
			t.Fatalf("Exec(create) error = %v", err)
		}
		if _, err := db.Exec("INSERT INTO users VALUES (1, 'solo')"); err != nil {
			t.Fatalf("Exec(insert) error = %v", err)
		}
		if err := db.defineLegacyBasicIndex("users", "name"); err != nil {
			t.Fatalf("defineLegacyBasicIndex() error = %v", err)
		}

		assertQueryIntRows(t, db, "SELECT id FROM users WHERE name = 'solo'", 1)
		assertIndexConsistency(t, db.tables["users"])
	})

	t.Run("large-ish identical values", func(t *testing.T) {
		db, err := Open(testDBPath(t))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer db.Close()

		if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
			t.Fatalf("Exec(create) error = %v", err)
		}
		for i := 1; i <= 50; i++ {
			if _, err := db.Exec("INSERT INTO users VALUES (" + itoa(i) + ", 'same')"); err != nil {
				t.Fatalf("Exec(insert %d) error = %v", i, err)
			}
		}
		if err := db.defineLegacyBasicIndex("users", "name"); err != nil {
			t.Fatalf("defineLegacyBasicIndex() error = %v", err)
		}

		got := collectIntRows(t, db, "SELECT COUNT(*) FROM users WHERE name = 'same'")
		if !reflect.DeepEqual(got, []int{50}) {
			t.Fatalf("count rows = %#v, want []int{50}", got)
		}
		assertIndexConsistency(t, db.tables["users"])
	})
}

func TestStage7OpenPreIndexCatalogStillWorks(t *testing.T) {
	path := testDBPath(t)
	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	rootPage := pager.NewPage()
	storage.InitTableRootPage(rootPage)
	row, err := storage.EncodeRow([]parser.Value{parser.Int64Value(1), parser.StringValue("legacy")})
	if err != nil {
		t.Fatalf("EncodeRow() error = %v", err)
	}
	if err := storage.AppendRowToTablePage(rootPage, row); err != nil {
		t.Fatalf("AppendRowToTablePage() error = %v", err)
	}
	writeMalformedCatalogPage(t, pager, malformedCatalogBytes([]malformedCatalogTable{
		{
			name:       "users",
			rootPageID: uint32(rootPage.ID()),
			rowCount:   1,
			columns: []malformedCatalogColumn{
				{name: "id", typ: storage.CatalogColumnTypeInt},
				{name: "name", typ: storage.CatalogColumnTypeText},
			},
		},
	}))

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	assertQueryIntRows(t, db, "SELECT id FROM users", 1)
	if db.tables["users"] == nil || len(db.tables["users"].Indexes) != 0 {
		t.Fatalf("legacy db indexes = %#v, want empty", db.tables["users"])
	}
}

func assertIndexConsistency(t *testing.T, table *executor.Table) {
	t.Helper()

	if table == nil || len(table.Indexes) == 0 {
		return
	}

	columnNames := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		columnNames = append(columnNames, column.Name)
	}
	for columnName, index := range table.Indexes {
		if index == nil {
			t.Fatalf("table.Indexes[%q] = nil", columnName)
		}
		tmp := planner.NewBasicIndex(table.Name, columnName)
		if err := tmp.Rebuild(columnNames, table.Rows); err != nil {
			t.Fatalf("tmp.Rebuild(%q) error = %v", columnName, err)
		}
		if !reflect.DeepEqual(tmp.Entries, index.Entries) {
			t.Fatalf("index %q entries = %#v, want %#v", columnName, index.Entries, tmp.Entries)
		}
	}
}

func assertQueryIntRows(t *testing.T, db *DB, sql string, want ...int) {
	t.Helper()
	got := collectIntRows(t, db, sql)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("collectIntRows(%q) = %#v, want %#v", sql, got, want)
	}
}

func collectIntRows(t *testing.T, db *DB, sql string) []int {
	t.Helper()

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q) error = %v", sql, err)
	}
	defer rows.Close()

	got := []int{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan(%q) error = %v", sql, err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err(%q) = %v", sql, err)
	}
	return got
}

func itoa(v int) string {
	return strconv.Itoa(v)
}
