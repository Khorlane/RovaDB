package rovadb

import (
	"errors"
	"testing"
	"time"
)

func TestRowsScanBeforeNext(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	var got int64
	err = rows.Scan(&got)
	if !errors.Is(err, ErrScanBeforeNext) {
		t.Fatalf("Scan() error = %v, want ErrScanBeforeNext", err)
	}
}

func TestRowsScanAfterIterationEnds(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var got int64
	if err := rows.Scan(&got); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if rows.Next() {
		t.Fatal("Next() second = true, want false")
	}

	err = rows.Scan(&got)
	if !errors.Is(err, ErrScanBeforeNext) {
		t.Fatalf("Scan() after end error = %v, want ErrScanBeforeNext", err)
	}
}

func TestResultRowsAffectedCleanup(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	insertResult, err := db.Exec("INSERT INTO users VALUES (1, 'steve')")
	if err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if insertResult.RowsAffected() != 1 {
		t.Fatalf("Exec(insert).RowsAffected() = %d, want 1", insertResult.RowsAffected())
	}

	updateResult, err := db.Exec("UPDATE users SET name = 'sam' WHERE id = 999")
	if err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if updateResult.RowsAffected() != 0 {
		t.Fatalf("Exec(update).RowsAffected() = %d, want 0", updateResult.RowsAffected())
	}

	deleteResult, err := db.Exec("DELETE FROM users WHERE id = 999")
	if err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	if deleteResult.RowsAffected() != 0 {
		t.Fatalf("Exec(delete).RowsAffected() = %d, want 0", deleteResult.RowsAffected())
	}
}

func TestQueryZeroRowSelectCleanup(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}

	var id int32
	var name string
	err = rows.Scan(&id, &name)
	if !errors.Is(err, ErrScanBeforeNext) {
		t.Fatalf("Scan() error = %v, want ErrScanBeforeNext", err)
	}
}

func TestRowsNextIteratesUntilExhausted(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if !rows.Next() {
		t.Fatal("first Next() = false, want true")
	}
	if rows.idx != 0 {
		t.Fatalf("rows.idx after first Next() = %d, want 0", rows.idx)
	}
	if !rows.Next() {
		t.Fatal("second Next() = false, want true")
	}
	if rows.idx != 1 {
		t.Fatalf("rows.idx after second Next() = %d, want 1", rows.idx)
	}
	if rows.Next() {
		t.Fatal("third Next() = true, want false")
	}
	if rows.idx != len(rows.data) {
		t.Fatalf("rows.idx after exhaustion = %d, want %d", rows.idx, len(rows.data))
	}
	if rows.Next() {
		t.Fatal("Next() after exhaustion = true, want false")
	}
	if rows.idx != len(rows.data) {
		t.Fatalf("rows.idx after repeated exhaustion = %d, want %d", rows.idx, len(rows.data))
	}
}

func TestRowsNextEmptyResultSet(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() != nil {
		t.Fatalf("Err() = %v, want nil", rows.Err())
	}
}

func TestRowsDeferredErrorBlocksIteration(t *testing.T) {
	wantErr := errors.New("boom")
	rows := &Rows{
		idx: -1,
		err: wantErr,
	}

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if !errors.Is(rows.Err(), wantErr) {
		t.Fatalf("Err() = %v, want %v", rows.Err(), wantErr)
	}
}

func TestRowsCloseLifecycle(t *testing.T) {
	rows := newRows(nil, [][]any{{1}})

	if err := rows.Close(); err != nil {
		t.Fatalf("first Close() error = %v, want nil", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want nil", err)
	}
	if rows.Next() {
		t.Fatal("Next() after Close() = true, want false")
	}
	if rows.Err() != nil {
		t.Fatalf("Err() after Close() = %v, want nil", rows.Err())
	}

	rows = &Rows{idx: -1, err: wantErrRowsClose}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close() with deferred error = %v, want nil", err)
	}
	if !errors.Is(rows.Err(), wantErrRowsClose) {
		t.Fatalf("Err() after Close() = %v, want %v", rows.Err(), wantErrRowsClose)
	}
}

var wantErrRowsClose = errors.New("deferred")

func TestRowsNilReceiverLifecycle(t *testing.T) {
	var rows *Rows

	if rows.Next() {
		t.Fatal("nil Rows Next() = true, want false")
	}
	if rows.Err() != nil {
		t.Fatalf("nil Rows Err() = %v, want nil", rows.Err())
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("nil Rows Close() = %v, want nil", err)
	}
}

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

func TestRowsColumnTypesReturnsNilWhenMetadataUnavailable(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if got := rows.ColumnTypes(); got != nil {
		t.Fatalf("ColumnTypes() = %#v, want nil when metadata is unavailable", got)
	}
}

func TestRowsColumnTypesReturnsCopyForTableSelect(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, event_date DATE)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice', '2026-04-15')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT id, name, event_date FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	first := rows.ColumnTypes()
	if len(first) != 3 || first[0] != "INT" || first[1] != "TEXT" || first[2] != "DATE" {
		t.Fatalf("ColumnTypes() = %#v, want [INT TEXT DATE]", first)
	}

	first[0] = "mutated"
	second := rows.ColumnTypes()
	if len(second) != 3 || second[0] != "INT" || second[1] != "TEXT" || second[2] != "DATE" {
		t.Fatalf("ColumnTypes() after mutation = %#v, want [INT TEXT DATE]", second)
	}
}

func TestRowsColumnTypesEmptyResultAfterCloseAndExhaustion(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	rows, err := db.Query("SELECT id FROM users WHERE id = 999")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if got := rows.ColumnTypes(); len(got) != 1 || got[0] != "INT" {
		t.Fatalf("ColumnTypes() on empty result = %#v, want [INT]", got)
	}
	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if got := rows.ColumnTypes(); len(got) != 1 || got[0] != "INT" {
		t.Fatalf("ColumnTypes() after exhaustion = %#v, want [INT]", got)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := rows.ColumnTypes(); len(got) != 1 || got[0] != "INT" {
		t.Fatalf("ColumnTypes() after Close = %#v, want [INT]", got)
	}
}

func TestRowsColumnTypesNilReceiver(t *testing.T) {
	var rows *Rows
	if got := rows.ColumnTypes(); got != nil {
		t.Fatalf("nil Rows ColumnTypes() = %#v, want nil", got)
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

func TestRowsScanHappyPath(t *testing.T) {
	rows := newRows(nil, [][]any{{int64(1), "a"}})
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var i int64
	var s string
	if err := rows.Scan(&i, &s); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if i != 1 || s != "a" {
		t.Fatalf("Scan() = (%d, %q), want (1, %q)", i, s, "a")
	}
}

func TestRowsScanExactDestinationCountRequired(t *testing.T) {
	rows := newRows(nil, [][]any{{1, "a"}})
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var i int
	if err := rows.Scan(&i); !errors.Is(err, ErrScanMismatch) {
		t.Fatalf("Scan() with too few dests = %v, want ErrScanMismatch", err)
	}

	var s string
	var extra any
	if err := rows.Scan(&i, &s, &extra); !errors.Is(err, ErrScanMismatch) {
		t.Fatalf("Scan() with too many dests = %v, want ErrScanMismatch", err)
	}
}

func TestRowsScanRequiresNext(t *testing.T) {
	rows := newRows(nil, [][]any{{1}})

	var i int
	if err := rows.Scan(&i); !errors.Is(err, ErrScanBeforeNext) {
		t.Fatalf("Scan() before Next = %v, want ErrScanBeforeNext", err)
	}
}

func TestRowsScanAfterExhaustion(t *testing.T) {
	rows := newRows(nil, [][]any{{1}})
	if !rows.Next() {
		t.Fatal("first Next() = false, want true")
	}
	if rows.Next() {
		t.Fatal("second Next() = true, want false")
	}

	var i int
	if err := rows.Scan(&i); !errors.Is(err, ErrScanBeforeNext) {
		t.Fatalf("Scan() after exhaustion = %v, want ErrScanBeforeNext", err)
	}
}

func TestRowsScanTypeMismatchCases(t *testing.T) {
	t.Run("untyped int64 into int", func(t *testing.T) {
		rows := newRows(nil, [][]any{{int64(1)}})
		rows.Next()
		var i int
		if err := rows.Scan(&i); !errors.Is(err, ErrUnsupportedScanType) {
			t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
		}
	})

	t.Run("string into int", func(t *testing.T) {
		rows := newRows(nil, [][]any{{"a"}})
		rows.Next()
		var i int
		if err := rows.Scan(&i); !errors.Is(err, ErrUnsupportedScanType) {
			t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
		}
	})

	t.Run("int into string", func(t *testing.T) {
		rows := newRows(nil, [][]any{{1}})
		rows.Next()
		var s string
		if err := rows.Scan(&s); !errors.Is(err, ErrUnsupportedScanType) {
			t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
		}
	})

	t.Run("nil into int", func(t *testing.T) {
		rows := newRows(nil, [][]any{{nil}})
		rows.Next()
		var i int
		if err := rows.Scan(&i); !errors.Is(err, ErrUnsupportedScanType) {
			t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
		}
	})

	t.Run("nil into string", func(t *testing.T) {
		rows := newRows(nil, [][]any{{nil}})
		rows.Next()
		var s string
		if err := rows.Scan(&s); !errors.Is(err, ErrUnsupportedScanType) {
			t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
		}
	})

	t.Run("real into int", func(t *testing.T) {
		rows := newRows(nil, [][]any{{3.14}})
		rows.Next()
		var i int
		if err := rows.Scan(&i); !errors.Is(err, ErrUnsupportedScanType) {
			t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
		}
	})

	t.Run("int into real", func(t *testing.T) {
		rows := newRows(nil, [][]any{{1}})
		rows.Next()
		var f float64
		if err := rows.Scan(&f); !errors.Is(err, ErrUnsupportedScanType) {
			t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
		}
	})
}

func TestRowsScanTypedIntegerResultsRequireExactDestinations(t *testing.T) {
	rows := newRowsWithScanTypes(nil, [][]any{{int16(7), int32(8), int64(9)}}, []string{"SMALLINT", "INT", "BIGINT"})
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var small int16
	var regular int32
	var big int64
	if err := rows.Scan(&small, &regular, &big); err != nil {
		t.Fatalf("Scan(exact widths) error = %v", err)
	}
	if small != 7 || regular != 8 || big != 9 {
		t.Fatalf("Scan(exact widths) = (%d, %d, %d), want (7, 8, 9)", small, regular, big)
	}

	rows = newRowsWithScanTypes(nil, [][]any{{int16(7), int32(8), int64(9)}}, []string{"SMALLINT", "INT", "BIGINT"})
	if !rows.Next() {
		t.Fatal("Next() mismatch = false, want true")
	}

	tests := []struct {
		name string
		dest []any
	}{
		{name: "smallint rejects int64", dest: []any{new(int64), new(int32), new(int64)}},
		{name: "smallint rejects int", dest: []any{new(int), new(int32), new(int64)}},
		{name: "int rejects int16", dest: []any{new(int16), new(int16), new(int64)}},
		{name: "int rejects int64", dest: []any{new(int16), new(int64), new(int64)}},
		{name: "bigint rejects int32", dest: []any{new(int16), new(int32), new(int32)}},
		{name: "bigint rejects int", dest: []any{new(int16), new(int32), new(int)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := rows.Scan(tc.dest...); !errors.Is(err, ErrUnsupportedScanType) {
				t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
			}
		})
	}
}

func TestRowsScanLegacySchemaLessIntegerSeamRemainsUntyped(t *testing.T) {
	rows := newRows(nil, [][]any{{int64(3)}})
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var got int64
	if err := rows.Scan(&got); err != nil {
		t.Fatalf("Scan(*int64) error = %v", err)
	}
	if got != 3 {
		t.Fatalf("Scan(*int64) = %d, want 3", got)
	}

	rows = newRows(nil, [][]any{{int64(3)}})
	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}

	var wrong int16
	if err := rows.Scan(&wrong); !errors.Is(err, ErrUnsupportedScanType) {
		t.Fatalf("Scan(*int16) error = %v, want ErrUnsupportedScanType", err)
	}
}

func TestRowsScanAnySupport(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		rows := newRows(nil, [][]any{{1}})
		rows.Next()
		var got any
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		if got != 1 {
			t.Fatalf("Scan() got %#v, want 1", got)
		}
	})

	t.Run("string", func(t *testing.T) {
		rows := newRows(nil, [][]any{{"a"}})
		rows.Next()
		var got any
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		if got != "a" {
			t.Fatalf("Scan() got %#v, want %q", got, "a")
		}
	})

	t.Run("nil", func(t *testing.T) {
		rows := newRows(nil, [][]any{{nil}})
		rows.Next()
		var got any
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		if got != nil {
			t.Fatalf("Scan() got %#v, want nil", got)
		}
	})

	t.Run("bool", func(t *testing.T) {
		rows := newRows(nil, [][]any{{true}})
		rows.Next()
		var got any
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		if got != true {
			t.Fatalf("Scan() got %#v, want true", got)
		}
	})

	t.Run("real", func(t *testing.T) {
		rows := newRows(nil, [][]any{{3.14}})
		rows.Next()
		var got any
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		if got != 3.14 {
			t.Fatalf("Scan() got %#v, want 3.14", got)
		}
	})

	t.Run("int boundary", func(t *testing.T) {
		rows := newRows(nil, [][]any{{2147483647}})
		rows.Next()
		var got any
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		if got != 2147483647 {
			t.Fatalf("Scan() got %#v, want 2147483647", got)
		}
	})
}

func TestRowsScanUnsupportedDestinations(t *testing.T) {
	rows := newRows(nil, [][]any{{1}})
	rows.Next()

	if err := rows.Scan(nil); !errors.Is(err, ErrUnsupportedScanType) {
		t.Fatalf("Scan(nil) = %v, want ErrUnsupportedScanType", err)
	}

	var i int
	if err := rows.Scan(i); !errors.Is(err, ErrUnsupportedScanType) {
		t.Fatalf("Scan(non-pointer) = %v, want ErrUnsupportedScanType", err)
	}
}

func TestRowsScanBoolSupport(t *testing.T) {
	rows := newRows(nil, [][]any{{true}})
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var b bool
	if err := rows.Scan(&b); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if !b {
		t.Fatalf("Scan() got %v, want true", b)
	}
}

func TestRowsScanRealSupport(t *testing.T) {
	rows := newRows(nil, [][]any{{3.14}})
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var f float64
	if err := rows.Scan(&f); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if f != 3.14 {
		t.Fatalf("Scan() got %v, want 3.14", f)
	}
}

func TestTimeStringAndAccessors(t *testing.T) {
	got, err := NewTime(9, 8, 7)
	if err != nil {
		t.Fatalf("NewTime() error = %v", err)
	}
	if got.String() != "09:08:07" {
		t.Fatalf("Time.String() = %q, want %q", got.String(), "09:08:07")
	}
	if got.Hour() != 9 || got.Minute() != 8 || got.Second() != 7 {
		t.Fatalf("Time accessors = (%d, %d, %d), want (9, 8, 7)", got.Hour(), got.Minute(), got.Second())
	}
}

func TestNewTimeRejectsInvalidClockValues(t *testing.T) {
	tests := []struct {
		name   string
		hour   int
		minute int
		second int
	}{
		{name: "hour low", hour: -1, minute: 0, second: 0},
		{name: "hour high", hour: 24, minute: 0, second: 0},
		{name: "minute low", hour: 0, minute: -1, second: 0},
		{name: "minute high", hour: 0, minute: 60, second: 0},
		{name: "second low", hour: 0, minute: 0, second: -1},
		{name: "second high", hour: 0, minute: 0, second: 60},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewTime(tc.hour, tc.minute, tc.second); err == nil {
				t.Fatalf("NewTime(%d, %d, %d) error = nil, want validation failure", tc.hour, tc.minute, tc.second)
			}
		})
	}
}

func TestRowsScanTemporalSupport(t *testing.T) {
	clock, err := NewTime(12, 34, 56)
	if err != nil {
		t.Fatalf("NewTime() error = %v", err)
	}
	date := time.Date(2026, time.April, 15, 0, 0, 0, 0, time.UTC)
	timestamp := time.Date(2026, time.April, 15, 16, 17, 18, 123000000, time.UTC)

	rows := newRowsWithScanTypes(nil, [][]any{{date, clock, timestamp}}, []string{"DATE", "TIME", "TIMESTAMP"})
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}

	var gotDate time.Time
	var gotTime Time
	var gotTimestamp time.Time
	if err := rows.Scan(&gotDate, &gotTime, &gotTimestamp); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if !gotDate.Equal(date) {
		t.Fatalf("Scan(DATE) = %v, want %v", gotDate, date)
	}
	if gotTime != clock {
		t.Fatalf("Scan(Time) = %#v, want %#v", gotTime, clock)
	}
	if !gotTimestamp.Equal(timestamp) {
		t.Fatalf("Scan(TIMESTAMP) = %v, want %v", gotTimestamp, timestamp)
	}
}

func TestRowsScanTemporalTypeMismatch(t *testing.T) {
	clock, err := NewTime(1, 2, 3)
	if err != nil {
		t.Fatalf("NewTime() error = %v", err)
	}
	date := time.Date(2026, time.April, 15, 0, 0, 0, 0, time.UTC)
	timestamp := time.Date(2026, time.April, 15, 16, 17, 18, 0, time.UTC)

	tests := []struct {
		name     string
		rows     *Rows
		scanDest func() any
	}{
		{
			name:     "DATE rejects *Time",
			rows:     newRowsWithScanTypes(nil, [][]any{{date}}, []string{"DATE"}),
			scanDest: func() any { return new(Time) },
		},
		{
			name:     "TIME rejects *time.Time",
			rows:     newRowsWithScanTypes(nil, [][]any{{clock}}, []string{"TIME"}),
			scanDest: func() any { return new(time.Time) },
		},
		{
			name:     "TIMESTAMP rejects *Time",
			rows:     newRowsWithScanTypes(nil, [][]any{{timestamp}}, []string{"TIMESTAMP"}),
			scanDest: func() any { return new(Time) },
		},
		{
			name:     "DATE rejects *string",
			rows:     newRowsWithScanTypes(nil, [][]any{{date}}, []string{"DATE"}),
			scanDest: func() any { return new(string) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.rows.Next() {
				t.Fatal("Next() = false, want true")
			}
			if err := tc.rows.Scan(tc.scanDest()); !errors.Is(err, ErrUnsupportedScanType) {
				t.Fatalf("Scan() error = %v, want ErrUnsupportedScanType", err)
			}
		})
	}
}

func TestRowsScanDeferredQueryErrorPassthrough(t *testing.T) {
	wantErr := errors.New("deferred")
	rows := &Rows{idx: -1, err: wantErr}

	var i int64
	if err := rows.Scan(&i); !errors.Is(err, wantErr) {
		t.Fatalf("Scan() = %v, want %v", err, wantErr)
	}
}

func TestRowsScanClosedRows(t *testing.T) {
	rows := newRows(nil, [][]any{{1}})
	if err := rows.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	var i int64
	if err := rows.Scan(&i); !errors.Is(err, ErrRowsClosed) {
		t.Fatalf("Scan() on closed rows = %v, want ErrRowsClosed", err)
	}
}
