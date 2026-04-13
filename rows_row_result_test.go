package rovadb

import (
	"errors"
	"testing"
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
