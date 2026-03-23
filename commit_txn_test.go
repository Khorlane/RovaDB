package rovadb

import (
	"context"
	"errors"
	"testing"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
	"github.com/Khorlane/RovaDB/internal/txn"
)

func TestCommitFlushesDirtyPages(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	db.beginTxn()
	stagedTables := cloneTables(db.tables)
	if _, err := executor.Execute(&parser.CreateTableStmt{
		Name: "t",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
		},
	}, stagedTables); err != nil {
		t.Fatalf("executor.Execute(create) error = %v", err)
	}
	if err := db.applyStagedCreate(stagedTables, "t"); err != nil {
		t.Fatalf("applyStagedCreate() error = %v", err)
	}

	dirtyBefore := db.pager.DirtyPages()
	if len(dirtyBefore) == 0 {
		t.Fatal("len(db.pager.DirtyPages()) = 0, want dirty pages before commit")
	}

	db.txn.MarkDirty()
	if err := db.commitTxn(); err != nil {
		t.Fatalf("commitTxn() error = %v", err)
	}
	db.tables = stagedTables

	if len(db.pager.DirtyPages()) != 0 {
		t.Fatalf("len(db.pager.DirtyPages()) = %d, want 0 after commit", len(db.pager.DirtyPages()))
	}
	catalogPage, err := db.pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get(0) error = %v", err)
	}
	rootPage, err := db.pager.Get(1)
	if err != nil {
		t.Fatalf("pager.Get(1) error = %v", err)
	}
	if db.pager.HasOriginal(catalogPage) || db.pager.HasOriginal(rootPage) {
		t.Fatal("rollback snapshots still tracked after commit")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t")
}

func TestCommitWithoutDirtyIsNoOp(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	db.beginTxn()
	if err := db.commitTxn(); err != nil {
		t.Fatalf("commitTxn() error = %v", err)
	}
	if db.txn == nil {
		t.Fatal("db.txn = nil, want committed txn preserved")
	}
	if db.txn.CanCommit() {
		t.Fatal("db.txn.CanCommit() = true, want false after commit")
	}
	if len(db.pager.DirtyPages()) != 0 {
		t.Fatalf("len(db.pager.DirtyPages()) = %d, want 0", len(db.pager.DirtyPages()))
	}
}

func TestCommitErrorPropagates(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	page := db.pager.NewPage()
	copy(page.Data(), []byte("boom"))
	db.pager.MarkDirty(page)
	db.txn = txn.NewTxn()
	db.txn.MarkDirty()

	if err := db.file.File().Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	err = db.commitTxn()
	if err == nil {
		t.Fatal("commitTxn() error = nil, want failure")
	}
	if db.txn == nil {
		t.Fatal("db.txn = nil, want active txn retained")
	}
	if !db.txn.CanCommit() {
		t.Fatal("db.txn.CanCommit() = false, want true after failed commit")
	}
	if len(db.pager.DirtyPages()) == 0 {
		t.Fatal("len(db.pager.DirtyPages()) = 0, want dirty pages retained after failed commit")
	}
}

func TestRollbackRestoresDirtyPages(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	page := db.pager.NewPage()
	copy(page.Data(), []byte("before"))
	db.txn = txn.NewTxn()
	db.txn.MarkDirty()
	if err := db.commitTxn(); err != nil {
		t.Fatalf("commitTxn() error = %v", err)
	}

	db.pager.MarkDirtyWithOriginal(page)
	copy(page.Data(), []byte("after!"))
	db.beginTxn()
	if !db.pager.IsDirty(page) {
		t.Fatal("page not dirty before rollback")
	}

	db.rollbackTxn()

	if db.pager.IsDirty(page) {
		t.Fatal("page still dirty after rollback")
	}
	if db.pager.HasOriginal(page) {
		t.Fatal("page still has rollback snapshot after rollback")
	}
	if got := string(page.Data()[:6]); got != "before" {
		t.Fatalf("page data after rollback = %q, want %q", got, "before")
	}
}

func TestApplyErrorTriggersRollback(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	err = db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		table := stagedTables["t"]
		table.Rows = append(table.Rows, []parser.Value{parser.Int64Value(2)})
		table.SetStorageMeta(table.RootPageID(), uint32(len(table.Rows)))

		encodedRows, err := encodeRows(table.Rows)
		if err != nil {
			return err
		}
		tablePageData, err := storage.BuildTablePageData(encodedRows)
		if err != nil {
			return err
		}
		catalogData, err := storage.BuildCatalogPageData(catalogFromTables(stagedTables))
		if err != nil {
			return err
		}
		if err := db.stageDirtyState(catalogData, []stagedPage{{
			id:   table.RootPageID(),
			data: tablePageData,
		}}); err != nil {
			return err
		}
		return errors.New("boom")
	})
	if err == nil {
		t.Fatal("execMutatingStatement() error = nil, want failure")
	}
	if len(db.pager.DirtyPages()) != 0 {
		t.Fatalf("len(db.pager.DirtyPages()) = %d, want 0 after rollback", len(db.pager.DirtyPages()))
	}
	assertSelectIntRows(t, db, "SELECT * FROM t", 1)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestCommitClearsRollbackSnapshots(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	db.beginTxn()
	stagedTables := cloneTables(db.tables)
	if _, err := executor.Execute(&parser.CreateTableStmt{
		Name: "t",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
		},
	}, stagedTables); err != nil {
		t.Fatalf("executor.Execute(create) error = %v", err)
	}
	if err := db.applyStagedCreate(stagedTables, "t"); err != nil {
		t.Fatalf("applyStagedCreate() error = %v", err)
	}
	if len(db.pager.DirtyPages()) == 0 {
		t.Fatal("no dirty pages before commit")
	}

	db.txn.MarkDirty()
	if err := db.commitTxn(); err != nil {
		t.Fatalf("commitTxn() error = %v", err)
	}
	db.tables = stagedTables

	catalogPage, err := db.pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get(0) error = %v", err)
	}
	rootPage, err := db.pager.Get(1)
	if err != nil {
		t.Fatalf("pager.Get(1) error = %v", err)
	}
	if db.pager.HasOriginal(catalogPage) || db.pager.HasOriginal(rootPage) {
		t.Fatal("rollback snapshot still tracked after commit")
	}
	if db.pager.IsDirty(catalogPage) || db.pager.IsDirty(rootPage) {
		t.Fatal("page still dirty after commit")
	}
}

func TestRollbackAfterFailedCommitRestoresState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	err = db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		table := stagedTables["t"]
		table.Rows[0][0] = parser.Int64Value(99)
		if err := db.applyStagedTableRewrite(stagedTables, "t"); err != nil {
			return err
		}
		if err := db.file.File().Close(); err != nil {
			return err
		}
		return nil
	})
	if err == nil {
		t.Fatal("execMutatingStatement() error = nil, want commit failure")
	}
	if len(db.pager.DirtyPages()) != 0 {
		t.Fatalf("len(db.pager.DirtyPages()) = %d, want 0 after rollback", len(db.pager.DirtyPages()))
	}
	if db.txn != nil {
		t.Fatalf("db.txn = %#v, want nil after failed commit cleanup", db.txn)
	}

	if db.file, err = storage.OpenOrCreate(path); err != nil {
		t.Fatalf("storage.OpenOrCreate() error = %v", err)
	}
	db.pager, err = storage.NewPager(db.file.File())
	if err != nil {
		t.Fatalf("storage.NewPager() error = %v", err)
	}

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}
