package rovadb

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
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
