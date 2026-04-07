package rovadb

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/Khorlane/RovaDB/internal/bufferpool"
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

	if err := db.beginTxn(); err != nil {
		t.Fatalf("beginTxn() error = %v", err)
	}
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

	if err := db.txn.MarkDirty(); err != nil {
		t.Fatalf("txn.MarkDirty() error = %v", err)
	}
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
	rootPage, err := db.pager.Get(stagedTables["t"].RootPageID())
	if err != nil {
		t.Fatalf("pager.Get(root) error = %v", err)
	}
	if db.pager.HasOriginal(catalogPage) || db.pager.HasOriginal(rootPage) {
		t.Fatal("rollback snapshots still tracked after commit")
	}
	if _, err := os.Stat(storage.JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists", err)
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

	if err := db.beginTxn(); err != nil {
		t.Fatalf("beginTxn() error = %v", err)
	}
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
	if err := db.txn.MarkDirty(); err != nil {
		t.Fatalf("txn.MarkDirty() error = %v", err)
	}

	if err := db.file.File().Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	err = db.commitTxn()
	if err == nil {
		t.Fatal("commitTxn() error = nil, want failure")
	}
	if db.txn == nil {
		t.Fatal("db.txn = nil, want committed txn retained")
	}
	if db.txn.CanCommit() {
		t.Fatal("db.txn.CanCommit() = true, want false after post-WAL checkpoint failure")
	}
	if len(db.pager.DirtyPages()) != 0 {
		t.Fatalf("len(db.pager.DirtyPages()) = %d, want 0 after post-WAL checkpoint failure", len(db.pager.DirtyPages()))
	}
}

func TestJournaledCommitCreatesAndRemovesJournal(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if _, err := db.Exec("UPDATE t SET id = 2 WHERE id = 1"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if _, err := os.Stat(storage.JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 2)
}

func TestJournalWrittenBeforeDatabaseOverwrite(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	dataPageIDs, err := committedTableDataPageIDs(db.pool, db.tables["t"])
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	if len(dataPageIDs) != 1 {
		t.Fatalf("len(committedTableDataPageIDs()) = %d, want 1", len(dataPageIDs))
	}
	dataPageID := dataPageIDs[0]

	hookCalled := false
	db.afterJournalWriteHook = func() error {
		hookCalled = true

		journalFile, err := storage.OpenJournalFile(storage.JournalPath(path))
		if err != nil {
			return err
		}
		defer journalFile.Close()

		header, err := storage.ReadJournalHeader(journalFile)
		if err != nil {
			return err
		}
		if header.EntryCount == 0 {
			t.Fatal("journal entry count = 0, want > 0")
		}

		rawDB, pager := openRawStorage(t, path)
		defer rawDB.Close()
		page, err := pager.Get(dataPageID)
		if err != nil {
			return err
		}
		payloads, err := storage.ReadRowsFromTablePage(page)
		if err != nil {
			return err
		}
		if len(payloads) != 1 {
			t.Fatalf("len(payloads) = %d, want 1", len(payloads))
		}
		values, err := storage.DecodeSlottedRow(payloads[0], []uint8{storage.CatalogColumnTypeInt})
		if err != nil {
			return err
		}
		if values[0].I64 != 1 {
			t.Fatalf("disk value before flush = %d, want 1", values[0].I64)
		}
		return nil
	}

	if _, err := db.Exec("UPDATE t SET id = 2 WHERE id = 1"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if !hookCalled {
		t.Fatal("afterJournalWriteHook was not called")
	}
}

func TestCommitFailureLeavesJournalForRecovery(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	db.afterJournalWriteHook = func() error {
		return errors.New("boom after journal")
	}
	if _, err := db.Exec("UPDATE t SET id = 2 WHERE id = 1"); err == nil {
		t.Fatal("Exec(update) error = nil, want failure")
	}

	if _, err := os.Stat(storage.JournalPath(path)); err != nil {
		t.Fatalf("journal stat error = %v, want present journal", err)
	}
}

func TestCommitWithoutOriginalPagesSkipsJournal(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if err := db.beginTxn(); err != nil {
		t.Fatalf("beginTxn() error = %v", err)
	}
	page := db.pager.NewPage()
	copy(page.Data(), []byte("new"))
	if err := db.txn.MarkDirty(); err != nil {
		t.Fatalf("txn.MarkDirty() error = %v", err)
	}
	if err := db.commitTxn(); err != nil {
		t.Fatalf("commitTxn() error = %v", err)
	}

	if _, err := os.Stat(storage.JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists", err)
	}
}

func TestSecondWriterAttemptReturnsWriteConflict(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if err := db.beginWriteTxn(); err != nil {
		t.Fatalf("beginWriteTxn() error = %v", err)
	}
	defer db.endWriteTxn()

	if _, err := db.Exec("CREATE TABLE t (id INT)"); err == nil {
		t.Fatal("Exec(create) error = nil, want write conflict")
	} else if err.Error() != "execution: write conflict" {
		t.Fatalf("Exec(create) error = %q, want %q", err.Error(), "execution: write conflict")
	}
}

func TestReadCanProceedWhileWriterGateActive(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if err := db.beginWriteTxn(); err != nil {
		t.Fatalf("beginWriteTxn() error = %v", err)
	}
	defer db.endWriteTxn()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestWriterGateReleasedAfterMutationFailure(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	wantErr := errors.New("boom")
	if _, err := db.execMutatingStatement(func() error {
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("execMutatingStatement() error = %v, want %v", err, wantErr)
	}
	if err := db.beginWriteTxn(); err != nil {
		t.Fatalf("beginWriteTxn() after failure error = %v, want nil", err)
	}
	db.endWriteTxn()
}

func TestSequentialWritesStillWorkWithWriterGate(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(first insert) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (2)"); err != nil {
		t.Fatalf("Exec(second insert) error = %v", err)
	}

	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)
}

func TestStageDirtyStateUsesPrivateFramesAndLeavesCommittedReadUnchanged(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	dataPageID := singleCommittedDataPageIDForTest(t, db, "t")
	committedBefore, err := readCommittedPageData(db.pool, dataPageID)
	if err != nil {
		t.Fatalf("readCommittedPageData(before) error = %v", err)
	}

	if err := db.beginTxn(); err != nil {
		t.Fatalf("beginTxn() error = %v", err)
	}
	stagedTables := cloneTables(db.tables)
	if err := db.loadRowsIntoTables(stagedTables, "t"); err != nil {
		t.Fatalf("loadRowsIntoTables() error = %v", err)
	}
	stagedTables["t"].Rows[0][0] = parser.Int64Value(2)

	if err := db.applyStagedTableRewrite(stagedTables, "t"); err != nil {
		t.Fatalf("applyStagedTableRewrite() error = %v", err)
	}

	committedDuring, err := readCommittedPageData(db.pool, dataPageID)
	if err != nil {
		t.Fatalf("readCommittedPageData(during) error = %v", err)
	}
	if !bytes.Equal(committedDuring, committedBefore) {
		t.Fatal("committed page bytes changed during active staged write")
	}

	privateDataPageID := privateOwnedDataPageIDForTest(t, db, stagedTables["t"])
	privateFrame, err := db.pool.GetPrivatePage(bufferpool.PageID(privateDataPageID))
	if err != nil {
		t.Fatalf("GetPrivatePage() error = %v", err)
	}
	if !db.pool.IsDirty(privateFrame) {
		t.Fatal("private frame is clean, want dirty after staged write")
	}
	privateRows, err := storage.ReadSlottedRowsFromTablePageData(privateFrame.Data[:], []uint8{storage.CatalogColumnTypeInt})
	if err != nil {
		t.Fatalf("ReadSlottedRowsFromTablePageData(private) error = %v", err)
	}
	if len(privateRows) != 1 || privateRows[0][0] != parser.Int64Value(2) {
		t.Fatalf("private rows = %#v, want [[2]]", privateRows)
	}
	db.pool.UnlatchExclusive(privateFrame)
	db.pool.Unpin(privateFrame)

	if err := db.rollbackTxn(); err != nil {
		t.Fatalf("rollbackTxn() error = %v", err)
	}
	db.clearTxn()
}

func TestReaderHelpersStayOnCommittedRowsWhilePrivateFrameExists(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
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

	if err := db.beginTxn(); err != nil {
		t.Fatalf("beginTxn() error = %v", err)
	}
	stagedTables := cloneTables(db.tables)
	if err := db.loadRowsIntoTables(stagedTables, "users"); err != nil {
		t.Fatalf("loadRowsIntoTables() error = %v", err)
	}
	stagedTables["users"].Rows[0][1] = parser.StringValue("beth")

	if err := db.applyStagedTableRewrite(stagedTables, "users"); err != nil {
		t.Fatalf("applyStagedTableRewrite() error = %v", err)
	}

	rows, err := db.scanTableRows(db.tables["users"])
	if err != nil {
		t.Fatalf("scanTableRows() error = %v", err)
	}
	if len(rows) != 1 || rows[0][0] != parser.Int64Value(1) || rows[0][1] != parser.StringValue("alice") {
		t.Fatalf("scanTableRows() = %#v, want committed row [1 alice]", rows)
	}

	row, err := db.fetchRowByLocator(db.tables["users"], storage.RowLocator{
		PageID: uint32(singleCommittedDataPageIDForTest(t, db, "users")),
		SlotID: 0,
	})
	if err != nil {
		t.Fatalf("fetchRowByLocator() error = %v", err)
	}
	if len(row) != 2 || row[0] != parser.Int64Value(1) || row[1] != parser.StringValue("alice") {
		t.Fatalf("fetchRowByLocator() = %#v, want committed row [1 alice]", row)
	}

	privateDataPageID := privateOwnedDataPageIDForTest(t, db, stagedTables["users"])
	privateFrame, err := db.pool.GetPrivatePage(bufferpool.PageID(privateDataPageID))
	if err != nil {
		t.Fatalf("GetPrivatePage() error = %v", err)
	}
	privateRows, err := storage.ReadSlottedRowsFromTablePageData(privateFrame.Data[:], []uint8{
		storage.CatalogColumnTypeInt,
		storage.CatalogColumnTypeText,
	})
	if err != nil {
		t.Fatalf("ReadSlottedRowsFromTablePageData(private) error = %v", err)
	}
	if len(privateRows) != 1 || privateRows[0][1] != parser.StringValue("beth") {
		t.Fatalf("private rows = %#v, want private row [1 beth]", privateRows)
	}
	db.pool.UnlatchExclusive(privateFrame)
	db.pool.Unpin(privateFrame)

	if err := db.rollbackTxn(); err != nil {
		t.Fatalf("rollbackTxn() error = %v", err)
	}
	if db.pool.HasPrivatePage(bufferpool.PageID(singleCommittedDataPageIDForTest(t, db, "users"))) {
		t.Fatal("private frame still present after rollback")
	}
	db.clearTxn()
}

func TestRollbackDiscardsPrivatePagesAndFreshWriteRecreatesFromCommitted(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	dataPageID := singleCommittedDataPageIDForTest(t, db, "t")
	committedBefore, err := readCommittedPageData(db.pool, dataPageID)
	if err != nil {
		t.Fatalf("readCommittedPageData(before) error = %v", err)
	}

	if err := db.beginTxn(); err != nil {
		t.Fatalf("beginTxn() error = %v", err)
	}
	stagedTables := cloneTables(db.tables)
	if err := db.loadRowsIntoTables(stagedTables, "t"); err != nil {
		t.Fatalf("loadRowsIntoTables() error = %v", err)
	}
	stagedTables["t"].Rows[0][0] = parser.Int64Value(2)
	if err := db.applyStagedTableRewrite(stagedTables, "t"); err != nil {
		t.Fatalf("applyStagedTableRewrite() error = %v", err)
	}
	privateDataPageID := privateOwnedDataPageIDForTest(t, db, stagedTables["t"])
	if !db.pool.HasPrivatePage(bufferpool.PageID(privateDataPageID)) {
		t.Fatal("HasPrivatePage(root) = false, want private frame before rollback")
	}

	if err := db.rollbackTxn(); err != nil {
		t.Fatalf("rollbackTxn() error = %v", err)
	}
	if db.pool.HasPrivatePage(bufferpool.PageID(dataPageID)) {
		t.Fatal("HasPrivatePage(root) = true, want false after rollback")
	}

	committedAfter, err := readCommittedPageData(db.pool, dataPageID)
	if err != nil {
		t.Fatalf("readCommittedPageData(after rollback) error = %v", err)
	}
	if !bytes.Equal(committedAfter, committedBefore) {
		t.Fatal("committed page bytes changed after rollback")
	}
	rows, err := db.scanTableRows(db.tables["t"])
	if err != nil {
		t.Fatalf("scanTableRows() after rollback error = %v", err)
	}
	if len(rows) != 1 || rows[0][0] != parser.Int64Value(1) {
		t.Fatalf("scanTableRows() after rollback = %#v, want [[1]]", rows)
	}

	privateAfterRollback, err := db.pool.GetPrivatePage(bufferpool.PageID(dataPageID))
	if err != nil {
		t.Fatalf("GetPrivatePage() after rollback error = %v", err)
	}
	privateRows, err := storage.ReadSlottedRowsFromTablePageData(privateAfterRollback.Data[:], []uint8{storage.CatalogColumnTypeInt})
	if err != nil {
		t.Fatalf("ReadSlottedRowsFromTablePageData(private after rollback) error = %v", err)
	}
	if len(privateRows) != 1 || privateRows[0][0] != parser.Int64Value(1) {
		t.Fatalf("privateRows after rollback = %#v, want recreated committed row [[1]]", privateRows)
	}
	db.pool.UnlatchExclusive(privateAfterRollback)
	db.pool.Unpin(privateAfterRollback)
	db.clearTxn()
}

func TestCommitPromotesPrivatePagesAndReadersSeeNewContent(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
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

	if err := db.beginTxn(); err != nil {
		t.Fatalf("beginTxn() error = %v", err)
	}
	stagedTables := cloneTables(db.tables)
	if err := db.loadRowsIntoTables(stagedTables, "users"); err != nil {
		t.Fatalf("loadRowsIntoTables() error = %v", err)
	}
	stagedTables["users"].Rows[0][1] = parser.StringValue("beth")
	if err := db.applyStagedTableRewrite(stagedTables, "users"); err != nil {
		t.Fatalf("applyStagedTableRewrite() error = %v", err)
	}

	rowsBeforeCommit, err := db.scanTableRows(db.tables["users"])
	if err != nil {
		t.Fatalf("scanTableRows(before commit) error = %v", err)
	}
	if len(rowsBeforeCommit) != 1 || rowsBeforeCommit[0][1] != parser.StringValue("alice") {
		t.Fatalf("scanTableRows(before commit) = %#v, want committed row [1 alice]", rowsBeforeCommit)
	}

	if err := db.txn.MarkDirty(); err != nil {
		t.Fatalf("txn.MarkDirty() error = %v", err)
	}
	if err := db.commitTxn(); err != nil {
		t.Fatalf("commitTxn() error = %v", err)
	}
	db.tables = stagedTables

	dataPageID := singleCommittedDataPageIDForTest(t, db, "users")
	if db.pool.HasPrivatePage(bufferpool.PageID(dataPageID)) {
		t.Fatal("private frame still present after commit promotion")
	}

	rowsAfterCommit, err := db.scanTableRows(db.tables["users"])
	if err != nil {
		t.Fatalf("scanTableRows(after commit) error = %v", err)
	}
	if len(rowsAfterCommit) != 1 || rowsAfterCommit[0][0] != parser.Int64Value(1) || rowsAfterCommit[0][1] != parser.StringValue("beth") {
		t.Fatalf("scanTableRows(after commit) = %#v, want promoted row [1 beth]", rowsAfterCommit)
	}

	row, err := db.fetchRowByLocator(db.tables["users"], storage.RowLocator{
		PageID: uint32(dataPageID),
		SlotID: 0,
	})
	if err != nil {
		t.Fatalf("fetchRowByLocator(after commit) error = %v", err)
	}
	if len(row) != 2 || row[0] != parser.Int64Value(1) || row[1] != parser.StringValue("beth") {
		t.Fatalf("fetchRowByLocator(after commit) = %#v, want promoted row [1 beth]", row)
	}

	committedPageData, err := readCommittedPageData(db.pool, dataPageID)
	if err != nil {
		t.Fatalf("readCommittedPageData(after commit) error = %v", err)
	}
	decoded, err := storage.ReadSlottedRowsFromTablePageData(committedPageData, []uint8{
		storage.CatalogColumnTypeInt,
		storage.CatalogColumnTypeText,
	})
	if err != nil {
		t.Fatalf("ReadSlottedRowsFromTablePageData(after commit) error = %v", err)
	}
	if len(decoded) != 1 || decoded[0][1] != parser.StringValue("beth") {
		t.Fatalf("decoded committed rows = %#v, want promoted row [1 beth]", decoded)
	}

	db.clearTxn()
}

func TestCommitCheckpointResetsWALToHeaderOnly(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	records, err := storage.ReadWALRecords(path)
	if err != nil {
		t.Fatalf("ReadWALRecords() error = %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("len(ReadWALRecords()) = %d, want 0 after successful checkpoint", len(records))
	}

	assertSelectIntRows(t, db, "SELECT * FROM t")
}

func TestInsertCheckpointLeavesWALHeaderOnly(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	after, err := storage.ReadWALRecords(path)
	if err != nil {
		t.Fatalf("ReadWALRecords(after) error = %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("len(ReadWALRecords(after)) = %d, want 0 after successful checkpoint", len(after))
	}
}

func TestWALAppendFailurePreventsPromotionAndCommitSuccess(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
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

	beforeRecords, err := storage.ReadWALRecords(path)
	if err != nil {
		t.Fatalf("ReadWALRecords(before) error = %v", err)
	}

	originalAppendFrame := appendWALFrameRecord
	appendWALFrameRecord = func(path string, frame storage.WALFrame) error {
		return errors.New("wal frame append failed")
	}
	defer func() {
		appendWALFrameRecord = originalAppendFrame
	}()

	_, err = db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		if err := db.loadRowsIntoTables(stagedTables, "users"); err != nil {
			return err
		}
		stagedTables["users"].Rows[0][1] = parser.StringValue("beth")
		return db.applyStagedTableRewrite(stagedTables, "users")
	})
	if err == nil {
		t.Fatal("execMutatingStatement() error = nil, want commit failure")
	}

	if db.pool.HasPrivatePage(bufferpool.PageID(singleCommittedDataPageIDForTest(t, db, "users"))) {
		t.Fatal("private frame still present after WAL append failure")
	}

	afterRecords, err := storage.ReadWALRecords(path)
	if err != nil {
		t.Fatalf("ReadWALRecords(after) error = %v", err)
	}
	if len(afterRecords) != len(beforeRecords) {
		t.Fatalf("len(ReadWALRecords(after)) = %d, want %d", len(afterRecords), len(beforeRecords))
	}

	assertSelectTextRows(t, db, "SELECT name FROM users", "alice")
}

func TestWALSyncFailurePreventsPromotionAndCommitSuccess(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
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

	beforeRecords, err := storage.ReadWALRecords(path)
	if err != nil {
		t.Fatalf("ReadWALRecords(before) error = %v", err)
	}

	originalSyncWAL := syncWAL
	syncWAL = func(path string) error {
		return errors.New("wal sync failed")
	}
	defer func() {
		syncWAL = originalSyncWAL
	}()

	_, err = db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		if err := db.loadRowsIntoTables(stagedTables, "users"); err != nil {
			return err
		}
		stagedTables["users"].Rows[0][1] = parser.StringValue("beth")
		return db.applyStagedTableRewrite(stagedTables, "users")
	})
	if err == nil {
		t.Fatal("execMutatingStatement() error = nil, want commit failure")
	}

	if db.pool.HasPrivatePage(bufferpool.PageID(singleCommittedDataPageIDForTest(t, db, "users"))) {
		t.Fatal("private frame still present after WAL sync failure")
	}

	afterRecords, err := storage.ReadWALRecords(path)
	if err != nil {
		t.Fatalf("ReadWALRecords(after) error = %v", err)
	}
	if len(afterRecords) <= len(beforeRecords) {
		t.Fatalf("len(ReadWALRecords(after)) = %d, want > %d", len(afterRecords), len(beforeRecords))
	}
	newRecords := afterRecords[len(beforeRecords):]
	if len(newRecords) < 2 {
		t.Fatalf("len(newRecords) = %d, want at least 2", len(newRecords))
	}
	for i := 0; i < len(newRecords)-1; i++ {
		if newRecords[i].Type != storage.WALRecordTypeFrame {
			t.Fatalf("newRecords[%d].Type = %d, want frame", i, newRecords[i].Type)
		}
	}
	if newRecords[len(newRecords)-1].Type != storage.WALRecordTypeCommit {
		t.Fatalf("newRecords[last].Type = %d, want commit", newRecords[len(newRecords)-1].Type)
	}

	assertSelectTextRows(t, db, "SELECT name FROM users", "alice")
}

func TestCheckpointFailureAfterWALDurabilityPreservesCommittedState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	beforeRecords, err := storage.ReadWALRecords(path)
	if err != nil {
		t.Fatalf("ReadWALRecords(before) error = %v", err)
	}

	db.afterDatabaseSyncHook = func() error {
		return errors.New("checkpoint failed after WAL durability")
	}
	_, err = db.Exec("UPDATE users SET name = 'beth' WHERE id = 1")
	if err == nil {
		t.Fatal("Exec(update) error = nil, want checkpoint failure")
	}
	db.afterDatabaseSyncHook = nil

	afterRecords, err := storage.ReadWALRecords(path)
	if err != nil {
		t.Fatalf("ReadWALRecords(after) error = %v", err)
	}
	if len(afterRecords) <= len(beforeRecords) {
		t.Fatalf("len(ReadWALRecords(after)) = %d, want > %d", len(afterRecords), len(beforeRecords))
	}

	assertSelectTextRows(t, db, "SELECT name FROM users", "beth")

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectTextRows(t, db, "SELECT name FROM users", "beth")
}

func TestWALResetFailureAfterCheckpointSurfacesErrorAndLeavesStateCorrect(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	originalResetWAL := resetWAL
	resetWAL = func(path string, dbFormatVersion uint32) error {
		return errors.New("wal reset failed")
	}
	defer func() {
		resetWAL = originalResetWAL
	}()

	_, err = db.Exec("UPDATE users SET name = 'beth' WHERE id = 1")
	if err == nil {
		t.Fatal("Exec(update) error = nil, want WAL reset failure")
	}

	records, err := storage.ReadWALRecords(path)
	if err != nil {
		t.Fatalf("ReadWALRecords() error = %v", err)
	}
	if len(records) == 0 {
		t.Fatal("len(ReadWALRecords()) = 0, want WAL intact after reset failure")
	}

	assertSelectTextRows(t, db, "SELECT name FROM users", "beth")

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectTextRows(t, db, "SELECT name FROM users", "beth")
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
	if err := db.txn.MarkDirty(); err != nil {
		t.Fatalf("txn.MarkDirty() error = %v", err)
	}
	if err := db.commitTxn(); err != nil {
		t.Fatalf("commitTxn() error = %v", err)
	}

	db.pager.MarkDirtyWithOriginal(page)
	copy(page.Data(), []byte("after!"))
	if err := db.beginTxn(); err != nil {
		t.Fatalf("beginTxn() error = %v", err)
	}
	if !db.pager.IsDirty(page) {
		t.Fatal("page not dirty before rollback")
	}

	if err := db.rollbackTxn(); err != nil {
		t.Fatalf("rollbackTxn() error = %v", err)
	}

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
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	_, err = db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		if err := db.loadRowsIntoTables(stagedTables, "t"); err != nil {
			return err
		}
		table := stagedTables["t"]
		table.Rows = append(table.Rows, []parser.Value{parser.Int64Value(2)})
		table.SetStorageMeta(table.RootPageID(), uint32(len(table.Rows)))

		tablePageData, err := storage.BuildSlottedTablePageData(uint32(table.RootPageID()), table.Rows)
		if err != nil {
			return err
		}
		catalogWrite, err := db.buildCatalogPageData(stagedTables, []stagedPage{{
			id:   table.RootPageID(),
			data: tablePageData,
		}})
		if err != nil {
			return err
		}
		if err := db.stageDirtyState(catalogWrite, []stagedPage{{
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

	if err := db.beginTxn(); err != nil {
		t.Fatalf("beginTxn() error = %v", err)
	}
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

	if err := db.txn.MarkDirty(); err != nil {
		t.Fatalf("txn.MarkDirty() error = %v", err)
	}
	if err := db.commitTxn(); err != nil {
		t.Fatalf("commitTxn() error = %v", err)
	}
	db.tables = stagedTables

	catalogPage, err := db.pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get(0) error = %v", err)
	}
	rootPage, err := db.pager.Get(stagedTables["t"].RootPageID())
	if err != nil {
		t.Fatalf("pager.Get(root) error = %v", err)
	}
	if db.pager.HasOriginal(catalogPage) || db.pager.HasOriginal(rootPage) {
		t.Fatal("rollback snapshot still tracked after commit")
	}
	if db.pager.IsDirty(catalogPage) || db.pager.IsDirty(rootPage) {
		t.Fatal("page still dirty after commit")
	}
	if _, err := os.Stat(storage.JournalPath(db.path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists", err)
	}
}

func TestRollbackAfterFailedCommitRestoresState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	_, err = db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		if err := db.loadRowsIntoTables(stagedTables, "t"); err != nil {
			return err
		}
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

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 99)
}

func TestBeginTxnWhileActiveReturnsError(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if err := db.beginTxn(); err != nil {
		t.Fatalf("first beginTxn() error = %v", err)
	}
	if err := db.beginTxn(); !errors.Is(err, ErrTxnAlreadyActive) {
		t.Fatalf("second beginTxn() error = %v, want %v", err, ErrTxnAlreadyActive)
	}
}

func TestCommitTxnWithoutActiveTxnReturnsError(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	db.txn = txn.NewTxn()
	if err := db.txn.Commit(); err != nil {
		t.Fatalf("txn.Commit() error = %v", err)
	}
	if err := db.commitTxn(); err == nil || err.Error() != "execution: invalid transaction state" {
		t.Fatalf("commitTxn() error = %v, want %q", err, "execution: invalid transaction state")
	}
}

func TestRollbackTxnWithoutActiveTxnIsHandled(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if err := db.rollbackTxn(); err != nil {
		t.Fatalf("rollbackTxn() nil txn error = %v", err)
	}

	db.txn = txn.NewTxn()
	if err := db.txn.Commit(); err != nil {
		t.Fatalf("txn.Commit() error = %v", err)
	}
	if err := db.rollbackTxn(); err != nil {
		t.Fatalf("rollbackTxn() terminal txn error = %v", err)
	}
}

func TestSuccessfulCommitLeavesNoTxnAndNoTracking(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if db.txn != nil {
		t.Fatalf("db.txn = %#v, want nil", db.txn)
	}
	if len(db.pager.DirtyPages()) != 0 || len(db.pager.DirtyPagesWithOriginals()) != 0 {
		t.Fatal("dirty/original tracking remained after successful commit")
	}
	if _, err := os.Stat(storage.JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists", err)
	}
}

func TestSuccessfulRollbackLeavesNoTxnAndNoTracking(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	_, err = db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		if err := db.loadRowsIntoTables(stagedTables, "t"); err != nil {
			return err
		}
		table := stagedTables["t"]
		table.Rows[0][0] = parser.Int64Value(2)
		if err := db.applyStagedTableRewrite(stagedTables, "t"); err != nil {
			return err
		}
		return errors.New("boom")
	})
	if err == nil {
		t.Fatal("execMutatingStatement() error = nil, want failure")
	}
	if db.txn != nil {
		t.Fatalf("db.txn = %#v, want nil", db.txn)
	}
	if len(db.pager.DirtyPages()) != 0 || len(db.pager.DirtyPagesWithOriginals()) != 0 {
		t.Fatal("dirty/original tracking remained after successful rollback")
	}
}

func TestSuccessfulCommitPersistsDirectoryCheckpointMetadata(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	const wantCheckpointPageCount = 7
	if db.lastCheckpointLSN == 0 {
		t.Fatal("db.lastCheckpointLSN = 0, want non-zero after checkpointed commit")
	}
	if db.lastCheckpointPageCount != wantCheckpointPageCount {
		t.Fatalf("db.lastCheckpointPageCount = %d, want %d", db.lastCheckpointPageCount, wantCheckpointPageCount)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	meta, err := storage.ReadDirectoryCheckpointMetadata(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryCheckpointMetadata() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}
	if meta.LastCheckpointLSN == 0 {
		t.Fatal("meta.LastCheckpointLSN = 0, want non-zero")
	}
	if meta.LastCheckpointPageCount != wantCheckpointPageCount {
		t.Fatalf("meta.LastCheckpointPageCount = %d, want %d", meta.LastCheckpointPageCount, wantCheckpointPageCount)
	}

	db = reopenDB(t, path)
	defer db.Close()
	if db.lastCheckpointLSN != meta.LastCheckpointLSN {
		t.Fatalf("reopened db.lastCheckpointLSN = %d, want %d", db.lastCheckpointLSN, meta.LastCheckpointLSN)
	}
	if db.lastCheckpointPageCount != meta.LastCheckpointPageCount {
		t.Fatalf("reopened db.lastCheckpointPageCount = %d, want %d", db.lastCheckpointPageCount, meta.LastCheckpointPageCount)
	}
}

func TestBoolRollbackCloseReopenKeepsCommittedState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE flags (id INT, name TEXT, active BOOL)",
		"INSERT INTO flags VALUES (1, 'alice', TRUE)",
		"INSERT INTO flags VALUES (2, 'bob', FALSE)",
		"INSERT INTO flags VALUES (3, 'cara', NULL)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	_, err = db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		if err := db.loadRowsIntoTables(stagedTables, "flags"); err != nil {
			return err
		}
		table := stagedTables["flags"]
		table.Rows[0][2] = parser.BoolValue(false)
		table.Rows[1][2] = parser.BoolValue(true)
		table.Rows[2][2] = parser.BoolValue(true)
		if err := db.applyStagedTableRewrite(stagedTables, "flags"); err != nil {
			return err
		}
		return errors.New("force rollback")
	})
	if err == nil || err.Error() != "force rollback" {
		t.Fatalf("execMutatingStatement() error = %v, want %q", err, "force rollback")
	}

	assertSelectBoolRows(t, db, "SELECT id, name, active FROM flags ORDER BY id", [][3]any{
		{int(1), "alice", true},
		{int(2), "bob", false},
		{int(3), "cara", nil},
	})

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertSelectBoolRows(t, db, "SELECT id, name, active FROM flags ORDER BY id", [][3]any{
		{int(1), "alice", true},
		{int(2), "bob", false},
		{int(3), "cara", nil},
	})
}

func TestRealRollbackCloseReopenKeepsCommittedState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE measurements (id INT, label TEXT, x REAL)",
		"INSERT INTO measurements VALUES (1, 'zero', 0.0)",
		"INSERT INTO measurements VALUES (2, 'pi', 3.14)",
		"INSERT INTO measurements VALUES (3, 'missing', NULL)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	_, err = db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		if err := db.loadRowsIntoTables(stagedTables, "measurements"); err != nil {
			return err
		}
		table := stagedTables["measurements"]
		table.Rows[0][2] = parser.RealValue(1.25)
		table.Rows[1][2] = parser.RealValue(-2.5)
		table.Rows[2][2] = parser.RealValue(10.25)
		if err := db.applyStagedTableRewrite(stagedTables, "measurements"); err != nil {
			return err
		}
		return errors.New("force rollback")
	})
	if err == nil || err.Error() != "force rollback" {
		t.Fatalf("execMutatingStatement() error = %v, want %q", err, "force rollback")
	}

	assertSelectRealCommitRows(t, db, "SELECT id, label, x FROM measurements ORDER BY id", [][3]any{
		{int(1), "zero", 0.0},
		{int(2), "pi", 3.14},
		{int(3), "missing", nil},
	})

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertSelectRealCommitRows(t, db, "SELECT id, label, x FROM measurements ORDER BY id", [][3]any{
		{int(1), "zero", 0.0},
		{int(2), "pi", 3.14},
		{int(3), "missing", nil},
	})
}

func assertSelectBoolRows(t *testing.T, db *DB, sql string, want [][3]any) {
	t.Helper()

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q) error = %v", sql, err)
	}
	defer rows.Close()

	got := make([][3]any, 0, len(want))
	for rows.Next() {
		var id int
		var name string
		var active any
		if err := rows.Scan(&id, &name, &active); err != nil {
			t.Fatalf("Scan(%q) error = %v", sql, err)
		}
		got = append(got, [3]any{id, name, active})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err(%q) = %v", sql, err)
	}
	if len(got) != len(want) {
		t.Fatalf("rows(%q) len = %d, want %d; got = %#v", sql, len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rows(%q)[%d] = %#v, want %#v", sql, i, got[i], want[i])
		}
	}
}

func assertSelectRealCommitRows(t *testing.T, db *DB, sql string, want [][3]any) {
	t.Helper()

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q) error = %v", sql, err)
	}
	defer rows.Close()

	got := make([][3]any, 0, len(want))
	for rows.Next() {
		var id int
		var label string
		var x any
		if err := rows.Scan(&id, &label, &x); err != nil {
			t.Fatalf("Scan(%q) error = %v", sql, err)
		}
		got = append(got, [3]any{id, label, x})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err(%q) = %v", sql, err)
	}
	if len(got) != len(want) {
		t.Fatalf("rows(%q) len = %d, want %d; got = %#v", sql, len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rows(%q)[%d] = %#v, want %#v", sql, i, got[i], want[i])
		}
	}
}

func assertSelectTextRows(t *testing.T, db *DB, sql string, want ...string) {
	t.Helper()

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q) error = %v", sql, err)
	}
	defer rows.Close()

	got := make([]string, 0, len(want))
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("Scan(%q) error = %v", sql, err)
		}
		got = append(got, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err(%q) = %v", sql, err)
	}
	if len(got) != len(want) {
		t.Fatalf("rows(%q) len = %d, want %d; got = %#v", sql, len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rows(%q)[%d] = %q, want %q", sql, i, got[i], want[i])
		}
	}
}

func singleCommittedDataPageIDForTest(t *testing.T, db *DB, tableName string) storage.PageID {
	t.Helper()
	if db == nil || db.tables == nil {
		t.Fatal("singleCommittedDataPageIDForTest() requires db tables")
	}
	table := db.tables[tableName]
	if table == nil {
		t.Fatalf("db.tables[%q] = nil", tableName)
	}
	dataPageIDs, err := committedTableDataPageIDs(db.pool, table)
	if err != nil {
		t.Fatalf("committedTableDataPageIDs(%q) error = %v", tableName, err)
	}
	if len(dataPageIDs) != 1 {
		t.Fatalf("len(committedTableDataPageIDs(%q)) = %d, want 1", tableName, len(dataPageIDs))
	}
	return dataPageIDs[0]
}

func privateOwnedDataPageIDForTest(t *testing.T, db *DB, table *executor.Table) storage.PageID {
	t.Helper()
	if db == nil || table == nil {
		t.Fatal("privateOwnedDataPageIDForTest() requires db and table")
	}
	for _, staged := range db.pendingPages {
		if err := storage.ValidateOwnedDataPage(staged.data, table.TableID); err == nil {
			return staged.id
		}
	}
	t.Fatalf("no private owned data page found for table %q", table.Name)
	return 0
}
