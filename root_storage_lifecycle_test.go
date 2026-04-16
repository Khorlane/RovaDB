package rovadb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Khorlane/RovaDB/internal/bufferpool"
	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
	"github.com/Khorlane/RovaDB/internal/storage"
	"github.com/Khorlane/RovaDB/internal/txn"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestQueryReloadsRowsFromStorageInsteadOfStaleTableCache(t *testing.T) {
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

	db.tables["users"].Rows = [][]parser.Value{
		{parser.IntValue(99), parser.StringValue("stale-a")},
		{parser.IntValue(100), parser.StringValue("stale-b")},
	}

	rows, err := db.Query("SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	want := []struct {
		id   int32
		name string
	}{
		{1, "alice"},
		{2, "bob"},
	}
	for i, tc := range want {
		if !rows.Next() {
			t.Fatalf("Next() row %d = false, want true", i)
		}
		var id int32
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan() row %d error = %v", i, err)
		}
		if id != tc.id || name != tc.name {
			t.Fatalf("row %d = (%d, %q), want (%d, %q)", i, id, name, tc.id, tc.name)
		}
	}
	if rows.Next() {
		t.Fatal("Next() after rows = true, want false")
	}
}

func TestTableNamesForSelectUsesExecutorAccessPathForIndexScan(t *testing.T) {
	plan := &planner.SelectPlan{
		Query:    &planner.SelectQuery{TableName: "users"},
		ScanType: planner.ScanTypeIndex,
		IndexScan: &planner.IndexScan{
			TableName:   "users",
			ColumnName:  "name",
			LookupValue: planner.StringValue("alice"),
		},
	}

	handoff, err := executor.NewSelectExecutionHandoff(plan)
	if err != nil {
		t.Fatalf("NewSelectExecutionHandoff() error = %v", err)
	}
	names := tableNamesForSelectHandoff(handoff)
	if len(names) != 1 || names[0] != "users" {
		t.Fatalf("tableNamesForSelectHandoff() = %#v, want [users]", names)
	}

	accessPath, err := executor.DescribeSelectAccessPath(plan)
	if err != nil {
		t.Fatalf("DescribeSelectAccessPath() error = %v", err)
	}
	if accessPath.Kind != executor.SelectAccessPathKindIndex {
		t.Fatalf("DescribeSelectAccessPath().Kind = %v, want %v", accessPath.Kind, executor.SelectAccessPathKindIndex)
	}
	if accessPath.IndexLookup.TableName != "users" || accessPath.IndexLookup.ColumnName != "name" {
		t.Fatalf("DescribeSelectAccessPath() = %#v, want users/name", accessPath)
	}
	if accessPath.IndexLookup.LookupValue != parser.StringValue("alice") {
		t.Fatalf("DescribeSelectAccessPath().IndexLookup.LookupValue = %#v, want %#v", accessPath.IndexLookup.LookupValue, parser.StringValue("alice"))
	}
}

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

func TestExecMutatingStatementRollbackKeepsCommittedStateAcrossReopen(t *testing.T) {
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

	_, err = db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		if err := db.loadRowsIntoTables(stagedTables, "users"); err != nil {
			return err
		}
		table := stagedTables["users"]
		table.Rows[0][1] = parser.StringValue("rolled-back")
		if err := db.applyStagedTableRewrite(stagedTables, "users"); err != nil {
			return err
		}
		return errors.New("force rollback")
	})
	if err == nil || err.Error() != "force rollback" {
		t.Fatalf("execMutatingStatement() error = %v, want %q", err, "force rollback")
	}

	assertSelectTextRows(t, db, "SELECT name FROM users", "alice")

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertSelectTextRows(t, db, "SELECT name FROM users", "alice")
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
		storageValues, err := storage.DecodeSlottedRow(payloads[0], []uint8{storage.CatalogColumnTypeInt})
		if err != nil {
			return err
		}
		values := parserValuesFromStorage(storageValues)
		if values[0].IntegerValue() != 1 {
			t.Fatalf("disk value before flush = %d, want 1", values[0].IntegerValue())
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
	stagedTables["t"].Rows[0][0] = parser.IntValue(2)

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
	storagePrivateRows, err := storage.ReadSlottedRowsFromTablePageData(privateFrame.Data[:], []uint8{storage.CatalogColumnTypeInt})
	if err != nil {
		t.Fatalf("ReadSlottedRowsFromTablePageData(private) error = %v", err)
	}
	privateRows := parserRowsFromStorage(storagePrivateRows)
	if len(privateRows) != 1 || privateRows[0][0] != parser.IntValue(2) {
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
	if len(rows) != 1 || rows[0][0] != parser.IntValue(1) || rows[0][1] != parser.StringValue("alice") {
		t.Fatalf("scanTableRows() = %#v, want committed row [1 alice]", rows)
	}

	row, err := db.fetchRowByLocator(db.tables["users"], storage.RowLocator{
		PageID: uint32(singleCommittedDataPageIDForTest(t, db, "users")),
		SlotID: 0,
	})
	if err != nil {
		t.Fatalf("fetchRowByLocator() error = %v", err)
	}
	if len(row) != 2 || row[0] != parser.IntValue(1) || row[1] != parser.StringValue("alice") {
		t.Fatalf("fetchRowByLocator() = %#v, want committed row [1 alice]", row)
	}

	privateDataPageID := privateOwnedDataPageIDForTest(t, db, stagedTables["users"])
	privateFrame, err := db.pool.GetPrivatePage(bufferpool.PageID(privateDataPageID))
	if err != nil {
		t.Fatalf("GetPrivatePage() error = %v", err)
	}
	storagePrivateRows, err := storage.ReadSlottedRowsFromTablePageData(privateFrame.Data[:], []uint8{
		storage.CatalogColumnTypeInt,
		storage.CatalogColumnTypeText,
	})
	if err != nil {
		t.Fatalf("ReadSlottedRowsFromTablePageData(private) error = %v", err)
	}
	privateRows := parserRowsFromStorage(storagePrivateRows)
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
	stagedTables["t"].Rows[0][0] = parser.IntValue(2)
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
	if len(rows) != 1 || rows[0][0] != parser.IntValue(1) {
		t.Fatalf("scanTableRows() after rollback = %#v, want [[1]]", rows)
	}

	privateAfterRollback, err := db.pool.GetPrivatePage(bufferpool.PageID(dataPageID))
	if err != nil {
		t.Fatalf("GetPrivatePage() after rollback error = %v", err)
	}
	storagePrivateRows, err := storage.ReadSlottedRowsFromTablePageData(privateAfterRollback.Data[:], []uint8{storage.CatalogColumnTypeInt})
	if err != nil {
		t.Fatalf("ReadSlottedRowsFromTablePageData(private after rollback) error = %v", err)
	}
	privateRows := parserRowsFromStorage(storagePrivateRows)
	if len(privateRows) != 1 || privateRows[0][0] != parser.IntValue(1) {
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
	if len(rowsAfterCommit) != 1 || rowsAfterCommit[0][0] != parser.IntValue(1) || rowsAfterCommit[0][1] != parser.StringValue("beth") {
		t.Fatalf("scanTableRows(after commit) = %#v, want promoted row [1 beth]", rowsAfterCommit)
	}

	row, err := db.fetchRowByLocator(db.tables["users"], storage.RowLocator{
		PageID: uint32(dataPageID),
		SlotID: 0,
	})
	if err != nil {
		t.Fatalf("fetchRowByLocator(after commit) error = %v", err)
	}
	if len(row) != 2 || row[0] != parser.IntValue(1) || row[1] != parser.StringValue("beth") {
		t.Fatalf("fetchRowByLocator(after commit) = %#v, want promoted row [1 beth]", row)
	}

	committedPageData, err := readCommittedPageData(db.pool, dataPageID)
	if err != nil {
		t.Fatalf("readCommittedPageData(after commit) error = %v", err)
	}
	storageDecoded, err := storage.ReadSlottedRowsFromTablePageData(committedPageData, []uint8{
		storage.CatalogColumnTypeInt,
		storage.CatalogColumnTypeText,
	})
	if err != nil {
		t.Fatalf("ReadSlottedRowsFromTablePageData(after commit) error = %v", err)
	}
	decoded := parserRowsFromStorage(storageDecoded)
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

func TestCheckpointFailureAfterBootstrapInsertReopensWithPhysicalStorage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	beforeRecords, err := storage.ReadWALRecords(path)
	if err != nil {
		t.Fatalf("ReadWALRecords(before) error = %v", err)
	}

	db.afterDatabaseSyncHook = func() error {
		return errors.New("checkpoint failed after WAL durability")
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err == nil {
		t.Fatal("Exec(insert) error = nil, want checkpoint failure")
	}
	db.afterDatabaseSyncHook = nil

	afterRecords, err := storage.ReadWALRecords(path)
	if err != nil {
		t.Fatalf("ReadWALRecords(after) error = %v", err)
	}
	if len(afterRecords) <= len(beforeRecords) {
		t.Fatalf("len(ReadWALRecords(after)) = %d, want > %d", len(afterRecords), len(beforeRecords))
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal(`db.tables["users"] = nil`)
	}
	if table.TableHeaderPageID() == 0 {
		t.Fatal("TableHeaderPageID() = 0, want durable physical root")
	}
	if table.FirstSpaceMapPageID() == 0 {
		t.Fatal("FirstSpaceMapPageID() = 0, want first insert bootstrap to allocate SpaceMap")
	}
	if table.OwnedSpaceMapPageCount() != 1 {
		t.Fatalf("OwnedSpaceMapPageCount() = %d, want 1", table.OwnedSpaceMapPageCount())
	}
	if table.OwnedDataPageCount() != 1 {
		t.Fatalf("OwnedDataPageCount() = %d, want 1", table.OwnedDataPageCount())
	}
	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}
	assertSelectIntRows(t, db, "SELECT id FROM users WHERE name = 'alice'", 1)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() after reopen error = %v", err)
	}
	assertSelectIntRows(t, db, "SELECT id FROM users WHERE name = 'alice'", 1)
}

func TestCheckpointFailureAfterRelocationReopensWithCurrentIndexLocator(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, note TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for id := 1; id <= 18; id++ {
		name := "filler"
		if id == 1 {
			name = "alice"
		}
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", int32(id), name, strings.Repeat("seed-", 100)); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	before := committedLocatorsByIDForTest(t, db, "users")
	oldLocator := before[1]
	bigNote := strings.Repeat("grown-row-", 220)

	db.afterDatabaseSyncHook = func() error {
		return errors.New("checkpoint failed after WAL durability")
	}
	if _, err := db.Exec("UPDATE users SET note = ? WHERE id = 1", bigNote); err == nil {
		t.Fatal("Exec(relocating update) error = nil, want checkpoint failure")
	}
	db.afterDatabaseSyncHook = nil

	after := committedLocatorsByIDForTest(t, db, "users")
	newLocator := after[1]
	if newLocator == oldLocator {
		t.Fatalf("locator after relocation = %#v, want different from %#v", newLocator, oldLocator)
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal(`db.tables["users"] = nil`)
	}
	if _, err := db.fetchRowByLocator(table, oldLocator); err == nil {
		t.Fatal("fetchRowByLocator(old locator) error = nil, want explicit failure")
	}
	row, err := db.fetchRowByLocator(table, newLocator)
	if err != nil {
		t.Fatalf("fetchRowByLocator(new locator) error = %v", err)
	}
	if got := row[2]; got != parser.StringValue(bigNote) {
		t.Fatalf("updated note = %#v, want %#v", got, parser.StringValue(bigNote))
	}
	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}
	assertSelectIntRows(t, db, "SELECT id FROM users WHERE name = 'alice'", 1)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	reopenedLocators := committedLocatorsByIDForTest(t, db, "users")
	if reopenedLocators[1] != newLocator {
		t.Fatalf("reopened locator = %#v, want %#v", reopenedLocators[1], newLocator)
	}
	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() after reopen error = %v", err)
	}
	assertSelectTextRows(t, db, "SELECT note FROM users WHERE name = 'alice'", bigNote)
}

func TestCheckpointFailureAfterDropTableReopensWithFreedPhysicalPages(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, note TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for id := 1; id <= 20; id++ {
		name := "bulk"
		if id == 7 {
			name = "alice"
		}
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", int32(id), name, strings.Repeat("payload-", 110)); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	droppedPages := droppedTableRootPageIDs(db.pool, db.tables, "users")
	if len(droppedPages) < 4 {
		t.Fatalf("len(droppedPages) = %d, want at least header/index/spacemap/data pages", len(droppedPages))
	}

	beforeRecords, err := storage.ReadWALRecords(path)
	if err != nil {
		t.Fatalf("ReadWALRecords(before) error = %v", err)
	}

	db.afterDatabaseSyncHook = func() error {
		return errors.New("checkpoint failed after WAL durability")
	}
	if _, err := db.Exec("DROP TABLE users"); err == nil {
		t.Fatal("Exec(drop table) error = nil, want checkpoint failure")
	}
	db.afterDatabaseSyncHook = nil

	afterRecords, err := storage.ReadWALRecords(path)
	if err != nil {
		t.Fatalf("ReadWALRecords(after) error = %v", err)
	}
	if len(afterRecords) <= len(beforeRecords) {
		t.Fatalf("len(ReadWALRecords(after)) = %d, want > %d", len(afterRecords), len(beforeRecords))
	}

	rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query(dropped table) error = %v", err)
	}
	if rows.Next() {
		t.Fatal("rows.Next() on dropped table = true, want no rows and table-not-found error")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: table not found: users" {
		t.Fatalf("rows.Err() after drop = %v, want %q", rows.Err(), "execution: table not found: users")
	}
	rows.Close()

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err = db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query(reopen dropped table) error = %v", err)
	}
	if rows.Next() {
		t.Fatal("rows.Next() on reopened dropped table = true, want no rows and table-not-found error")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: table not found: users" {
		t.Fatalf("rows.Err() after reopen drop = %v, want %q", rows.Err(), "execution: table not found: users")
	}
	rows.Close()

	rawDB, pager := openRawStorage(t, path)
	defer rawDB.Close()
	head, err := storage.ReadDirectoryFreeListHead(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	chain := freeListChainForTest(t, pager, storage.PageID(head))
	for _, pageID := range droppedPages {
		if !containsPageID(chain, pageID) {
			t.Fatalf("free list chain = %#v, want dropped page %d present", chain, pageID)
		}
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
		table.Rows = append(table.Rows, []parser.Value{parser.IntValue(2)})
		table.SetStorageMeta(table.RootPageID(), uint32(len(table.Rows)))

		tablePageData, err := storage.BuildSlottedTablePageData(uint32(table.RootPageID()), storageColumnTypes(table.Columns), parserRowsToStorage(table.Rows))
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
		table.Rows[0][0] = parser.IntValue(99)
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
		table.Rows[0][0] = parser.IntValue(2)
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
	if db.lastCheckpointLSN == 0 {
		t.Fatal("db.lastCheckpointLSN = 0, want non-zero after checkpointed commit")
	}
	if db.lastCheckpointPageCount == 0 {
		t.Fatal("db.lastCheckpointPageCount = 0, want non-zero after checkpointed commit")
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
	if meta.LastCheckpointPageCount != db.lastCheckpointPageCount {
		t.Fatalf("meta.LastCheckpointPageCount = %d, want %d", meta.LastCheckpointPageCount, db.lastCheckpointPageCount)
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

func TestTemporalPhysicalRowsRoundTripAcrossRewriteAndReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	wantRows := [][]parser.Value{
		{
			parser.IntValue(1),
			parser.DateValue(20553),
			parser.TimeValue(49521),
			parser.TimestampValue(1775828721000, 7),
			parser.StringValue("alpha"),
		},
		{
			parser.IntValue(2),
			parser.DateValue(-7),
			parser.TimeValue(0),
			parser.TimestampValue(-12345, -3),
			parser.NullValue(),
		},
	}
	for _, sql := range []string{
		"CREATE TABLE events (id INT, event_date DATE, event_time TIME, recorded_at TIMESTAMP, note TEXT)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	_, err = db.execMutatingStatement(func() error {
		stagedTables := cloneTables(db.tables)
		if err := db.loadRowsIntoTables(stagedTables, "events"); err != nil {
			return err
		}
		stagedTables["events"].Rows = cloneRows(wantRows)
		return db.applyStagedTableRewrite(stagedTables, "events")
	})
	if err != nil {
		t.Fatalf("execMutatingStatement() error = %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)

	reopened := db.tables["events"]
	if reopened == nil {
		t.Fatal("reopened db.tables[events] = nil")
	}
	gotTypes := []string{
		reopened.Columns[0].Type,
		reopened.Columns[1].Type,
		reopened.Columns[2].Type,
		reopened.Columns[3].Type,
		reopened.Columns[4].Type,
	}
	wantTypes := []string{
		parser.ColumnTypeInt,
		parser.ColumnTypeDate,
		parser.ColumnTypeTime,
		parser.ColumnTypeTimestamp,
		parser.ColumnTypeText,
	}
	if !reflect.DeepEqual(gotTypes, wantTypes) {
		t.Fatalf("reopened column types = %#v, want %#v", gotTypes, wantTypes)
	}
	if err := db.loadRowsIntoTables(db.tables, "events"); err != nil {
		t.Fatalf("loadRowsIntoTables() error = %v", err)
	}
	if got := db.tables["events"].Rows; !reflect.DeepEqual(got, wantRows) {
		t.Fatalf("reopened rows = %#v, want %#v", got, wantRows)
	}
	if got := db.tables["events"].Rows[0][3].TimestampZoneID; got != 7 {
		t.Fatalf("reopened rows[0][3].TimestampZoneID = %d, want 7", got)
	}
	if got := db.tables["events"].Rows[1][3].TimestampZoneID; got != -3 {
		t.Fatalf("reopened rows[1][3].TimestampZoneID = %d, want -3", got)
	}

	dataPageIDs, err := committedTableDataPageIDs(db.pool, reopened)
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	if len(dataPageIDs) != 1 {
		t.Fatalf("len(committedTableDataPageIDs()) = %d, want 1", len(dataPageIDs))
	}
	dataPageID := dataPageIDs[0]
	tableID := reopened.TableID

	if err := db.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	dbFile, err := storage.OpenOrCreate(path)
	if err != nil {
		t.Fatalf("storage.OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	pager, err := storage.NewPager(dbFile.File())
	if err != nil {
		t.Fatalf("storage.NewPager() error = %v", err)
	}
	page, err := pager.Get(dataPageID)
	if err != nil {
		t.Fatalf("pager.Get() error = %v", err)
	}
	if err := storage.ValidateOwnedDataPage(page.Data(), tableID); err != nil {
		t.Fatalf("storage.ValidateOwnedDataPage() error = %v", err)
	}
	storageRows, err := storage.ReadSlottedRowsFromTablePageData(page.Data(), []uint8{
		storage.CatalogColumnTypeInt,
		storage.CatalogColumnTypeDate,
		storage.CatalogColumnTypeTime,
		storage.CatalogColumnTypeTimestamp,
		storage.CatalogColumnTypeText,
	})
	if err != nil {
		t.Fatalf("storage.ReadSlottedRowsFromTablePageData() error = %v", err)
	}
	if got := parserRowsFromStorage(storageRows); !reflect.DeepEqual(got, wantRows) {
		t.Fatalf("parserRowsFromStorage(storageRows) = %#v, want %#v", got, wantRows)
	}
}

func TestTypedIntegerCloseReopenPreservesExactWidthsAcrossMaterializeAndScan(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE numbers (id INT, small_col SMALLINT, int_col INT, big_col BIGINT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO numbers VALUES (?, ?, ?, ?)", int32(1), int16(11), int32(22), int64(33)); err != nil {
		t.Fatalf("Exec(insert exact widths) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query("SELECT small_col, int_col, big_col FROM numbers WHERE id = 1")
	if err != nil {
		t.Fatalf("Query(materialize) error = %v", err)
	}
	if got := rows.data; len(got) != 1 || len(got[0]) != 3 {
		t.Fatalf("rows.data = %#v, want one typed integer row", got)
	}
	if got, ok := rows.data[0][0].(int16); !ok || got != 11 {
		t.Fatalf("rows.data[0][0] = %#v, want int16(11)", rows.data[0][0])
	}
	if got, ok := rows.data[0][1].(int32); !ok || got != 22 {
		t.Fatalf("rows.data[0][1] = %#v, want int32(22)", rows.data[0][1])
	}
	if got, ok := rows.data[0][2].(int64); !ok || got != 33 {
		t.Fatalf("rows.data[0][2] = %#v, want int64(33)", rows.data[0][2])
	}
	if _, ok := rows.data[0][0].(int); ok {
		t.Fatalf("rows.data[0][0] = %#v, want no Go int on typed path", rows.data[0][0])
	}
	if _, ok := rows.data[0][1].(int); ok {
		t.Fatalf("rows.data[0][1] = %#v, want no Go int on typed path", rows.data[0][1])
	}
	if _, ok := rows.data[0][2].(int); ok {
		t.Fatalf("rows.data[0][2] = %#v, want no Go int on typed path", rows.data[0][2])
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close() error = %v", err)
	}

	rows, err = db.Query("SELECT small_col, int_col, big_col FROM numbers WHERE id = 1")
	if err != nil {
		t.Fatalf("Query(scan exact) error = %v", err)
	}
	if !rows.Next() {
		t.Fatal("rows.Next() = false, want true")
	}
	var small int16
	var regular int32
	var big int64
	if err := rows.Scan(&small, &regular, &big); err != nil {
		t.Fatalf("rows.Scan(exact widths) error = %v", err)
	}
	if small != 11 || regular != 22 || big != 33 {
		t.Fatalf("rows.Scan(exact widths) = (%d, %d, %d), want (11, 22, 33)", small, regular, big)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close() second error = %v", err)
	}

	for _, tc := range []struct {
		name string
		dest []any
	}{
		{name: "smallint rejects int64", dest: []any{new(int64), new(int32), new(int64)}},
		{name: "smallint rejects int", dest: []any{new(int), new(int32), new(int64)}},
		{name: "int rejects int16", dest: []any{new(int16), new(int16), new(int64)}},
		{name: "bigint rejects int32", dest: []any{new(int16), new(int32), new(int32)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := db.QueryRow("SELECT small_col, int_col, big_col FROM numbers WHERE id = 1").Scan(tc.dest...); !errors.Is(err, ErrUnsupportedScanType) {
				t.Fatalf("QueryRow().Scan() error = %v, want ErrUnsupportedScanType", err)
			}
		})
	}

	if err := db.QueryRow("SELECT 1 + 2").Scan(&big); err != nil {
		t.Fatalf("QueryRow(untyped literal arithmetic).Scan(*int64) error = %v", err)
	}
	if big != 3 {
		t.Fatalf("QueryRow(untyped literal arithmetic).Scan(*int64) = %d, want 3", big)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM numbers").Scan(&big); err != nil {
		t.Fatalf("QueryRow(COUNT(*)).Scan(*int64) error = %v", err)
	}
	if big != 1 {
		t.Fatalf("QueryRow(COUNT(*)).Scan(*int64) = %d, want 1", big)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM numbers").Scan(new(int)); !errors.Is(err, ErrUnsupportedScanType) {
		t.Fatalf("QueryRow(COUNT(*)).Scan(*int) error = %v, want ErrUnsupportedScanType", err)
	}
}

func TestTemporalCloseReopenPreservesPublicMaterializationAndScan(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE events (id INT, event_date DATE, event_time TIME, recorded_at TIMESTAMP)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO events VALUES (1, '2026-04-15', '12:34:56', '2026-04-15 16:17:18')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	wantDate := time.Date(2026, time.April, 15, 0, 0, 0, 0, time.UTC)
	wantTimestamp := time.Date(2026, time.April, 15, 16, 17, 18, 0, time.UTC)
	wantTime, err := NewTime(12, 34, 56)
	if err != nil {
		t.Fatalf("NewTime() error = %v", err)
	}

	var gotDate time.Time
	var gotTime Time
	var gotTimestamp time.Time
	if err := db.QueryRow("SELECT event_date, event_time, recorded_at FROM events WHERE id = 1").Scan(&gotDate, &gotTime, &gotTimestamp); err != nil {
		t.Fatalf("QueryRow().Scan() error = %v", err)
	}
	if !gotDate.Equal(wantDate) {
		t.Fatalf("QueryRow().Scan(DATE) = %v, want %v", gotDate, wantDate)
	}
	if gotTime != wantTime {
		t.Fatalf("QueryRow().Scan(TIME) = %#v, want %#v", gotTime, wantTime)
	}
	if !gotTimestamp.Equal(wantTimestamp) {
		t.Fatalf("QueryRow().Scan(TIMESTAMP) = %v, want %v", gotTimestamp, wantTimestamp)
	}
}

func TestAlterTableAddColumnCloseReopenPreservesExpandedExistingRows(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT)",
		"INSERT INTO users VALUES (1)",
		"INSERT INTO users VALUES (2)",
		"ALTER TABLE users ADD COLUMN nickname TEXT DEFAULT 'guest'",
		"ALTER TABLE users ADD COLUMN active BOOL",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id, nickname, active FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query(before reopen) error = %v", err)
	}
	if got := rows.data; len(got) != 2 || got[0][0] != int32(1) || got[0][1] != "guest" || got[0][2] != nil || got[1][0] != int32(2) || got[1][1] != "guest" || got[1][2] != nil {
		t.Fatalf("before reopen rows = %#v, want existing rows expanded with default/null", got)
	}
	rows.Close()

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err = db.Query("SELECT id, nickname, active FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query(after reopen) error = %v", err)
	}
	if got := rows.data; len(got) != 2 || got[0][0] != int32(1) || got[0][1] != "guest" || got[0][2] != nil || got[1][0] != int32(2) || got[1][1] != "guest" || got[1][2] != nil {
		t.Fatalf("after reopen rows = %#v, want existing rows expanded with default/null", got)
	}
	rows.Close()

	if _, err := db.Exec("INSERT INTO users (id) VALUES (3)"); err != nil {
		t.Fatalf("Exec(insert after reopen) error = %v", err)
	}
	rows, err = db.Query("SELECT id, nickname, active FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query(after reopen insert) error = %v", err)
	}
	if got := rows.data; len(got) != 3 || got[2][0] != int32(3) || got[2][1] != "guest" || got[2][2] != nil {
		t.Fatalf("after reopen insert rows = %#v, want new row to follow updated schema defaults", got)
	}
	rows.Close()
}

func TestCreateTablePersistsAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if name != "steve" {
		t.Fatalf("Scan() got %q, want %q", name, "steve")
	}
}

func TestOpenEmptyDBHasEmptyCatalog(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db.Close()
}

func TestCreateTableAllocatesPersistentRootPage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[\"users\"] = nil")
	}
	if table.TableID == 0 {
		t.Fatal("table.TableID = 0, want nonzero")
	}
	if table.RootPageID() < 1 {
		t.Fatalf("table.RootPageID() = %d, want >= 1", table.RootPageID())
	}
	if table.PersistedRowCount() != 0 {
		t.Fatalf("table.PersistedRowCount() = %d, want 0", table.PersistedRowCount())
	}
}

func TestCreateMultipleTablesGetDistinctRootPages(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}

	usersRoot := db.tables["users"].RootPageID()
	teamsRoot := db.tables["teams"].RootPageID()
	if usersRoot == teamsRoot {
		t.Fatalf("root page ids are equal: users=%d teams=%d", usersRoot, teamsRoot)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	users := db.tables["users"]
	teams := db.tables["teams"]
	if users == nil || teams == nil {
		t.Fatalf("reopened tables missing: users=%v teams=%v", users, teams)
	}
	if users.RootPageID() == teams.RootPageID() {
		t.Fatalf("reopened root page ids are equal: users=%d teams=%d", users.RootPageID(), teams.RootPageID())
	}
	if users.RootPageID() != usersRoot {
		t.Fatalf("users.RootPageID() = %d, want %d", users.RootPageID(), usersRoot)
	}
	if teams.RootPageID() != teamsRoot {
		t.Fatalf("teams.RootPageID() = %d, want %d", teams.RootPageID(), teamsRoot)
	}
	if users.PersistedRowCount() != 0 || teams.PersistedRowCount() != 0 {
		t.Fatalf("persisted row counts = (%d,%d), want (0,0)", users.PersistedRowCount(), teams.PersistedRowCount())
	}
	if users.TableID == 0 || teams.TableID == 0 {
		t.Fatalf("table IDs = (%d,%d), want both nonzero", users.TableID, teams.TableID)
	}
	if users.TableID == teams.TableID {
		t.Fatalf("table IDs = (%d,%d), want distinct values", users.TableID, teams.TableID)
	}
}

func TestOpenFailsWhenCurrentCatalogIDsAreMissing(t *testing.T) {
	path := testDBPath(t)

	rawDB, pager := openRawStorage(t, path)
	writeMalformedCatalogPage(t, pager, currentCatalogBytesForTest([]currentCatalogTableForTest{
		{
			name:    "users",
			tableID: 0,
			columns: []currentCatalogColumnForTest{
				{name: "id", typ: storage.CatalogColumnTypeInt},
				{name: "name", typ: storage.CatalogColumnTypeText},
			},
			indexes: []currentCatalogIndexForTest{
				{name: "idx_users_name", indexID: 9, columns: []currentCatalogIndexColumnForTest{{name: "name"}}},
			},
		},
	}))
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err := Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("reopen Open() error = nil, want zero durable table ID rejection")
	}
}

func TestOpenRejectsZeroDurableIndexID(t *testing.T) {
	path := testDBPath(t)

	rawDB, pager := openRawStorage(t, path)
	writeMalformedCatalogPage(t, pager, currentCatalogBytesForTest([]currentCatalogTableForTest{
		{
			name:    "users",
			tableID: 7,
			columns: []currentCatalogColumnForTest{
				{name: "id", typ: storage.CatalogColumnTypeInt},
				{name: "name", typ: storage.CatalogColumnTypeText},
			},
			indexes: []currentCatalogIndexForTest{
				{name: "idx_users_name", indexID: 0, columns: []currentCatalogIndexColumnForTest{{name: "name"}}},
			},
		},
	}))
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err := Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("reopen Open() error = nil, want zero durable index ID rejection")
	}
}

func TestOpenBootstrapsInternalSystemCatalogTablesOnCurrentFormatDB(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	wantSchemas := map[string][]string{
		systemTableTables:       {"table_id", "table_name"},
		systemTableColumns:      {"table_id", "column_name", "column_type", "ordinal_position"},
		systemTableIndexes:      {"index_id", "index_name", "table_id", "is_unique"},
		systemTableIndexColumns: {"index_id", "column_name", "ordinal_position"},
	}

	for name, wantColumns := range wantSchemas {
		table := db.tables[name]
		if table == nil {
			t.Fatalf("db.tables[%q] = nil", name)
		}
		if !table.IsSystem {
			t.Fatalf("db.tables[%q].IsSystem = false, want true", name)
		}
		if table.TableID == 0 {
			t.Fatalf("db.tables[%q].TableID = 0, want nonzero", name)
		}
		if table.RootPageID() == 0 {
			t.Fatalf("db.tables[%q].RootPageID() = 0, want nonzero", name)
		}
		if len(table.Columns) != len(wantColumns) {
			t.Fatalf("len(db.tables[%q].Columns) = %d, want %d", name, len(table.Columns), len(wantColumns))
		}
		for i, wantColumn := range wantColumns {
			if table.Columns[i].Name != wantColumn {
				t.Fatalf("db.tables[%q].Columns[%d].Name = %q, want %q", name, i, table.Columns[i].Name, wantColumn)
			}
		}
	}

	tables, err := db.ListTables()
	if err != nil {
		t.Fatalf("ListTables() error = %v", err)
	}
	if len(tables) != 0 {
		t.Fatalf("len(ListTables()) = %d, want 0 for empty user catalog", len(tables))
	}
}

func TestOpenPreservesBootstrappedInternalSystemCatalogTables(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	snapshots := make(map[string]struct {
		tableID uint32
		rootID  storage.PageID
	}, 4)
	for _, name := range []string{
		systemTableTables,
		systemTableColumns,
		systemTableIndexes,
		systemTableIndexColumns,
	} {
		table := db.tables[name]
		if table == nil {
			t.Fatalf("db.tables[%q] = nil", name)
		}
		snapshots[name] = struct {
			tableID uint32
			rootID  storage.PageID
		}{
			tableID: table.TableID,
			rootID:  table.RootPageID(),
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	for name, snapshot := range snapshots {
		table := db.tables[name]
		if table == nil {
			t.Fatalf("reopened db.tables[%q] = nil", name)
		}
		if table.TableID != snapshot.tableID {
			t.Fatalf("reopened db.tables[%q].TableID = %d, want %d", name, table.TableID, snapshot.tableID)
		}
		if table.RootPageID() != snapshot.rootID {
			t.Fatalf("reopened db.tables[%q].RootPageID() = %d, want %d", name, table.RootPageID(), snapshot.rootID)
		}
	}
}

func TestOpenBootstrapsMissingInternalSystemCatalogTablesOnCurrentFormatDB(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	catalog = catalogWithDirectoryRootsForSave(t, rawDB.File(), catalog)
	filtered := make([]storage.CatalogTable, 0, len(catalog.Tables))
	for _, table := range catalog.Tables {
		if isSystemCatalogTableName(table.Name) {
			continue
		}
		filtered = append(filtered, table)
	}
	catalog.Tables = filtered
	if err := storage.SaveCatalog(pager, catalog); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	rewriteDirectoryRootMappingsForCatalogTables(t, rawDB.File(), catalog)
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("reopen Open() error = nil, want corrupted header page")
	}
	if !strings.Contains(err.Error(), "storage: corrupted header page:") || !strings.Contains(err.Error(), "orphan table-header page") {
		t.Fatalf("reopen Open() error = %v, want orphan table-header detail", err)
	}
}

func TestSystemCatalogRowsTrackSchemaMetadata(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	assertSystemCatalogRows(t, db,
		[][]any{{int64(db.tables["users"].TableID), "users"}},
		[][]any{
			{int64(db.tables["users"].TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(db.tables["users"].TableID), "name", parser.ColumnTypeText, int64(2)},
		},
		nil,
		nil,
	)

	if _, err := db.Exec("ALTER TABLE users ADD COLUMN active INT"); err != nil {
		t.Fatalf("Exec(alter table) error = %v", err)
	}
	assertSystemCatalogRows(t, db,
		[][]any{{int64(db.tables["users"].TableID), "users"}},
		[][]any{
			{int64(db.tables["users"].TableID), "active", parser.ColumnTypeInt, int64(3)},
			{int64(db.tables["users"].TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(db.tables["users"].TableID), "name", parser.ColumnTypeText, int64(2)},
		},
		nil,
		nil,
	)

	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create unique index) error = %v", err)
	}
	indexDef := db.tables["users"].IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatal("IndexDefinition(idx_users_name) = nil")
	}
	assertSystemCatalogRows(t, db,
		[][]any{{int64(db.tables["users"].TableID), "users"}},
		[][]any{
			{int64(db.tables["users"].TableID), "active", parser.ColumnTypeInt, int64(3)},
			{int64(db.tables["users"].TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(db.tables["users"].TableID), "name", parser.ColumnTypeText, int64(2)},
		},
		[][]any{{int64(indexDef.IndexID), "idx_users_name", int64(db.tables["users"].TableID), true}},
		[][]any{{int64(indexDef.IndexID), "name", int64(1)}},
	)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	indexDef = db.tables["users"].IndexDefinition("idx_users_name")
	assertSystemCatalogRows(t, db,
		[][]any{{int64(db.tables["users"].TableID), "users"}},
		[][]any{
			{int64(db.tables["users"].TableID), "active", parser.ColumnTypeInt, int64(3)},
			{int64(db.tables["users"].TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(db.tables["users"].TableID), "name", parser.ColumnTypeText, int64(2)},
		},
		[][]any{{int64(indexDef.IndexID), "idx_users_name", int64(db.tables["users"].TableID), true}},
		[][]any{{int64(indexDef.IndexID), "name", int64(1)}},
	)
}

func TestSystemCatalogRowsRebuildAcrossDropOperations(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	usersTable := db.tables["users"]
	teamsTable := db.tables["teams"]
	indexDef := usersTable.IndexDefinition("idx_users_name")
	if usersTable == nil || teamsTable == nil || indexDef == nil {
		t.Fatalf("schema setup failed: users=%v teams=%v index=%v", usersTable, teamsTable, indexDef)
	}

	assertSystemCatalogRows(t, db,
		[][]any{
			{int64(teamsTable.TableID), "teams"},
			{int64(usersTable.TableID), "users"},
		},
		[][]any{
			{int64(teamsTable.TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(usersTable.TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(usersTable.TableID), "name", parser.ColumnTypeText, int64(2)},
		},
		[][]any{{int64(indexDef.IndexID), "idx_users_name", int64(usersTable.TableID), false}},
		[][]any{{int64(indexDef.IndexID), "name", int64(1)}},
	)

	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	assertSystemCatalogRows(t, db,
		[][]any{
			{int64(teamsTable.TableID), "teams"},
			{int64(usersTable.TableID), "users"},
		},
		[][]any{
			{int64(teamsTable.TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(usersTable.TableID), "id", parser.ColumnTypeInt, int64(1)},
			{int64(usersTable.TableID), "name", parser.ColumnTypeText, int64(2)},
		},
		nil,
		nil,
	)

	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}
	assertSystemCatalogRows(t, db,
		[][]any{{int64(teamsTable.TableID), "teams"}},
		[][]any{{int64(teamsTable.TableID), "id", parser.ColumnTypeInt, int64(1)}},
		nil,
		nil,
	)
}

func TestOpenRebuildsSystemCatalogRowsForCurrentFormatDBMissingSystemTables(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	catalog = catalogWithDirectoryRootsForSave(t, rawDB.File(), catalog)
	filtered := make([]storage.CatalogTable, 0, len(catalog.Tables))
	for _, table := range catalog.Tables {
		if isSystemCatalogTableName(table.Name) {
			continue
		}
		filtered = append(filtered, table)
	}
	catalog.Tables = filtered
	if err := storage.SaveCatalog(pager, catalog); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	rewriteDirectoryRootMappingsForCatalogTables(t, rawDB.File(), catalog)
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("reopen Open() error = nil, want corrupted header page")
	}
	if !strings.Contains(err.Error(), "storage: corrupted header page:") || !strings.Contains(err.Error(), "orphan table-header page") {
		t.Fatalf("reopen Open() error = %v, want orphan table-header detail", err)
	}
}

func TestCATDIRDualModeLifecycleAcrossReopens(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if err := db.VerifySystemCatalogDigest(); err != nil {
		t.Fatalf("VerifySystemCatalogDigest() baseline error = %v", err)
	}
	baselineDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest() baseline error = %v", err)
	}
	baselineTables := cloneTables(db.tables)
	if err := db.Close(); err != nil {
		t.Fatalf("Close() baseline error = %v", err)
	}

	assertCATDIRModeForPath(t, path, storage.DirectoryCATDIRStorageModeEmbedded)

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() baseline error = %v", err)
	}
	reopenBaselineDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest() reopened baseline error = %v", err)
	}
	if reopenBaselineDigest != baselineDigest {
		t.Fatalf("reopened baseline digest = %q, want %q", reopenBaselineDigest, baselineDigest)
	}
	if err := db.VerifySystemCatalogDigest(); err != nil {
		t.Fatalf("VerifySystemCatalogDigest() reopened baseline error = %v", err)
	}

	largeTables, largePages := buildSyntheticCATDIRTablesForTest(db, baselineTables, 1, "promote")
	if err := db.persistCatalogState(largeTables, largePages); err != nil {
		t.Fatalf("persistCatalogState(large) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() after promotion error = %v", err)
	}

	assertCATDIRModeForPath(t, path, storage.DirectoryCATDIRStorageModeOverflow)

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() overflow error = %v", err)
	}
	overflowDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest() overflow error = %v", err)
	}
	if overflowDigest == baselineDigest {
		t.Fatalf("overflow digest = %q, want different from baseline %q", overflowDigest, baselineDigest)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() overflow error = %v", err)
	}

	assertCATDIRModeForPath(t, path, storage.DirectoryCATDIRStorageModeOverflow)

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second reopen Open() overflow error = %v", err)
	}
	overflowDigestReopen, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest() second overflow reopen error = %v", err)
	}
	if overflowDigestReopen != overflowDigest {
		t.Fatalf("overflow digest after reopen = %q, want %q", overflowDigestReopen, overflowDigest)
	}

	freedPages, err := db.buildFreedPages(stagedPageIDsForTest(largePages)...)
	if err != nil {
		t.Fatalf("buildFreedPages(demote baseline) error = %v", err)
	}
	if err := db.persistCatalogState(cloneTables(baselineTables), freedPages); err != nil {
		t.Fatalf("persistCatalogState(demote baseline) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() after demotion error = %v", err)
	}

	assertCATDIRModeForPath(t, path, storage.DirectoryCATDIRStorageModeEmbedded)

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() demoted error = %v", err)
	}
	defer db.Close()

	demotedDigest, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest() demoted error = %v", err)
	}
	if demotedDigest != baselineDigest {
		t.Fatalf("demoted digest = %q, want baseline %q", demotedDigest, baselineDigest)
	}
	if err := db.VerifySystemCatalogDigest(); err != nil {
		t.Fatalf("VerifySystemCatalogDigest() demoted error = %v", err)
	}
	assertCATDIRModeForPath(t, path, storage.DirectoryCATDIRStorageModeEmbedded)
}

func TestCATDIROverflowRewriteReclaimsAndPagesBecomeReusable(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	baselineTables := cloneTables(db.tables)

	largeA, pagesA := buildSyntheticCATDIRTablesForTest(db, baselineTables, 1, "overflow_a")
	if err := db.persistCatalogState(largeA, pagesA); err != nil {
		t.Fatalf("persistCatalogState(largeA) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() largeA error = %v", err)
	}

	mode, head, count, freeListHead := readCATDIRStateForPath(t, path)
	if mode != storage.DirectoryCATDIRStorageModeOverflow {
		t.Fatalf("CAT/DIR mode after largeA = %d, want %d", mode, storage.DirectoryCATDIRStorageModeOverflow)
	}
	oldChainIDs := readCATDIROverflowChainIDsForPath(t, path, head, count)
	if len(oldChainIDs) == 0 {
		t.Fatal("old CAT/DIR overflow chain ids = empty, want non-empty")
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() after largeA error = %v", err)
	}
	largeB := rewriteSyntheticCATDIRTablesForTest(db.tables, "overflow_b")
	if err := db.persistCatalogState(largeB, nil); err != nil {
		t.Fatalf("persistCatalogState(largeB) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() largeB error = %v", err)
	}

	mode, head, count, freeListHead = readCATDIRStateForPath(t, path)
	if mode != storage.DirectoryCATDIRStorageModeOverflow {
		t.Fatalf("CAT/DIR mode after largeB = %d, want %d", mode, storage.DirectoryCATDIRStorageModeOverflow)
	}
	if freeListHead == 0 {
		t.Fatal("DirectoryFreeListHead() after overflow rewrite = 0, want reusable reclaimed pages")
	}
	reused := allocateFreePagesFromHeadForTest(t, path, freeListHead, 1)
	if len(reused) != 1 {
		t.Fatalf("len(reused free pages) = %d, want 1", len(reused))
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("final reopen Open() error = %v", err)
	}
	defer db.Close()

	digestA, err := db.SchemaDigest()
	if err != nil {
		t.Fatalf("SchemaDigest() final overflow error = %v", err)
	}
	if digestA == "" {
		t.Fatal("SchemaDigest() final overflow = empty, want non-empty")
	}
	if _, ok := db.tables["users"]; !ok {
		t.Fatal(`db.tables["users"] missing after overflow->overflow lifecycle`)
	}
}

func TestCommitDurabilityAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	for _, sql := range []string{
		"CREATE TABLE t (id INT)",
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
		"INSERT INTO t VALUES (3)",
		"UPDATE t SET id = 10 WHERE id = 1",
		"DELETE FROM t WHERE id = 2",
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
	assertSelectIntRows(t, db, "SELECT * FROM t", 10, 3)
}

func TestFailedMutationDoesNotLeakState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	for _, sql := range []string{
		"INSERT INTO t VALUES (1, 2)",
		"UPDATE t SET missing = 10 WHERE id = 1",
		"DELETE FROM t WHERE missing = 2",
		"CREATE TABLE t (id INT)",
	} {
		if _, err := db.Exec(sql); err == nil {
			t.Fatalf("Exec(%q) error = nil, want failure", sql)
		}
		assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)
		assertAutocommitClean(t, db, path)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)
}

func TestInterruptedCommitRecoversLastCommittedState(t *testing.T) {
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

	db.afterDatabaseSyncHook = func() error {
		return errors.New("boom after db sync")
	}
	if _, err := db.Exec("UPDATE t SET id = 2 WHERE id = 1"); err == nil {
		t.Fatal("Exec(update) error = nil, want interrupted commit failure")
	}
	if _, err := os.Stat(storage.JournalPath(path)); err != nil {
		t.Fatalf("journal stat error = %v, want surviving journal", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 2)
	if _, err := os.Stat(storage.JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists after recovery", err)
	}
}

func TestMultipleCommittedMutationsAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE t (id INT)",
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	if _, err := db.Exec("UPDATE t SET id = 10 WHERE id = 1"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	assertSelectIntRows(t, db, "SELECT * FROM t", 10, 2)
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	if _, err := db.Exec("DELETE FROM t WHERE id = 2"); err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (30)"); err != nil {
		t.Fatalf("Exec(insert 30) error = %v", err)
	}
	assertSelectIntRows(t, db, "SELECT * FROM t", 10, 30)
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 10, 30)
}

func TestJournalAbsentAfterSuccessfulCommitAndRecovery(t *testing.T) {
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
	assertAutocommitClean(t, db, path)

	db.afterDatabaseSyncHook = func() error {
		return errors.New("boom after db sync")
	}
	if _, err := db.Exec("UPDATE t SET id = 2 WHERE id = 1"); err == nil {
		t.Fatal("Exec(update) error = nil, want interrupted commit failure")
	}
	if _, err := os.Stat(storage.JournalPath(path)); err != nil {
		t.Fatalf("journal stat error = %v, want surviving journal", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertAutocommitClean(t, db, path)
}

func TestAutocommitMutationLeavesNoTxnOrTracking(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE t (id INT)",
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET id = 10 WHERE id = 1",
		"DELETE FROM t WHERE id = 10",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
		assertAutocommitClean(t, db, path)
	}
}

func TestCorruptedDatabaseHeaderDetected(t *testing.T) {
	path := testDBPath(t)
	if err := os.WriteFile(path, []byte("bad-header"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err := Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want non-nil")
	}
	if err.Error() != "storage: corrupted database header" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: corrupted database header")
	}
}

func TestCorruptedPageHeaderDetected(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	if _, err := f.Write([]byte{0xff}); err != nil {
		_ = f.Close()
		t.Fatalf("Write() error = %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want non-nil")
	}
	if err.Error() != "storage: corrupted page header" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: corrupted page header")
	}
}

func TestCorruptedTablePageDetected(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	dataPageIDs, err := committedTableDataPageIDs(db.pool, db.tables["users"])
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	if len(dataPageIDs) != 1 {
		t.Fatalf("len(committedTableDataPageIDs()) = %d, want 1", len(dataPageIDs))
	}
	dataPageID := dataPageIDs[0]
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := os.Remove(storage.WALPath(path)); err != nil {
		t.Fatalf("Remove(WALPath) error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	page, err := pager.Get(dataPageID)
	if err != nil {
		t.Fatalf("pager.Get(data) error = %v", err)
	}
	binary.LittleEndian.PutUint32(page.Data()[4:8], 4)
	pager.MarkDirty(page)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want non-nil")
	}
	assertErrorContainsAll(t, err, "storage: corrupted table page:", `table "users"`, "data page", "wrong owning table id")
}

func TestCorruptedRowDataDetected(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	dataPageIDs, err := committedTableDataPageIDs(db.pool, db.tables["users"])
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	if len(dataPageIDs) != 1 {
		t.Fatalf("len(committedTableDataPageIDs()) = %d, want 1", len(dataPageIDs))
	}
	dataPageID := dataPageIDs[0]
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := os.Remove(storage.WALPath(path)); err != nil {
		t.Fatalf("Remove(WALPath) error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	page, err := pager.Get(dataPageID)
	if err != nil {
		t.Fatalf("pager.Get(data) error = %v", err)
	}
	offset, _, err := storage.TablePageSlot(page.Data(), 0)
	if err != nil {
		t.Fatalf("storage.TablePageSlot() error = %v", err)
	}
	binary.LittleEndian.PutUint16(page.Data()[offset:offset+2], 2)
	pager.MarkDirty(page)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want non-nil")
	}
	if err.Error() != "storage: corrupted table page" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: corrupted table page")
	}
}

func TestCorruptedIndexMetadataDetected(t *testing.T) {
	path := testDBPath(t)

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	rootPage := pager.NewPage()
	storage.InitTableRootPage(rootPage)
	indexPage := pager.NewPage()
	copy(indexPage.Data(), storage.InitIndexLeafPage(uint32(indexPage.ID())))
	writeMalformedCatalogPageWithIDMappings(t, pager, corruptedIndexCatalogBytes(uint32(rootPage.ID())), []storage.DirectoryRootIDMapping{
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: 7, RootPageID: uint32(rootPage.ID())},
		{ObjectType: storage.DirectoryRootMappingObjectIndex, ObjectID: 9, RootPageID: uint32(indexPage.ID())},
	})

	_, err := Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want non-nil")
	}
	if err.Error() != "storage: corrupted index metadata" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: corrupted index metadata")
	}
}

func TestOpenRejectsWrongOwnedDataPageCountInTableHeader(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	headerPageID := db.tables["users"].TableHeaderPageID()
	rawDB, pager := closeAndOpenRawWithoutWAL(t, path, db)
	defer rawDB.Close()

	headerPage, err := pager.Get(headerPageID)
	if err != nil {
		t.Fatalf("pager.Get(header) error = %v", err)
	}
	if err := storage.SetTableHeaderOwnedDataPageCount(headerPage.Data(), 2); err != nil {
		t.Fatalf("SetTableHeaderOwnedDataPageCount() error = %v", err)
	}
	pager.MarkDirty(headerPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err = Open(path)
	assertErrorContainsAll(t, err, "storage: corrupted header page:", `table "users"`, "owned data page count mismatch")
}

func TestOpenRejectsWrongOwnedSpaceMapPageCountInTableHeader(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	headerPageID := db.tables["users"].TableHeaderPageID()
	rawDB, pager := closeAndOpenRawWithoutWAL(t, path, db)
	defer rawDB.Close()

	headerPage, err := pager.Get(headerPageID)
	if err != nil {
		t.Fatalf("pager.Get(header) error = %v", err)
	}
	if err := storage.SetTableHeaderOwnedSpaceMapPageCount(headerPage.Data(), 2); err != nil {
		t.Fatalf("SetTableHeaderOwnedSpaceMapPageCount() error = %v", err)
	}
	pager.MarkDirty(headerPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err = Open(path)
	assertErrorContainsAll(t, err, "storage: corrupted header page:", `table "users"`, "owned space-map page count mismatch")
}

func TestOpenRejectsDuplicateDataPageIDsInSpaceMapInventory(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	table := db.tables["users"]
	spaceMapPageID := table.FirstSpaceMapPageID()
	dataPageIDs, err := committedTableDataPageIDs(db.pool, table)
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	headerPageID := table.TableHeaderPageID()
	rawDB, pager := closeAndOpenRawWithoutWAL(t, path, db)
	defer rawDB.Close()

	headerPage, err := pager.Get(headerPageID)
	if err != nil {
		t.Fatalf("pager.Get(header) error = %v", err)
	}
	if err := storage.SetTableHeaderOwnedDataPageCount(headerPage.Data(), 2); err != nil {
		t.Fatalf("SetTableHeaderOwnedDataPageCount() error = %v", err)
	}
	pager.MarkDirty(headerPage)

	spaceMapPage, err := pager.Get(spaceMapPageID)
	if err != nil {
		t.Fatalf("pager.Get(space map) error = %v", err)
	}
	if _, err := storage.AppendSpaceMapEntry(spaceMapPage.Data(), storage.SpaceMapEntry{
		DataPageID:      dataPageIDs[0],
		FreeSpaceBucket: storage.SpaceMapBucketHigh,
	}); err != nil {
		t.Fatalf("AppendSpaceMapEntry() error = %v", err)
	}
	pager.MarkDirty(spaceMapPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err = Open(path)
	assertErrorContainsAll(t, err, "storage: corrupted space map page:", `table "users"`, "duplicate data page")
}

func TestOpenRejectsSpaceMapEntryPointingAtWrongPageType(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	table := db.tables["users"]
	rawDB, pager := closeAndOpenRawWithoutWAL(t, path, db)
	defer rawDB.Close()

	spaceMapPage, err := pager.Get(table.FirstSpaceMapPageID())
	if err != nil {
		t.Fatalf("pager.Get(space map) error = %v", err)
	}
	entry, err := storage.SpaceMapPageEntry(spaceMapPage.Data(), 0)
	if err != nil {
		t.Fatalf("SpaceMapPageEntry() error = %v", err)
	}
	entry.DataPageID = table.TableHeaderPageID()
	if err := storage.UpdateSpaceMapEntry(spaceMapPage.Data(), 0, entry); err != nil {
		t.Fatalf("UpdateSpaceMapEntry() error = %v", err)
	}
	pager.MarkDirty(spaceMapPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err = Open(path)
	assertErrorContainsAll(t, err, "storage: corrupted table page:", `table "users"`, "data page", "wrong owning table id")
}

func TestOpenRejectsReferencedDataPageWithWrongOwningTableID(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	table := db.tables["users"]
	dataPageIDs, err := committedTableDataPageIDs(db.pool, table)
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	rawDB, pager := closeAndOpenRawWithoutWAL(t, path, db)
	defer rawDB.Close()

	dataPage, err := pager.Get(dataPageIDs[0])
	if err != nil {
		t.Fatalf("pager.Get(data) error = %v", err)
	}
	clear(dataPage.Data())
	copy(dataPage.Data(), storage.InitOwnedDataPage(uint32(dataPageIDs[0]), table.TableID+99))
	pager.MarkDirty(dataPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err = Open(path)
	assertErrorContainsAll(t, err, "storage: corrupted table page:", `table "users"`, "data page", "wrong owning table id")
}

func TestOpenRejectsBrokenSpaceMapChain(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	table := db.tables["users"]
	rawDB, pager := closeAndOpenRawWithoutWAL(t, path, db)
	defer rawDB.Close()

	secondSpaceMap := pager.NewPage()
	clear(secondSpaceMap.Data())
	copy(secondSpaceMap.Data(), storage.InitSpaceMapPage(uint32(secondSpaceMap.ID()), table.TableID))

	firstPage, err := pager.Get(table.FirstSpaceMapPageID())
	if err != nil {
		t.Fatalf("pager.Get(first space map) error = %v", err)
	}
	if err := storage.SetSpaceMapNextPageID(firstPage.Data(), uint32(secondSpaceMap.ID())); err != nil {
		t.Fatalf("SetSpaceMapNextPageID(first) error = %v", err)
	}
	if err := storage.SetSpaceMapNextPageID(secondSpaceMap.Data(), uint32(firstPage.ID())); err != nil {
		t.Fatalf("SetSpaceMapNextPageID(second) error = %v", err)
	}
	headerPage, err := pager.Get(table.TableHeaderPageID())
	if err != nil {
		t.Fatalf("pager.Get(header) error = %v", err)
	}
	if err := storage.SetTableHeaderOwnedSpaceMapPageCount(headerPage.Data(), 2); err != nil {
		t.Fatalf("SetTableHeaderOwnedSpaceMapPageCount() error = %v", err)
	}
	pager.MarkDirty(firstPage)
	pager.MarkDirty(secondSpaceMap)
	pager.MarkDirty(headerPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err = Open(path)
	assertErrorContainsAll(t, err, "storage: corrupted space map page:", `table "users"`, "space-map chain revisits page")
}

func TestOpenRejectsOrphanOwnedDataPage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}
	table := db.tables["users"]
	rawDB, pager := closeAndOpenRawWithoutWAL(t, path, db)
	defer rawDB.Close()

	orphan := pager.NewPage()
	clear(orphan.Data())
	copy(orphan.Data(), storage.InitOwnedDataPage(uint32(orphan.ID()), table.TableID))
	pager.MarkDirty(orphan)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}

	_, err = Open(path)
	assertErrorContainsAll(t, err, "storage: corrupted table page:", "orphan owned data page", "claims table id")
}

func TestInterruptedDropIndexRecoversLastCommittedIndexState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	db.afterDatabaseSyncHook = func() error {
		return errors.New("boom after db sync")
	}
	if _, err := db.Exec("DROP INDEX idx_users_name"); err == nil {
		t.Fatal("Exec(drop index) error = nil, want interrupted commit failure")
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
	if table.IndexDefinition("idx_users_name") != nil {
		t.Fatalf("IndexDefinition(idx_users_name) = %#v, want nil after WAL recovery", table.IndexDefinition("idx_users_name"))
	}
	if _, err := os.Stat(storage.JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists after recovery", err)
	}
}

func TestDropIndexStateStaysGoneAcrossRepeatedReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE INDEX idx_users_name ON users (name)",
		"DROP INDEX idx_users_name",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	if table := db.tables["users"]; table == nil || table.IndexDefinition("idx_users_name") != nil {
		t.Fatalf("after first reopen table = %#v, want no surviving index", table)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	if table := db.tables["users"]; table == nil || table.IndexDefinition("idx_users_name") != nil {
		t.Fatalf("after second reopen table = %#v, want no surviving index", table)
	}
}

func TestInterruptedDropTableRecoversLastCommittedTableState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	db.afterDatabaseSyncHook = func() error {
		return errors.New("boom after db sync")
	}
	if _, err := db.Exec("DROP TABLE users"); err == nil {
		t.Fatal("Exec(drop table) error = nil, want interrupted commit failure")
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
	if table != nil {
		t.Fatalf("db.tables[users] = %#v, want dropped table to stay absent after WAL recovery", table)
	}
	if _, err := os.Stat(storage.JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists after recovery", err)
	}
}

func TestDropTableStateStaysGoneAcrossRepeatedReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"DROP TABLE users",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	if _, ok := db.tables["users"]; ok {
		t.Fatalf("after first reopen db.tables[users] = %#v, want absent", db.tables["users"])
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	if _, ok := db.tables["users"]; ok {
		t.Fatalf("after second reopen db.tables[users] = %#v, want absent", db.tables["users"])
	}
}

func TestInterruptedDropTableRecoveryPreservesUnrelatedObjects(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE teams (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO teams VALUES (1, 'ops')",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	db.afterDatabaseSyncHook = func() error {
		return errors.New("boom after db sync")
	}
	if _, err := db.Exec("DROP TABLE users"); err == nil {
		t.Fatal("Exec(drop table) error = nil, want interrupted commit failure")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if db.tables["users"] != nil {
		t.Fatalf("db.tables[users] = %#v, want dropped table absent after WAL recovery", db.tables["users"])
	}
	if db.tables["teams"] == nil {
		t.Fatal("db.tables[teams] = nil, want unrelated table preserved after recovery")
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
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
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
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
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

	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
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
		if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
			t.Fatalf("Exec(create index) error = %v", err)
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
		if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
			t.Fatalf("Exec(create index) error = %v", err)
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
		if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
			t.Fatalf("Exec(create index) error = %v", err)
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
		if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
			t.Fatalf("Exec(create index) error = %v", err)
		}

		got := collectIntRows(t, db, "SELECT COUNT(*) FROM users WHERE name = 'same'")
		if !reflect.DeepEqual(got, []int{50}) {
			t.Fatalf("count rows = %#v, want []int{50}", got)
		}
		assertIndexConsistency(t, db.tables["users"])
	})
}

func TestStage7OpenPreIndexCatalogFails(t *testing.T) {
	path := testDBPath(t)
	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	rootPage := pager.NewPage()
	storage.InitTableRootPage(rootPage)
	row, err := storage.EncodeRow(storageValuesFromParser([]parser.Value{parser.Int64Value(1), parser.StringValue("legacy")}))
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
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want legacy catalog payload rejection")
	}
}

func TestIndexMetadataPersistsAcrossReopen(t *testing.T) {
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
	if _, err := db.Exec("CREATE INDEX name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[\"users\"] = nil")
	}
	indexDef := table.IndexDefinition("name")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(name) = nil, defs=%#v", table.IndexDefs)
	}
	if indexDef.IndexID == 0 || indexDef.RootPageID == 0 {
		t.Fatalf("indexDef = %#v, want nonzero durable metadata", indexDef)
	}
}

func TestCatalogRoundTripPreservesIndexMetadataForOpen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX id ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	usersTable := findCatalogTableByName(catalog, "users")
	if usersTable == nil || len(usersTable.Indexes) != 1 {
		t.Fatalf("catalog.Tables = %#v, want one persisted users index", catalog.Tables)
	}
	index := usersTable.Indexes[0]
	if index.Name != "id" || index.Unique || len(index.Columns) != 1 || index.Columns[0].Name != "id" || index.Columns[0].Desc {
		t.Fatalf("catalog.Tables[0].Indexes[0] = %#v, want named single-column ASC non-unique id index", index)
	}
	if catalog.Version != 9 {
		t.Fatalf("catalog.Version = %d, want 9", catalog.Version)
	}
	if usersTable.RootPageID != 0 {
		t.Fatalf("catalog.Tables[0].RootPageID = %d, want 0 on new writes", usersTable.RootPageID)
	}
	if index.RootPageID != 0 {
		t.Fatalf("catalog.Tables[0].Indexes[0].RootPageID = %d, want 0 on new writes", index.RootPageID)
	}
	idMappings, err := storage.ReadDirectoryRootIDMappings(dbFile.File())
	if err != nil {
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	var indexRoot uint32
	for _, mapping := range idMappings {
		if mapping.ObjectType == storage.DirectoryRootMappingObjectIndex && mapping.ObjectID == index.IndexID {
			indexRoot = mapping.RootPageID
			break
		}
	}
	if indexRoot == 0 {
		t.Fatal("directory index ID root mapping not found")
	}
	rootPage, err := pager.Get(storage.PageID(indexRoot))
	if err != nil {
		t.Fatalf("pager.Get(index root) error = %v", err)
	}
	if got := storage.PageType(binary.LittleEndian.Uint16(rootPage.Data()[4:6])); got != storage.PageTypeIndexLeaf {
		t.Fatalf("index root page type = %d, want %d", got, storage.PageTypeIndexLeaf)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := table.IndexDefinition("id")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(id) = nil, defs=%#v", table.IndexDefs)
	}
	if indexDef.RootPageID != indexRoot {
		t.Fatalf("IndexDefinition(id).RootPageID = %d, want %d", indexDef.RootPageID, indexRoot)
	}
}

func TestOpenRetainsUnsupportedIndexDefinitions(t *testing.T) {
	path := testDBPath(t)

	dbFile, pager := openRawStorage(t, path)
	rootPage := pager.NewPage()
	storage.InitTableRootPage(rootPage)
	indexRoot := pager.NewPage()
	clear(indexRoot.Data())
	copy(indexRoot.Data(), storage.InitIndexLeafPage(uint32(indexRoot.ID())))
	if err := storage.SaveCatalog(pager, &storage.CatalogData{
		Tables: []storage.CatalogTable{
			{
				Name:       "users",
				TableID:    1,
				RootPageID: uint32(rootPage.ID()),
				RowCount:   0,
				Columns: []storage.CatalogColumn{
					{Name: "id", Type: storage.CatalogColumnTypeInt},
					{Name: "name", Type: storage.CatalogColumnTypeText},
				},
				Indexes: []storage.CatalogIndex{
					{
						Name:       "idx_users_id_name",
						Unique:     true,
						IndexID:    2,
						RootPageID: uint32(indexRoot.ID()),
						Columns: []storage.CatalogIndexColumn{
							{Name: "id"},
							{Name: "name", Desc: true},
						},
					},
				},
			},
		},
	}); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	persistStrictPhysicalMetaForTests(t, dbFile.File(), pager, []strictTablePhysicalMetaForTest{{
		tableID:       1,
		rowRootPageID: rootPage.ID(),
		indexRoots: []strictIndexRootMappingForTest{{
			indexID:    2,
			rootPageID: indexRoot.ID(),
		}},
	}})
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	if len(table.IndexDefs) != 1 || table.IndexDefs[0].Name != "idx_users_id_name" {
		t.Fatalf("table.IndexDefs = %#v, want retained rich index definition", table.IndexDefs)
	}
}

func TestInsertMaintainsPersistedIndexLeafEntries(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'bob')",
		"INSERT INTO users VALUES (2, 'alice')",
		"INSERT INTO users VALUES (3, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := table.IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil, defs=%#v", table.IndexDefs)
	}
	rootPageID := indexDef.RootPageID
	dataPageIDs, err := committedTableDataPageIDs(db.pool, table)
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	if len(dataPageIDs) != 1 {
		t.Fatalf("len(committedTableDataPageIDs()) = %d, want 1", len(dataPageIDs))
	}
	tableDataPageID := uint32(dataPageIDs[0])
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	indexPage, err := pager.Get(storage.PageID(rootPageID))
	if err != nil {
		t.Fatalf("pager.Get(index root) error = %v", err)
	}
	records, err := storage.ReadIndexLeafRecords(indexPage.Data())
	if err != nil {
		t.Fatalf("ReadIndexLeafRecords() error = %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("len(records) = %d, want 3", len(records))
	}
	aliceKey, err := storage.EncodeIndexKey(storageValuesFromParser([]parser.Value{parser.StringValue("alice")}))
	if err != nil {
		t.Fatalf("EncodeIndexKey(alice) error = %v", err)
	}
	bobKey, err := storage.EncodeIndexKey(storageValuesFromParser([]parser.Value{parser.StringValue("bob")}))
	if err != nil {
		t.Fatalf("EncodeIndexKey(bob) error = %v", err)
	}
	if !bytes.Equal(records[0].Key, aliceKey) || records[0].Locator != (storage.RowLocator{PageID: tableDataPageID, SlotID: 1}) {
		t.Fatalf("records[0] = %#v, want alice -> (%d,1)", records[0], tableDataPageID)
	}
	if !bytes.Equal(records[1].Key, aliceKey) || records[1].Locator != (storage.RowLocator{PageID: tableDataPageID, SlotID: 2}) {
		t.Fatalf("records[1] = %#v, want alice -> (%d,2)", records[1], tableDataPageID)
	}
	if !bytes.Equal(records[2].Key, bobKey) || records[2].Locator != (storage.RowLocator{PageID: tableDataPageID, SlotID: 0}) {
		t.Fatalf("records[2] = %#v, want bob -> (%d,0)", records[2], tableDataPageID)
	}

	pageReader := func(pageID uint32) ([]byte, error) {
		return pager.ReadPage(storage.PageID(pageID))
	}
	locators, err := storage.LookupIndexExact(pageReader, rootPageID, aliceKey)
	if err != nil {
		t.Fatalf("LookupIndexExact(alice) error = %v", err)
	}
	if len(locators) != 2 || locators[0] != (storage.RowLocator{PageID: tableDataPageID, SlotID: 1}) || locators[1] != (storage.RowLocator{PageID: tableDataPageID, SlotID: 2}) {
		t.Fatalf("LookupIndexExact(alice) = %#v, want [(%d,1), (%d,2)]", locators, tableDataPageID, tableDataPageID)
	}
}

func TestFetchRowByLocatorReturnsPersistedBaseRow(t *testing.T) {
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
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'bob')",
		"INSERT INTO users VALUES (2, 'alice')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := table.IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil, defs=%#v", table.IndexDefs)
	}
	rootPageData, err := readCommittedPageData(db.pool, storage.PageID(indexDef.RootPageID))
	if err != nil {
		t.Fatalf("readCommittedPageData(index root) error = %v", err)
	}
	records, err := storage.ReadIndexLeafRecords(rootPageData)
	if err != nil {
		t.Fatalf("ReadIndexLeafRecords() error = %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("len(records) = %d, want 3", len(records))
	}

	row, err := db.fetchRowByLocator(table, records[0].Locator)
	if err != nil {
		t.Fatalf("fetchRowByLocator() error = %v", err)
	}
	want := []parser.Value{parser.IntValue(2), parser.StringValue("alice")}
	for i := range want {
		if row[i] != want[i] {
			t.Fatalf("row[%d] = %#v, want %#v", i, row[i], want[i])
		}
	}
}

func TestFetchRowByLocatorFromIndexLeafSurvivesReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'bob')",
		"INSERT INTO users VALUES (2, 'alice')",
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

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := table.IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil, defs=%#v", table.IndexDefs)
	}
	rootPageData, err := readCommittedPageData(db.pool, storage.PageID(indexDef.RootPageID))
	if err != nil {
		t.Fatalf("readCommittedPageData(index root) error = %v", err)
	}
	records, err := storage.ReadIndexLeafRecords(rootPageData)
	if err != nil {
		t.Fatalf("ReadIndexLeafRecords() error = %v", err)
	}
	aliceKey, err := storage.EncodeIndexKey(storageValuesFromParser([]parser.Value{parser.StringValue("alice")}))
	if err != nil {
		t.Fatalf("EncodeIndexKey(alice) error = %v", err)
	}
	var locator storage.RowLocator
	found := false
	for _, record := range records {
		if bytes.Equal(record.Key, aliceKey) {
			locator = record.Locator
			found = true
			break
		}
	}
	if !found {
		t.Fatal("alice locator not found in index leaf records")
	}

	row, err := db.fetchRowByLocator(table, locator)
	if err != nil {
		t.Fatalf("fetchRowByLocator() error = %v", err)
	}
	want := []parser.Value{parser.IntValue(2), parser.StringValue("alice")}
	for i := range want {
		if row[i] != want[i] {
			t.Fatalf("row[%d] = %#v, want %#v", i, row[i], want[i])
		}
	}
}

func TestFetchRowByLocatorRejectsWrongOwnedDataPage(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE teams (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO teams VALUES (7, 'ops')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	otherPageIDs, err := committedTableDataPageIDs(db.pool, db.tables["teams"])
	if err != nil {
		t.Fatalf("committedTableDataPageIDs(teams) error = %v", err)
	}
	if len(otherPageIDs) != 1 {
		t.Fatalf("len(committedTableDataPageIDs(teams)) = %d, want 1", len(otherPageIDs))
	}

	_, err = db.fetchRowByLocator(db.tables["users"], storage.RowLocator{
		PageID: uint32(otherPageIDs[0]),
		SlotID: 0,
	})
	if err == nil {
		t.Fatal("fetchRowByLocator() error = nil, want corruption error")
	}
	if err.Error() != "storage: corrupted table page" {
		t.Fatalf("fetchRowByLocator() error = %q, want %q", err.Error(), "storage: corrupted table page")
	}
}

func TestIndexedCountStarLookupSurvivesReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'alice')",
		"INSERT INTO users VALUES (3, 'bob')",
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
	rows, err := db.Query("SELECT COUNT(*) FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 2)
}

func TestIndexedQueryRowLookupSurvivesReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
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
	row := db.QueryRow("SELECT id FROM users WHERE name = 'bob'")
	var id int32
	if err := row.Scan(&id); err != nil {
		t.Fatalf("QueryRow(indexed reopen).Scan() error = %v", err)
	}
	if id != 2 {
		t.Fatalf("QueryRow(indexed reopen).Scan() got %d, want 2", id)
	}
}

func TestIndexedQueryLookupSurvivesReopenWithoutLegacyEntries(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'alice')",
		"INSERT INTO users VALUES (3, 'bob')",
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
	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice' ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 1, 2)
}

func TestIndexedQueryLookupSurvivesReopenWithoutRuntimeIndexShell(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'alice')",
		"INSERT INTO users VALUES (3, 'bob')",
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
	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice' ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 1, 2)
}

func TestIndexedCountStarSurvivesReopenWithoutRuntimeIndexShell(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'alice')",
		"INSERT INTO users VALUES (3, 'bob')",
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
	rows, err := db.Query("SELECT COUNT(*) FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertRowsIntSequence(t, rows, 2)
}

func TestDeleteRebuildsPersistedIndexEntriesAndSurvivesReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, age INT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice', 10)",
		"INSERT INTO users VALUES (2, 'bob', 20)",
		"INSERT INTO users VALUES (3, 'cara', 30)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("DELETE FROM users WHERE id = 2"); err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}

	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("bob")}, nil)
	rows := assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("alice")}, [][]parser.Value{
		{parser.IntValue(1), parser.StringValue("alice"), parser.IntValue(10)},
	})
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1", len(rows))
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("bob")}, nil)
	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("cara")}, [][]parser.Value{
		{parser.IntValue(3), parser.StringValue("cara"), parser.IntValue(30)},
	})
}

func TestUpdateIndexedColumnRebuildsPersistedIndexEntriesAndSurvivesReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, age INT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice', 10)",
		"INSERT INTO users VALUES (2, 'bob', 20)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("UPDATE users SET name = 'zoe' WHERE id = 2"); err != nil {
		t.Fatalf("Exec(update indexed column) error = %v", err)
	}

	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("bob")}, nil)
	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("zoe")}, [][]parser.Value{
		{parser.IntValue(2), parser.StringValue("zoe"), parser.IntValue(20)},
	})

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("bob")}, nil)
	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("zoe")}, [][]parser.Value{
		{parser.IntValue(2), parser.StringValue("zoe"), parser.IntValue(20)},
	})
}

func TestUpdateNonIndexedColumnPreservesIndexMembership(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, active BOOL)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice', false)",
		"INSERT INTO users VALUES (2, 'bob', true)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("UPDATE users SET active = true WHERE id = 1"); err != nil {
		t.Fatalf("Exec(update non-indexed column) error = %v", err)
	}

	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("alice")}, [][]parser.Value{
		{parser.IntValue(1), parser.StringValue("alice"), parser.BoolValue(true)},
	})
	assertIndexedRowLookup(t, db, "users", "idx_users_name", []parser.Value{parser.StringValue("bob")}, [][]parser.Value{
		{parser.IntValue(2), parser.StringValue("bob"), parser.BoolValue(true)},
	})
}

func TestInsertMaintainsIndexAcrossRootSplitAndReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := table.IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil, defs=%#v", table.IndexDefs)
	}
	initialRootPageID := indexDef.RootPageID

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	usersTable := findCatalogTableByName(catalog, "users")
	if usersTable == nil || len(usersTable.Indexes) != 1 {
		t.Fatalf("catalog = %#v, want one table with one index", catalog)
	}

	leafPageIDs := make([]uint32, 0, 7)
	for i := 0; i < 7; i++ {
		page := pager.NewPage()
		leafPageIDs = append(leafPageIDs, uint32(page.ID()))
	}

	insertedValue := string(bytes.Repeat([]byte("z"), 512))
	insertedKey, err := storage.EncodeIndexKey(storageValuesFromParser([]parser.Value{parser.StringValue(insertedValue)}))
	if err != nil {
		t.Fatalf("EncodeIndexKey(insertedValue) error = %v", err)
	}

	separatorKeys := make([][]byte, 0, 6)
	for i := 0; i < 6; i++ {
		value := string(bytes.Repeat([]byte{byte('b' + i)}, 512))
		encodedKey, err := storage.EncodeIndexKey(storageValuesFromParser([]parser.Value{parser.StringValue(value)}))
		if err != nil {
			t.Fatalf("EncodeIndexKey(separator %d) error = %v", i, err)
		}
		separatorKeys = append(separatorKeys, encodedKey)
	}

	for i, pageID := range leafPageIDs {
		records := make([]storage.IndexLeafRecord, 0)
		if i == len(leafPageIDs)-1 {
			for j := 0; j < 7; j++ {
				records = append(records, storage.IndexLeafRecord{
					Key:     append([]byte(nil), insertedKey...),
					Locator: storage.RowLocator{PageID: uint32(table.RootPageID()), SlotID: uint16(j)},
				})
			}
		} else {
			records = append(records, storage.IndexLeafRecord{
				Key:     append([]byte(nil), separatorKeys[i]...),
				Locator: storage.RowLocator{PageID: uint32(table.RootPageID()), SlotID: uint16(i)},
			})
		}
		var rightSibling uint32
		if i+1 < len(leafPageIDs) {
			rightSibling = leafPageIDs[i+1]
		}
		pageData, err := storage.BuildIndexLeafPageData(pageID, records, rightSibling)
		if err != nil {
			t.Fatalf("BuildIndexLeafPageData(%d) error = %v", pageID, err)
		}
		page, err := pager.Get(storage.PageID(pageID))
		if err != nil {
			t.Fatalf("pager.Get(leaf %d) error = %v", pageID, err)
		}
		pager.MarkDirtyWithOriginal(page)
		clear(page.Data())
		copy(page.Data(), pageData)
	}

	rootPageData, err := storage.BuildIndexInternalPageData(indexDef.RootPageID, []storage.IndexInternalRecord{
		{Key: append([]byte(nil), separatorKeys[0]...), ChildPageID: leafPageIDs[0]},
		{Key: append([]byte(nil), separatorKeys[1]...), ChildPageID: leafPageIDs[1]},
		{Key: append([]byte(nil), separatorKeys[2]...), ChildPageID: leafPageIDs[2]},
		{Key: append([]byte(nil), separatorKeys[3]...), ChildPageID: leafPageIDs[3]},
		{Key: append([]byte(nil), separatorKeys[4]...), ChildPageID: leafPageIDs[4]},
		{Key: append([]byte(nil), separatorKeys[5]...), ChildPageID: leafPageIDs[5]},
		{Key: append([]byte(nil), separatorKeys[5]...), ChildPageID: leafPageIDs[6]},
	})
	if err != nil {
		t.Fatalf("BuildIndexInternalPageData(root) error = %v", err)
	}
	rootPage, err := pager.Get(storage.PageID(indexDef.RootPageID))
	if err != nil {
		t.Fatalf("pager.Get(root) error = %v", err)
	}
	pager.MarkDirtyWithOriginal(rootPage)
	clear(rootPage.Data())
	copy(rootPage.Data(), rootPageData)
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	walFrames := make([]storage.WALFrame, 0, len(leafPageIDs)+1)
	for i, pageID := range leafPageIDs {
		var pageData []byte
		var rightSibling uint32
		if i+1 < len(leafPageIDs) {
			rightSibling = leafPageIDs[i+1]
		}
		if i == len(leafPageIDs)-1 {
			records := make([]storage.IndexLeafRecord, 0, 7)
			for j := 0; j < 7; j++ {
				records = append(records, storage.IndexLeafRecord{
					Key:     append([]byte(nil), insertedKey...),
					Locator: storage.RowLocator{PageID: uint32(table.RootPageID()), SlotID: uint16(j)},
				})
			}
			pageData, err = storage.BuildIndexLeafPageData(pageID, records, rightSibling)
		} else {
			pageData, err = storage.BuildIndexLeafPageData(pageID, []storage.IndexLeafRecord{
				{Key: append([]byte(nil), separatorKeys[i]...), Locator: storage.RowLocator{PageID: uint32(table.RootPageID()), SlotID: uint16(i)}},
			}, rightSibling)
		}
		if err != nil {
			t.Fatalf("BuildIndexLeafPageData(%d for wal) error = %v", pageID, err)
		}
		walFrames = append(walFrames, stagedWALFrame(storage.PageID(pageID), pageData, uint64(1100+i)))
	}
	walFrames = append(walFrames, stagedWALFrame(storage.PageID(indexDef.RootPageID), rootPageData, 1200))
	if err := appendCommittedWALFramesForTest(path, walFrames...); err != nil {
		t.Fatalf("appendCommittedWALFramesForTest() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if _, err := db.Exec("INSERT INTO users VALUES (?)", insertedValue); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	table = db.tables["users"]
	indexDef = table.IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil after reopen insert, defs=%#v", table.IndexDefs)
	}
	if indexDef.RootPageID == initialRootPageID {
		t.Fatalf("RootPageID = %d, want changed after root split from %d", indexDef.RootPageID, initialRootPageID)
	}
	rootPageData, err = readCommittedPageData(db.pool, storage.PageID(indexDef.RootPageID))
	if err != nil {
		t.Fatalf("readCommittedPageData(root) error = %v", err)
	}
	if got := storage.PageType(binary.LittleEndian.Uint16(rootPageData[4:6])); got != storage.PageTypeIndexInternal {
		t.Fatalf("root page type = %d, want %d", got, storage.PageTypeIndexInternal)
	}
	rawDB, _ := openRawStorage(t, path)
	idMappings, err := storage.ReadDirectoryRootIDMappings(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}
	foundIDRootMapping := false
	for _, mapping := range idMappings {
		if mapping.ObjectType == storage.DirectoryRootMappingObjectIndex && mapping.ObjectID == indexDef.IndexID {
			foundIDRootMapping = true
			if mapping.RootPageID != indexDef.RootPageID {
				t.Fatalf("directory index ID root mapping = %d, want %d", mapping.RootPageID, indexDef.RootPageID)
			}
		}
	}
	if !foundIDRootMapping {
		t.Fatal("directory index ID root mapping not found after root split")
	}

	rows, err := db.Query("SELECT name FROM users WHERE name = ?", insertedValue)
	if err != nil {
		t.Fatalf("Query(index lookup after split) error = %v", err)
	}
	defer rows.Close()
	assertRowsStringSequence(t, rows, insertedValue)

	pageReader := func(pageID uint32) ([]byte, error) {
		return readCommittedPageData(db.pool, storage.PageID(pageID))
	}
	locators, err := storage.LookupIndexExact(pageReader, indexDef.RootPageID, insertedKey)
	if err != nil {
		t.Fatalf("LookupIndexExact() error = %v", err)
	}
	if len(locators) != 4 {
		t.Fatalf("len(locators) = %d, want 4 from the located rightmost duplicate leaf", len(locators))
	}
	foundInsertedRow := false
	for _, locator := range locators {
		row, err := db.fetchRowByLocator(table, locator)
		if err != nil {
			continue
		}
		if len(row) == 1 && row[0] == parser.StringValue(insertedValue) {
			foundInsertedRow = true
			break
		}
	}
	if !foundInsertedRow {
		t.Fatalf("locators = %#v, want one locator resolving to inserted row %q", locators, insertedValue)
	}
}

func TestSplitIndexLookupSurvivesReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, value := range []string{"alice", "zoe"} {
		if _, err := db.Exec("INSERT INTO users VALUES (?)", value); err != nil {
			t.Fatalf("Exec(insert %q) error = %v", value, err)
		}
	}

	indexDef := db.tables["users"].IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil, defs=%#v", db.tables["users"].IndexDefs)
	}
	dataPageIDs, err := committedTableDataPageIDs(db.pool, db.tables["users"])
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	if len(dataPageIDs) != 1 {
		t.Fatalf("len(committedTableDataPageIDs()) = %d, want 1", len(dataPageIDs))
	}
	tableDataPageID := uint32(dataPageIDs[0])

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	leftLeafPage := pager.NewPage()
	rightLeafPage := pager.NewPage()

	aliceKey, err := storage.EncodeIndexKey(storageValuesFromParser([]parser.Value{parser.StringValue("alice")}))
	if err != nil {
		t.Fatalf("EncodeIndexKey(alice) error = %v", err)
	}
	zoeKey, err := storage.EncodeIndexKey(storageValuesFromParser([]parser.Value{parser.StringValue("zoe")}))
	if err != nil {
		t.Fatalf("EncodeIndexKey(zoe) error = %v", err)
	}
	leftLeafData, err := storage.BuildIndexLeafPageData(uint32(leftLeafPage.ID()), []storage.IndexLeafRecord{
		{Key: aliceKey, Locator: storage.RowLocator{PageID: tableDataPageID, SlotID: 0}},
	}, uint32(rightLeafPage.ID()))
	if err != nil {
		t.Fatalf("BuildIndexLeafPageData(left) error = %v", err)
	}
	rightLeafData, err := storage.BuildIndexLeafPageData(uint32(rightLeafPage.ID()), []storage.IndexLeafRecord{
		{Key: zoeKey, Locator: storage.RowLocator{PageID: tableDataPageID, SlotID: 1}},
	}, 0)
	if err != nil {
		t.Fatalf("BuildIndexLeafPageData(right) error = %v", err)
	}
	rootPageData, err := storage.BuildIndexInternalPageData(indexDef.RootPageID, []storage.IndexInternalRecord{
		{Key: zoeKey, ChildPageID: uint32(leftLeafPage.ID())},
		{Key: zoeKey, ChildPageID: uint32(rightLeafPage.ID())},
	})
	if err != nil {
		t.Fatalf("BuildIndexInternalPageData(root) error = %v", err)
	}
	for _, staged := range []struct {
		id   storage.PageID
		data []byte
	}{
		{id: leftLeafPage.ID(), data: leftLeafData},
		{id: rightLeafPage.ID(), data: rightLeafData},
		{id: storage.PageID(indexDef.RootPageID), data: rootPageData},
	} {
		page, err := pager.Get(staged.id)
		if err != nil {
			t.Fatalf("pager.Get(%d) error = %v", staged.id, err)
		}
		pager.MarkDirtyWithOriginal(page)
		clear(page.Data())
		copy(page.Data(), staged.data)
	}
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := appendCommittedWALFramesForTest(path,
		stagedWALFrame(leftLeafPage.ID(), leftLeafData, 1000),
		stagedWALFrame(rightLeafPage.ID(), rightLeafData, 1001),
		stagedWALFrame(storage.PageID(indexDef.RootPageID), rootPageData, 1002),
	); err != nil {
		t.Fatalf("appendCommittedWALFramesForTest() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	table := db.tables["users"]
	indexDef = table.IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatalf("IndexDefinition(idx_users_name) = nil after reopen, defs=%#v", table.IndexDefs)
	}
	rootPageData, err = readCommittedPageData(db.pool, storage.PageID(indexDef.RootPageID))
	if err != nil {
		t.Fatalf("readCommittedPageData(root after reopen) error = %v", err)
	}
	if got := storage.PageType(binary.LittleEndian.Uint16(rootPageData[4:6])); got != storage.PageTypeIndexInternal {
		t.Fatalf("root page type after reopen = %d, want %d", got, storage.PageTypeIndexInternal)
	}

	pageReader := func(pageID uint32) ([]byte, error) {
		return readCommittedPageData(db.pool, storage.PageID(pageID))
	}
	for _, searchValue := range []string{"alice", "zoe"} {
		searchKey, err := storage.EncodeIndexKey(storageValuesFromParser([]parser.Value{parser.StringValue(searchValue)}))
		if err != nil {
			t.Fatalf("EncodeIndexKey(%q) error = %v", searchValue, err)
		}
		locators, err := storage.LookupIndexExact(pageReader, indexDef.RootPageID, searchKey)
		if err != nil {
			t.Fatalf("LookupIndexExact(%q) error = %v", searchValue, err)
		}
		if len(locators) != 1 {
			t.Fatalf("len(locators) for %q = %d, want 1", searchValue, len(locators))
		}
		row, err := db.fetchRowByLocator(table, locators[0])
		if err != nil {
			t.Fatalf("fetchRowByLocator(%q) error = %v", searchValue, err)
		}
		if len(row) != 1 || row[0] != parser.StringValue(searchValue) {
			t.Fatalf("row for %q = %#v, want [%#v]", searchValue, row, parser.StringValue(searchValue))
		}
	}
}

func TestOpenFailsWhenPersistedIndexRootHasWrongPageType(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := os.Remove(storage.WALPath(path)); err != nil {
		t.Fatalf("Remove(WALPath) error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	usersTable := findCatalogTableByName(catalog, "users")
	if usersTable == nil || len(usersTable.Indexes) == 0 {
		t.Fatalf("catalog = %#v, want users table with persisted index", catalog)
	}
	idMappings, err := storage.ReadDirectoryRootIDMappings(dbFile.File())
	if err != nil {
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	rootPageID := storage.PageID(0)
	for _, mapping := range idMappings {
		if mapping.ObjectType == storage.DirectoryRootMappingObjectIndex && mapping.ObjectID == usersTable.Indexes[0].IndexID {
			rootPageID = storage.PageID(mapping.RootPageID)
			break
		}
	}
	if rootPageID == 0 {
		t.Fatal("directory index ID root mapping not found")
	}
	rootPage, err := pager.Get(rootPageID)
	if err != nil {
		t.Fatalf("pager.Get(index root) error = %v", err)
	}
	pager.MarkDirtyWithOriginal(rootPage)
	wrongPage := storage.InitializeTablePage(uint32(rootPageID))
	clear(rootPage.Data())
	copy(rootPage.Data(), wrongPage)
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		db.Close()
		t.Fatal("Open() error = nil, want corrupted index page")
	}
	if err.Error() != "storage: corrupted index page" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: corrupted index page")
	}
}

func TestOpenRejectsPersistedOutOfRangeInt(t *testing.T) {
	path := testDBPath(t)

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	rootPage := pager.NewPage()
	storage.InitTableRootPage(rootPage)

	row := encodedOutOfRangeIntRow(t, 2147483648)
	if err := storage.AppendRowToTablePage(rootPage, row); err != nil {
		t.Fatalf("AppendRowToTablePage() error = %v", err)
	}
	if err := storage.SaveCatalog(pager, &storage.CatalogData{
		Tables: []storage.CatalogTable{
			{
				Name:       "users",
				TableID:    1,
				RootPageID: uint32(rootPage.ID()),
				RowCount:   1,
				Columns: []storage.CatalogColumn{
					{Name: "id", Type: storage.CatalogColumnTypeInt},
				},
			},
		},
	}); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	persistStrictPhysicalMetaForTests(t, dbFile.File(), pager, []strictTablePhysicalMetaForTest{{
		tableID:       1,
		rowRootPageID: rootPage.ID(),
	}})

	_, err := Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want corruption error")
	}
	if err.Error() != "storage: corrupted row data" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: corrupted row data")
	}
}

func TestOpenRejectsExactStorageRowCountMismatch(t *testing.T) {
	path := testDBPath(t)

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	rootPage := pager.NewPage()
	storage.InitTableRootPage(rootPage)
	row, err := storage.EncodeSlottedRow(storageValuesFromParser([]parser.Value{parser.IntValue(1)}), []uint8{storage.CatalogColumnTypeInt})
	if err != nil {
		t.Fatalf("EncodeRow() error = %v", err)
	}
	if err := storage.AppendRowToTablePage(rootPage, row); err != nil {
		t.Fatalf("AppendRowToTablePage() error = %v", err)
	}
	if err := storage.SaveCatalog(pager, &storage.CatalogData{
		Tables: []storage.CatalogTable{
			{
				Name:       "users",
				TableID:    1,
				RootPageID: uint32(rootPage.ID()),
				RowCount:   2,
				Columns: []storage.CatalogColumn{
					{Name: "id", Type: storage.CatalogColumnTypeInt},
				},
			},
		},
	}); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	persistStrictPhysicalMetaForTests(t, dbFile.File(), pager, []strictTablePhysicalMetaForTest{{
		tableID:       1,
		rowRootPageID: rootPage.ID(),
	}})

	_, err = Open(path)
	if err == nil {
		t.Fatal("Open() error = nil, want non-nil")
	}
	if err.Error() != "storage: row count mismatch" {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "storage: row count mismatch")
	}
}

func TestQueryRejectsIndexScanWhenRootPageIsNotAnIndexPage(t *testing.T) {
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
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := table.IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatalf("index setup failed: indexDef=%v", indexDef)
	}
	indexDef.RootPageID = uint32(table.RootPageID())

	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "storage: corrupted index page" {
		t.Fatalf("Rows.Err() = %v, want %q", rows.Err(), "storage: corrupted index page")
	}
}

func TestQueryRejectsIndexScanWhenLogicalIndexMetadataIsIncomplete(t *testing.T) {
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

	indexDef := db.tables["users"].IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatal("IndexDefinition(idx_users_name) = nil")
	}
	indexDef.IndexID = 0

	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: invalid select plan" {
		t.Fatalf("Rows.Err() = %v, want %q", rows.Err(), "execution: invalid select plan")
	}
}

func TestQueryRejectsInvalidTransactionState(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	page := db.pager.NewPage()
	db.pager.MarkDirty(page)

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil || rows.Err().Error() != "execution: invalid transaction state" {
		t.Fatalf("Rows.Err() = %v, want %q", rows.Err(), "execution: invalid transaction state")
	}
}

func TestLifecycleWriteCloseReopenQuery(t *testing.T) {
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

	assertSelectRowsWithNames(t, db, "SELECT id, name FROM users", [][2]any{
		{int(1), "alice"},
		{int(2), "bob"},
	})
}

func TestLifecycleUpdateCloseReopenQuery(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"UPDATE users SET name = 'bobby' WHERE id = 2",
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

	assertSelectRowsWithNames(t, db, "SELECT id, name FROM users", [][2]any{
		{int(1), "alice"},
		{int(2), "bobby"},
	})
}

func TestLifecycleDeleteCloseReopenQuery(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
		"DELETE FROM users WHERE id = 2",
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

	assertSelectRowsWithNames(t, db, "SELECT id, name FROM users", [][2]any{
		{int(1), "alice"},
		{int(3), "cara"},
	})
}

func TestLifecycleMultipleWritesAcrossReopenBoundaries(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	db = reopenDB(t, path)
	if _, err := db.Exec("UPDATE users SET name = 'bobby' WHERE id = 2"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	db = reopenDB(t, path)
	if _, err := db.Exec("DELETE FROM users WHERE id = 1"); err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("third Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertSelectRowsWithNames(t, db, "SELECT id, name FROM users", [][2]any{
		{int(2), "bobby"},
	})
}

func TestLifecycleIndexedQueryAfterReopenRemainsCorrect(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	assertQueryIntRows(t, db, "SELECT id FROM users WHERE name = 'alice' ORDER BY id", 1, 3)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertQueryIntRows(t, db, "SELECT id FROM users WHERE name = 'alice' ORDER BY id", 1, 3)
}

func TestLifecycleBoundWritesCloseReopenRemainCorrect(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, active BOOL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", int32(1), "alice", true); err != nil {
		t.Fatalf("Exec(insert 1) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", int32(2), "bob", false); err != nil {
		t.Fatalf("Exec(insert 2) error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET name = ?, active = ? WHERE id = ?", "bobby", true, int32(2)); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertSelectRowsWithNames(t, db, "SELECT id, name FROM users WHERE active = TRUE ORDER BY id", [][2]any{
		{int(1), "alice"},
		{int(2), "bobby"},
	})
}

func TestOpenCreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	_ = os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("os.Stat(%q) error = %v", path, err)
	}
	if _, err := os.Stat(storage.WALPath(path)); err != nil {
		t.Fatalf("os.Stat(%q) error = %v", storage.WALPath(path), err)
	}

	rawDB, pager := openRawStorage(t, path)
	defer rawDB.Close()

	page, err := pager.Get(storage.DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.Get(directory) error = %v", err)
	}
	if err := storage.ValidateDirectoryPage(page.Data()); err != nil {
		t.Fatalf("ValidateDirectoryPage() error = %v", err)
	}
	mode, err := storage.DirectoryCATDIRStorageMode(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIRStorageMode() error = %v", err)
	}
	if mode != storage.DirectoryCATDIRStorageModeEmbedded {
		t.Fatalf("DirectoryCATDIRStorageMode() = %d, want %d", mode, storage.DirectoryCATDIRStorageModeEmbedded)
	}
	overflowHead, err := storage.DirectoryCATDIROverflowHeadPageID(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID() error = %v", err)
	}
	if overflowHead != 0 {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID() = %d, want 0", overflowHead)
	}
	overflowCount, err := storage.DirectoryCATDIROverflowPageCount(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowPageCount() error = %v", err)
	}
	if overflowCount != 0 {
		t.Fatalf("DirectoryCATDIROverflowPageCount() = %d, want 0", overflowCount)
	}
}

func TestOpenExistingValidFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db.Close()
}

func TestOpenExistingValidFileWithValidWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db.Close()
}

func TestOpenRevalidatesDirectoryPageOnReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	page, err := pager.Get(storage.DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.Get(directory) error = %v", err)
	}
	if err := storage.ValidateDirectoryPage(page.Data()); err != nil {
		t.Fatalf("ValidateDirectoryPage() error = %v", err)
	}
	payloadBytes, err := storage.DirectoryCATDIRPayloadByteLength(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIRPayloadByteLength() error = %v", err)
	}
	payload, err := storage.LoadCatalog(storage.PageReaderFunc(func(pageID storage.PageID) ([]byte, error) {
		return page.Data(), nil
	}))
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	if payload.Version != 0 && payloadBytes == 0 {
		t.Fatalf("DirectoryCATDIRPayloadByteLength() = %d, want nonzero for non-empty wrapped payload", payloadBytes)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db.Close()
}

func TestOpenLoadsCatalogFromCATDIROverflowMode(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'ada')"); err != nil {
		t.Fatalf("Exec(insert users) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	page, err := storage.ReadDirectoryPage(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryPage() error = %v", err)
	}
	payloadBytes, err := storage.DirectoryCATDIRPayloadByteLength(page)
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("DirectoryCATDIRPayloadByteLength() error = %v", err)
	}
	payload := append([]byte(nil), page[testDirectoryCatalogOffset:testDirectoryCatalogOffset+int(payloadBytes)]...)
	mappings, err := storage.ReadDirectoryRootIDMappings(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	freeListHead, err := storage.ReadDirectoryFreeListHead(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	overflowSlot := pager.NewPage()
	overflowPages, err := storage.BuildCatalogOverflowPageChain(payload, []storage.PageID{overflowSlot.ID()})
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("BuildCatalogOverflowPageChain() error = %v", err)
	}
	clear(overflowSlot.Data())
	copy(overflowSlot.Data(), overflowPages[0].Data)
	pager.MarkDirty(overflowSlot)
	writeOverflowCatalogPageWithIDMappings(t, pager, payloadBytes, overflowPages[0].PageID, uint32(len(overflowPages)), freeListHead, mappings)
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("rows.Next() = false, want one row")
	}
	var id int32
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if id != 1 || name != "ada" {
		t.Fatalf("row = (%d, %q), want (1, %q)", id, name, "ada")
	}
	if rows.Next() {
		t.Fatal("rows.Next() = true after first row, want one row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() error = %v", err)
	}
}

func TestPersistCatalogStatePromotesCATDIRToOverflowWhenNeeded(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	stagedTables := cloneTables(db.tables)
	stagedPages := make([]stagedPage, 0, 64)
	nextTableID := uint32(1000)
	nextRootPageID := db.pager.NextPageID()
	for i := 0; i < 48; i++ {
		rootPageID := nextRootPageID
		nextRootPageID++
		tableName := fmt.Sprintf("table_%03d_%s", i, strings.Repeat("x", 48))
		columns := []parser.ColumnDef{
			{Name: fmt.Sprintf("id_%02d_%s", i, strings.Repeat("a", 20)), Type: parser.ColumnTypeInt},
			{Name: fmt.Sprintf("name_%02d_%s", i, strings.Repeat("b", 20)), Type: parser.ColumnTypeText},
			{Name: fmt.Sprintf("city_%02d_%s", i, strings.Repeat("c", 20)), Type: parser.ColumnTypeText},
			{Name: fmt.Sprintf("state_%02d_%s", i, strings.Repeat("d", 20)), Type: parser.ColumnTypeText},
			{Name: fmt.Sprintf("zip_%02d_%s", i, strings.Repeat("e", 20)), Type: parser.ColumnTypeText},
		}
		table := &executor.Table{
			Name:    tableName,
			TableID: nextTableID,
			Columns: columns,
		}
		table.SetStorageMeta(rootPageID, 0)
		stagedTables[tableName] = table
		stagedPages = append(stagedPages, stagedPage{
			id:    rootPageID,
			data:  storage.InitializeTablePage(uint32(rootPageID)),
			isNew: true,
		})
		nextTableID++
	}
	if err := db.persistCatalogState(stagedTables, stagedPages); err != nil {
		t.Fatalf("persistCatalogState() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	defer rawDB.Close()
	page, err := pager.Get(storage.DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.Get(directory) error = %v", err)
	}
	mode, err := storage.DirectoryCATDIRStorageMode(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIRStorageMode() error = %v", err)
	}
	if mode != storage.DirectoryCATDIRStorageModeOverflow {
		t.Fatalf("DirectoryCATDIRStorageMode() = %d, want %d", mode, storage.DirectoryCATDIRStorageModeOverflow)
	}
	head, err := storage.DirectoryCATDIROverflowHeadPageID(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID() error = %v", err)
	}
	if head == 0 {
		t.Fatal("DirectoryCATDIROverflowHeadPageID() = 0, want nonzero")
	}
	count, err := storage.DirectoryCATDIROverflowPageCount(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowPageCount() error = %v", err)
	}
	if count == 0 {
		t.Fatal("DirectoryCATDIROverflowPageCount() = 0, want > 0")
	}

	got, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	if len(got.Tables) != len(stagedTables) {
		t.Fatalf("len(LoadCatalog().Tables) = %d, want %d", len(got.Tables), len(stagedTables))
	}
}

func TestPersistCatalogStateDemotesCATDIRBackToEmbeddedWhenPayloadFits(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	largeTables := cloneTables(db.tables)
	largePages := make([]stagedPage, 0, 64)
	nextTableID := uint32(1000)
	nextRootPageID := db.pager.NextPageID()
	for i := 0; i < 48; i++ {
		rootPageID := nextRootPageID
		nextRootPageID++
		tableName := fmt.Sprintf("table_%03d_%s", i, strings.Repeat("x", 48))
		table := &executor.Table{
			Name:    tableName,
			TableID: nextTableID,
			Columns: []parser.ColumnDef{
				{Name: fmt.Sprintf("id_%02d_%s", i, strings.Repeat("a", 20)), Type: parser.ColumnTypeInt},
				{Name: fmt.Sprintf("name_%02d_%s", i, strings.Repeat("b", 20)), Type: parser.ColumnTypeText},
				{Name: fmt.Sprintf("city_%02d_%s", i, strings.Repeat("c", 20)), Type: parser.ColumnTypeText},
				{Name: fmt.Sprintf("state_%02d_%s", i, strings.Repeat("d", 20)), Type: parser.ColumnTypeText},
				{Name: fmt.Sprintf("zip_%02d_%s", i, strings.Repeat("e", 20)), Type: parser.ColumnTypeText},
			},
		}
		table.SetStorageMeta(rootPageID, 0)
		largeTables[tableName] = table
		largePages = append(largePages, stagedPage{
			id:    rootPageID,
			data:  storage.InitializeTablePage(uint32(rootPageID)),
			isNew: true,
		})
		nextTableID++
	}
	if err := db.persistCatalogState(largeTables, largePages); err != nil {
		t.Fatalf("persistCatalogState(large) error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	page, err := pager.Get(storage.DirectoryControlPageID)
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("pager.Get(directory large) error = %v", err)
	}
	mode, err := storage.DirectoryCATDIRStorageMode(page.Data())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("DirectoryCATDIRStorageMode(large) error = %v", err)
	}
	if mode != storage.DirectoryCATDIRStorageModeOverflow {
		_ = rawDB.Close()
		t.Fatalf("DirectoryCATDIRStorageMode(large) = %d, want %d", mode, storage.DirectoryCATDIRStorageModeOverflow)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	smallTables := cloneTables(db.tables)
	if err := db.persistCatalogState(smallTables, nil); err != nil {
		t.Fatalf("persistCatalogState(small) error = %v", err)
	}

	rawDB, pager = openRawStorage(t, path)
	defer rawDB.Close()
	page, err = pager.Get(storage.DirectoryControlPageID)
	if err != nil {
		t.Fatalf("pager.Get(directory small) error = %v", err)
	}
	mode, err = storage.DirectoryCATDIRStorageMode(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIRStorageMode(small) error = %v", err)
	}
	if mode != storage.DirectoryCATDIRStorageModeEmbedded {
		t.Fatalf("DirectoryCATDIRStorageMode(small) = %d, want %d", mode, storage.DirectoryCATDIRStorageModeEmbedded)
	}
	head, err := storage.DirectoryCATDIROverflowHeadPageID(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID(small) error = %v", err)
	}
	if head != 0 {
		t.Fatalf("DirectoryCATDIROverflowHeadPageID(small) = %d, want 0", head)
	}
	count, err := storage.DirectoryCATDIROverflowPageCount(page.Data())
	if err != nil {
		t.Fatalf("DirectoryCATDIROverflowPageCount(small) error = %v", err)
	}
	if count != 0 {
		t.Fatalf("DirectoryCATDIROverflowPageCount(small) = %d, want 0", count)
	}
	freeListHead, err := storage.DirectoryFreeListHead(page.Data())
	if err != nil {
		t.Fatalf("DirectoryFreeListHead(small) error = %v", err)
	}
	if freeListHead == 0 {
		t.Fatal("DirectoryFreeListHead(small) = 0, want reclaimed overflow pages on free list")
	}

	got, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() after demotion error = %v", err)
	}
	if len(got.Tables) != len(smallTables) {
		t.Fatalf("len(LoadCatalog().Tables) after demotion = %d, want %d", len(got.Tables), len(smallTables))
	}
}

func TestOpenRejectsMalformedCATDIROverflowModeWithZeroHead(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	page, err := storage.ReadDirectoryPage(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryPage() error = %v", err)
	}
	binary.LittleEndian.PutUint32(page[testDirectoryCATDIRModeOffset:testDirectoryCATDIRModeOffset+4], storage.DirectoryCATDIRStorageModeOverflow)
	binary.LittleEndian.PutUint32(page[testDirectoryCATDIROverflowHeadOff:testDirectoryCATDIROverflowHeadOff+4], 0)
	binary.LittleEndian.PutUint32(page[testDirectoryCATDIROverflowCountOff:testDirectoryCATDIROverflowCountOff+4], 1)
	binary.LittleEndian.PutUint32(page[testDirectoryCATDIRPayloadBytesOff:testDirectoryCATDIRPayloadBytesOff+4], 9)
	if err := storage.RecomputePageChecksum(page); err != nil {
		_ = rawDB.Close()
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	if _, err := rawDB.File().WriteAt(page, int64(storage.HeaderSize)); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteAt() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want malformed CAT/DIR overflow state failure")
	}
}

func TestOpenRejectsMalformedCATDIROverflowChainPageType(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	page, err := storage.ReadDirectoryPage(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryPage() error = %v", err)
	}
	payloadBytes, err := storage.DirectoryCATDIRPayloadByteLength(page)
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("DirectoryCATDIRPayloadByteLength() error = %v", err)
	}
	mappings, err := storage.ReadDirectoryRootIDMappings(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	freeListHead, err := storage.ReadDirectoryFreeListHead(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	overflowPage := pager.NewPage()
	clear(overflowPage.Data())
	copy(overflowPage.Data(), storage.InitializeTablePage(uint32(overflowPage.ID())))
	pager.MarkDirty(overflowPage)
	writeOverflowCatalogPageWithIDMappings(t, pager, payloadBytes, overflowPage.ID(), 1, freeListHead, mappings)
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want malformed CAT/DIR overflow chain failure")
	}
}

func TestOpenWithHeaderOnlyWALSucceeds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := storage.ResetWALFile(path, storage.DBFormatVersion()); err != nil {
		t.Fatalf("ResetWALFile() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db.Close()
}

func TestOpenInvalidHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.db")
	if err := os.WriteFile(path, []byte("not-a-rovadb-file"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	db, err := Open(path)
	if err == nil {
		db.Close()
		t.Fatal("Open() error = nil, want non-nil")
	}
}

func TestOpenFailsOnUnsupportedDBHeaderVersion(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	var header [storage.HeaderSize]byte
	if _, err := file.ReadAt(header[:], 0); err != nil {
		_ = file.Close()
		t.Fatalf("file.ReadAt() error = %v", err)
	}
	binary.LittleEndian.PutUint32(header[8:12], storage.CurrentDBFormatVersion+1)
	if _, err := file.WriteAt(header[:], 0); err != nil {
		_ = file.Close()
		t.Fatalf("file.WriteAt() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want unsupported DB header version failure")
	}
}

func TestRecoveryOnOpenRestoresLastCommittedState(t *testing.T) {
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
	db.afterDatabaseSyncHook = func() error {
		return errors.New("boom after db sync")
	}
	if _, err := db.Exec("UPDATE t SET id = 2 WHERE id = 1"); err == nil {
		t.Fatal("Exec(update) error = nil, want failure")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open(recover) error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 2)
	if _, err := os.Stat(storage.JournalPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat error = %v, want not exists", err)
	}
}

func TestOpenWithoutJournalSkipsRecovery(t *testing.T) {
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
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestOpenFailsOnMalformedJournal(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := os.WriteFile(storage.JournalPath(path), []byte("bad-journal"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		db.Close()
		t.Fatal("Open() error = nil, want malformed journal error")
	}
}

func TestOpenFailsOnMalformedWAL(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := os.WriteFile(storage.WALPath(path), []byte("bad-wal"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		db.Close()
		t.Fatal("Open() error = nil, want malformed wal error")
	}
}

func TestOpenFailsOnUnsupportedDirectoryFormatVersion(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	page, err := storage.ReadDirectoryPage(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryPage() error = %v", err)
	}
	binary.LittleEndian.PutUint32(page[32:36], storage.CurrentDBFormatVersion+1)
	if _, err := rawDB.File().WriteAt(page, storage.HeaderSize); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteAt() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want unsupported directory format failure")
	}
}

func TestOpenFailsOnWALDBFormatVersionMismatch(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	walFile, err := os.OpenFile(storage.WALPath(path), os.O_RDWR|os.O_TRUNC, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	if err := storage.WriteWALHeader(walFile, storage.WALHeader{
		Magic:           [8]byte{'R', 'O', 'V', 'A', 'W', 'A', 'L', '1'},
		WALVersion:      storage.CurrentWALVersion,
		DBFormatVersion: storage.CurrentDBFormatVersion + 1,
		PageSize:        storage.PageSize,
	}); err != nil {
		_ = walFile.Close()
		t.Fatalf("WriteWALHeader() error = %v", err)
	}
	if err := walFile.Close(); err != nil {
		t.Fatalf("walFile.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want WAL/DB format mismatch failure")
	}
}

func TestOpenFailsOnMalformedDirectoryPage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	corrupted := make([]byte, storage.PageSize)
	copy(corrupted, []byte("bad-directory"))
	if _, err := file.WriteAt(corrupted, storage.HeaderSize); err != nil {
		_ = file.Close()
		t.Fatalf("file.WriteAt() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want malformed directory page error")
	}
}

func TestOpenFailsOnLegacyNameBasedDirectoryMappings(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := injectLegacyNameMappingsForOpenTest(rawDB.File(), []byte{storage.DirectoryRootMappingObjectTable, 5, 0, 'u', 's', 'e', 'r', 's', 0, 0, 2, 0, 0, 0}); err != nil {
		_ = rawDB.Close()
		t.Fatalf("injectLegacyNameMappingsForOpenTest() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("reopen Open() error = nil, want legacy directory mapping rejection")
	}
}

func TestDirectoryRootIDMappingsPersistAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	tableID := db.tables["users"].TableID
	tableRoot := uint32(db.tables["users"].RootPageID())
	indexDef := db.tables["users"].IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatal("IndexDefinition(idx_users_name) = nil")
	}
	indexID := indexDef.IndexID
	indexRoot := indexDef.RootPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	idMappings, err := storage.ReadDirectoryRootIDMappings(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}
	foundTable := false
	foundIndex := false
	for _, mapping := range idMappings {
		switch {
		case mapping.ObjectType == storage.DirectoryRootMappingObjectTable && mapping.ObjectID == tableID:
			foundTable = true
			if mapping.RootPageID != tableRoot {
				t.Fatalf("table ID root mapping = %d, want %d", mapping.RootPageID, tableRoot)
			}
		case mapping.ObjectType == storage.DirectoryRootMappingObjectIndex && mapping.ObjectID == indexID:
			foundIndex = true
			if mapping.RootPageID != indexRoot {
				t.Fatalf("index ID root mapping = %d, want %d", mapping.RootPageID, indexRoot)
			}
		}
	}
	if !foundTable {
		t.Fatal("table ID root mapping not found")
	}
	if !foundIndex {
		t.Fatal("index ID root mapping not found")
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()
	if got := db.tables["users"].RootPageID(); uint32(got) != tableRoot {
		t.Fatalf("reopened users.RootPageID() = %d, want %d", got, tableRoot)
	}
	if got := db.tables["users"].IndexDefinition("idx_users_name").RootPageID; got != indexRoot {
		t.Fatalf("reopened index RootPageID = %d, want %d", got, indexRoot)
	}
}

func TestDirectoryWritePathWritesIDMappingsOnly(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	teamsTableID := db.tables["teams"].TableID
	teamsRoot := uint32(db.tables["teams"].RootPageID())
	usersTableID := db.tables["users"].TableID
	usersRoot := uint32(db.tables["users"].RootPageID())
	indexDef := db.tables["users"].IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatal("IndexDefinition(idx_users_name) = nil")
	}
	indexID := indexDef.IndexID
	indexRoot := indexDef.RootPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	idMappings, err := storage.ReadDirectoryRootIDMappings(rawDB.File())
	if err != nil {
		_ = rawDB.Close()
		t.Fatalf("ReadDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	if catalog.Version != 9 {
		t.Fatalf("catalog.Version = %d, want 9", catalog.Version)
	}
	for _, table := range catalog.Tables {
		if table.RootPageID != 0 {
			t.Fatalf("catalog table %q RootPageID = %d, want 0 on new writes", table.Name, table.RootPageID)
		}
		for _, index := range table.Indexes {
			if index.RootPageID != 0 {
				t.Fatalf("catalog index %q RootPageID = %d, want 0 on new writes", index.Name, index.RootPageID)
			}
		}
	}
	wantIDMappings := map[storage.DirectoryRootIDMapping]struct{}{
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: teamsTableID, RootPageID: teamsRoot}: {},
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: usersTableID, RootPageID: usersRoot}: {},
		{ObjectType: storage.DirectoryRootMappingObjectIndex, ObjectID: indexID, RootPageID: indexRoot}:      {},
	}
	for want := range wantIDMappings {
		found := false
		for _, got := range idMappings {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("directory ID mapping %#v not found in %#v", want, idMappings)
		}
	}
}

func TestOpenFailsWhenLegacyNameMappingsPresentAlongsideIDMappings(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := injectLegacyNameMappingsForOpenTest(rawDB.File(), []byte{
		storage.DirectoryRootMappingObjectTable, 5, 0, 'u', 's', 'e', 'r', 's', 0, 0, 2, 0, 0, 0,
	}); err != nil {
		_ = rawDB.Close()
		t.Fatalf("injectLegacyNameMappingsForOpenTest() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want legacy directory mapping rejection")
	}
}

func TestOpenFailsWhenNewCatalogPayloadHasNoDirectoryRoots(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := storage.WriteDirectoryRootIDMappings(rawDB.File(), nil); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want missing directory roots failure")
	}
}

func TestOpenFailsWithoutDirectoryRootsEvenIfCurrentCatalogSnapshotCarriesRoots(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	catalog = catalogWithDirectoryRootsForSave(t, rawDB.File(), catalog)
	if err := storage.SaveCatalog(pager, catalog); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := storage.WriteDirectoryRootIDMappings(rawDB.File(), nil); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want missing directory roots failure without catalog fallback")
	}
}

func TestOpenRejectsLegacyCatalogPayloadWithoutDirectoryMappings(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	users := db.tables["users"]
	if users == nil {
		t.Fatal("db.tables[users] = nil")
	}
	indexDef := users.IndexDefinition("idx_users_name")
	if indexDef == nil {
		t.Fatal("IndexDefinition(idx_users_name) = nil")
	}
	tableRoot := uint32(users.RootPageID())
	indexRoot := indexDef.RootPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	legacyPage := buildLegacyV5DirectoryPageForOpenTest(&storage.CatalogData{
		Tables: []storage.CatalogTable{
			{
				Name:       "users",
				TableID:    users.TableID,
				RootPageID: tableRoot,
				RowCount:   0,
				Columns: []storage.CatalogColumn{
					{Name: "name", Type: storage.CatalogColumnTypeText},
				},
				Indexes: []storage.CatalogIndex{
					{
						Name:       "idx_users_name",
						Unique:     false,
						IndexID:    indexDef.IndexID,
						RootPageID: indexRoot,
						Columns:    []storage.CatalogIndexColumn{{Name: "name"}},
					},
				},
			},
		},
	})
	if _, err := rawDB.File().WriteAt(legacyPage, int64(storage.HeaderSize)); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteAt() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("reopen Open() error = nil, want legacy catalog payload rejection")
	}
}

func TestOpenFailsWhenFreeListHeadPointsToNonFreePage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), 1); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want invalid free-list head failure")
	}
}

func TestOpenFailsWhenTableRootMappingPointsToIndexPage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	tableID := db.tables["users"].TableID
	indexRoot := db.tables["users"].IndexDefinition("idx_users_name").RootPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := storage.WriteDirectoryRootIDMappings(rawDB.File(), []storage.DirectoryRootIDMapping{{
		ObjectType: storage.DirectoryRootMappingObjectTable,
		ObjectID:   tableID,
		RootPageID: indexRoot,
	}}); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want invalid table root mapping failure")
	}
}

func TestOpenFailsWhenIndexRootMappingPointsToTablePage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	tableRoot := uint32(db.tables["users"].RootPageID())
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	tableID := db.tables["users"].TableID
	indexDef := db.tables["users"].IndexDefinition("idx_users_name")
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ := openRawStorage(t, path)
	if err := storage.WriteDirectoryRootIDMappings(rawDB.File(), []storage.DirectoryRootIDMapping{
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: tableID, RootPageID: tableRoot},
		{ObjectType: storage.DirectoryRootMappingObjectIndex, ObjectID: indexDef.IndexID, RootPageID: tableRoot},
	}); err != nil {
		_ = rawDB.Close()
		t.Fatalf("WriteDirectoryRootIDMappings() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want invalid index root mapping failure")
	}
}

func TestOpenLoadsDurableFreeListHead(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	freeHead := appendFreePageForTest(t, pager, 0)
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), uint32(freeHead)); err != nil {
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer db.Close()

	if db.freeListHead != uint32(freeHead) {
		t.Fatalf("db.freeListHead = %d, want %d", db.freeListHead, freeHead)
	}
}

func TestCreateTableReusesDurableFreeListHeadAndAdvancesIt(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatalf("Exec(create t1) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	secondFreePageID := appendFreePageForTest(t, pager, 0)
	firstFreePageID := appendFreePageForTest(t, pager, secondFreePageID)
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), uint32(firstFreePageID)); err != nil {
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t2 (id INT)"); err != nil {
		t.Fatalf("Exec(create t2) error = %v", err)
	}
	if got := db.tables["t2"].RootPageID(); got != firstFreePageID {
		t.Fatalf("t2 rootPageID = %d, want %d", got, firstFreePageID)
	}
	if db.freeListHead == 0 || db.freeListHead == uint32(firstFreePageID) {
		t.Fatalf("db.freeListHead = %d, want advanced nonzero head after reuse of %d", db.freeListHead, firstFreePageID)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, _ = openRawStorage(t, path)
	defer rawDB.Close()
	head, err := storage.ReadDirectoryFreeListHead(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	if head == 0 || head == uint32(firstFreePageID) {
		t.Fatalf("ReadDirectoryFreeListHead() = %d, want advanced nonzero head after reuse of %d", head, firstFreePageID)
	}
}

func TestCreateTableFallsBackToFreshAllocationWhenFreeListEmpty(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatalf("Exec(create t1) error = %v", err)
	}
	t1Root := db.tables["t1"].RootPageID()
	if _, err := db.Exec("CREATE TABLE t2 (id INT)"); err != nil {
		t.Fatalf("Exec(create t2) error = %v", err)
	}
	defer db.Close()

	if got := db.tables["t2"].RootPageID(); got <= t1Root {
		t.Fatalf("t2 rootPageID = %d, want a fresh page id after %d", got, t1Root)
	}
}

func TestReopenPreservesFreeListHeadForSubsequentAllocation(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatalf("Exec(create t1) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	secondFreePageID := appendFreePageForTest(t, pager, 0)
	firstFreePageID := appendFreePageForTest(t, pager, secondFreePageID)
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), uint32(firstFreePageID)); err != nil {
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t2 (id INT)"); err != nil {
		t.Fatalf("Exec(create t2) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second reopen Open() error = %v", err)
	}
	defer db.Close()
	if db.freeListHead == 0 {
		t.Fatal("db.freeListHead = 0, want remaining free-list pages after reopen")
	}
	headBeforeCreate := db.freeListHead
	if _, err := db.Exec("CREATE TABLE t3 (id INT)"); err != nil {
		t.Fatalf("Exec(create t3) error = %v", err)
	}
	if got := db.tables["t3"].RootPageID(); got != storage.PageID(headBeforeCreate) {
		t.Fatalf("t3 rootPageID = %d, want %d", got, headBeforeCreate)
	}
}

func TestCreateTableFailsOnMalformedFreePageLinkage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatalf("Exec(create t1) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	badFreePageID := appendFreePageForTest(t, pager, 0)
	page, err := pager.Get(badFreePageID)
	if err != nil {
		t.Fatalf("pager.Get(%d) error = %v", badFreePageID, err)
	}
	clear(page.Data())
	copy(page.Data(), []byte("not-a-free-page"))
	pager.MarkDirty(page)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := storage.WriteDirectoryFreeListHead(rawDB.File(), uint32(badFreePageID)); err != nil {
		t.Fatalf("WriteDirectoryFreeListHead() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		defer db.Close()
		t.Fatal("reopen Open() error = nil, want malformed free page failure")
	}
	if err.Error() != "storage: corrupted page header" {
		t.Fatalf("reopen Open() error = %v, want %q", err, "storage: corrupted page header")
	}
}

func TestOpenFailsOnJournalPageSizeMismatch(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	journalFile, err := storage.CreateJournalFile(storage.JournalPath(path), 1234, 0)
	if err != nil {
		t.Fatalf("CreateJournalFile() error = %v", err)
	}
	if err := journalFile.Close(); err != nil {
		t.Fatalf("journalFile.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		db.Close()
		t.Fatal("Open() error = nil, want journal page size mismatch")
	}
}

func TestRecoveryRunsBeforeCatalogLoad(t *testing.T) {
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
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dbFile, pager := openRawStorage(t, path)
	catalogPage, err := pager.Get(0)
	if err != nil {
		t.Fatalf("pager.Get(0) error = %v", err)
	}
	originalCatalog := append([]byte(nil), catalogPage.Data()...)
	clear(catalogPage.Data())
	copy(catalogPage.Data(), []byte("corrupt-catalog"))
	pager.MarkDirty(catalogPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}

	journalFile, err := storage.CreateJournalFile(storage.JournalPath(path), storage.PageSize, 1)
	if err != nil {
		t.Fatalf("CreateJournalFile() error = %v", err)
	}
	if err := storage.WriteJournalEntry(journalFile, 0, originalCatalog); err != nil {
		t.Fatalf("WriteJournalEntry() error = %v", err)
	}
	if err := journalFile.Sync(); err != nil {
		t.Fatalf("journalFile.Sync() error = %v", err)
	}
	if err := journalFile.Close(); err != nil {
		t.Fatalf("journalFile.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open(recover) error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestOpenReplaysCommittedWALState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	db.afterDatabaseSyncHook = func() error {
		return errors.New("checkpoint failed after WAL durability")
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err == nil {
		t.Fatal("Exec(insert) error = nil, want checkpoint failure")
	}
	db.afterDatabaseSyncHook = nil
	if records, err := storage.ReadWALRecords(path); err != nil {
		t.Fatalf("ReadWALRecords() error = %v", err)
	} else if len(records) == 0 {
		t.Fatal("len(ReadWALRecords()) = 0, want committed WAL after checkpoint failure")
	}
	dataPageIDs, err := committedTableDataPageIDs(db.pool, db.tables["t"])
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	if len(dataPageIDs) != 1 {
		t.Fatalf("len(committedTableDataPageIDs()) = %d, want 1", len(dataPageIDs))
	}
	dataPageID := dataPageIDs[0]
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(dataPageID)
	if err != nil {
		t.Fatalf("pager.Get(data) error = %v", err)
	}
	corrupted := storage.InitializeTablePage(uint32(dataPageID))
	if err := storage.RecomputePageChecksum(corrupted); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	clear(rootPage.Data())
	copy(rootPage.Data(), corrupted)
	pager.MarkDirty(rootPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestOpenIgnoresTrailingUncommittedWALFrames(t *testing.T) {
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
	dataPageIDs, err := committedTableDataPageIDs(db.pool, db.tables["t"])
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	if len(dataPageIDs) != 1 {
		t.Fatalf("len(committedTableDataPageIDs()) = %d, want 1", len(dataPageIDs))
	}
	dataPageID := dataPageIDs[0]
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(dataPageID)
	if err != nil {
		t.Fatalf("pager.Get(data) error = %v", err)
	}
	uncommitted := append([]byte(nil), rootPage.Data()...)
	row, err := storage.EncodeSlottedRow(storageValuesFromParser([]parser.Value{parser.IntValue(2)}), []uint8{storage.CatalogColumnTypeInt})
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}
	slotCount, err := storage.TablePageSlotCount(uncommitted)
	if err != nil {
		t.Fatalf("TablePageSlotCount() error = %v", err)
	}
	if slotCount != 1 {
		t.Fatalf("TablePageSlotCount() = %d, want 1", slotCount)
	}
	if _, err := storage.InsertRowIntoTablePage(uncommitted, row); err != nil {
		t.Fatalf("InsertRowIntoTablePage() error = %v", err)
	}
	if err := storage.SetPageLSN(uncommitted, 999); err != nil {
		t.Fatalf("SetPageLSN() error = %v", err)
	}
	if err := storage.RecomputePageChecksum(uncommitted); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	var frame storage.WALFrame
	frame.FrameLSN = 999
	frame.PageID = uint32(dataPageID)
	frame.PageLSN = 999
	copy(frame.PageData[:], uncommitted)
	if err := storage.AppendWALFrame(path, frame); err != nil {
		t.Fatalf("AppendWALFrame() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestOpenReplayIsIdempotentAcrossRepeatedOpens(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	db.afterDatabaseSyncHook = func() error {
		return errors.New("checkpoint failed after WAL durability")
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err == nil {
		t.Fatal("Exec(insert) error = nil, want checkpoint failure")
	}
	db.afterDatabaseSyncHook = nil
	rootPageID := db.tables["t"].RootPageID()
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(rootPageID)
	if err != nil {
		t.Fatalf("pager.Get(root) error = %v", err)
	}
	corrupted := storage.InitializeTablePage(uint32(rootPageID))
	if err := storage.RecomputePageChecksum(corrupted); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	clear(rootPage.Data())
	copy(rootPage.Data(), corrupted)
	pager.MarkDirty(rootPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("first replay Open() error = %v", err)
	}
	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
	if err := db.Close(); err != nil {
		t.Fatalf("first replay Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second replay Open() error = %v", err)
	}
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestOpenReplaysMultipleCommittedWALTransactions(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(first insert) error = %v", err)
	}
	db.afterDatabaseSyncHook = func() error {
		return errors.New("checkpoint failed after WAL durability")
	}
	if _, err := db.Exec("INSERT INTO t VALUES (2)"); err == nil {
		t.Fatal("Exec(second insert) error = nil, want checkpoint failure")
	}
	db.afterDatabaseSyncHook = nil
	rootPageID := db.tables["t"].RootPageID()
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	rootPage, err := pager.Get(rootPageID)
	if err != nil {
		t.Fatalf("pager.Get(root) error = %v", err)
	}
	older := storage.InitializeTablePage(uint32(rootPageID))
	row, err := storage.EncodeSlottedRow(storageValuesFromParser([]parser.Value{parser.IntValue(1)}), []uint8{storage.CatalogColumnTypeInt})
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}
	if _, err := storage.InsertRowIntoTablePage(older, row); err != nil {
		t.Fatalf("InsertRowIntoTablePage() error = %v", err)
	}
	if err := storage.SetPageLSN(older, 5); err != nil {
		t.Fatalf("SetPageLSN() error = %v", err)
	}
	if err := storage.RecomputePageChecksum(older); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}
	clear(rootPage.Data())
	copy(rootPage.Data(), older)
	pager.MarkDirty(rootPage)
	if err := pager.Flush(); err != nil {
		t.Fatalf("pager.Flush() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer db.Close()

	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)
}

func TestOpenFailsOnTruncatedWALFrameDuringReplay(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	file, err := os.OpenFile(storage.WALPath(path), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	raw := make([]byte, storage.WALFrameSize-1)
	binary.LittleEndian.PutUint32(raw[0:4], storage.WALRecordTypeFrame)
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		t.Fatalf("file.Write() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want truncated WAL replay failure")
	}
}

func TestPlannerGuardrailIndexedEqualityChoosesIndexScan(t *testing.T) {
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
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	stmt, ok := parser.ParseSelectExpr("SELECT id FROM users WHERE name = 'alice'")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := planner.PlanSelect(stmt, plannerTableMetadata(db.tables))
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != planner.ScanTypeIndex || plan.IndexScan == nil {
		t.Fatalf("plan = %#v, want index scan", plan)
	}
}

func TestPlannerGuardrailReopenedIndexMetadataStillChoosesIndexScan(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	stmt, ok := parser.ParseSelectExpr("SELECT id FROM users WHERE name = 'alice'")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := planner.PlanSelect(stmt, plannerTableMetadata(db.tables))
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != planner.ScanTypeIndex || plan.IndexScan == nil {
		t.Fatalf("reopened plan = %#v, want index scan", plan)
	}
}

func TestIndexedEqualityGuardrailAfterMutationAndReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	assertQueryIntRows(t, db, "SELECT id FROM users WHERE name = 'alice' ORDER BY id", 1, 3)

	if _, err := db.Exec("UPDATE users SET name = 'alice' WHERE id = 2"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if _, err := db.Exec("DELETE FROM users WHERE id = 1"); err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}

	assertQueryIntRows(t, db, "SELECT id FROM users WHERE name = 'alice' ORDER BY id", 2, 3)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertQueryIntRows(t, db, "SELECT id FROM users WHERE name = 'alice' ORDER BY id", 2, 3)
}

func TestPlannerGuardrailUnsupportedIndexedPredicateFallsBackToTableScan(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	stmt, ok := parser.ParseSelectExpr("SELECT id FROM users WHERE name != 'alice'")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := planner.PlanSelect(stmt, plannerTableMetadata(db.tables))
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != planner.ScanTypeTable || plan.TableScan == nil {
		t.Fatalf("plan = %#v, want table scan fallback", plan)
	}
	if plan.IndexScan != nil {
		t.Fatalf("plan.IndexScan = %#v, want nil fallback", plan.IndexScan)
	}
}

func TestCreateTablePersistence(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertIntRows(t, rows)
}

func TestInsertPersistence(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
		"INSERT INTO t VALUES (3)",
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

	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertIntRows(t, rows, 1, 2, 3)
}

func TestUpdatePersistence(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
		"INSERT INTO t VALUES (3)",
		"UPDATE t SET id = 10 WHERE id = 1",
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

	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertIntRows(t, rows, 10, 2, 3)
}

func TestDeletePersistence(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
		"INSERT INTO t VALUES (3)",
		"DELETE FROM t WHERE id = 2",
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

	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertIntRows(t, rows, 1, 3)
}

func TestMixedMutationPersistence(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
		"INSERT INTO t VALUES (3)",
		"INSERT INTO t VALUES (4)",
		"UPDATE t SET id = 10 WHERE id = 1",
		"UPDATE t SET id = 30 WHERE id = 3",
		"DELETE FROM t WHERE id = 2",
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

	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertIntRows(t, rows, 10, 30, 4)
}

func TestMutationsStillPersistUnderAutocommit(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
		"INSERT INTO t VALUES (3)",
		"UPDATE t SET id = 10 WHERE id = 1",
		"DELETE FROM t WHERE id = 2",
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

	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	assertIntRows(t, rows, 10, 3)
}

func TestFailedMutatingStatementClearsTxn(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1, 2)"); err == nil {
		t.Fatal("Exec(insert) error = nil, want failure")
	}
	if db.txn != nil {
		t.Fatalf("db.txn = %#v, want nil", db.txn)
	}
}

func TestSuccessfulMutatingStatementClearsTxn(t *testing.T) {
	db, err := Open(testDBPath(t))
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
}

func TestFailedInsertDoesNotChangeState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("Exec(insert baseline) error = %v", err)
	}

	if _, err := db.Exec("INSERT INTO t VALUES (1, 2)"); err == nil {
		t.Fatal("Exec(failing insert) error = nil, want failure")
	}

	assertSelectIntRows(t, db, "SELECT * FROM t", 1)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 1)
}

func TestFailedUpdateDoesNotChangeState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("UPDATE t SET missing = 10 WHERE id = 1"); err == nil {
		t.Fatal("Exec(failing update) error = nil, want failure")
	}

	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)
}

func TestFailedDeleteDoesNotChangeState(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("DELETE FROM t WHERE missing = 2"); err == nil {
		t.Fatal("Exec(failing delete) error = nil, want failure")
	}

	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	assertSelectIntRows(t, db, "SELECT * FROM t", 1, 2)
}

func TestFailedCreateTableDoesNotPartiallyRegisterTable(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	if _, err := db.Exec("CREATE TABLE t (id INT)"); err == nil {
		t.Fatal("Exec(duplicate create) error = nil, want failure")
	}
	if got := userTableCount(db.tables); got != 1 {
		t.Fatalf("userTableCount(db.tables) = %d, want 1", got)
	}
	assertSelectIntRows(t, db, "SELECT * FROM t")

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if got := userTableCount(db.tables); got != 1 {
		t.Fatalf("userTableCount(reopened db.tables) = %d, want 1", got)
	}
	assertSelectIntRows(t, db, "SELECT * FROM t")
}

func TestPhysicalStorageLayerMilestoneLifecycle(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, note TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for id := 1; id <= 40; id++ {
		name := "user"
		if id == 7 {
			name = "target"
		}
		note := strings.Repeat("seed-", 35)
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", int32(id), name, note); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	dataPageIDs, err := committedTableDataPageIDs(db.pool, table)
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	if len(dataPageIDs) < 2 {
		t.Fatalf("len(committedTableDataPageIDs()) = %d, want at least 2", len(dataPageIDs))
	}
	before := committedLocatorsByIDForTest(t, db, "users")[7]

	relocatedNote := strings.Repeat("relocated-", 260)
	if _, err := db.Exec("UPDATE users SET note = ? WHERE id = 7", relocatedNote); err != nil {
		t.Fatalf("Exec(relocating update) error = %v", err)
	}

	after := committedLocatorsByIDForTest(t, db, "users")[7]
	if after == before {
		t.Fatalf("relocated locator = %#v, want different from %#v", after, before)
	}
	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query("SELECT id FROM users WHERE name = 'target'")
	if err != nil {
		t.Fatalf("Query(indexed lookup) error = %v", err)
	}
	assertRowsIntSequence(t, rows, 7)
	rows.Close()

	rows, err = db.Query("SELECT note FROM users WHERE id = 7")
	if err != nil {
		t.Fatalf("Query(relocated row) error = %v", err)
	}
	assertRowsStringSequence(t, rows, relocatedNote)
	rows.Close()

	rows, err = db.Query("SELECT id FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query(full scan) error = %v", err)
	}
	wantIDs := make([]int, 0, 40)
	for id := 1; id <= 40; id++ {
		wantIDs = append(wantIDs, id)
	}
	assertRowsIntSequence(t, rows, wantIDs...)
	rows.Close()

	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() after reopen error = %v", err)
	}
}

func TestPhysicalStoragePolishMilestoneLifecycleAndDiagnostics(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, note TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for id := 1; id <= 36; id++ {
		name := "bulk"
		switch id % 6 {
		case 0:
			name = "blue"
		case 1:
			name = "green"
		case 2:
			name = "red"
		}
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", int32(id), name, strings.Repeat("payload-", 110)); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	if _, err := db.Exec("UPDATE users SET note = ? WHERE id = 1", strings.Repeat("grow-a-", 220)); err != nil {
		t.Fatalf("Exec(update relocate 1) error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET name = 'amber', note = ? WHERE id = 7", strings.Repeat("grow-b-", 210)); err != nil {
		t.Fatalf("Exec(update relocate 7) error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET name = 'violet' WHERE id = 12"); err != nil {
		t.Fatalf("Exec(update indexed value 12) error = %v", err)
	}
	if _, err := db.Exec("DELETE FROM users WHERE id = 6 OR id = 18 OR id = 24 OR id = 30 OR id = 36"); err != nil {
		t.Fatalf("Exec(delete group) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (101, 'blue', ?)", strings.Repeat("new-", 90)); err != nil {
		t.Fatalf("Exec(insert 101) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (102, 'amber', ?)", strings.Repeat("newer-", 95)); err != nil {
		t.Fatalf("Exec(insert 102) error = %v", err)
	}

	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	check, err := db.CheckEngineConsistency()
	if err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}
	if !check.OK {
		t.Fatal("CheckEngineConsistency().OK = false, want true")
	}
	if check.CheckedTableHeaders != 1 {
		t.Fatalf("CheckEngineConsistency().CheckedTableHeaders = %d, want 1", check.CheckedTableHeaders)
	}
	if check.CheckedSpaceMapPages == 0 {
		t.Fatal("CheckEngineConsistency().CheckedSpaceMapPages = 0, want > 0")
	}
	if check.CheckedDataPages < 2 {
		t.Fatalf("CheckEngineConsistency().CheckedDataPages = %d, want at least 2", check.CheckedDataPages)
	}

	snapshot, err := db.EngineSnapshot()
	if err != nil {
		t.Fatalf("EngineSnapshot() error = %v", err)
	}
	if len(snapshot.Inventory.Tables) != 1 {
		t.Fatalf("len(EngineSnapshot().Inventory.Tables) = %d, want 1", len(snapshot.Inventory.Tables))
	}
	tableInfo := snapshot.Inventory.Tables[0]
	if !tableInfo.PhysicalMetaPresent || !tableInfo.PhysicalMetaValid || !tableInfo.PhysicalInventoryMatch {
		t.Fatalf("EngineSnapshot().Inventory.Tables[0] = %#v, want physical metadata and inventory marked valid", tableInfo)
	}
	if tableInfo.EnumeratedDataPages < 2 {
		t.Fatalf("EngineSnapshot().Inventory.Tables[0].EnumeratedDataPages = %d, want at least 2", tableInfo.EnumeratedDataPages)
	}
	report := snapshot.String()
	for _, want := range []string{"physical=ok", "space_maps=", "data_pages="} {
		if !strings.Contains(report, want) {
			t.Fatalf("EngineSnapshot().String() missing %q in %q", want, report)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	db = reopenDB(t, path)
	defer db.Close()

	checkIDs := func(label, sql string, want ...int) {
		t.Helper()
		rows, err := db.Query(sql)
		if err != nil {
			t.Fatalf("Query(%s) error = %v", label, err)
		}
		assertRowsIntSequence(t, rows, want...)
		rows.Close()
	}

	checkIDs("amber", "SELECT id FROM users WHERE name = 'amber' ORDER BY id", 7, 102)
	checkIDs("violet", "SELECT id FROM users WHERE name = 'violet' ORDER BY id", 12)
	checkIDs("green", "SELECT id FROM users WHERE name = 'green' ORDER BY id", 1, 13, 19, 25, 31)
	checkIDs("blue", "SELECT id FROM users WHERE name = 'blue' ORDER BY id", 101)

	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() after reopen error = %v", err)
	}
}

func TestIllegalCascadeGraphDDLLeavesSchemaUnchangedAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	mustExec(t, db, "CREATE TABLE a (id INT, CONSTRAINT pk_a PRIMARY KEY (id) USING INDEX idx_a_pk)")
	mustExec(t, db, "CREATE TABLE b (id INT, a_id INT, CONSTRAINT pk_b PRIMARY KEY (id) USING INDEX idx_b_pk, CONSTRAINT fk_b_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_b_a ON DELETE CASCADE)")
	mustExec(t, db, "CREATE TABLE c (id INT, a_id INT, CONSTRAINT pk_c PRIMARY KEY (id) USING INDEX idx_c_pk, CONSTRAINT fk_c_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_c_a ON DELETE CASCADE)")
	mustExec(t, db, "CREATE TABLE d (id INT, b_id INT, c_id INT, CONSTRAINT pk_d PRIMARY KEY (id) USING INDEX idx_d_pk, CONSTRAINT fk_d_b FOREIGN KEY (b_id) REFERENCES b (id) USING INDEX idx_d_b ON DELETE CASCADE)")
	mustExec(t, db, "CREATE INDEX idx_d_c ON d(c_id)")

	if _, err := db.Exec("ALTER TABLE d ADD CONSTRAINT fk_d_c FOREIGN KEY (c_id) REFERENCES c (id) USING INDEX idx_d_c ON DELETE CASCADE"); err == nil {
		t.Fatal("Exec(add illegal multiple path fk) error = nil, want failure")
	}
	if len(db.tables["d"].ForeignKeyDefs) != 1 || db.tables["d"].ForeignKeyDefs[0].Name != "fk_d_b" {
		t.Fatalf("d.ForeignKeyDefs = %#v, want unchanged single fk after failed alter", db.tables["d"].ForeignKeyDefs)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if len(db.tables["d"].ForeignKeyDefs) != 1 || db.tables["d"].ForeignKeyDefs[0].Name != "fk_d_b" {
		t.Fatalf("reopened d.ForeignKeyDefs = %#v, want unchanged single fk after failed alter", db.tables["d"].ForeignKeyDefs)
	}
}

func TestLegalCascadeGraphSchemaPersistsAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	mustExec(t, db, "CREATE TABLE a (id INT, CONSTRAINT pk_a PRIMARY KEY (id) USING INDEX idx_a_pk)")
	mustExec(t, db, "CREATE TABLE b (id INT, a_id INT, CONSTRAINT pk_b PRIMARY KEY (id) USING INDEX idx_b_pk, CONSTRAINT fk_b_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_b_a ON DELETE CASCADE)")
	mustExec(t, db, "CREATE TABLE c (id INT, a_id INT, CONSTRAINT pk_c PRIMARY KEY (id) USING INDEX idx_c_pk, CONSTRAINT fk_c_a FOREIGN KEY (a_id) REFERENCES a (id) USING INDEX idx_c_a ON DELETE RESTRICT)")
	mustExec(t, db, "CREATE TABLE d (id INT, b_id INT, c_id INT, CONSTRAINT pk_d PRIMARY KEY (id) USING INDEX idx_d_pk, CONSTRAINT fk_d_b FOREIGN KEY (b_id) REFERENCES b (id) USING INDEX idx_d_b ON DELETE CASCADE, CONSTRAINT fk_d_c FOREIGN KEY (c_id) REFERENCES c (id) USING INDEX idx_d_c ON DELETE RESTRICT)")
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if db.tables["d"] == nil || len(db.tables["d"].ForeignKeyDefs) != 2 {
		t.Fatalf("reopened d table = %#v, want persisted legal cascade graph", db.tables["d"])
	}
}

func TestFailedConstraintTeardownStatementsLeaveSchemaUnchangedAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE nopk (id INT)",
		"CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)",
		"CREATE TABLE parents (id INT, CONSTRAINT pk_parents PRIMARY KEY (id) USING INDEX idx_parents_pk)",
		"CREATE TABLE users (id INT, team_id INT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	if _, err := db.Exec("ALTER TABLE nopk DROP PRIMARY KEY"); err == nil || err.Error() != "execution: primary key not found: table=nopk" {
		t.Fatalf("Exec(drop missing primary key) error = %v, want deterministic missing primary key error", err)
	}
	if _, err := db.Exec("ALTER TABLE users DROP FOREIGN KEY fk_missing"); err == nil || err.Error() != "execution: foreign key not found: table=users constraint=fk_missing" {
		t.Fatalf("Exec(drop missing foreign key) error = %v, want deterministic missing foreign key error", err)
	}
	if _, err := db.Exec("DROP INDEX idx_users_team"); err == nil {
		t.Fatal("Exec(drop supporting fk index) error = nil, want blocked drop")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()
	if db.tables["nopk"] == nil || db.tables["nopk"].PrimaryKeyDef != nil {
		t.Fatalf("reopened nopk.PrimaryKeyDef = %#v, want unchanged nil primary key", db.tables["nopk"].PrimaryKeyDef)
	}
	if db.tables["teams"] == nil || db.tables["teams"].PrimaryKeyDef == nil || db.tables["teams"].PrimaryKeyDef.Name != "pk_teams" {
		t.Fatalf("reopened teams.PrimaryKeyDef = %#v, want referenced primary key intact after failed drops", db.tables["teams"].PrimaryKeyDef)
	}
	if db.tables["parents"] == nil || db.tables["parents"].PrimaryKeyDef == nil || db.tables["parents"].PrimaryKeyDef.Name != "pk_parents" {
		t.Fatalf("reopened parents.PrimaryKeyDef = %#v, want unrelated primary key intact", db.tables["parents"].PrimaryKeyDef)
	}
	if db.tables["users"] == nil || len(db.tables["users"].ForeignKeyDefs) != 1 || db.tables["users"].ForeignKeyDefs[0].Name != "fk_users_team" {
		t.Fatalf("reopened users.ForeignKeyDefs = %#v, want unchanged fk after failed fk/index drops", db.tables["users"].ForeignKeyDefs)
	}
	if db.tables["users"].IndexDefinition("idx_users_team") == nil {
		t.Fatalf("reopened IndexDefinition(idx_users_team) = nil, want supporting index retained after failed drop")
	}
}

func TestDropPrimaryKeyAndDependentForeignKeysPersistAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)",
		"CREATE TABLE users (id INT, team_id INT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT)",
		"INSERT INTO teams VALUES (1)",
		"INSERT INTO users VALUES (10, 1)",
		"ALTER TABLE teams DROP PRIMARY KEY",
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
	if db.tables["teams"] == nil || db.tables["teams"].PrimaryKeyDef != nil {
		t.Fatalf("reopened teams.PrimaryKeyDef = %#v, want nil", db.tables["teams"].PrimaryKeyDef)
	}
	if db.tables["teams"].IndexDefinition("idx_teams_pk") == nil {
		t.Fatalf("reopened IndexDefinition(idx_teams_pk) = nil, want retained supporting index")
	}
	if db.tables["users"] == nil || len(db.tables["users"].ForeignKeyDefs) != 0 {
		t.Fatalf("reopened users.ForeignKeyDefs = %#v, want dependent foreign keys removed", db.tables["users"].ForeignKeyDefs)
	}

	rows, err := db.Query("SELECT id, team_id FROM users")
	if err != nil {
		t.Fatalf("Query(users) error = %v", err)
	}
	defer rows.Close()
	var id, teamID int32
	if !rows.Next() {
		t.Fatal("rows.Next() = false, want true")
	}
	if err := rows.Scan(&id, &teamID); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if id != 10 || teamID != 1 {
		t.Fatalf("users row = (%d,%d), want (10,1)", id, teamID)
	}
}

func TestDropTableDependencyTeardownPersistsAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)",
		"CREATE TABLE users (id INT, team_id INT, note TEXT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT)",
		"CREATE INDEX idx_users_note ON users (note)",
		"INSERT INTO teams VALUES (1)",
		"INSERT INTO users VALUES (10, 1, 'ready')",
		"DROP TABLE teams",
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
	if db.tables["teams"] != nil {
		t.Fatalf("reopened db.tables[teams] = %#v, want nil", db.tables["teams"])
	}
	if db.tables["users"] == nil || len(db.tables["users"].ForeignKeyDefs) != 0 {
		t.Fatalf("reopened users.ForeignKeyDefs = %#v, want dependent foreign keys removed", db.tables["users"].ForeignKeyDefs)
	}
	if db.tables["users"].IndexDefinition("idx_users_team") == nil || db.tables["users"].IndexDefinition("idx_users_note") == nil {
		t.Fatalf("reopened users indexes = %#v, want surviving indexes retained", db.tables["users"].IndexDefs)
	}

	rows, err := db.Query("SELECT id, team_id, note FROM users")
	if err != nil {
		t.Fatalf("Query(users) error = %v", err)
	}
	defer rows.Close()
	var id, teamID int32
	var note string
	if !rows.Next() {
		t.Fatal("rows.Next() = false, want true")
	}
	if err := rows.Scan(&id, &teamID, &note); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if id != 10 || teamID != 1 || note != "ready" {
		t.Fatalf("users row = (%d,%d,%q), want (10,1,\"ready\")", id, teamID, note)
	}
}

func TestPKFKMilestoneEndToEndContract(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	for _, sql := range []string{
		"CREATE TABLE parent_create (id INT, name TEXT, CONSTRAINT pk_parent_create PRIMARY KEY (id) USING INDEX idx_parent_create_pk)",
		"CREATE TABLE child_create (id INT, parent_id INT, note TEXT, CONSTRAINT pk_child_create PRIMARY KEY (id) USING INDEX idx_child_create_pk, CONSTRAINT fk_child_create_parent FOREIGN KEY (parent_id) REFERENCES parent_create (id) USING INDEX idx_child_create_parent ON DELETE RESTRICT)",
		"CREATE TABLE parent_alter (id INT, region_id INT, label TEXT)",
		"CREATE UNIQUE INDEX idx_parent_alter_pk ON parent_alter (id, region_id)",
		"ALTER TABLE parent_alter ADD CONSTRAINT pk_parent_alter PRIMARY KEY (id, region_id) USING INDEX idx_parent_alter_pk",
		"CREATE TABLE child_alter (id INT, parent_id INT, region_id INT, note TEXT)",
		"CREATE INDEX idx_child_alter_parent ON child_alter (parent_id, region_id, note)",
		"ALTER TABLE child_alter ADD CONSTRAINT fk_child_alter_parent FOREIGN KEY (parent_id, region_id) REFERENCES parent_alter (id, region_id) USING INDEX idx_child_alter_parent ON DELETE CASCADE",
		"INSERT INTO parent_create VALUES (1, 'alpha')",
		"INSERT INTO child_create VALUES (10, 1, 'restrict-row')",
		"INSERT INTO parent_alter VALUES (7, 70, 'west')",
		"INSERT INTO child_alter VALUES (20, 7, 70, 'cascade-row')",
	} {
		mustExec(t, db, sql)
	}

	if _, err := db.Exec("INSERT INTO parent_create VALUES (1, 'duplicate')"); err == nil || !isConstraintError(err, "table=parent_create", "constraint=pk_parent_create", "type=primary_key_duplicate") {
		t.Fatalf("Exec(insert duplicate pk row) error = %v, want primary key duplicate violation", err)
	}
	if _, err := db.Exec("UPDATE parent_create SET id = 2 WHERE id = 1"); err == nil || !isConstraintError(err, "table=parent_create", "constraint=pk_parent_create", "type=primary_key_update_forbidden") {
		t.Fatalf("Exec(update pk column) error = %v, want primary key update forbidden violation", err)
	}
	if got := mustQueryInt(t, db, "SELECT id FROM parent_create WHERE name = 'alpha'"); got != 1 {
		t.Fatalf("parent_create id after failed pk update = %d, want 1", got)
	}

	if _, err := db.Exec("INSERT INTO child_create VALUES (11, 99, 'orphan')"); err == nil || !isConstraintError(err, "table=child_create", "constraint=fk_child_create_parent", "type=foreign_key_missing_parent") {
		t.Fatalf("Exec(insert orphan child) error = %v, want foreign key missing parent violation", err)
	}
	if _, err := db.Exec("UPDATE child_create SET parent_id = 99 WHERE id = 10"); err == nil || !isConstraintError(err, "table=child_create", "constraint=fk_child_create_parent", "type=foreign_key_missing_parent") {
		t.Fatalf("Exec(update child to missing parent) error = %v, want foreign key missing parent violation", err)
	}
	if got := mustQueryInt(t, db, "SELECT parent_id FROM child_create WHERE id = 10"); got != 1 {
		t.Fatalf("child_create parent_id after failed fk update = %d, want 1", got)
	}

	if _, err := db.Exec("DELETE FROM parent_create WHERE id = 1"); err == nil || !isConstraintError(err, "table=child_create", "constraint=fk_child_create_parent", "type=foreign_key_restrict") {
		t.Fatalf("Exec(delete restricted parent) error = %v, want foreign key restrict violation", err)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parent_create WHERE id = 1"); got != 1 {
		t.Fatalf("parent_create count after failed restrict delete = %d, want 1", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM child_create WHERE id = 10"); got != 1 {
		t.Fatalf("child_create count after failed restrict delete = %d, want 1", got)
	}

	if _, err := db.Exec("DELETE FROM parent_alter WHERE id = 7 AND region_id = 70"); err != nil {
		t.Fatalf("Exec(delete cascading parent) error = %v, want nil", err)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parent_alter"); got != 0 {
		t.Fatalf("parent_alter count after cascade delete = %d, want 0", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM child_alter"); got != 0 {
		t.Fatalf("child_alter count after cascade delete = %d, want 0", got)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if db.tables["parent_create"] == nil || db.tables["parent_create"].PrimaryKeyDef == nil || db.tables["parent_create"].PrimaryKeyDef.Name != "pk_parent_create" {
		t.Fatalf("reopened parent_create.PrimaryKeyDef = %#v, want pk_parent_create", db.tables["parent_create"].PrimaryKeyDef)
	}
	if db.tables["child_create"] == nil || len(db.tables["child_create"].ForeignKeyDefs) != 1 || db.tables["child_create"].ForeignKeyDefs[0].Name != "fk_child_create_parent" {
		t.Fatalf("reopened child_create.ForeignKeyDefs = %#v, want fk_child_create_parent", db.tables["child_create"].ForeignKeyDefs)
	}
	if db.tables["parent_alter"] == nil || db.tables["parent_alter"].PrimaryKeyDef == nil || db.tables["parent_alter"].PrimaryKeyDef.Name != "pk_parent_alter" {
		t.Fatalf("reopened parent_alter.PrimaryKeyDef = %#v, want pk_parent_alter", db.tables["parent_alter"].PrimaryKeyDef)
	}
	if db.tables["child_alter"] == nil || len(db.tables["child_alter"].ForeignKeyDefs) != 1 || db.tables["child_alter"].ForeignKeyDefs[0].Name != "fk_child_alter_parent" {
		t.Fatalf("reopened child_alter.ForeignKeyDefs = %#v, want fk_child_alter_parent", db.tables["child_alter"].ForeignKeyDefs)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM child_alter"); got != 0 {
		t.Fatalf("reopened child_alter count = %d, want 0 after persisted cascade delete", got)
	}
	if _, err := db.Exec("INSERT INTO child_create VALUES (12, 1, 'post-reopen')"); err != nil {
		t.Fatalf("Exec(insert valid child after reopen) error = %v", err)
	}
}

func TestPKFKMilestoneDestructiveDDLContract(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	for _, sql := range []string{
		"CREATE TABLE drop_parent (id INT, CONSTRAINT pk_drop_parent PRIMARY KEY (id) USING INDEX idx_drop_parent_pk)",
		"CREATE TABLE drop_child (id INT, parent_id INT, CONSTRAINT pk_drop_child PRIMARY KEY (id) USING INDEX idx_drop_child_pk, CONSTRAINT fk_drop_child_parent FOREIGN KEY (parent_id) REFERENCES drop_parent (id) USING INDEX idx_drop_child_parent ON DELETE RESTRICT)",
		"INSERT INTO drop_parent VALUES (1)",
		"INSERT INTO drop_child VALUES (10, 1)",
		"CREATE TABLE gone_parent (id INT, CONSTRAINT pk_gone_parent PRIMARY KEY (id) USING INDEX idx_gone_parent_pk)",
		"CREATE TABLE survive_child (id INT, parent_id INT, note TEXT, CONSTRAINT fk_survive_child_parent FOREIGN KEY (parent_id) REFERENCES gone_parent (id) USING INDEX idx_survive_child_parent ON DELETE CASCADE)",
		"CREATE INDEX idx_survive_child_note ON survive_child (note)",
		"INSERT INTO gone_parent VALUES (5)",
		"INSERT INTO survive_child VALUES (50, 5, 'kept-index')",
	} {
		mustExec(t, db, sql)
	}

	if _, err := db.Exec("DROP INDEX idx_drop_parent_pk"); err == nil || err.Error() != "execution: index required by constraint: table=drop_parent index=idx_drop_parent_pk constraint=pk_drop_parent type=primary_key" {
		t.Fatalf("Exec(drop pk index) error = %v, want deterministic blocking error", err)
	}
	if _, err := db.Exec("DROP INDEX idx_drop_child_parent"); err == nil || err.Error() != "execution: index required by constraint: table=drop_child index=idx_drop_child_parent constraint=fk_drop_child_parent type=foreign_key" {
		t.Fatalf("Exec(drop fk index) error = %v, want deterministic blocking error", err)
	}

	mustExec(t, db, "ALTER TABLE drop_parent DROP PRIMARY KEY")
	if db.tables["drop_parent"].PrimaryKeyDef != nil {
		t.Fatalf("drop_parent.PrimaryKeyDef = %#v, want nil after drop", db.tables["drop_parent"].PrimaryKeyDef)
	}
	if len(db.tables["drop_child"].ForeignKeyDefs) != 0 {
		t.Fatalf("drop_child.ForeignKeyDefs = %#v, want dependent fks removed", db.tables["drop_child"].ForeignKeyDefs)
	}
	if db.tables["drop_parent"].IndexDefinition("idx_drop_parent_pk") == nil || db.tables["drop_child"].IndexDefinition("idx_drop_child_parent") == nil {
		t.Fatalf("supporting indexes should remain after drop primary key")
	}

	mustExec(t, db, "DROP TABLE gone_parent")
	if db.tables["gone_parent"] != nil {
		t.Fatalf("db.tables[gone_parent] = %#v, want nil", db.tables["gone_parent"])
	}
	if len(db.tables["survive_child"].ForeignKeyDefs) != 0 {
		t.Fatalf("survive_child.ForeignKeyDefs = %#v, want dependent fks removed after drop table", db.tables["survive_child"].ForeignKeyDefs)
	}
	if db.tables["survive_child"].IndexDefinition("idx_survive_child_parent") == nil || db.tables["survive_child"].IndexDefinition("idx_survive_child_note") == nil {
		t.Fatalf("surviving child indexes should remain after drop table")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if db.tables["drop_parent"] == nil || db.tables["drop_parent"].PrimaryKeyDef != nil {
		t.Fatalf("reopened drop_parent.PrimaryKeyDef = %#v, want nil", db.tables["drop_parent"].PrimaryKeyDef)
	}
	if db.tables["drop_child"] == nil || len(db.tables["drop_child"].ForeignKeyDefs) != 0 {
		t.Fatalf("reopened drop_child.ForeignKeyDefs = %#v, want empty", db.tables["drop_child"].ForeignKeyDefs)
	}
	if db.tables["gone_parent"] != nil {
		t.Fatalf("reopened db.tables[gone_parent] = %#v, want nil", db.tables["gone_parent"])
	}
	if db.tables["survive_child"] == nil || len(db.tables["survive_child"].ForeignKeyDefs) != 0 {
		t.Fatalf("reopened survive_child.ForeignKeyDefs = %#v, want empty", db.tables["survive_child"].ForeignKeyDefs)
	}
	if db.tables["survive_child"].IndexDefinition("idx_survive_child_parent") == nil || db.tables["survive_child"].IndexDefinition("idx_survive_child_note") == nil {
		t.Fatalf("reopened surviving child indexes should remain after drop table")
	}
}

func TestPKFKRuntimeEnforcementPersistsAcrossReopenAndStaysAtomic(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	mustExec(t, db, "CREATE TABLE parents (id INT, CONSTRAINT pk_parents PRIMARY KEY (id) USING INDEX idx_parents_pk)")
	mustExec(t, db, "CREATE TABLE children (id INT, parent_id INT, note TEXT, CONSTRAINT pk_children PRIMARY KEY (id) USING INDEX idx_children_pk, CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id) USING INDEX idx_children_parent ON DELETE RESTRICT)")
	mustExec(t, db, "INSERT INTO parents VALUES (1)")
	mustExec(t, db, "INSERT INTO children VALUES (10, 1, 'seed')")
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if _, err := db.Exec("INSERT INTO parents VALUES (1)"); err == nil || !strings.Contains(err.Error(), "table=parents") || !strings.Contains(err.Error(), "constraint=pk_parents") || !strings.Contains(err.Error(), "type=primary_key_duplicate") {
		t.Fatalf("Exec(insert duplicate parent after reopen) error = %v, want persisted pk enforcement", err)
	}
	if _, err := db.Exec("INSERT INTO children VALUES (11, 99, 'orphan')"); err == nil || !strings.Contains(err.Error(), "table=children") || !strings.Contains(err.Error(), "constraint=fk_children_parent") || !strings.Contains(err.Error(), "type=foreign_key_missing_parent") {
		t.Fatalf("Exec(insert orphan child after reopen) error = %v, want persisted fk enforcement", err)
	}
	if _, err := db.Exec("DELETE FROM parents WHERE id = 1"); err == nil || !strings.Contains(err.Error(), "table=children") || !strings.Contains(err.Error(), "constraint=fk_children_parent") || !strings.Contains(err.Error(), "type=foreign_key_restrict") {
		t.Fatalf("Exec(delete referenced parent after reopen) error = %v, want restrict violation", err)
	}

	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parents WHERE id = 1"); got != 1 {
		t.Fatalf("persisted parent count after failed writes = %d, want 1", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM children WHERE id = 10"); got != 1 {
		t.Fatalf("persisted child count after failed writes = %d, want 1", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM children WHERE id = 11"); got != 0 {
		t.Fatalf("orphan child count after failed insert = %d, want 0", got)
	}
}

func TestCascadeDeletePersistsAcrossReopenAndMixedFailuresStayAtomic(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	mustExec(t, db, "CREATE TABLE parents (id INT, CONSTRAINT pk_parents PRIMARY KEY (id) USING INDEX idx_parents_pk)")
	mustExec(t, db, "CREATE TABLE children (id INT, parent_id INT, CONSTRAINT pk_children PRIMARY KEY (id) USING INDEX idx_children_pk, CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id) USING INDEX idx_children_parent ON DELETE CASCADE)")
	mustExec(t, db, "CREATE TABLE audits (id INT, child_id INT, CONSTRAINT pk_audits PRIMARY KEY (id) USING INDEX idx_audits_pk, CONSTRAINT fk_audits_child FOREIGN KEY (child_id) REFERENCES children (id) USING INDEX idx_audits_child ON DELETE RESTRICT)")
	for _, sql := range []string{
		"INSERT INTO parents VALUES (1)",
		"INSERT INTO parents VALUES (2)",
		"INSERT INTO children VALUES (10, 1)",
		"INSERT INTO children VALUES (20, 2)",
		"INSERT INTO audits VALUES (100, 10)",
	} {
		mustExec(t, db, sql)
	}

	if _, err := db.Exec("DELETE FROM parents WHERE id = 1"); err == nil || !strings.Contains(err.Error(), "table=audits") || !strings.Contains(err.Error(), "constraint=fk_audits_child") || !strings.Contains(err.Error(), "type=foreign_key_restrict") {
		t.Fatalf("Exec(delete mixed restrict/cascade before reopen) error = %v, want restrict violation", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parents WHERE id = 1"); got != 1 {
		t.Fatalf("parent count after failed mixed delete and reopen = %d, want 1", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM children WHERE id = 10"); got != 1 {
		t.Fatalf("child count after failed mixed delete and reopen = %d, want 1", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM audits WHERE id = 100"); got != 1 {
		t.Fatalf("audit count after failed mixed delete and reopen = %d, want 1", got)
	}

	mustExec(t, db, "DELETE FROM audits WHERE id = 100")
	if _, err := db.Exec("DELETE FROM parents WHERE id = 1 OR id = 2"); err != nil {
		t.Fatalf("Exec(delete cascade after clearing restrict rows) error = %v, want nil", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM parents"); got != 0 {
		t.Fatalf("parent count after successful cascade and reopen = %d, want 0", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM children"); got != 0 {
		t.Fatalf("child count after successful cascade and reopen = %d, want 0", got)
	}
	if got := mustQueryInt(t, db, "SELECT COUNT(*) FROM audits"); got != 0 {
		t.Fatalf("audit count after successful cascade and reopen = %d, want 0", got)
	}
}

func TestForeignKeyMetadataPersistsAcrossReopenViaDDL(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, team_id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_team ON users (team_id)"); err != nil {
		t.Fatalf("Exec(create users team index) error = %v", err)
	}
	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE CASCADE"); err != nil {
		t.Fatalf("Exec(add foreign key) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	users := db.tables["users"]
	if users == nil {
		t.Fatal("users table = nil")
	}
	if len(users.ForeignKeyDefs) != 1 || users.ForeignKeyDefs[0].Name != "fk_users_team" {
		t.Fatalf("users.ForeignKeyDefs = %#v, want fk_users_team", users.ForeignKeyDefs)
	}
	if users.ForeignKeyDefs[0].OnDeleteAction != storage.CatalogForeignKeyDeleteActionCascade {
		t.Fatalf("users.ForeignKeyDefs[0].OnDeleteAction = %d, want %d", users.ForeignKeyDefs[0].OnDeleteAction, storage.CatalogForeignKeyDeleteActionCascade)
	}
}

func TestInvalidAlterAddPrimaryAndForeignKeyRollbackAtomically(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT, CONSTRAINT pk_teams PRIMARY KEY (id) USING INDEX idx_teams_pk)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, team_id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_team ON users (team_id)"); err != nil {
		t.Fatalf("Exec(create fk index) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 99)"); err != nil {
		t.Fatalf("Exec(insert orphan child row) error = %v", err)
	}

	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id) USING INDEX idx_users_pk_missing"); err == nil {
		t.Fatal("Exec(add invalid pk) error = nil, want failure")
	}
	if db.tables["users"].PrimaryKeyDef != nil {
		t.Fatalf("users.PrimaryKeyDef = %#v, want nil after failed add pk", db.tables["users"].PrimaryKeyDef)
	}

	if _, err := db.Exec("ALTER TABLE users ADD CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT"); err == nil {
		t.Fatal("Exec(add invalid fk) error = nil, want failure")
	}
	if len(db.tables["users"].ForeignKeyDefs) != 0 {
		t.Fatalf("users.ForeignKeyDefs = %#v, want unchanged empty after failed add fk", db.tables["users"].ForeignKeyDefs)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	if db.tables["users"].PrimaryKeyDef != nil {
		t.Fatalf("reopened users.PrimaryKeyDef = %#v, want nil", db.tables["users"].PrimaryKeyDef)
	}
	if len(db.tables["users"].ForeignKeyDefs) != 0 {
		t.Fatalf("reopened users.ForeignKeyDefs = %#v, want empty", db.tables["users"].ForeignKeyDefs)
	}
}

func TestInsertPersistsRowsToOwnedDataPage(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'steve')"); err != nil {
		t.Fatalf("Exec(insert 1) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (2, 'bob')"); err != nil {
		t.Fatalf("Exec(insert 2) error = %v", err)
	}

	rootPageID := db.tables["users"].RootPageID()
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[\"users\"] = nil")
	}
	if table.PersistedRowCount() == 0 {
		t.Fatal("table.PersistedRowCount() = 0, want > 0")
	}
	if table.RootPageID() != rootPageID {
		t.Fatalf("table.RootPageID() = %d, want %d", table.RootPageID(), rootPageID)
	}
	dataPageIDs, err := committedTableDataPageIDs(db.pool, table)
	if err != nil {
		t.Fatalf("committedTableDataPageIDs() error = %v", err)
	}
	if len(dataPageIDs) != 1 {
		t.Fatalf("len(committedTableDataPageIDs()) = %d, want 1", len(dataPageIDs))
	}
	dataPageID := dataPageIDs[0]
	if err := db.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	dbFile, err := storage.OpenOrCreate(path)
	if err != nil {
		t.Fatalf("storage.OpenOrCreate() error = %v", err)
	}
	defer dbFile.Close()

	pager, err := storage.NewPager(dbFile.File())
	if err != nil {
		t.Fatalf("storage.NewPager() error = %v", err)
	}
	page, err := pager.Get(dataPageID)
	if err != nil {
		t.Fatalf("pager.Get() error = %v", err)
	}
	if err := storage.ValidateOwnedDataPage(page.Data(), table.TableID); err != nil {
		t.Fatalf("storage.ValidateOwnedDataPage() error = %v", err)
	}
	if got := storage.TablePageRowCount(page); got != 2 {
		t.Fatalf("storage.TablePageRowCount() = %d, want 2", got)
	}
	storageRows, err := storage.ReadSlottedRowsFromTablePageData(page.Data(), []uint8{
		storage.CatalogColumnTypeInt,
		storage.CatalogColumnTypeText,
	})
	if err != nil {
		t.Fatalf("storage.ReadSlottedRowsFromTablePageData() error = %v", err)
	}
	rows := parserRowsFromStorage(storageRows)
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(rows))
	}
	if rows[0][0].IntegerValue() != 1 || rows[0][1].Str != "steve" {
		t.Fatalf("rows[0] = %#v, want id=1 name=steve", rows[0])
	}
	if rows[1][0].IntegerValue() != 2 || rows[1][1].Str != "bob" {
		t.Fatalf("rows[1] = %#v, want id=2 name=bob", rows[1])
	}
}

func TestSchemaLifecycleRoundTripConfidence(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE teams (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO teams VALUES (1, 'ops')",
		"CREATE INDEX idx_users_name ON users (name)",
		"CREATE UNIQUE INDEX idx_users_id ON users (id)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	tables, err := db.ListTables()
	if err != nil {
		t.Fatalf("ListTables() after first reopen error = %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("len(ListTables()) after first reopen = %d, want 2", len(tables))
	}
	if _, err := db.GetTableSchema("users"); err != nil {
		t.Fatalf("GetTableSchema(users) after first reopen error = %v", err)
	}
	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query(users by indexable predicate) error = %v", err)
	}
	if got := collectIntRowsFromRows(t, rows); len(got) != 1 || got[0] != 1 {
		t.Fatalf("users query rows after first reopen = %#v, want []int{1}", got)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close() error = %v", err)
	}
	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Fatalf("Exec(drop index) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() after drop index error = %v", err)
	}

	db = reopenDB(t, path)
	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil after second reopen")
	}
	if table.IndexDefinition("idx_users_name") != nil {
		t.Fatalf("IndexDefinition(idx_users_name) = %#v, want nil after reopen", table.IndexDefinition("idx_users_name"))
	}
	if table.IndexDefinition("idx_users_id") == nil {
		t.Fatalf("IndexDefinition(idx_users_id) = nil, want surviving unique definition (defs=%#v)", table.IndexDefs)
	}
	rows, err = db.Query("SELECT id, name FROM teams")
	if err != nil {
		t.Fatalf("Query(teams) after second reopen error = %v", err)
	}
	var teamID int32
	var teamName string
	if !rows.Next() {
		t.Fatal("teams rows.Next() = false, want true")
	}
	if err := rows.Scan(&teamID, &teamName); err != nil {
		t.Fatalf("teams rows.Scan() error = %v", err)
	}
	if teamID != 1 || teamName != "ops" {
		t.Fatalf("teams row = (%d,%q), want (1,\"ops\")", teamID, teamName)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("teams rows.Err() = %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("teams rows.Close() error = %v", err)
	}
	if _, err := db.Exec("DROP TABLE users"); err != nil {
		t.Fatalf("Exec(drop table) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() after drop table error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	tables, err = db.ListTables()
	if err != nil {
		t.Fatalf("ListTables() after final reopen error = %v", err)
	}
	if len(tables) != 1 || tables[0].Name != "teams" {
		t.Fatalf("ListTables() after final reopen = %#v, want only teams", tables)
	}
	if _, err := db.GetTableSchema("users"); err == nil || err.Error() != "table not found: users" {
		t.Fatalf("GetTableSchema(users) error = %v, want %q", err, "table not found: users")
	}
	rows, err = db.Query("SELECT id, name FROM teams")
	if err != nil {
		t.Fatalf("Query(teams) after final reopen error = %v", err)
	}
	if !rows.Next() {
		t.Fatal("final teams rows.Next() = false, want true")
	}
	if err := rows.Scan(&teamID, &teamName); err != nil {
		t.Fatalf("final teams rows.Scan() error = %v", err)
	}
	if teamID != 1 || teamName != "ops" {
		t.Fatalf("final teams row = (%d,%q), want (1,\"ops\")", teamID, teamName)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("final teams rows.Err() = %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("final teams rows.Close() error = %v", err)
	}
}

func TestOpenRejectsInvalidPagerSizedFile(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	if _, err := f.Write([]byte{0xff}); err != nil {
		_ = f.Close()
		t.Fatalf("f.Write() error = %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("f.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want non-nil")
	}
}

func TestOpenRejectsDuplicateRootPageIDs(t *testing.T) {
	path := testDBPath(t)

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	writeMalformedCatalogPageWithIDMappings(t, pager, currentCatalogBytesForTest([]currentCatalogTableForTest{
		{
			name:    "users",
			tableID: 1,
			columns: []currentCatalogColumnForTest{{name: "id", typ: storage.CatalogColumnTypeInt}},
		},
		{
			name:    "teams",
			tableID: 2,
			columns: []currentCatalogColumnForTest{{name: "id", typ: storage.CatalogColumnTypeInt}},
		},
	}), []storage.DirectoryRootIDMapping{
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: 1, RootPageID: 1},
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: 2, RootPageID: 1},
	})

	db, err := Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want non-nil")
	}
}

func TestOpenRejectsInvalidRootPageIDZero(t *testing.T) {
	path := testDBPath(t)

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	writeMalformedCatalogPage(t, pager, malformedCatalogBytes([]malformedCatalogTable{
		{
			name:       "users",
			rootPageID: 0,
			rowCount:   0,
			columns: []malformedCatalogColumn{
				{name: "id", typ: storage.CatalogColumnTypeInt},
			},
		},
	}))

	db, err := Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want non-nil")
	}
}

func TestOpenRejectsPersistedRowCountMismatch(t *testing.T) {
	path := testDBPath(t)

	dbFile, pager := openRawStorage(t, path)
	defer dbFile.Close()

	rootPage := pager.NewPage()
	storage.InitTableRootPage(rootPage)
	row, err := storage.EncodeRow(storageValuesFromParser([]parser.Value{parser.Int64Value(1), parser.StringValue("steve")}))
	if err != nil {
		t.Fatalf("EncodeRow() error = %v", err)
	}
	if err := storage.AppendRowToTablePage(rootPage, row); err != nil {
		t.Fatalf("AppendRowToTablePage() error = %v", err)
	}
	writeMalformedCatalogPageWithIDMappings(t, pager, currentCatalogBytesForTest([]currentCatalogTableForTest{
		{
			name:     "users",
			tableID:  1,
			rowCount: 2,
			columns: []currentCatalogColumnForTest{
				{name: "id", typ: storage.CatalogColumnTypeInt},
				{name: "name", typ: storage.CatalogColumnTypeText},
			},
		},
	}), []storage.DirectoryRootIDMapping{
		{ObjectType: storage.DirectoryRootMappingObjectTable, ObjectID: 1, RootPageID: uint32(rootPage.ID())},
	})

	db, err := Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want non-nil")
	}
}

func TestCloseReopenRoundTripFinal(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert 1) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (2, 'bob')"); err != nil {
		t.Fatalf("Exec(insert 2) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (3, 'carol')"); err != nil {
		t.Fatalf("Exec(insert 3) error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET name = 'bobby' WHERE id = 2"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if _, err := db.Exec("DELETE FROM users WHERE id = 1"); err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	if err := assertFinalUsersState(db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second reopen Open() error = %v", err)
	}
	defer db.Close()
	if err := assertFinalUsersState(db); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateThatFitsKeepsLocatorStable(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'amy')",
		"INSERT INTO users VALUES (2, 'bob')",
		"CREATE INDEX idx_users_name ON users (name)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	before := committedLocatorsByIDForTest(t, db, "users")
	if _, err := db.Exec("UPDATE users SET name = 'ann' WHERE id = 1"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}

	after := committedLocatorsByIDForTest(t, db, "users")
	if after[1] != before[1] {
		t.Fatalf("locator after fit update = %#v, want %#v", after[1], before[1])
	}

	rows, err := db.Query("SELECT id FROM users WHERE name = 'ann'")
	if err != nil {
		t.Fatalf("Query(new indexed value) error = %v", err)
	}
	assertRowsIntSequence(t, rows, 1)
	rows.Close()

	rows, err = db.Query("SELECT id FROM users WHERE name = 'amy'")
	if err != nil {
		t.Fatalf("Query(old indexed value) error = %v", err)
	}
	assertRowsIntSequence(t, rows)
	rows.Close()
}

func TestUpdateGrowthRelocatesRowAndPreservesIndexReadsAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, note TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for id := 1; id <= 18; id++ {
		name := "filler"
		if id == 1 {
			name = "alice"
		}
		note := strings.Repeat("x", 120)
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", int32(id), name, note); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	before := committedLocatorsByIDForTest(t, db, "users")
	oldLocator := before[1]
	bigNote := strings.Repeat("grown-row-", 220)
	if _, err := db.Exec("UPDATE users SET note = ? WHERE id = 1", bigNote); err != nil {
		t.Fatalf("Exec(growth update) error = %v", err)
	}

	after := committedLocatorsByIDForTest(t, db, "users")
	newLocator := after[1]
	if newLocator == oldLocator {
		t.Fatalf("relocated locator = %#v, want different from old locator %#v", newLocator, oldLocator)
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	if _, err := db.fetchRowByLocator(table, oldLocator); err == nil {
		t.Fatal("fetchRowByLocator(old locator) error = nil, want explicit failure")
	}
	row, err := db.fetchRowByLocator(table, newLocator)
	if err != nil {
		t.Fatalf("fetchRowByLocator(new locator) error = %v", err)
	}
	if got := row[2]; got != parser.StringValue(bigNote) {
		t.Fatalf("updated note = %#v, want %#v", got, parser.StringValue(bigNote))
	}

	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query(indexed read after relocation) error = %v", err)
	}
	assertRowsIntSequence(t, rows, 1)
	rows.Close()

	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err = db.Query("SELECT note FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query(reopen indexed read) error = %v", err)
	}
	assertRowsStringSequence(t, rows, bigNote)
	rows.Close()

	reopenedLocators := committedLocatorsByIDForTest(t, db, "users")
	if reopenedLocators[1] != newLocator {
		t.Fatalf("reopened locator = %#v, want %#v", reopenedLocators[1], newLocator)
	}
}

func TestDeleteRewriteReclaimsSupersededPhysicalPages(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE users (id INT, note TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	for id := 1; id <= 30; id++ {
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?)", int32(id), strings.Repeat("payload-", 120)); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil")
	}
	oldSpaceMapPageIDs, oldDataPageIDs, err := committedTablePhysicalStorageInventory(db.pool, table)
	if err != nil {
		t.Fatalf("committedTablePhysicalStorageInventory(before) error = %v", err)
	}
	if len(oldDataPageIDs) < 2 {
		t.Fatalf("len(oldDataPageIDs) = %d, want at least 2", len(oldDataPageIDs))
	}

	if _, err := db.Exec("DELETE FROM users WHERE id >= 20"); err != nil {
		t.Fatalf("Exec(delete) error = %v", err)
	}
	table = db.tables["users"]
	newSpaceMapPageIDs, newDataPageIDs, err := committedTablePhysicalStorageInventory(db.pool, table)
	if err != nil {
		t.Fatalf("committedTablePhysicalStorageInventory(after) error = %v", err)
	}
	for _, pageID := range append(append([]storage.PageID(nil), oldSpaceMapPageIDs...), oldDataPageIDs...) {
		if containsPageID(newSpaceMapPageIDs, pageID) || containsPageID(newDataPageIDs, pageID) {
			t.Fatalf("superseded page %d still present in authoritative inventory", pageID)
		}
	}
	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	rawDB, pager := openRawStorage(t, path)
	defer rawDB.Close()
	head, err := storage.ReadDirectoryFreeListHead(rawDB.File())
	if err != nil {
		t.Fatalf("ReadDirectoryFreeListHead() error = %v", err)
	}
	chain := freeListChainForTest(t, pager, storage.PageID(head))
	for _, pageID := range append(append([]storage.PageID(nil), oldSpaceMapPageIDs...), oldDataPageIDs...) {
		if !containsPageID(chain, pageID) {
			t.Fatalf("free list chain = %#v, want reclaimed superseded page %d present", chain, pageID)
		}
	}
}

func TestDeleteAfterRelocationAndKeyReuseLeavesNoStaleIndexVisibility(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, note TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for id := 1; id <= 20; id++ {
		name := "filler"
		if id == 1 {
			name = "alice"
		}
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", int32(id), name, strings.Repeat("seed-", 90)); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	before := committedLocatorsByIDForTest(t, db, "users")
	if _, err := db.Exec("UPDATE users SET note = ? WHERE id = 1", strings.Repeat("relocate-", 220)); err != nil {
		t.Fatalf("Exec(relocating update) error = %v", err)
	}
	after := committedLocatorsByIDForTest(t, db, "users")
	if after[1] == before[1] {
		t.Fatalf("locator after relocation = %#v, want different from %#v", after[1], before[1])
	}

	if _, err := db.Exec("DELETE FROM users WHERE id = 1"); err != nil {
		t.Fatalf("Exec(delete relocated row) error = %v", err)
	}
	rows, err := db.Query("SELECT id FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("Query(deleted key) error = %v", err)
	}
	assertRowsIntSequence(t, rows)
	rows.Close()

	if _, err := db.Exec("INSERT INTO users VALUES (101, 'alice', ?)", strings.Repeat("fresh-", 80)); err != nil {
		t.Fatalf("Exec(insert reused key) error = %v", err)
	}
	rows, err = db.Query("SELECT id FROM users WHERE name = 'alice' ORDER BY id")
	if err != nil {
		t.Fatalf("Query(reused key) error = %v", err)
	}
	assertRowsIntSequence(t, rows, 101)
	rows.Close()

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	db = reopenDB(t, path)
	defer db.Close()

	rows, err = db.Query("SELECT id FROM users WHERE name = 'alice' ORDER BY id")
	if err != nil {
		t.Fatalf("Query(reopen reused key) error = %v", err)
	}
	assertRowsIntSequence(t, rows, 101)
	rows.Close()
}

func TestMultiPageMutationIndexChurnAcrossReopenKeepsOnlyCurrentRowsVisible(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, note TEXT)"); err != nil {
		t.Fatalf("Exec(create table) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_name ON users (name)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for id := 1; id <= 36; id++ {
		name := "bulk"
		switch id % 6 {
		case 0:
			name = "blue"
		case 1:
			name = "green"
		case 2:
			name = "red"
		}
		if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", int32(id), name, strings.Repeat("payload-", 110)); err != nil {
			t.Fatalf("Exec(insert %d) error = %v", id, err)
		}
	}

	if _, err := db.Exec("UPDATE users SET note = ? WHERE id = 1", strings.Repeat("grow-a-", 220)); err != nil {
		t.Fatalf("Exec(update relocate 1) error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET name = 'amber', note = ? WHERE id = 7", strings.Repeat("grow-b-", 210)); err != nil {
		t.Fatalf("Exec(update relocate 7) error = %v", err)
	}
	if _, err := db.Exec("UPDATE users SET name = 'violet' WHERE id = 12"); err != nil {
		t.Fatalf("Exec(update indexed value 12) error = %v", err)
	}
	if _, err := db.Exec("DELETE FROM users WHERE id = 6 OR id = 18 OR id = 24 OR id = 30 OR id = 36"); err != nil {
		t.Fatalf("Exec(delete group) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (101, 'blue', ?)", strings.Repeat("new-", 90)); err != nil {
		t.Fatalf("Exec(insert 101) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (102, 'amber', ?)", strings.Repeat("newer-", 95)); err != nil {
		t.Fatalf("Exec(insert 102) error = %v", err)
	}

	check := func(label, sql string, want ...int) {
		t.Helper()
		rows, err := db.Query(sql)
		if err != nil {
			t.Fatalf("Query(%s) error = %v", label, err)
		}
		assertRowsIntSequence(t, rows, want...)
		rows.Close()
	}

	check("amber", "SELECT id FROM users WHERE name = 'amber' ORDER BY id", 7, 102)
	check("violet", "SELECT id FROM users WHERE name = 'violet' ORDER BY id", 12)
	check("green", "SELECT id FROM users WHERE name = 'green' ORDER BY id", 1, 13, 19, 25, 31)
	check("blue", "SELECT id FROM users WHERE name = 'blue' ORDER BY id", 101)

	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() error = %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	db = reopenDB(t, path)
	defer db.Close()

	check = func(label, sql string, want ...int) {
		t.Helper()
		rows, err := db.Query(sql)
		if err != nil {
			t.Fatalf("Query(reopen %s) error = %v", label, err)
		}
		assertRowsIntSequence(t, rows, want...)
		rows.Close()
	}

	check("amber", "SELECT id FROM users WHERE name = 'amber' ORDER BY id", 7, 102)
	check("violet", "SELECT id FROM users WHERE name = 'violet' ORDER BY id", 12)
	check("green", "SELECT id FROM users WHERE name = 'green' ORDER BY id", 1, 13, 19, 25, 31)
	check("blue", "SELECT id FROM users WHERE name = 'blue' ORDER BY id", 101)

	verifyPhysicalTableInventoryMatchesMetadata(t, db, "users")
	if _, err := db.CheckEngineConsistency(); err != nil {
		t.Fatalf("CheckEngineConsistency() after reopen error = %v", err)
	}
}

func TestMinimalUsabilityContractExampleFlow(t *testing.T) {
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
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	assertSelectRowsWithNames(t, db, "SELECT id, name FROM users", [][2]any{
		{1, "alice"},
		{2, "bob"},
	})

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	assertSelectRowsWithNames(t, db, "SELECT id, name FROM users", [][2]any{
		{1, "alice"},
		{2, "bob"},
	})
}

func TestSQLUsabilityMilestoneJoinAndAliasSurface(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE customers (cust_nbr INT, name TEXT, city TEXT)",
		"CREATE UNIQUE INDEX customers_ix1 ON customers (cust_nbr)",
		"CREATE TABLE orders (cust_nbr INT, order_nbr INT, total_amt INT)",
		"CREATE UNIQUE INDEX orders_ix1 ON orders (cust_nbr, order_nbr)",
		"INSERT INTO customers VALUES (1, 'Alice Carter', 'Boston')",
		"INSERT INTO customers VALUES (2, 'Brian Lewis', 'Chicago')",
		"INSERT INTO customers VALUES (3, 'Carla Gomez', 'Denver')",
		"INSERT INTO orders VALUES (1, 101, 75)",
		"INSERT INTO orders VALUES (1, 102, 25)",
		"INSERT INTO orders VALUES (2, 103, 60)",
		"INSERT INTO orders VALUES (3, 104, 10)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	explicitRows, err := db.Query("SELECT a.cust_nbr AS customer_number, a.name, b.order_nbr, b.total_amt FROM customers a JOIN orders b ON a.cust_nbr = b.cust_nbr WHERE b.total_amt > 50 ORDER BY a.name")
	if err != nil {
		t.Fatalf("Query(explicit join) error = %v", err)
	}
	defer explicitRows.Close()
	if got := explicitRows.columns; len(got) != 4 || got[0] != "a.cust_nbr AS customer_number" || got[1] != "a.name" || got[2] != "b.order_nbr" || got[3] != "b.total_amt" {
		t.Fatalf("explicitRows.columns = %#v, want [a.cust_nbr AS customer_number a.name b.order_nbr b.total_amt]", got)
	}
	wantExplicit := [][]any{
		{int32(1), "Alice Carter", int32(101), int32(75)},
		{int32(2), "Brian Lewis", int32(103), int32(60)},
	}
	assertMaterializedRowsEqual(t, explicitRows.data, wantExplicit)

	commaRows, err := db.Query("SELECT a.cust_nbr AS customer_number, a.name, b.order_nbr, b.total_amt FROM customers a, orders b WHERE a.cust_nbr = b.cust_nbr AND b.total_amt > 50 ORDER BY a.name")
	if err != nil {
		t.Fatalf("Query(comma join) error = %v", err)
	}
	defer commaRows.Close()
	if got := commaRows.columns; len(got) != len(explicitRows.columns) {
		t.Fatalf("commaRows.columns = %#v, want %#v", got, explicitRows.columns)
	}
	for i := range explicitRows.columns {
		if commaRows.columns[i] != explicitRows.columns[i] {
			t.Fatalf("commaRows.columns[%d] = %q, want %q", i, commaRows.columns[i], explicitRows.columns[i])
		}
	}
	assertMaterializedRowsEqual(t, commaRows.data, explicitRows.data)

	aliasRows, err := db.Query("SELECT cust_nbr AS customer_number, name FROM customers ORDER BY customer_number")
	if err != nil {
		t.Fatalf("Query(alias order by) error = %v", err)
	}
	defer aliasRows.Close()
	if got := aliasRows.columns; len(got) != 2 || got[0] != "customer_number" || got[1] != "name" {
		t.Fatalf("aliasRows.columns = %#v, want [customer_number name]", got)
	}
	wantAlias := [][]any{
		{int32(1), "Alice Carter"},
		{int32(2), "Brian Lewis"},
		{int32(3), "Carla Gomez"},
	}
	assertMaterializedRowsEqual(t, aliasRows.data, wantAlias)
}

func TestSQLUsabilityMilestoneCatalogAndErrorSurface(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE customers (cust_nbr INT, name TEXT)",
		"CREATE UNIQUE INDEX customers_ix1 ON customers (cust_nbr)",
		"CREATE TABLE orders (cust_nbr INT, order_nbr INT, total_amt INT)",
		"CREATE UNIQUE INDEX orders_ix1 ON orders (cust_nbr, order_nbr)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	sysRows, err := db.Query("SELECT table_name FROM sys_tables ORDER BY table_name")
	if err != nil {
		t.Fatalf("Query(sys_tables) error = %v", err)
	}
	defer sysRows.Close()
	if got := sysRows.columns; len(got) != 1 || got[0] != "table_name" {
		t.Fatalf("sysRows.columns = %#v, want [table_name]", got)
	}
	wantSys := [][]any{
		{"customers"},
		{"orders"},
	}
	assertMaterializedRowsEqual(t, sysRows.data, wantSys)

	oldRows, err := db.Query("SELECT table_name FROM __sys_tables")
	if err != nil {
		t.Fatalf("Query(__sys_tables) error = %v, want deferred error rowset", err)
	}
	defer oldRows.Close()
	if oldRows.Next() {
		t.Fatalf("oldRows.Next() = true, want false")
	}
	if oldRows.Err() == nil || oldRows.Err().Error() != "execution: table not found: __sys_tables" {
		t.Fatalf("oldRows.Err() = %v, want %q", oldRows.Err(), "execution: table not found: __sys_tables")
	}

	missingRows, err := db.Query("SELECT missing_col FROM customers")
	if err != nil {
		t.Fatalf("Query(missing column) error = %v, want deferred error rowset", err)
	}
	defer missingRows.Close()
	if missingRows.Next() {
		t.Fatalf("missingRows.Next() = true, want false")
	}
	if missingRows.Err() == nil || !strings.Contains(missingRows.Err().Error(), "missing_col") {
		t.Fatalf("missingRows.Err() = %v, want error containing %q", missingRows.Err(), "missing_col")
	}
}
