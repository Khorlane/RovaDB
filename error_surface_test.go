package rovadb

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

func TestStructuredParseErrorIsDeterministic(t *testing.T) {
	_, parseErr1 := parser.Parse("DELETE FROM users WHERE id =")
	err1 := mustError(t, nil, parseErr1)
	_, parseErr2 := parser.Parse("DELETE FROM users WHERE id =")
	err2 := mustError(t, nil, parseErr2)

	if err1.Error() != "parse: invalid where clause" {
		t.Fatalf("err1.Error() = %q, want %q", err1.Error(), "parse: invalid where clause")
	}
	if err2.Error() != err1.Error() {
		t.Fatalf("err2.Error() = %q, want %q", err2.Error(), err1.Error())
	}
}

func TestStructuredPlanErrorPrefix(t *testing.T) {
	_, err := planner.PlanSelect(nil)
	if err == nil {
		t.Fatalf("PlanSelect(nil) error = nil, want non-nil")
	}
	if err.Error() != "plan: unsupported query form" {
		t.Fatalf("err.Error() = %q, want %q", err.Error(), "plan: unsupported query form")
	}
}

func TestStructuredExecutionErrorPrefix(t *testing.T) {
	plan := &planner.SelectPlan{
		Stmt:      &parser.SelectExpr{TableName: "missing"},
		ScanType:  planner.ScanTypeTable,
		TableScan: &planner.TableScan{TableName: "missing"},
	}

	_, err := executor.Select(plan, map[string]*executor.Table{})
	if err == nil {
		t.Fatalf("Select() error = nil, want non-nil")
	}
	if err.Error() != "execution: table not found" {
		t.Fatalf("err.Error() = %q, want %q", err.Error(), "execution: table not found")
	}
}

func TestStructuredStorageErrorPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.db")
	if err := os.WriteFile(path, []byte("bad"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err := Open(path)
	if err == nil {
		t.Fatalf("Open() error = nil, want non-nil")
	}
	if err.Error() != "storage: storage: file too small" {
		t.Fatalf("err.Error() = %q, want %q", err.Error(), "storage: storage: file too small")
	}
}

func TestQueryInvalidWhereReturnsParseError(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}

	rows, err := db.Query(context.Background(), "SELECT * FROM users WHERE id =")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Err() == nil {
		t.Fatalf("Rows.Err() = nil, want non-nil")
	}
	if rows.Err().Error() != "parse: invalid where clause" {
		t.Fatalf("rows.Err().Error() = %q, want %q", rows.Err().Error(), "parse: invalid where clause")
	}
}

func mustError(t *testing.T, _ any, err error) error {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want non-nil")
	}
	return err
}
