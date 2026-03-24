package rovadb

import (
	"context"
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

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
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.defineBasicIndex("users", "name"); err != nil {
		t.Fatalf("defineBasicIndex() error = %v", err)
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
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.defineBasicIndex("users", "name"); err != nil {
		t.Fatalf("defineBasicIndex() error = %v", err)
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
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.defineBasicIndex("users", "name"); err != nil {
		t.Fatalf("defineBasicIndex() error = %v", err)
	}

	assertQueryIntRows(t, db, "SELECT id FROM users WHERE name = 'alice' ORDER BY id", 1, 3)

	if _, err := db.Exec(context.Background(), "UPDATE users SET name = 'alice' WHERE id = 2"); err != nil {
		t.Fatalf("Exec(update) error = %v", err)
	}
	if _, err := db.Exec(context.Background(), "DELETE FROM users WHERE id = 1"); err != nil {
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
		if _, err := db.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.defineBasicIndex("users", "name"); err != nil {
		t.Fatalf("defineBasicIndex() error = %v", err)
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
