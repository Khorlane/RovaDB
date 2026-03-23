package planner

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func testPlannerTables(indexedColumns ...string) map[string]*TableMetadata {
	indexes := make(map[string]*BasicIndex, len(indexedColumns))
	for _, columnName := range indexedColumns {
		indexes[columnName] = NewBasicIndex("users", columnName)
	}
	return map[string]*TableMetadata{
		"users": {Indexes: indexes},
	}
}

func TestPlanSelectLiteralLeavesTableScanNil(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT 1")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := PlanSelect(stmt)
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan == nil {
		t.Fatal("PlanSelect() = nil, want value")
	}
	if plan.Stmt != stmt {
		t.Fatalf("PlanSelect().Stmt = %#v, want original stmt", plan.Stmt)
	}
	if plan.TableScan != nil {
		t.Fatalf("PlanSelect().TableScan = %#v, want nil", plan.TableScan)
	}
}

func TestPlanSelectFromTableUsesTableScan(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT id FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := PlanSelect(stmt)
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan == nil {
		t.Fatal("PlanSelect() = nil, want value")
	}
	if plan.ScanType != ScanTypeTable {
		t.Fatalf("PlanSelect().ScanType = %q, want %q", plan.ScanType, ScanTypeTable)
	}
	if plan.TableScan == nil {
		t.Fatal("PlanSelect().TableScan = nil, want value")
	}
	if plan.TableScan.TableName != "users" {
		t.Fatalf("PlanSelect().TableScan.TableName = %q, want %q", plan.TableScan.TableName, "users")
	}
}

func TestPlanSelectEqualityWithIndexUsesIndexScan(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT id FROM users WHERE id = 1")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := PlanSelect(stmt, testPlannerTables("id"))
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != ScanTypeIndex {
		t.Fatalf("PlanSelect().ScanType = %q, want %q", plan.ScanType, ScanTypeIndex)
	}
	if plan.IndexScan == nil {
		t.Fatal("PlanSelect().IndexScan = nil, want value")
	}
	if plan.IndexScan.TableName != "users" || plan.IndexScan.ColumnName != "id" || plan.IndexScan.Value != parser.Int64Value(1) {
		t.Fatalf("PlanSelect().IndexScan = %#v, want users.id = 1", plan.IndexScan)
	}
}

func TestPlanSelectEqualityWithoutIndexFallsBackToTableScan(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT id FROM users WHERE id = 1")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := PlanSelect(stmt, testPlannerTables("name"))
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != ScanTypeTable || plan.TableScan == nil {
		t.Fatalf("PlanSelect() = %#v, want table scan", plan)
	}
}

func TestPlanSelectComplexWhereFallsBackToTableScan(t *testing.T) {
	tests := []string{
		"SELECT id FROM users WHERE id = 1 AND name = 'bob'",
		"SELECT id FROM users WHERE id = 1 OR id = 2",
		"SELECT id FROM users WHERE id > 1",
	}

	for _, sql := range tests {
		stmt, ok := parser.ParseSelectExpr(sql)
		if !ok {
			t.Fatalf("ParseSelectExpr(%q) ok = false, want true", sql)
		}

		plan, err := PlanSelect(stmt, testPlannerTables("id", "name"))
		if err != nil {
			t.Fatalf("PlanSelect(%q) error = %v", sql, err)
		}
		if plan.ScanType != ScanTypeTable || plan.TableScan == nil {
			t.Fatalf("PlanSelect(%q) = %#v, want table scan", sql, plan)
		}
	}
}
