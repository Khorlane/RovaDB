package planner

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

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
