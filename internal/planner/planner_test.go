package planner

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func testPlannerTables(indexedColumns ...string) map[string]*TableMetadata {
	indexes := make(map[string]SimpleIndex, len(indexedColumns))
	for i, columnName := range indexedColumns {
		indexes[columnName] = SimpleIndex{
			TableName:  "users",
			ColumnName: columnName,
			IndexID:    uint32(i + 1),
			RootPageID: uint32(i + 10),
		}
	}
	return map[string]*TableMetadata{
		"users": {SimpleIndexes: indexes},
	}
}

func TestPlanSelectIgnoresLegacyPostingContentForSimpleIndexEligibility(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT id FROM users WHERE id = 1")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := PlanSelect(stmt, map[string]*TableMetadata{
		"users": {
			SimpleIndexes: map[string]SimpleIndex{
				"id": {TableName: "users", ColumnName: "id", IndexID: 1, RootPageID: 9},
			},
		},
	})
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != ScanTypeIndex || plan.IndexScan == nil {
		t.Fatalf("PlanSelect() = %#v, want simple index scan", plan)
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
	if plan.TableScan != nil {
		t.Fatalf("PlanSelect().TableScan = %#v, want nil for index scan", plan.TableScan)
	}
}

func TestPlanSelectQualifiedEqualityWithIndexUsesIndexScan(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT users.id FROM users WHERE users.id = 1")
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
	if plan.IndexScan == nil || plan.IndexScan.ColumnName != "id" || plan.IndexScan.Value != parser.Int64Value(1) {
		t.Fatalf("PlanSelect().IndexScan = %#v, want users.id = 1", plan.IndexScan)
	}
}

func TestPlanSelectAliasQualifiedEqualityWithIndexUsesIndexScan(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT u.id FROM users AS u WHERE u.id = 1")
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
	if plan.IndexScan == nil || plan.IndexScan.TableName != "users" || plan.IndexScan.ColumnName != "id" || plan.IndexScan.Value != parser.Int64Value(1) {
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
		if plan.IndexScan != nil {
			t.Fatalf("PlanSelect(%q).IndexScan = %#v, want nil fallback index scan", sql, plan.IndexScan)
		}
	}
}

func TestPlanSelectCommaJoinUsesJoinScan(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT u.id FROM users u, accounts a WHERE u.id = a.id AND a.name = 'eng'")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := PlanSelect(stmt)
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != ScanTypeJoin {
		t.Fatalf("PlanSelect().ScanType = %q, want %q", plan.ScanType, ScanTypeJoin)
	}
	if plan.JoinScan == nil {
		t.Fatal("PlanSelect().JoinScan = nil, want value")
	}
	if plan.JoinScan.LeftTableName != "users" || plan.JoinScan.RightTableName != "accounts" || plan.JoinScan.LeftColumnName != "id" || plan.JoinScan.RightColumnName != "id" {
		t.Fatalf("PlanSelect().JoinScan = %#v, want users.id = accounts.id", plan.JoinScan)
	}
}

func TestPlanSelectExplicitJoinUsesJoinScan(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT u.id FROM users u JOIN accounts a ON u.id = a.id")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := PlanSelect(stmt)
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != ScanTypeJoin {
		t.Fatalf("PlanSelect().ScanType = %q, want %q", plan.ScanType, ScanTypeJoin)
	}
	if plan.JoinScan == nil {
		t.Fatal("PlanSelect().JoinScan = nil, want value")
	}
	if plan.JoinScan.LeftTableName != "users" || plan.JoinScan.RightTableName != "accounts" || plan.JoinScan.LeftColumnName != "id" || plan.JoinScan.RightColumnName != "id" {
		t.Fatalf("PlanSelect().JoinScan = %#v, want users.id = accounts.id", plan.JoinScan)
	}
}

func TestPlanSelectIsDeterministicForIndexedEquality(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT id FROM users WHERE id = 1")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan1, err := PlanSelect(stmt, testPlannerTables("id"))
	if err != nil {
		t.Fatalf("first PlanSelect() error = %v", err)
	}
	plan2, err := PlanSelect(stmt, testPlannerTables("id"))
	if err != nil {
		t.Fatalf("second PlanSelect() error = %v", err)
	}

	if plan1.ScanType != ScanTypeIndex || plan2.ScanType != ScanTypeIndex {
		t.Fatalf("scan types = (%q,%q), want both %q", plan1.ScanType, plan2.ScanType, ScanTypeIndex)
	}
	if plan1.IndexScan == nil || plan2.IndexScan == nil {
		t.Fatalf("index scans = (%#v,%#v), want both non-nil", plan1.IndexScan, plan2.IndexScan)
	}
	if *plan1.IndexScan != *plan2.IndexScan {
		t.Fatalf("index scans differ: %#v vs %#v", plan1.IndexScan, plan2.IndexScan)
	}
}
