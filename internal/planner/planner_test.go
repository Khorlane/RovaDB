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
	if plan.Query == nil || plan.Query.TableName != "" {
		t.Fatalf("PlanSelect().Query = %#v, want literal query with empty table name", plan.Query)
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
	if plan.IndexScan.TableName != "users" || plan.IndexScan.ColumnName != "id" || plan.IndexScan.LookupValue != Int64Value(1) {
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
	if plan.IndexScan == nil || plan.IndexScan.ColumnName != "id" || plan.IndexScan.LookupValue != Int64Value(1) {
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
	if plan.IndexScan == nil || plan.IndexScan.TableName != "users" || plan.IndexScan.ColumnName != "id" || plan.IndexScan.LookupValue != Int64Value(1) {
		t.Fatalf("PlanSelect().IndexScan = %#v, want users.id = 1", plan.IndexScan)
	}
}

func TestPlanSelectCountStarWithSimpleIndexUsesIndexOnlyScan(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT COUNT(*) FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := PlanSelect(stmt, testPlannerTables("id"))
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != ScanTypeIndexOnly {
		t.Fatalf("PlanSelect().ScanType = %q, want %q", plan.ScanType, ScanTypeIndexOnly)
	}
	if plan.IndexOnlyScan == nil {
		t.Fatal("PlanSelect().IndexOnlyScan = nil, want value")
	}
	if plan.IndexOnlyScan.TableName != "users" || !plan.IndexOnlyScan.CountStar || len(plan.IndexOnlyScan.ColumnNames) != 1 || plan.IndexOnlyScan.ColumnNames[0] != "id" {
		t.Fatalf("PlanSelect().IndexOnlyScan = %#v, want count-star index-only plan for users", plan.IndexOnlyScan)
	}
	if plan.TableScan != nil || plan.IndexScan != nil {
		t.Fatalf("PlanSelect() = %#v, want only index-only payload populated", plan)
	}
}

func TestPlanSelectCountStarWithWhereFallsBackToTableScan(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT COUNT(*) FROM users WHERE id > 1")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := PlanSelect(stmt, testPlannerTables("id"))
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != ScanTypeTable || plan.TableScan == nil {
		t.Fatalf("PlanSelect() = %#v, want table scan fallback", plan)
	}
	if plan.IndexOnlyScan != nil {
		t.Fatalf("PlanSelect().IndexOnlyScan = %#v, want nil fallback index-only scan", plan.IndexOnlyScan)
	}
}

func TestPlanSelectIndexedProjectionUsesIndexOnlyScan(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT id FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := PlanSelect(stmt, testPlannerTables("id"))
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != ScanTypeIndexOnly {
		t.Fatalf("PlanSelect().ScanType = %q, want %q", plan.ScanType, ScanTypeIndexOnly)
	}
	if plan.IndexOnlyScan == nil {
		t.Fatal("PlanSelect().IndexOnlyScan = nil, want value")
	}
	if plan.IndexOnlyScan.TableName != "users" || plan.IndexOnlyScan.CountStar || len(plan.IndexOnlyScan.ColumnNames) != 1 || plan.IndexOnlyScan.ColumnNames[0] != "id" {
		t.Fatalf("PlanSelect().IndexOnlyScan = %#v, want users indexed projection on id", plan.IndexOnlyScan)
	}
}

func TestPlanSelectAliasedIndexedProjectionFallsBackToTableScan(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT id AS user_id FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := PlanSelect(stmt, testPlannerTables("id"))
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != ScanTypeTable || plan.TableScan == nil {
		t.Fatalf("PlanSelect() = %#v, want table scan fallback", plan)
	}
	if plan.IndexOnlyScan != nil {
		t.Fatalf("PlanSelect().IndexOnlyScan = %#v, want nil fallback index-only scan", plan.IndexOnlyScan)
	}
}

func TestPlanSelectIndexedProjectionWithOrderByFallsBackToTableScan(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT id FROM users ORDER BY id")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := PlanSelect(stmt, testPlannerTables("id"))
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != ScanTypeTable || plan.TableScan == nil {
		t.Fatalf("PlanSelect() = %#v, want table scan fallback", plan)
	}
	if plan.IndexOnlyScan != nil {
		t.Fatalf("PlanSelect().IndexOnlyScan = %#v, want nil fallback index-only scan", plan.IndexOnlyScan)
	}
}

func TestPlanSelectMultiColumnProjectionFallsBackToTableScan(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT id, name FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := PlanSelect(stmt, testPlannerTables("id", "name"))
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != ScanTypeTable || plan.TableScan == nil {
		t.Fatalf("PlanSelect() = %#v, want table scan fallback", plan)
	}
	if plan.IndexOnlyScan != nil {
		t.Fatalf("PlanSelect().IndexOnlyScan = %#v, want nil fallback index-only scan", plan.IndexOnlyScan)
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

func TestPlanSelectNonIndexedProjectionFallsBackToTableScan(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT name FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := PlanSelect(stmt, testPlannerTables("id"))
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != ScanTypeTable || plan.TableScan == nil {
		t.Fatalf("PlanSelect() = %#v, want table scan fallback", plan)
	}
	if plan.IndexOnlyScan != nil {
		t.Fatalf("PlanSelect().IndexOnlyScan = %#v, want nil fallback index-only scan", plan.IndexOnlyScan)
	}
}

func TestPlanSelectExpressionProjectionFallsBackToTableScan(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT id + 1 FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	plan, err := PlanSelect(stmt, testPlannerTables("id"))
	if err != nil {
		t.Fatalf("PlanSelect() error = %v", err)
	}
	if plan.ScanType != ScanTypeTable || plan.TableScan == nil {
		t.Fatalf("PlanSelect() = %#v, want table scan fallback", plan)
	}
	if plan.IndexOnlyScan != nil {
		t.Fatalf("PlanSelect().IndexOnlyScan = %#v, want nil fallback index-only scan", plan.IndexOnlyScan)
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
		if plan.IndexOnlyScan != nil {
			t.Fatalf("PlanSelect(%q).IndexOnlyScan = %#v, want nil fallback index-only scan", sql, plan.IndexOnlyScan)
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
	if plan.IndexOnlyScan != nil {
		t.Fatalf("PlanSelect().IndexOnlyScan = %#v, want nil for join scan", plan.IndexOnlyScan)
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
	if plan.IndexOnlyScan != nil {
		t.Fatalf("PlanSelect().IndexOnlyScan = %#v, want nil for join scan", plan.IndexOnlyScan)
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

func TestChooseIndexScanUsesPlannerQueryTranslation(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT u.id FROM users AS u WHERE u.id = 7")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	scan := chooseIndexScan(queryFromParser(stmt), testPlannerTables("id"))
	if scan == nil {
		t.Fatal("chooseIndexScan() = nil, want value")
	}
	if scan.TableName != "users" || scan.ColumnName != "id" || scan.LookupValue != Int64Value(7) {
		t.Fatalf("chooseIndexScan() = %#v, want users.id = 7", scan)
	}
}

func TestChooseIndexOnlyScanUsesPlannerQueryTranslation(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT users.id FROM users")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	scan := chooseIndexOnlyScan(queryFromParser(stmt), testPlannerTables("id"))
	if scan == nil {
		t.Fatal("chooseIndexOnlyScan() = nil, want value")
	}
	if scan.TableName != "users" || scan.CountStar || len(scan.ColumnNames) != 1 || scan.ColumnNames[0] != "id" {
		t.Fatalf("chooseIndexOnlyScan() = %#v, want users indexed projection on id", scan)
	}
}

func TestChooseJoinScanUsesPlannerQueryTranslation(t *testing.T) {
	stmt, ok := parser.ParseSelectExpr("SELECT u.id FROM users u JOIN accounts a ON u.id = a.id")
	if !ok {
		t.Fatal("ParseSelectExpr() ok = false, want true")
	}

	scan, ok := chooseJoinScan(queryFromParser(stmt))
	if !ok {
		t.Fatal("chooseJoinScan() ok = false, want true")
	}
	if scan.LeftTableName != "users" || scan.LeftTableAlias != "u" || scan.LeftColumnName != "id" ||
		scan.RightTableName != "accounts" || scan.RightTableAlias != "a" || scan.RightColumnName != "id" {
		t.Fatalf("chooseJoinScan() = %#v, want users(u).id = accounts(a).id", scan)
	}
}

func TestValueFromParserPreservesTemporalTypedValues(t *testing.T) {
	tests := []struct {
		name string
		in   parser.Value
		want Value
	}{
		{name: "date", in: parser.DateValue(20553), want: DateValue(20553)},
		{name: "time", in: parser.TimeValue(49521), want: TimeValue(49521)},
		{name: "timestamp", in: parser.TimestampValue(1775828721000, 0), want: TimestampValue(1775828721000, 0)},
		{name: "timestamp unresolved", in: parser.TimestampUnresolvedValue(2026, 4, 10, 13, 45, 21), want: TimestampUnresolvedValue(2026, 4, 10, 13, 45, 21)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := valueFromParser(tc.in); got != tc.want {
				t.Fatalf("valueFromParser() = %#v, want %#v", got, tc.want)
			}
		})
	}
}
