package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/planner"
)

func TestBridgeSelectPlanTableScan(t *testing.T) {
	bridge, err := bridgeSelectPlan(&planner.SelectPlan{
		Query:    &planner.SelectQuery{TableName: "users"},
		ScanType: planner.ScanTypeTable,
		TableScan: &planner.TableScan{
			TableName: "users",
		},
	})
	if err != nil {
		t.Fatalf("bridgeSelectPlan() error = %v", err)
	}
	if bridge.scanKind != selectScanKindTable {
		t.Fatalf("bridge.scanKind = %v, want %v", bridge.scanKind, selectScanKindTable)
	}
	if bridge.accessPath.Kind != SelectAccessPathKindTable {
		t.Fatalf("bridge.accessPath.Kind = %v, want %v", bridge.accessPath.Kind, SelectAccessPathKindTable)
	}
	if bridge.accessPath.SingleTableName != "users" {
		t.Fatalf("bridge.accessPath.SingleTableName = %q, want users", bridge.accessPath.SingleTableName)
	}
}

func TestBridgeSelectPlanIndexScan(t *testing.T) {
	bridge, err := bridgeSelectPlan(&planner.SelectPlan{
		Query:    &planner.SelectQuery{TableName: "users"},
		ScanType: planner.ScanTypeIndex,
		IndexScan: &planner.IndexScan{
			TableName:   "users",
			ColumnName:  "name",
			LookupValue: planner.StringValue("alice"),
		},
	})
	if err != nil {
		t.Fatalf("bridgeSelectPlan() error = %v", err)
	}
	if bridge.scanKind != selectScanKindIndex {
		t.Fatalf("bridge.scanKind = %v, want %v", bridge.scanKind, selectScanKindIndex)
	}
	if bridge.accessPath.Kind != SelectAccessPathKindIndex {
		t.Fatalf("bridge.accessPath.Kind = %v, want %v", bridge.accessPath.Kind, SelectAccessPathKindIndex)
	}
	if bridge.accessPath.IndexLookup.TableName != "users" || bridge.accessPath.IndexLookup.ColumnName != "name" {
		t.Fatalf("bridge.accessPath.IndexLookup = %#v, want users/name", bridge.accessPath.IndexLookup)
	}
	if got := bridge.accessPath.IndexLookup.LookupValue; got.Kind != planner.StringValue("alice").ParserValue().Kind || got.Str != "alice" {
		t.Fatalf("bridge.accessPath.IndexLookup.LookupValue = %#v, want alice", got)
	}
}

func TestBridgeSelectPlanJoinScan(t *testing.T) {
	bridge, err := bridgeSelectPlan(&planner.SelectPlan{
		Query:    &planner.SelectQuery{TableName: "users"},
		ScanType: planner.ScanTypeJoin,
		JoinScan: &planner.JoinScan{
			LeftTableName:   "users",
			LeftTableAlias:  "u",
			LeftColumnName:  "dept_id",
			RightTableName:  "departments",
			RightTableAlias: "d",
			RightColumnName: "id",
		},
	})
	if err != nil {
		t.Fatalf("bridgeSelectPlan() error = %v", err)
	}
	if bridge.scanKind != selectScanKindJoin {
		t.Fatalf("bridge.scanKind = %v, want %v", bridge.scanKind, selectScanKindJoin)
	}
	if bridge.accessPath.Kind != SelectAccessPathKindJoin {
		t.Fatalf("bridge.accessPath.Kind = %v, want %v", bridge.accessPath.Kind, SelectAccessPathKindJoin)
	}
	if bridge.accessPath.JoinLeftTable != "users" || bridge.accessPath.JoinRightTable != "departments" {
		t.Fatalf("bridge.accessPath = %#v, want users/departments", bridge.accessPath)
	}
	if bridge.join.leftTableAlias != "u" || bridge.join.rightTableAlias != "d" {
		t.Fatalf("bridge.join aliases = %#v, want u/d", bridge.join)
	}
}

func TestDescribeSelectAccessPathReturnsJoinTables(t *testing.T) {
	accessPath, err := DescribeSelectAccessPath(&planner.SelectPlan{
		Query:    &planner.SelectQuery{TableName: "users"},
		ScanType: planner.ScanTypeJoin,
		JoinScan: &planner.JoinScan{
			LeftTableName:   "users",
			LeftColumnName:  "dept_id",
			RightTableName:  "departments",
			RightColumnName: "id",
		},
	})
	if err != nil {
		t.Fatalf("DescribeSelectAccessPath() error = %v", err)
	}
	if accessPath.Kind != SelectAccessPathKindJoin {
		t.Fatalf("DescribeSelectAccessPath().Kind = %v, want %v", accessPath.Kind, SelectAccessPathKindJoin)
	}
	if accessPath.JoinLeftTable != "users" || accessPath.JoinRightTable != "departments" {
		t.Fatalf("DescribeSelectAccessPath() = %#v, want users/departments", accessPath)
	}
}

func TestNewSelectExecutionHandoffExposesAccessPath(t *testing.T) {
	handoff, err := NewSelectExecutionHandoff(&planner.SelectPlan{
		Query:    &planner.SelectQuery{TableName: "users"},
		ScanType: planner.ScanTypeIndex,
		IndexScan: &planner.IndexScan{
			TableName:   "users",
			ColumnName:  "name",
			LookupValue: planner.StringValue("alice"),
		},
	})
	if err != nil {
		t.Fatalf("NewSelectExecutionHandoff() error = %v", err)
	}

	accessPath := handoff.AccessPath()
	if accessPath.Kind != SelectAccessPathKindIndex {
		t.Fatalf("handoff.AccessPath().Kind = %v, want %v", accessPath.Kind, SelectAccessPathKindIndex)
	}
	if accessPath.IndexLookup.TableName != "users" || accessPath.IndexLookup.ColumnName != "name" {
		t.Fatalf("handoff.AccessPath().IndexLookup = %#v, want users/name", accessPath.IndexLookup)
	}
}

func TestBridgeSelectPlanRejectsMismatchedIndexScanTable(t *testing.T) {
	_, err := bridgeSelectPlan(&planner.SelectPlan{
		Query:    &planner.SelectQuery{TableName: "users"},
		ScanType: planner.ScanTypeIndex,
		IndexScan: &planner.IndexScan{
			TableName:   "accounts",
			ColumnName:  "name",
			LookupValue: planner.StringValue("alice"),
		},
	})
	if err != errInvalidSelectPlan {
		t.Fatalf("bridgeSelectPlan() error = %v, want %v", err, errInvalidSelectPlan)
	}
}
