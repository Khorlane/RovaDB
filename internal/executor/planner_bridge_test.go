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
	if bridge.scanType != planner.ScanTypeTable {
		t.Fatalf("bridge.scanType = %q, want %q", bridge.scanType, planner.ScanTypeTable)
	}
	if bridge.table.tableName != "users" {
		t.Fatalf("bridge.table.tableName = %q, want users", bridge.table.tableName)
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
	if bridge.scanType != planner.ScanTypeIndex {
		t.Fatalf("bridge.scanType = %q, want %q", bridge.scanType, planner.ScanTypeIndex)
	}
	if bridge.index.tableName != "users" || bridge.index.columnName != "name" {
		t.Fatalf("bridge.index = %#v, want users/name", bridge.index)
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
	if bridge.scanType != planner.ScanTypeJoin {
		t.Fatalf("bridge.scanType = %q, want %q", bridge.scanType, planner.ScanTypeJoin)
	}
	if bridge.join.leftTableName != "users" || bridge.join.rightTableName != "departments" {
		t.Fatalf("bridge.join = %#v, want users/departments", bridge.join)
	}
	if bridge.join.leftTableAlias != "u" || bridge.join.rightTableAlias != "d" {
		t.Fatalf("bridge.join aliases = %#v, want u/d", bridge.join)
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
