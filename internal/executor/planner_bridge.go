package executor

import (
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

func parserValueFromPlan(value planner.Value) parser.Value {
	return value.ParserValue()
}

type selectPlanBridge struct {
	query    *planner.SelectQuery
	scanType planner.ScanType
	table    selectPlanTableScan
	index    selectPlanIndexScan
	join     selectPlanJoinScan
}

type selectPlanTableScan struct {
	tableName string
}

type selectPlanIndexScan struct {
	tableName  string
	columnName string
}

type selectPlanJoinScan struct {
	leftTableName   string
	leftTableAlias  string
	leftColumnName  string
	rightTableName  string
	rightTableAlias string
	rightColumnName string
}

func bridgeSelectPlan(plan *planner.SelectPlan) (*selectPlanBridge, error) {
	if plan == nil || plan.Query == nil {
		return nil, errUnsupportedStatement
	}

	bridge := &selectPlanBridge{
		query:    plan.Query,
		scanType: plan.ScanType,
	}
	if bridge.query.TableName == "" {
		return bridge, nil
	}

	switch plan.ScanType {
	case planner.ScanTypeTable:
		if plan.TableScan == nil || plan.TableScan.TableName != plan.Query.TableName {
			return nil, errInvalidSelectPlan
		}
		bridge.table = selectPlanTableScan{tableName: plan.TableScan.TableName}
	case planner.ScanTypeIndex:
		if plan.IndexScan == nil || plan.IndexScan.TableName != plan.Query.TableName || plan.IndexScan.ColumnName == "" {
			return nil, errInvalidSelectPlan
		}
		bridge.index = selectPlanIndexScan{
			tableName:  plan.IndexScan.TableName,
			columnName: plan.IndexScan.ColumnName,
		}
	case planner.ScanTypeJoin:
		if plan.JoinScan == nil || plan.JoinScan.LeftTableName == "" || plan.JoinScan.RightTableName == "" || plan.JoinScan.LeftColumnName == "" || plan.JoinScan.RightColumnName == "" {
			return nil, errInvalidSelectPlan
		}
		bridge.join = selectPlanJoinScan{
			leftTableName:   plan.JoinScan.LeftTableName,
			leftTableAlias:  plan.JoinScan.LeftTableAlias,
			leftColumnName:  plan.JoinScan.LeftColumnName,
			rightTableName:  plan.JoinScan.RightTableName,
			rightTableAlias: plan.JoinScan.RightTableAlias,
			rightColumnName: plan.JoinScan.RightColumnName,
		}
	default:
		return nil, errInvalidSelectPlan
	}
	return bridge, nil
}

func (b *selectPlanBridge) singleTable(tableMap map[string]*Table) (*Table, error) {
	if b == nil {
		return nil, errInvalidSelectPlan
	}

	tableName := ""
	switch b.scanType {
	case planner.ScanTypeTable:
		tableName = b.table.tableName
	case planner.ScanTypeIndex:
		tableName = b.index.tableName
	default:
		return nil, errInvalidSelectPlan
	}

	table, ok := tableMap[tableName]
	if !ok {
		return nil, newTableNotFoundError(tableName)
	}
	return table, nil
}

func (b *selectPlanBridge) joinTables(tableMap map[string]*Table) (*Table, *Table, error) {
	if b == nil || b.scanType != planner.ScanTypeJoin {
		return nil, nil, errInvalidSelectPlan
	}

	leftTable := tableMap[b.join.leftTableName]
	if leftTable == nil {
		return nil, nil, newTableNotFoundError(b.join.leftTableName)
	}
	rightTable := tableMap[b.join.rightTableName]
	if rightTable == nil {
		return nil, nil, newTableNotFoundError(b.join.rightTableName)
	}
	return leftTable, rightTable, nil
}
