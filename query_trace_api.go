package rovadb

import (
	"strings"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/planner"
)

// QueryExecutionTrace is a compact read-only summary of planned query shape.
type QueryExecutionTrace struct {
	ScanType  string
	TableName string
	IndexName string
	UsesBTree bool
}

// ExplainQueryPath reports the high-level planned execution shape for a query.
func (db *DB) ExplainQueryPath(sql string, args ...any) (QueryExecutionTrace, error) {
	if db == nil {
		return QueryExecutionTrace{}, ErrInvalidArgument
	}
	if db.closed {
		return QueryExecutionTrace{}, ErrClosed
	}
	if strings.TrimSpace(sql) == "" {
		return QueryExecutionTrace{}, ErrInvalidArgument
	}
	if err := db.validateTxnState(); err != nil {
		return QueryExecutionTrace{}, err
	}

	stmt, err := parser.Parse(sql)
	if err != nil {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SELECT ") {
			return QueryExecutionTrace{}, classifyQueryParseError(sql)
		}
		return QueryExecutionTrace{}, err
	}
	sel, ok := stmt.(*parser.SelectExpr)
	if !ok {
		return QueryExecutionTrace{}, ErrQueryRequiresSelect
	}
	if err := parser.BindPlaceholders(stmt, args); err != nil {
		return QueryExecutionTrace{}, err
	}

	trace := QueryExecutionTrace{}
	if sel.TableName == "" {
		return trace, nil
	}
	if err := validateTables(db.tables); err != nil {
		return QueryExecutionTrace{}, err
	}

	plan, err := planner.PlanSelect(sel, plannerTableMetadata(db.tables))
	if err != nil {
		return QueryExecutionTrace{}, err
	}
	if plan.ScanType == planner.ScanTypeTable {
		if tableName, columnName, ok := simpleEqualityPlanningTarget(sel); ok {
			table := db.tables[tableName]
			if hasMalformedSimpleLogicalIndex(table, columnName) {
				return QueryExecutionTrace{}, newExecError("invalid select plan")
			}
		}
	}

	switch plan.ScanType {
	case planner.ScanTypeTable:
		trace.ScanType = "table"
		trace.TableName = plan.Stmt.TableName
	case planner.ScanTypeIndex:
		table := db.tables[plan.IndexScan.TableName]
		if table == nil {
			return QueryExecutionTrace{}, newExecError("table not found: " + plan.IndexScan.TableName)
		}
		indexDef, err := db.resolveSimpleLogicalIndex(table, plan.IndexScan.ColumnName)
		if err != nil {
			return QueryExecutionTrace{}, err
		}
		indexDef, err = db.validateIndexLookupMetadata(table, indexDef)
		if err != nil {
			return QueryExecutionTrace{}, err
		}
		trace.ScanType = "index"
		trace.TableName = plan.IndexScan.TableName
		trace.IndexName = indexDef.Name
		trace.UsesBTree = true
	case planner.ScanTypeJoin:
		trace.ScanType = "join"
	default:
		trace.ScanType = string(plan.ScanType)
	}

	return trace, nil
}
