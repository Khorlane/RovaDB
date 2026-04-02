package rovadb

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Khorlane/RovaDB/internal/executor"
)

// TableInfo describes one table in the public catalog API.
type TableInfo struct {
	Name    string
	Columns []ColumnInfo
}

// ColumnInfo describes one column in the public catalog API.
type ColumnInfo struct {
	Name string
	Type string
}

// ListTables returns catalog metadata for all tables in the open database.
func (db *DB) ListTables() ([]TableInfo, error) {
	if db == nil {
		return nil, ErrInvalidArgument
	}
	if db.closed {
		return nil, ErrClosed
	}
	if err := db.validateTxnState(); err != nil {
		return nil, err
	}

	return publicTableInfos(db.tables), nil
}

// GetTableSchema returns catalog metadata for one table in the open database.
func (db *DB) GetTableSchema(table string) (TableInfo, error) {
	if db == nil {
		return TableInfo{}, ErrInvalidArgument
	}
	if db.closed {
		return TableInfo{}, ErrClosed
	}
	if strings.TrimSpace(table) == "" {
		return TableInfo{}, ErrInvalidArgument
	}
	if err := db.validateTxnState(); err != nil {
		return TableInfo{}, err
	}

	info, ok := findPublicTableInfo(db.tables, table)
	if !ok {
		return TableInfo{}, fmt.Errorf("table not found: %s", table)
	}
	return info, nil
}

func publicTableInfos(tables map[string]*executor.Table) []TableInfo {
	if len(tables) == 0 {
		return []TableInfo{}
	}

	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)

	info := make([]TableInfo, 0, len(names))
	for _, name := range names {
		table := tables[name]
		if table == nil || table.IsSystem {
			continue
		}
		info = append(info, publicTableInfo(table))
	}
	return info
}

func findPublicTableInfo(tables map[string]*executor.Table, tableName string) (TableInfo, bool) {
	if len(tables) == 0 {
		return TableInfo{}, false
	}

	if table, ok := tables[tableName]; ok && table != nil {
		if table.IsSystem {
			return TableInfo{}, false
		}
		return publicTableInfo(table), true
	}
	for name, table := range tables {
		if table == nil || table.IsSystem {
			continue
		}
		if strings.EqualFold(name, tableName) {
			return publicTableInfo(table), true
		}
	}
	return TableInfo{}, false
}

func publicTableInfo(table *executor.Table) TableInfo {
	if table == nil {
		return TableInfo{}
	}

	columns := make([]ColumnInfo, 0, len(table.Columns))
	for _, column := range table.Columns {
		columns = append(columns, ColumnInfo{
			Name: column.Name,
			Type: column.Type,
		})
	}
	return TableInfo{
		Name:    table.Name,
		Columns: columns,
	}
}
