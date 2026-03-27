package main

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/Khorlane/RovaDB/internal/storage"
)

type cliCatalog struct {
	tables []cliTableInfo
}

type cliTableInfo struct {
	name    string
	columns []cliColumnInfo
}

type cliColumnInfo struct {
	name string
	kind string
}

func printTables(out io.Writer, path string) error {
	catalog, err := loadCLICatalog(path)
	if err != nil {
		return err
	}
	if len(catalog.tables) == 0 {
		return writeResponse(out, "no tables")
	}

	names := make([]string, 0, len(catalog.tables))
	for _, table := range catalog.tables {
		names = append(names, table.name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := writeResponse(out, name); err != nil {
			return err
		}
	}
	return nil
}

func printSchema(out io.Writer, path string, tableName string) error {
	catalog, err := loadCLICatalog(path)
	if err != nil {
		return err
	}
	table := catalog.findTable(tableName)
	if table == nil {
		return fmt.Errorf("table not found: %s", tableName)
	}
	if err := writeResponse(out, "table: %s", table.name); err != nil {
		return err
	}
	for _, column := range table.columns {
		if err := writeResponse(out, "%s %s", column.name, column.kind); err != nil {
			return err
		}
	}
	return nil
}

func loadCLICatalog(path string) (*cliCatalog, error) {
	file, err := storage.OpenOrCreate(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = file.Close()
	}()

	pager, err := storage.NewPager(file.File())
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = pager.Close()
	}()

	stored, err := storage.LoadCatalog(pager)
	if err != nil {
		return nil, err
	}
	return cliCatalogFromStorage(stored), nil
}

func cliCatalogFromStorage(catalog *storage.CatalogData) *cliCatalog {
	if catalog == nil {
		return &cliCatalog{}
	}
	info := &cliCatalog{tables: make([]cliTableInfo, 0, len(catalog.Tables))}
	for _, table := range catalog.Tables {
		columns := make([]cliColumnInfo, 0, len(table.Columns))
		for _, column := range table.Columns {
			columns = append(columns, cliColumnInfo{
				name: column.Name,
				kind: cliColumnTypeName(column.Type),
			})
		}
		info.tables = append(info.tables, cliTableInfo{
			name:    table.Name,
			columns: columns,
		})
	}
	return info
}

func (catalog *cliCatalog) findTable(tableName string) *cliTableInfo {
	if catalog == nil {
		return nil
	}
	for i := range catalog.tables {
		if catalog.tables[i].name == tableName {
			return &catalog.tables[i]
		}
	}
	for i := range catalog.tables {
		if strings.EqualFold(catalog.tables[i].name, tableName) {
			return &catalog.tables[i]
		}
	}
	return nil
}

func (catalog *cliCatalog) isEmpty() bool {
	return catalog == nil || len(catalog.tables) == 0
}

func cliColumnTypeName(columnType uint8) string {
	switch columnType {
	case storage.CatalogColumnTypeInt:
		return "INT"
	case storage.CatalogColumnTypeText:
		return "TEXT"
	case storage.CatalogColumnTypeBool:
		return "BOOL"
	case storage.CatalogColumnTypeReal:
		return "REAL"
	default:
		return "UNKNOWN"
	}
}
