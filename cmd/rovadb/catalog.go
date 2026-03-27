package main

import (
	"io"
	"sort"

	rovadb "github.com/Khorlane/RovaDB"
)

func printTables(out io.Writer, db *rovadb.DB) error {
	tables, err := db.ListTables()
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return writeResponse(out, "no tables")
	}

	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := writeResponse(out, name); err != nil {
			return err
		}
	}
	return nil
}

func printSchema(out io.Writer, db *rovadb.DB, tableName string) error {
	table, err := db.GetTableSchema(tableName)
	if err != nil {
		return err
	}
	if err := writeResponse(out, "table: %s", table.Name); err != nil {
		return err
	}
	for _, column := range table.Columns {
		if err := writeResponse(out, "%s %s", column.Name, column.Type); err != nil {
			return err
		}
	}
	return nil
}
