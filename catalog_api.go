package rovadb

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/storage"
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

// SchemaDigest returns a deterministic digest of the current logical schema.
func (db *DB) SchemaDigest() (string, error) {
	if db == nil {
		return "", ErrInvalidArgument
	}
	if db.closed {
		return "", ErrClosed
	}
	if err := db.validateTxnState(); err != nil {
		return "", err
	}

	sum := sha256.Sum256(schemaDigestPayload(db.tables))
	return hex.EncodeToString(sum[:]), nil
}

func schemaDigestPayload(tables map[string]*executor.Table) []byte {
	if len(tables) == 0 {
		return nil
	}

	userTables := make([]*executor.Table, 0, len(tables))
	for _, table := range tables {
		if table == nil || table.IsSystem {
			continue
		}
		userTables = append(userTables, table)
	}

	sort.Slice(userTables, func(i, j int) bool {
		if userTables[i].TableID != userTables[j].TableID {
			return userTables[i].TableID < userTables[j].TableID
		}
		return userTables[i].Name < userTables[j].Name
	})

	var b strings.Builder
	for _, table := range userTables {
		b.WriteString("T|")
		b.WriteString(strconv.FormatUint(uint64(table.TableID), 10))
		b.WriteString("|")
		b.WriteString(table.Name)
		b.WriteString("\n")

		for ordinal, column := range table.Columns {
			b.WriteString("C|")
			b.WriteString(strconv.FormatUint(uint64(table.TableID), 10))
			b.WriteString("|")
			b.WriteString(strconv.Itoa(ordinal + 1))
			b.WriteString("|")
			b.WriteString(column.Name)
			b.WriteString("|")
			b.WriteString(column.Type)
			b.WriteString("\n")
		}
	}

	indexEntries := make([]schemaDigestIndexEntry, 0)
	for _, table := range userTables {
		for _, indexDef := range table.IndexDefs {
			indexEntries = append(indexEntries, schemaDigestIndexEntry{
				TableID:  table.TableID,
				IndexID:  indexDef.IndexID,
				IndexDef: indexDef,
			})
		}
	}
	sort.Slice(indexEntries, func(i, j int) bool {
		if indexEntries[i].IndexID != indexEntries[j].IndexID {
			return indexEntries[i].IndexID < indexEntries[j].IndexID
		}
		if indexEntries[i].TableID != indexEntries[j].TableID {
			return indexEntries[i].TableID < indexEntries[j].TableID
		}
		return indexEntries[i].IndexDef.Name < indexEntries[j].IndexDef.Name
	})

	for _, entry := range indexEntries {
		b.WriteString("I|")
		b.WriteString(strconv.FormatUint(uint64(entry.IndexID), 10))
		b.WriteString("|")
		b.WriteString(strconv.FormatUint(uint64(entry.TableID), 10))
		b.WriteString("|")
		b.WriteString(entry.IndexDef.Name)
		b.WriteString("|")
		if entry.IndexDef.Unique {
			b.WriteString("1")
		} else {
			b.WriteString("0")
		}
		b.WriteString("\n")

		for ordinal, column := range entry.IndexDef.Columns {
			b.WriteString("K|")
			b.WriteString(strconv.FormatUint(uint64(entry.IndexID), 10))
			b.WriteString("|")
			b.WriteString(strconv.Itoa(ordinal + 1))
			b.WriteString("|")
			b.WriteString(column.Name)
			b.WriteString("\n")
		}
	}

	return []byte(b.String())
}

type schemaDigestIndexEntry struct {
	TableID  uint32
	IndexID  uint32
	IndexDef storage.CatalogIndex
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
