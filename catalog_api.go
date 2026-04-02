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

	sum := sha256.Sum256(schemaDigestPayloadFromTables(db.tables))
	return hex.EncodeToString(sum[:]), nil
}

// SchemaDigestFromSystemCatalog returns the logical schema digest from __sys_* rows.
func (db *DB) SchemaDigestFromSystemCatalog() (string, error) {
	if db == nil {
		return "", ErrInvalidArgument
	}
	if db.closed {
		return "", ErrClosed
	}
	if err := db.validateTxnState(); err != nil {
		return "", err
	}

	payload, err := db.schemaDigestPayloadFromSystemCatalog()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// VerifySystemCatalogDigest verifies that in-memory and SQL-visible schema digests match.
func (db *DB) VerifySystemCatalogDigest() error {
	inMemoryDigest, err := db.SchemaDigest()
	if err != nil {
		return err
	}
	systemDigest, err := db.SchemaDigestFromSystemCatalog()
	if err != nil {
		return err
	}
	if inMemoryDigest != systemDigest {
		return newExecError("system catalog digest mismatch")
	}
	return nil
}

func schemaDigestPayloadFromTables(tables map[string]*executor.Table) []byte {
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

	var b schemaDigestBuilder
	for _, table := range userTables {
		b.WriteTable(table.TableID, table.Name)

		for ordinal, column := range table.Columns {
			b.WriteColumn(table.TableID, ordinal+1, column.Name, column.Type)
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
		b.WriteIndex(entry.IndexID, entry.TableID, entry.IndexDef.Name, entry.IndexDef.Unique)

		for ordinal, column := range entry.IndexDef.Columns {
			b.WriteIndexColumn(entry.IndexID, ordinal+1, column.Name)
		}
	}

	return b.Bytes()
}

type schemaDigestIndexEntry struct {
	TableID  uint32
	IndexID  uint32
	IndexDef storage.CatalogIndex
}

type schemaDigestBuilder struct {
	b strings.Builder
}

func (b *schemaDigestBuilder) WriteTable(tableID uint32, tableName string) {
	b.b.WriteString("T|")
	b.b.WriteString(strconv.FormatUint(uint64(tableID), 10))
	b.b.WriteString("|")
	b.b.WriteString(tableName)
	b.b.WriteString("\n")
}

func (b *schemaDigestBuilder) WriteColumn(tableID uint32, ordinal int, columnName, columnType string) {
	b.b.WriteString("C|")
	b.b.WriteString(strconv.FormatUint(uint64(tableID), 10))
	b.b.WriteString("|")
	b.b.WriteString(strconv.Itoa(ordinal))
	b.b.WriteString("|")
	b.b.WriteString(columnName)
	b.b.WriteString("|")
	b.b.WriteString(columnType)
	b.b.WriteString("\n")
}

func (b *schemaDigestBuilder) WriteIndex(indexID, tableID uint32, indexName string, isUnique bool) {
	b.b.WriteString("I|")
	b.b.WriteString(strconv.FormatUint(uint64(indexID), 10))
	b.b.WriteString("|")
	b.b.WriteString(strconv.FormatUint(uint64(tableID), 10))
	b.b.WriteString("|")
	b.b.WriteString(indexName)
	b.b.WriteString("|")
	if isUnique {
		b.b.WriteString("1")
	} else {
		b.b.WriteString("0")
	}
	b.b.WriteString("\n")
}

func (b *schemaDigestBuilder) WriteIndexColumn(indexID uint32, ordinal int, columnName string) {
	b.b.WriteString("K|")
	b.b.WriteString(strconv.FormatUint(uint64(indexID), 10))
	b.b.WriteString("|")
	b.b.WriteString(strconv.Itoa(ordinal))
	b.b.WriteString("|")
	b.b.WriteString(columnName)
	b.b.WriteString("\n")
}

func (b *schemaDigestBuilder) Bytes() []byte {
	return []byte(b.b.String())
}

func (db *DB) schemaDigestPayloadFromSystemCatalog() ([]byte, error) {
	var b schemaDigestBuilder

	rows, err := db.Query("SELECT table_id, table_name FROM __sys_tables ORDER BY table_id")
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	for rows.Next() {
		var tableID int
		var tableName string
		if err := rows.Scan(&tableID, &tableName); err != nil {
			return nil, err
		}
		b.WriteTable(uint32(tableID), tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = db.Query("SELECT table_id, column_name, column_type, ordinal_position FROM __sys_columns ORDER BY table_id, ordinal_position, column_name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var tableID int
		var columnName string
		var columnType string
		var ordinal int
		if err := rows.Scan(&tableID, &columnName, &columnType, &ordinal); err != nil {
			return nil, err
		}
		b.WriteColumn(uint32(tableID), ordinal, columnName, columnType)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = db.Query("SELECT index_id, index_name, table_id, is_unique FROM __sys_indexes ORDER BY index_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var indexID int
		var indexName string
		var tableID int
		var isUnique bool
		if err := rows.Scan(&indexID, &indexName, &tableID, &isUnique); err != nil {
			return nil, err
		}
		b.WriteIndex(uint32(indexID), uint32(tableID), indexName, isUnique)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = db.Query("SELECT index_id, column_name, ordinal_position FROM __sys_index_columns ORDER BY index_id, ordinal_position, column_name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var indexID int
		var columnName string
		var ordinal int
		if err := rows.Scan(&indexID, &columnName, &ordinal); err != nil {
			return nil, err
		}
		b.WriteIndexColumn(uint32(indexID), ordinal, columnName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
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
