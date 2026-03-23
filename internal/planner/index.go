package planner

import (
	"errors"
	"strconv"

	"github.com/Khorlane/RovaDB/internal/parser"
)

var errIndexColumnDoesNotExist = errors.New("planner: index column does not exist")

// IndexKey is the normalized comparable key used by basic indexes.
type IndexKey string

// BasicIndex is a minimal single-column equality index.
type BasicIndex struct {
	TableName  string
	ColumnName string
	Entries    map[IndexKey][]int
}

// NewBasicIndex builds an empty single-column in-memory index.
func NewBasicIndex(tableName, columnName string) *BasicIndex {
	return &BasicIndex{
		TableName:  tableName,
		ColumnName: columnName,
		Entries:    make(map[IndexKey][]int),
	}
}

func normalizeIndexKey(v parser.Value) IndexKey {
	switch v.Kind {
	case parser.ValueKindNull:
		return "null"
	case parser.ValueKindInt64:
		return IndexKey("int:" + strconv.FormatInt(v.I64, 10))
	case parser.ValueKindString:
		return IndexKey("string:" + v.Str)
	default:
		return "invalid"
	}
}

// Rebuild clears and rebuilds the index from the given rows.
func (idx *BasicIndex) Rebuild(columns []string, rows [][]parser.Value) error {
	if idx == nil {
		return nil
	}

	columnIndex := -1
	for i, name := range columns {
		if name == idx.ColumnName {
			columnIndex = i
			break
		}
	}
	if columnIndex < 0 {
		return errIndexColumnDoesNotExist
	}

	idx.Entries = make(map[IndexKey][]int)
	for rowIndex, row := range rows {
		if columnIndex >= len(row) {
			continue
		}
		key := normalizeIndexKey(row[columnIndex])
		idx.Entries[key] = append(idx.Entries[key], rowIndex)
	}
	return nil
}

// LookupEqual returns a copy of the indexed row positions for the given value.
func (idx *BasicIndex) LookupEqual(v parser.Value) []int {
	if idx == nil || idx.Entries == nil {
		return nil
	}

	matches := idx.Entries[normalizeIndexKey(v)]
	if len(matches) == 0 {
		return nil
	}
	return append([]int(nil), matches...)
}
