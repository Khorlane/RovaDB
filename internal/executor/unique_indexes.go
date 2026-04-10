package executor

import (
	"math"
	"strconv"
	"strings"

	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

var errUniqueIndexDuplicateKeys = newExecError("duplicate indexed key values already exist")
var errUniqueIndexNullValues = newExecError("NULL exists in unique indexed key")

// ValidateUniqueIndexesForTable validates all unique index definitions against
// the table's current row set.
func ValidateUniqueIndexesForTable(table *Table) error {
	if table == nil {
		return nil
	}
	return validateUniqueIndexes(table, table.Rows)
}

func validateUniqueIndexes(table *Table, rows [][]parser.Value) error {
	if table == nil || len(table.IndexDefs) == 0 {
		return nil
	}

	columnPositions := make(map[string]int, len(table.Columns))
	for i, column := range table.Columns {
		columnPositions[column.Name] = i
	}

	for _, indexDef := range table.IndexDefs {
		if !indexDef.Unique {
			continue
		}
		if table.PrimaryKeyDef != nil && table.PrimaryKeyDef.IndexID == indexDef.IndexID {
			continue
		}

		seen := make(map[string]struct{}, len(rows))
		for _, row := range rows {
			key, err := buildUniqueIndexKey(row, columnPositions, indexDef)
			if err != nil {
				return err
			}
			if _, exists := seen[key]; exists {
				return errUniqueIndexDuplicateKeys
			}
			seen[key] = struct{}{}
		}
	}

	return nil
}

func buildUniqueIndexKey(row []parser.Value, columnPositions map[string]int, indexDef storage.CatalogIndex) (string, error) {
	parts := make([]string, 0, len(indexDef.Columns))
	for _, column := range indexDef.Columns {
		position, ok := columnPositions[column.Name]
		if !ok {
			return "", errColumnDoesNotExist
		}
		if position >= len(row) || row[position].Kind == parser.ValueKindNull {
			return "", errUniqueIndexNullValues
		}
		parts = append(parts, uniqueIndexKeyPart(row[position]))
	}
	return strings.Join(parts, "|"), nil
}

func uniqueIndexKeyPart(value parser.Value) string {
	switch value.Kind {
	case parser.ValueKindInt64:
		return "int:" + strconv.FormatInt(value.I64, 10)
	case parser.ValueKindString:
		return "string:" + value.Str
	case parser.ValueKindBool:
		if value.Bool {
			return "bool:true"
		}
		return "bool:false"
	case parser.ValueKindReal:
		return "real:" + strconv.FormatUint(math.Float64bits(value.F64), 10)
	default:
		return "invalid"
	}
}
