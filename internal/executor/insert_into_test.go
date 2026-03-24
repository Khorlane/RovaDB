package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestExecuteInsert(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}}},
	}

	affected, err := Execute(&parser.InsertStmt{
		TableName: "users",
		Values:    []parser.Value{parser.Int64Value(1), parser.StringValue("steve")},
	}, tables)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if affected != 1 {
		t.Fatalf("Execute() affected = %d, want 1", affected)
	}
	row := tables["users"].Rows[0]
	if len(row) != 2 || row[0] != parser.Int64Value(1) || row[1] != parser.StringValue("steve") {
		t.Fatalf("Execute() row = %#v, want [1 'steve']", row)
	}
}

func TestExecuteInsertWithColumnList(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}}},
	}

	affected, err := Execute(&parser.InsertStmt{
		TableName: "users",
		Columns:   []string{"id", "name"},
		Values:    []parser.Value{parser.Int64Value(1), parser.StringValue("steve")},
	}, tables)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if affected != 1 {
		t.Fatalf("Execute() affected = %d, want 1", affected)
	}
}

func TestExecuteInsertWithReorderedColumnList(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}}},
	}

	affected, err := Execute(&parser.InsertStmt{
		TableName: "users",
		Columns:   []string{"name", "id"},
		Values:    []parser.Value{parser.StringValue("steve"), parser.Int64Value(1)},
	}, tables)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if affected != 1 {
		t.Fatalf("Execute() affected = %d, want 1", affected)
	}
	row := tables["users"].Rows[0]
	if len(row) != 2 || row[0] != parser.Int64Value(1) || row[1] != parser.StringValue("steve") {
		t.Fatalf("Execute() row = %#v, want [1 'steve']", row)
	}
}

func TestExecuteInsertMissingTable(t *testing.T) {
	_, err := Execute(&parser.InsertStmt{
		TableName: "users",
		Values:    []parser.Value{parser.Int64Value(1)},
	}, map[string]*Table{})
	if err == nil {
		t.Fatal("Execute() error = nil, want missing table error")
	}
}

func TestExecuteInsertWrongValueCount(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}}},
	}

	_, err := Execute(&parser.InsertStmt{
		TableName: "users",
		Values:    []parser.Value{parser.Int64Value(1)},
	}, tables)
	if err == nil {
		t.Fatal("Execute() error = nil, want wrong value count error")
	}
}

func TestExecuteInsertUnknownColumn(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}}},
	}

	_, err := Execute(&parser.InsertStmt{
		TableName: "users",
		Columns:   []string{"id", "email"},
		Values:    []parser.Value{parser.Int64Value(1), parser.StringValue("steve")},
	}, tables)
	if err != errColumnDoesNotExist {
		t.Fatalf("Execute() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestExecuteInsertNotAllColumnsSpecified(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}}},
	}

	_, err := Execute(&parser.InsertStmt{
		TableName: "users",
		Columns:   []string{"id"},
		Values:    []parser.Value{parser.Int64Value(1)},
	}, tables)
	if err != errWrongValueCount {
		t.Fatalf("Execute() error = %v, want %v", err, errWrongValueCount)
	}
}

func TestExecuteInsertWrongType(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}}},
	}

	_, err := Execute(&parser.InsertStmt{
		TableName: "users",
		Values:    []parser.Value{parser.StringValue("steve"), parser.StringValue("bob")},
	}, tables)
	if err != errTypeMismatch {
		t.Fatalf("Execute() error = %v, want %v", err, errTypeMismatch)
	}
}

func TestExecuteInsertColumnListWrongType(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}}},
	}

	_, err := Execute(&parser.InsertStmt{
		TableName: "users",
		Columns:   []string{"name", "id"},
		Values:    []parser.Value{parser.StringValue("steve"), parser.StringValue("oops")},
	}, tables)
	if err != errTypeMismatch {
		t.Fatalf("Execute() error = %v, want %v", err, errTypeMismatch)
	}
}

func TestExecuteInsertNullValue(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}}},
	}

	affected, err := Execute(&parser.InsertStmt{
		TableName: "users",
		Values:    []parser.Value{parser.Int64Value(1), parser.NullValue()},
	}, tables)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if affected != 1 {
		t.Fatalf("Execute() affected = %d, want 1", affected)
	}
	if tables["users"].Rows[0][1] != parser.NullValue() {
		t.Fatalf("Execute() row[1] = %#v, want NULL", tables["users"].Rows[0][1])
	}
}

func TestExecuteInsertBoolValue(t *testing.T) {
	tests := []struct {
		name  string
		value parser.Value
	}{
		{name: "true", value: parser.BoolValue(true)},
		{name: "false", value: parser.BoolValue(false)},
		{name: "null", value: parser.NullValue()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			table := &Table{Name: "flags", Columns: []parser.ColumnDef{{Name: "flag", Type: parser.ColumnTypeBool}}}
			localTables := map[string]*Table{"flags": table}

			affected, err := Execute(&parser.InsertStmt{
				TableName: "flags",
				Values:    []parser.Value{tc.value},
			}, localTables)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if affected != 1 {
				t.Fatalf("Execute() affected = %d, want 1", affected)
			}
			if len(table.Rows) != 1 || len(table.Rows[0]) != 1 || table.Rows[0][0] != tc.value {
				t.Fatalf("Execute() row = %#v, want [%#v]", table.Rows, tc.value)
			}
		})
	}
}

func TestExecuteInsertBoolRejectsNonBoolScalars(t *testing.T) {
	tests := []struct {
		name  string
		value parser.Value
	}{
		{name: "one", value: parser.Int64Value(1)},
		{name: "zero", value: parser.Int64Value(0)},
		{name: "text true", value: parser.StringValue("true")},
		{name: "text false", value: parser.StringValue("false")},
		{name: "text yes", value: parser.StringValue("yes")},
		{name: "text no", value: parser.StringValue("no")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			localTables := map[string]*Table{
				"flags": {Name: "flags", Columns: []parser.ColumnDef{{Name: "flag", Type: parser.ColumnTypeBool}}},
			}

			_, err := Execute(&parser.InsertStmt{
				TableName: "flags",
				Values:    []parser.Value{tc.value},
			}, localTables)
			if err != errTypeMismatch {
				t.Fatalf("Execute() error = %v, want %v", err, errTypeMismatch)
			}
		})
	}
}
