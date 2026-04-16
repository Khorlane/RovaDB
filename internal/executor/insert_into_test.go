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
	if len(row) != 2 || row[0] != parser.IntValue(1) || row[1] != parser.StringValue("steve") {
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
	if len(row) != 2 || row[0] != parser.IntValue(1) || row[1] != parser.StringValue("steve") {
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

	affected, err := Execute(&parser.InsertStmt{
		TableName: "users",
		Columns:   []string{"id"},
		Values:    []parser.Value{parser.Int64Value(1)},
	}, tables)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if affected != 1 {
		t.Fatalf("Execute() affected = %d, want 1", affected)
	}
	if got := tables["users"].Rows[0]; len(got) != 2 || got[0] != parser.IntValue(1) || got[1] != parser.NullValue() {
		t.Fatalf("Execute() row = %#v, want [1 NULL]", got)
	}
}

func TestExecuteInsertOmittedColumnsUseDefaultsAndNullability(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "name", Type: parser.ColumnTypeText, HasDefault: true, DefaultValue: parser.StringValue("ready")},
			{Name: "active", Type: parser.ColumnTypeBool, NotNull: true, HasDefault: true, DefaultValue: parser.BoolValue(true)},
			{Name: "score", Type: parser.ColumnTypeReal},
		}},
	}

	affected, err := Execute(&parser.InsertStmt{
		TableName: "users",
		Columns:   []string{"id"},
		Values:    []parser.Value{parser.Int64Value(1)},
	}, tables)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if affected != 1 {
		t.Fatalf("Execute() affected = %d, want 1", affected)
	}
	want := []parser.Value{
		parser.IntValue(1),
		parser.StringValue("ready"),
		parser.BoolValue(true),
		parser.NullValue(),
	}
	if got := tables["users"].Rows[0]; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("Execute() row = %#v, want %#v", got, want)
	}
}

func TestExecuteInsertOmittedNotNullWithoutDefaultFails(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{
			{Name: "id", Type: parser.ColumnTypeInt},
			{Name: "active", Type: parser.ColumnTypeBool, NotNull: true},
		}},
	}

	_, err := Execute(&parser.InsertStmt{
		TableName: "users",
		Columns:   []string{"id"},
		Values:    []parser.Value{parser.Int64Value(1)},
	}, tables)
	if err == nil || err.Error() != "execution: NOT NULL constraint failed: users.active" {
		t.Fatalf("Execute() error = %v, want NOT NULL constraint failure", err)
	}
}

func TestExecuteInsertExplicitNullIntoNotNullFails(t *testing.T) {
	tests := []struct {
		name   string
		column parser.ColumnDef
	}{
		{
			name:   "not null without default",
			column: parser.ColumnDef{Name: "active", Type: parser.ColumnTypeBool, NotNull: true},
		},
		{
			name:   "not null with default",
			column: parser.ColumnDef{Name: "active", Type: parser.ColumnTypeBool, NotNull: true, HasDefault: true, DefaultValue: parser.BoolValue(true)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tables := map[string]*Table{
				"users": {Name: "users", Columns: []parser.ColumnDef{
					{Name: "id", Type: parser.ColumnTypeInt},
					tc.column,
				}},
			}

			_, err := Execute(&parser.InsertStmt{
				TableName: "users",
				Values:    []parser.Value{parser.Int64Value(1), parser.NullValue()},
			}, tables)
			if err == nil || err.Error() != "execution: NOT NULL constraint failed: users.active" {
				t.Fatalf("Execute() error = %v, want NOT NULL constraint failure", err)
			}
		})
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

func TestExecuteInsertRealValue(t *testing.T) {
	tests := []struct {
		name  string
		value parser.Value
	}{
		{name: "real", value: parser.RealValue(1.25)},
		{name: "null", value: parser.NullValue()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			table := &Table{Name: "measurements", Columns: []parser.ColumnDef{{Name: "x", Type: parser.ColumnTypeReal}}}
			localTables := map[string]*Table{"measurements": table}

			affected, err := Execute(&parser.InsertStmt{
				TableName: "measurements",
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

func TestExecuteInsertRealRejectsNonRealScalars(t *testing.T) {
	tests := []struct {
		name  string
		value parser.Value
	}{
		{name: "int", value: parser.Int64Value(1)},
		{name: "text", value: parser.StringValue("1.25")},
		{name: "bool true", value: parser.BoolValue(true)},
		{name: "bool false", value: parser.BoolValue(false)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			localTables := map[string]*Table{
				"measurements": {Name: "measurements", Columns: []parser.ColumnDef{{Name: "x", Type: parser.ColumnTypeReal}}},
			}

			_, err := Execute(&parser.InsertStmt{
				TableName: "measurements",
				Values:    []parser.Value{tc.value},
			}, localTables)
			if err != errTypeMismatch {
				t.Fatalf("Execute() error = %v, want %v", err, errTypeMismatch)
			}
		})
	}
}

func TestExecuteInsertValueExprFunction(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "name", Type: parser.ColumnTypeText}}},
	}

	affected, err := Execute(&parser.InsertStmt{
		TableName:  "users",
		ValueExprs: []*parser.ValueExpr{{Kind: parser.ValueExprKindFunctionCall, FuncName: "LOWER", Arg: &parser.ValueExpr{Kind: parser.ValueExprKindLiteral, Value: parser.StringValue("STEVE")}}},
		Values:     []parser.Value{{}},
	}, tables)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if affected != 1 || len(tables["users"].Rows) != 1 || tables["users"].Rows[0][0] != parser.StringValue("steve") {
		t.Fatalf("rows = %#v, want [[steve]]", tables["users"].Rows)
	}
}

func TestExecuteInsertValueExprArithmetic(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}}},
	}

	affected, err := Execute(&parser.InsertStmt{
		TableName: "users",
		ValueExprs: []*parser.ValueExpr{{
			Kind:  parser.ValueExprKindBinary,
			Op:    parser.ValueExprBinaryOpAdd,
			Left:  &parser.ValueExpr{Kind: parser.ValueExprKindLiteral, Value: parser.Int64Value(1)},
			Right: &parser.ValueExpr{Kind: parser.ValueExprKindLiteral, Value: parser.Int64Value(2)},
		}},
		Values: []parser.Value{{}},
	}, tables)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if affected != 1 || tables["users"].Rows[0][0] != parser.IntValue(3) {
		t.Fatalf("rows = %#v, want [[3]]", tables["users"].Rows)
	}
}

func TestExecuteInsertTemporalValues(t *testing.T) {
	tables := map[string]*Table{
		"events": {
			Name: "events",
			Columns: []parser.ColumnDef{
				{Name: "event_date", Type: parser.ColumnTypeDate},
				{Name: "event_time", Type: parser.ColumnTypeTime},
				{Name: "recorded_at", Type: parser.ColumnTypeTimestamp},
			},
		},
	}

	affected, err := Execute(&parser.InsertStmt{
		TableName: "events",
		Values: []parser.Value{
			parser.DateValue(20553),
			parser.TimeValue(49521),
			parser.TimestampValue(1775828721000, 0),
		},
	}, tables)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if affected != 1 {
		t.Fatalf("Execute() affected = %d, want 1", affected)
	}
	want := []parser.Value{
		parser.DateValue(20553),
		parser.TimeValue(49521),
		parser.TimestampValue(1775828721000, 0),
	}
	if got := tables["events"].Rows[0]; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("Execute() row = %#v, want %#v", got, want)
	}
}

func TestExecuteInsertTemporalRejectsMismatchedAndNonTemporalValues(t *testing.T) {
	tests := []struct {
		name   string
		column parser.ColumnDef
		value  parser.Value
	}{
		{name: "date rejects time", column: parser.ColumnDef{Name: "target", Type: parser.ColumnTypeDate}, value: parser.TimeValue(49521)},
		{name: "date rejects timestamp", column: parser.ColumnDef{Name: "target", Type: parser.ColumnTypeDate}, value: parser.TimestampValue(1775828721000, 0)},
		{name: "date rejects text", column: parser.ColumnDef{Name: "target", Type: parser.ColumnTypeDate}, value: parser.StringValue("2026-04-10")},
		{name: "date rejects int", column: parser.ColumnDef{Name: "target", Type: parser.ColumnTypeDate}, value: parser.Int64Value(1)},
		{name: "time rejects date", column: parser.ColumnDef{Name: "target", Type: parser.ColumnTypeTime}, value: parser.DateValue(20553)},
		{name: "time rejects timestamp", column: parser.ColumnDef{Name: "target", Type: parser.ColumnTypeTime}, value: parser.TimestampValue(1775828721000, 0)},
		{name: "time rejects real", column: parser.ColumnDef{Name: "target", Type: parser.ColumnTypeTime}, value: parser.RealValue(1.25)},
		{name: "timestamp rejects date", column: parser.ColumnDef{Name: "target", Type: parser.ColumnTypeTimestamp}, value: parser.DateValue(20553)},
		{name: "timestamp rejects time", column: parser.ColumnDef{Name: "target", Type: parser.ColumnTypeTimestamp}, value: parser.TimeValue(49521)},
		{name: "timestamp rejects unresolved timestamp", column: parser.ColumnDef{Name: "target", Type: parser.ColumnTypeTimestamp}, value: parser.TimestampUnresolvedValue(2026, 4, 10, 13, 45, 21)},
		{name: "timestamp rejects bool", column: parser.ColumnDef{Name: "target", Type: parser.ColumnTypeTimestamp}, value: parser.BoolValue(true)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tables := map[string]*Table{
				"events": {Name: "events", Columns: []parser.ColumnDef{tc.column}},
			}

			_, err := Execute(&parser.InsertStmt{
				TableName: "events",
				Values:    []parser.Value{tc.value},
			}, tables)
			wantErr := errTypeMismatch
			if tc.value.Kind == parser.ValueKindTimestampUnresolved {
				wantErr = errUnresolvedTimestamp
			}
			if err != wantErr {
				t.Fatalf("Execute() error = %v, want %v", err, wantErr)
			}
		})
	}
}

func TestNormalizeColumnValueForDefRequiresExactIntegerWidths(t *testing.T) {
	boundInt32Value := func(v int32) parser.Value {
		value := parser.IntValue(v)
		value.BoundIntegerType = parser.BoundIntegerTypeInt32
		return value
	}

	tests := []struct {
		name      string
		column    parser.ColumnDef
		value     parser.Value
		want      parser.Value
		wantError error
	}{
		{
			name:   "smallint accepts exact typed value",
			column: parser.ColumnDef{Name: "small_col", Type: parser.ColumnTypeSmallInt},
			value:  parser.SmallIntValue(7),
			want:   parser.SmallIntValue(7),
		},
		{
			name:   "smallint fits",
			column: parser.ColumnDef{Name: "small_col", Type: parser.ColumnTypeSmallInt},
			value:  parser.Int64Value(7),
			want:   parser.SmallIntValue(7),
		},
		{
			name:      "smallint rejects typed int",
			column:    parser.ColumnDef{Name: "small_col", Type: parser.ColumnTypeSmallInt},
			value:     parser.IntValue(7),
			wantError: errTypeMismatch,
		},
		{
			name:      "smallint rejects bound int32 placeholder",
			column:    parser.ColumnDef{Name: "small_col", Type: parser.ColumnTypeSmallInt},
			value:     boundInt32Value(7),
			wantError: errTypeMismatch,
		},
		{
			name:      "smallint rejects typed bigint",
			column:    parser.ColumnDef{Name: "small_col", Type: parser.ColumnTypeSmallInt},
			value:     parser.BigIntValue(7),
			wantError: errTypeMismatch,
		},
		{
			name:      "smallint rejects overflow",
			column:    parser.ColumnDef{Name: "small_col", Type: parser.ColumnTypeSmallInt},
			value:     parser.Int64Value(40000),
			wantError: errTypeMismatch,
		},
		{
			name:   "int accepts exact typed value",
			column: parser.ColumnDef{Name: "int_col", Type: parser.ColumnTypeInt},
			value:  parser.IntValue(42),
			want:   parser.IntValue(42),
		},
		{
			name:   "int accepts bound int32 placeholder",
			column: parser.ColumnDef{Name: "int_col", Type: parser.ColumnTypeInt},
			value:  boundInt32Value(42),
			want:   parser.IntValue(42),
		},
		{
			name:   "int fits",
			column: parser.ColumnDef{Name: "int_col", Type: parser.ColumnTypeInt},
			value:  parser.Int64Value(42),
			want:   parser.IntValue(42),
		},
		{
			name:      "int rejects typed smallint",
			column:    parser.ColumnDef{Name: "int_col", Type: parser.ColumnTypeInt},
			value:     parser.SmallIntValue(42),
			wantError: errTypeMismatch,
		},
		{
			name:      "int rejects typed bigint",
			column:    parser.ColumnDef{Name: "int_col", Type: parser.ColumnTypeInt},
			value:     parser.BigIntValue(42),
			wantError: errTypeMismatch,
		},
		{
			name:      "int rejects overflow",
			column:    parser.ColumnDef{Name: "int_col", Type: parser.ColumnTypeInt},
			value:     parser.Int64Value(1 << 40),
			wantError: errTypeMismatch,
		},
		{
			name:   "bigint accepts exact typed value",
			column: parser.ColumnDef{Name: "big_col", Type: parser.ColumnTypeBigInt},
			value:  parser.BigIntValue(1 << 40),
			want:   parser.BigIntValue(1 << 40),
		},
		{
			name:   "bigint fits",
			column: parser.ColumnDef{Name: "big_col", Type: parser.ColumnTypeBigInt},
			value:  parser.Int64Value(1 << 40),
			want:   parser.BigIntValue(1 << 40),
		},
		{
			name:      "bigint rejects typed smallint",
			column:    parser.ColumnDef{Name: "big_col", Type: parser.ColumnTypeBigInt},
			value:     parser.SmallIntValue(5),
			wantError: errTypeMismatch,
		},
		{
			name:      "bigint rejects typed int",
			column:    parser.ColumnDef{Name: "big_col", Type: parser.ColumnTypeBigInt},
			value:     parser.IntValue(5),
			wantError: errTypeMismatch,
		},
		{
			name:   "null remains unchanged",
			column: parser.ColumnDef{Name: "big_col", Type: parser.ColumnTypeBigInt},
			value:  parser.NullValue(),
			want:   parser.NullValue(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeColumnValueForDef(tc.column, tc.value)
			if tc.wantError != nil {
				if err != tc.wantError {
					t.Fatalf("normalizeColumnValueForDef() error = %v, want %v", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeColumnValueForDef() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("normalizeColumnValueForDef() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestNormalizeColumnValueForDefRequiresExactTemporalFamilies(t *testing.T) {
	tests := []struct {
		name      string
		column    parser.ColumnDef
		value     parser.Value
		want      parser.Value
		wantError error
	}{
		{
			name:   "date accepts exact typed value",
			column: parser.ColumnDef{Name: "event_date", Type: parser.ColumnTypeDate},
			value:  parser.DateValue(20553),
			want:   parser.DateValue(20553),
		},
		{
			name:      "date rejects time",
			column:    parser.ColumnDef{Name: "event_date", Type: parser.ColumnTypeDate},
			value:     parser.TimeValue(49521),
			wantError: errTypeMismatch,
		},
		{
			name:      "date rejects text",
			column:    parser.ColumnDef{Name: "event_date", Type: parser.ColumnTypeDate},
			value:     parser.StringValue("2026-04-10"),
			wantError: errTypeMismatch,
		},
		{
			name:   "time accepts exact typed value",
			column: parser.ColumnDef{Name: "event_time", Type: parser.ColumnTypeTime},
			value:  parser.TimeValue(49521),
			want:   parser.TimeValue(49521),
		},
		{
			name:      "time rejects timestamp",
			column:    parser.ColumnDef{Name: "event_time", Type: parser.ColumnTypeTime},
			value:     parser.TimestampValue(1775828721000, 0),
			wantError: errTypeMismatch,
		},
		{
			name:   "timestamp accepts exact typed value",
			column: parser.ColumnDef{Name: "recorded_at", Type: parser.ColumnTypeTimestamp},
			value:  parser.TimestampValue(1775828721000, 7),
			want:   parser.TimestampValue(1775828721000, 7),
		},
		{
			name:      "timestamp rejects date",
			column:    parser.ColumnDef{Name: "recorded_at", Type: parser.ColumnTypeTimestamp},
			value:     parser.DateValue(20553),
			wantError: errTypeMismatch,
		},
		{
			name:      "timestamp rejects unresolved timestamp",
			column:    parser.ColumnDef{Name: "recorded_at", Type: parser.ColumnTypeTimestamp},
			value:     parser.TimestampUnresolvedValue(2026, 4, 10, 13, 45, 21),
			wantError: errUnresolvedTimestamp,
		},
		{
			name:   "null remains unchanged",
			column: parser.ColumnDef{Name: "recorded_at", Type: parser.ColumnTypeTimestamp},
			value:  parser.NullValue(),
			want:   parser.NullValue(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeColumnValueForDef(tc.column, tc.value)
			if tc.wantError != nil {
				if err != tc.wantError {
					t.Fatalf("normalizeColumnValueForDef() error = %v, want %v", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeColumnValueForDef() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("normalizeColumnValueForDef() = %#v, want %#v", got, tc.want)
			}
		})
	}
}
