package executor

import (
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestExecuteUpdateAllRows(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
				{parser.Int64Value(2), parser.StringValue("sam")},
			},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("bob")},
		},
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 2 {
		t.Fatalf("executeUpdate() affected = %d, want 2", affected)
	}
}

func TestExecuteUpdateIntWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
				{parser.Int64Value(2), parser.StringValue("sam")},
			},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("bob")},
		},
		Where: where(parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(1)}),
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 1 {
		t.Fatalf("executeUpdate() affected = %d, want 1", affected)
	}
}

func TestExecuteUpdateStringWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
				{parser.Int64Value(2), parser.StringValue("sam")},
			},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "id", Value: parser.Int64Value(3)},
		},
		Where: where(parser.Condition{Left: "name", Operator: "=", Right: parser.StringValue("alice")}),
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 1 {
		t.Fatalf("executeUpdate() affected = %d, want 1", affected)
	}
}

func TestExecuteUpdateUnknownAssignmentColumn(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}}},
	}

	_, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "email", Value: parser.StringValue("bob")},
		},
	}, tables)
	if err != errColumnDoesNotExist {
		t.Fatalf("executeUpdate() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestExecuteUpdateUnknownWhereColumn(t *testing.T) {
	tables := map[string]*Table{
		"users": {Name: "users", Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}}},
	}

	_, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("bob")},
		},
		Where: where(parser.Condition{Left: "email", Operator: "=", Right: parser.StringValue("alice")}),
	}, tables)
	if err != errColumnDoesNotExist {
		t.Fatalf("executeUpdate() error = %v, want %v", err, errColumnDoesNotExist)
	}
}

func TestExecuteUpdateNoMatchesLeavesRows(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
			},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("bob")},
		},
		Where: where(parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(2)}),
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 0 {
		t.Fatalf("executeUpdate() affected = %d, want 0", affected)
	}
}

func TestExecuteUpdateMultipleAssignments(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
			},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("bob")},
			{Column: "id", Value: parser.Int64Value(2)},
		},
		Where: where(parser.Condition{Left: "name", Operator: "=", Right: parser.StringValue("alice")}),
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 1 {
		t.Fatalf("executeUpdate() affected = %d, want 1", affected)
	}
}

func TestExecuteUpdateWrongType(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
			},
		},
	}

	_, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "id", Value: parser.StringValue("oops")},
		},
	}, tables)
	if err != errTypeMismatch {
		t.Fatalf("executeUpdate() error = %v, want %v", err, errTypeMismatch)
	}
}

func TestExecuteUpdateExplicitNullIntoNotNullFails(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name: "users",
			Columns: []parser.ColumnDef{
				{Name: "id", Type: parser.ColumnTypeInt},
				{Name: "name", Type: parser.ColumnTypeText, NotNull: true, HasDefault: true, DefaultValue: parser.StringValue("ready")},
			},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
			},
		},
	}

	_, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.NullValue()},
		},
	}, tables)
	if err == nil || err.Error() != "execution: NOT NULL constraint failed: users.name" {
		t.Fatalf("executeUpdate() error = %v, want NOT NULL constraint failure", err)
	}
	if got := tables["users"].Rows[0][1]; got != parser.StringValue("alice") {
		t.Fatalf("rows[0][1] = %#v, want %#v", got, parser.StringValue("alice"))
	}
}

func TestExecuteUpdateLeavesUntouchedDefaultedNotNullColumnUnchanged(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name: "users",
			Columns: []parser.ColumnDef{
				{Name: "id", Type: parser.ColumnTypeInt},
				{Name: "name", Type: parser.ColumnTypeText},
				{Name: "active", Type: parser.ColumnTypeBool, NotNull: true, HasDefault: true, DefaultValue: parser.BoolValue(true)},
			},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice"), parser.BoolValue(true)},
			},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("bob")},
		},
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 1 {
		t.Fatalf("executeUpdate() affected = %d, want 1", affected)
	}
	if got := tables["users"].Rows[0]; got[1] != parser.StringValue("bob") || got[2] != parser.BoolValue(true) {
		t.Fatalf("rows[0] = %#v, want name updated and active unchanged", got)
	}
}

func TestExecuteUpdateWithAndWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
				{parser.Int64Value(2), parser.StringValue("bob")},
				{parser.Int64Value(3), parser.StringValue("bob")},
			},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("cara")},
		},
		Where: where(
			parser.Condition{Left: "id", Operator: ">", Right: parser.Int64Value(1)},
			parser.ConditionChainItem{Op: parser.BooleanOpAnd, Condition: parser.Condition{Left: "name", Operator: "=", Right: parser.StringValue("bob")}},
		),
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 2 {
		t.Fatalf("executeUpdate() affected = %d, want 2", affected)
	}
}

func TestExecuteUpdateWithOrWhere(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
			Rows: [][]parser.Value{
				{parser.Int64Value(1), parser.StringValue("alice")},
				{parser.Int64Value(2), parser.StringValue("bob")},
				{parser.Int64Value(3), parser.StringValue("cara")},
			},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Value: parser.StringValue("updated")},
		},
		Where: where(
			parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(1)},
			parser.ConditionChainItem{Op: parser.BooleanOpOr, Condition: parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(3)}},
		),
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 2 {
		t.Fatalf("executeUpdate() affected = %d, want 2", affected)
	}
	if tables["users"].Rows[0][1] != parser.StringValue("updated") || tables["users"].Rows[1][1] != parser.StringValue("bob") || tables["users"].Rows[2][1] != parser.StringValue("updated") {
		t.Fatalf("rows = %#v, want ids 1 and 3 updated", tables["users"].Rows)
	}
}

func TestExecuteUpdateBoolValue(t *testing.T) {
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
			tables := map[string]*Table{
				"flags": {
					Name:    "flags",
					Columns: []parser.ColumnDef{{Name: "flag", Type: parser.ColumnTypeBool}},
					Rows:    [][]parser.Value{{parser.BoolValue(false)}},
				},
			}

			affected, err := executeUpdate(&parser.UpdateStmt{
				TableName: "flags",
				Assignments: []parser.UpdateAssignment{
					{Column: "flag", Value: tc.value},
				},
			}, tables)
			if err != nil {
				t.Fatalf("executeUpdate() error = %v", err)
			}
			if affected != 1 {
				t.Fatalf("executeUpdate() affected = %d, want 1", affected)
			}
			if tables["flags"].Rows[0][0] != tc.value {
				t.Fatalf("rows[0][0] = %#v, want %#v", tables["flags"].Rows[0][0], tc.value)
			}
		})
	}
}

func TestExecuteUpdateBoolRejectsNonBoolScalars(t *testing.T) {
	tests := []struct {
		name  string
		value parser.Value
	}{
		{name: "one", value: parser.Int64Value(1)},
		{name: "zero", value: parser.Int64Value(0)},
		{name: "text true", value: parser.StringValue("true")},
		{name: "text false", value: parser.StringValue("false")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tables := map[string]*Table{
				"flags": {
					Name:    "flags",
					Columns: []parser.ColumnDef{{Name: "flag", Type: parser.ColumnTypeBool}},
					Rows:    [][]parser.Value{{parser.BoolValue(false)}},
				},
			}

			_, err := executeUpdate(&parser.UpdateStmt{
				TableName: "flags",
				Assignments: []parser.UpdateAssignment{
					{Column: "flag", Value: tc.value},
				},
			}, tables)
			if err != errTypeMismatch {
				t.Fatalf("executeUpdate() error = %v, want %v", err, errTypeMismatch)
			}
		})
	}
}

func TestExecuteUpdateRealValue(t *testing.T) {
	tests := []struct {
		name  string
		value parser.Value
	}{
		{name: "real", value: parser.RealValue(2.5)},
		{name: "null", value: parser.NullValue()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tables := map[string]*Table{
				"measurements": {
					Name:    "measurements",
					Columns: []parser.ColumnDef{{Name: "x", Type: parser.ColumnTypeReal}},
					Rows:    [][]parser.Value{{parser.RealValue(1.25)}},
				},
			}

			affected, err := executeUpdate(&parser.UpdateStmt{
				TableName: "measurements",
				Assignments: []parser.UpdateAssignment{
					{Column: "x", Value: tc.value},
				},
			}, tables)
			if err != nil {
				t.Fatalf("executeUpdate() error = %v", err)
			}
			if affected != 1 {
				t.Fatalf("executeUpdate() affected = %d, want 1", affected)
			}
			if tables["measurements"].Rows[0][0] != tc.value {
				t.Fatalf("rows[0][0] = %#v, want %#v", tables["measurements"].Rows[0][0], tc.value)
			}
		})
	}
}

func TestExecuteUpdateRealRejectsNonRealScalars(t *testing.T) {
	tests := []struct {
		name  string
		value parser.Value
	}{
		{name: "int", value: parser.Int64Value(2)},
		{name: "text", value: parser.StringValue("2.5")},
		{name: "bool false", value: parser.BoolValue(false)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tables := map[string]*Table{
				"measurements": {
					Name:    "measurements",
					Columns: []parser.ColumnDef{{Name: "x", Type: parser.ColumnTypeReal}},
					Rows:    [][]parser.Value{{parser.RealValue(1.25)}},
				},
			}

			_, err := executeUpdate(&parser.UpdateStmt{
				TableName: "measurements",
				Assignments: []parser.UpdateAssignment{
					{Column: "x", Value: tc.value},
				},
			}, tables)
			if err != errTypeMismatch {
				t.Fatalf("executeUpdate() error = %v, want %v", err, errTypeMismatch)
			}
		})
	}
}

func TestExecuteUpdateAssignmentExprFunction(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
			Rows:    [][]parser.Value{{parser.Int64Value(1), parser.StringValue("alice")}},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "name", Expr: &parser.ValueExpr{Kind: parser.ValueExprKindFunctionCall, FuncName: "UPPER", Arg: &parser.ValueExpr{Kind: parser.ValueExprKindColumnRef, Column: "name"}}},
		},
		Where: where(parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(1)}),
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 1 || tables["users"].Rows[0][1] != parser.StringValue("ALICE") {
		t.Fatalf("rows = %#v, want ALICE", tables["users"].Rows)
	}
}

func TestExecuteUpdateAssignmentExprArithmetic(t *testing.T) {
	tables := map[string]*Table{
		"users": {
			Name:    "users",
			Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "name", Type: parser.ColumnTypeText}},
			Rows:    [][]parser.Value{{parser.Int64Value(1), parser.StringValue("alice")}},
		},
	}

	affected, err := executeUpdate(&parser.UpdateStmt{
		TableName: "users",
		Assignments: []parser.UpdateAssignment{
			{Column: "id", Expr: &parser.ValueExpr{
				Kind:  parser.ValueExprKindBinary,
				Op:    parser.ValueExprBinaryOpAdd,
				Left:  &parser.ValueExpr{Kind: parser.ValueExprKindColumnRef, Column: "id"},
				Right: &parser.ValueExpr{Kind: parser.ValueExprKindLiteral, Value: parser.Int64Value(2)},
			}},
		},
		Where: where(parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(1)}),
	}, tables)
	if err != nil {
		t.Fatalf("executeUpdate() error = %v", err)
	}
	if affected != 1 || tables["users"].Rows[0][0] != parser.IntValue(3) {
		t.Fatalf("rows = %#v, want id 3", tables["users"].Rows)
	}
}

func TestExecuteUpdateTypedIntegerTargetRejectsUntypedLiteralOverflow(t *testing.T) {
	tables := map[string]*Table{
		"numbers": {
			Name:    "numbers",
			Columns: []parser.ColumnDef{{Name: "small_col", Type: parser.ColumnTypeSmallInt}},
			Rows:    [][]parser.Value{{parser.SmallIntValue(1)}},
		},
	}

	_, err := executeUpdate(&parser.UpdateStmt{
		TableName: "numbers",
		Assignments: []parser.UpdateAssignment{
			{Column: "small_col", Value: parser.Int64Value(40000)},
		},
	}, tables)
	if err != errTypeMismatch {
		t.Fatalf("executeUpdate() error = %v, want %v", err, errTypeMismatch)
	}
}

func TestExecuteUpdateTypedIntegerTargetsRequireExactWidthValues(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		initial    parser.Value
		value      parser.Value
		want       parser.Value
		wantErr    error
	}{
		{
			name:       "smallint accepts exact typed value",
			columnType: parser.ColumnTypeSmallInt,
			initial:    parser.SmallIntValue(1),
			value:      parser.SmallIntValue(11),
			want:       parser.SmallIntValue(11),
		},
		{
			name:       "smallint accepts fitting literal",
			columnType: parser.ColumnTypeSmallInt,
			initial:    parser.SmallIntValue(1),
			value:      parser.Int64Value(12),
			want:       parser.SmallIntValue(12),
		},
		{
			name:       "smallint rejects typed int",
			columnType: parser.ColumnTypeSmallInt,
			initial:    parser.SmallIntValue(1),
			value:      parser.IntValue(13),
			wantErr:    errTypeMismatch,
		},
		{
			name:       "smallint rejects typed bigint",
			columnType: parser.ColumnTypeSmallInt,
			initial:    parser.SmallIntValue(1),
			value:      parser.BigIntValue(14),
			wantErr:    errTypeMismatch,
		},
		{
			name:       "int accepts exact typed value",
			columnType: parser.ColumnTypeInt,
			initial:    parser.IntValue(2),
			value:      parser.IntValue(21),
			want:       parser.IntValue(21),
		},
		{
			name:       "int accepts fitting literal",
			columnType: parser.ColumnTypeInt,
			initial:    parser.IntValue(2),
			value:      parser.Int64Value(22),
			want:       parser.IntValue(22),
		},
		{
			name:       "int rejects typed smallint",
			columnType: parser.ColumnTypeInt,
			initial:    parser.IntValue(2),
			value:      parser.SmallIntValue(23),
			wantErr:    errTypeMismatch,
		},
		{
			name:       "int rejects typed bigint",
			columnType: parser.ColumnTypeInt,
			initial:    parser.IntValue(2),
			value:      parser.BigIntValue(24),
			wantErr:    errTypeMismatch,
		},
		{
			name:       "bigint accepts exact typed value",
			columnType: parser.ColumnTypeBigInt,
			initial:    parser.BigIntValue(3),
			value:      parser.BigIntValue(31),
			want:       parser.BigIntValue(31),
		},
		{
			name:       "bigint accepts fitting literal",
			columnType: parser.ColumnTypeBigInt,
			initial:    parser.BigIntValue(3),
			value:      parser.Int64Value(32),
			want:       parser.BigIntValue(32),
		},
		{
			name:       "bigint rejects typed smallint",
			columnType: parser.ColumnTypeBigInt,
			initial:    parser.BigIntValue(3),
			value:      parser.SmallIntValue(33),
			wantErr:    errTypeMismatch,
		},
		{
			name:       "bigint rejects typed int",
			columnType: parser.ColumnTypeBigInt,
			initial:    parser.BigIntValue(3),
			value:      parser.IntValue(34),
			wantErr:    errTypeMismatch,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			table := &Table{
				Name:    "numbers",
				Columns: []parser.ColumnDef{{Name: "id", Type: parser.ColumnTypeInt}, {Name: "target", Type: tc.columnType}},
				Rows:    [][]parser.Value{{parser.IntValue(1), tc.initial}},
			}
			tables := map[string]*Table{"numbers": table}

			_, err := executeUpdate(&parser.UpdateStmt{
				TableName: "numbers",
				Assignments: []parser.UpdateAssignment{
					{Column: "target", Value: tc.value},
				},
				Where: where(parser.Condition{Left: "id", Operator: "=", Right: parser.Int64Value(1)}),
			}, tables)
			if tc.wantErr != nil {
				if err != tc.wantErr {
					t.Fatalf("executeUpdate() error = %v, want %v", err, tc.wantErr)
				}
				if got := table.Rows[0][1]; got != tc.initial {
					t.Fatalf("row changed on failure = %#v, want %#v", got, tc.initial)
				}
				return
			}
			if err != nil {
				t.Fatalf("executeUpdate() error = %v", err)
			}
			if got := table.Rows[0][1]; got != tc.want {
				t.Fatalf("row target = %#v, want %#v", got, tc.want)
			}
		})
	}
}
