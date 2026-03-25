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
