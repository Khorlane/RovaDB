package parser

import "testing"

func TestInt64Value(t *testing.T) {
	got := Int64Value(42)
	if got.Kind != ValueKindIntegerLiteral {
		t.Fatalf("Int64Value().Kind = %v, want %v", got.Kind, ValueKindIntegerLiteral)
	}
	if got.I64 != 42 {
		t.Fatalf("Int64Value().I64 = %d, want 42", got.I64)
	}
}

func TestExactWidthIntegerConstructors(t *testing.T) {
	small := SmallIntValue(7)
	if small.Kind != ValueKindSmallInt || small.I16 != 7 {
		t.Fatalf("SmallIntValue() = %#v, want SMALLINT 7", small)
	}
	if got := small.Any(); got != int16(7) {
		t.Fatalf("SmallIntValue().Any() = %#v, want int16(7)", got)
	}

	regular := IntValue(8)
	if regular.Kind != ValueKindInt || regular.I32 != 8 {
		t.Fatalf("IntValue() = %#v, want INT 8", regular)
	}
	if got := regular.Any(); got != int32(8) {
		t.Fatalf("IntValue().Any() = %#v, want int32(8)", got)
	}

	big := BigIntValue(9)
	if big.Kind != ValueKindBigInt || big.I64 != 9 {
		t.Fatalf("BigIntValue() = %#v, want BIGINT 9", big)
	}
	if got := big.Any(); got != int64(9) {
		t.Fatalf("BigIntValue().Any() = %#v, want int64(9)", got)
	}
}

func TestExactWidthIntegerKindsAreDistinct(t *testing.T) {
	small := SmallIntValue(1)
	regular := IntValue(1)
	big := BigIntValue(1)

	if small.Kind == regular.Kind {
		t.Fatalf("SMALLINT kind = %v, INT kind = %v, want distinct", small.Kind, regular.Kind)
	}
	if regular.Kind == big.Kind {
		t.Fatalf("INT kind = %v, BIGINT kind = %v, want distinct", regular.Kind, big.Kind)
	}
	if !small.IsInteger() || !regular.IsInteger() || !big.IsInteger() {
		t.Fatalf("exact-width integer values must report IsInteger() = true")
	}
}

func TestNullValue(t *testing.T) {
	got := NullValue()
	if got.Kind != ValueKindNull {
		t.Fatalf("NullValue().Kind = %v, want %v", got.Kind, ValueKindNull)
	}
	if got.Any() != nil {
		t.Fatalf("NullValue().Any() = %#v, want nil", got.Any())
	}
}

func TestStringValue(t *testing.T) {
	got := StringValue("hello")
	if got.Kind != ValueKindString {
		t.Fatalf("StringValue().Kind = %v, want %v", got.Kind, ValueKindString)
	}
	if got.Str != "hello" {
		t.Fatalf("StringValue().Str = %q, want %q", got.Str, "hello")
	}
}

func TestBoolValue(t *testing.T) {
	got := BoolValue(true)
	if got.Kind != ValueKindBool {
		t.Fatalf("BoolValue().Kind = %v, want %v", got.Kind, ValueKindBool)
	}
	if !got.Bool {
		t.Fatalf("BoolValue().Bool = %v, want true", got.Bool)
	}
	if got.Any() != true {
		t.Fatalf("BoolValue().Any() = %#v, want true", got.Any())
	}
}

func TestRealValue(t *testing.T) {
	got := RealValue(3.14)
	if got.Kind != ValueKindReal {
		t.Fatalf("RealValue().Kind = %v, want %v", got.Kind, ValueKindReal)
	}
	if got.F64 != 3.14 {
		t.Fatalf("RealValue().F64 = %v, want 3.14", got.F64)
	}
	if got.Any() != 3.14 {
		t.Fatalf("RealValue().Any() = %#v, want 3.14", got.Any())
	}
}

func TestTemporalValueConstructors(t *testing.T) {
	date := DateValue(123)
	if date.Kind != ValueKindDate || date.DateDays != 123 {
		t.Fatalf("DateValue() = %#v, want DATE 123 days", date)
	}
	if got := date.Any(); got != int32(123) {
		t.Fatalf("DateValue().Any() = %#v, want int32(123)", got)
	}

	clock := TimeValue(45296)
	if clock.Kind != ValueKindTime || clock.TimeSeconds != 45296 {
		t.Fatalf("TimeValue() = %#v, want TIME 45296 seconds", clock)
	}
	if got := clock.Any(); got != int32(45296) {
		t.Fatalf("TimeValue().Any() = %#v, want int32(45296)", got)
	}

	stamp := TimestampValue(1700000000123, 7)
	if stamp.Kind != ValueKindTimestamp || stamp.TimestampMillis != 1700000000123 || stamp.TimestampZoneID != 7 {
		t.Fatalf("TimestampValue() = %#v, want TIMESTAMP millis+zone payload", stamp)
	}
	if got := stamp.Any(); got != int64(1700000000123) {
		t.Fatalf("TimestampValue().Any() = %#v, want int64(1700000000123)", got)
	}
}

func TestPlaceholderValue(t *testing.T) {
	got := PlaceholderValue()
	if got.Kind != ValueKindPlaceholder {
		t.Fatalf("PlaceholderValue().Kind = %v, want %v", got.Kind, ValueKindPlaceholder)
	}
	if got.PlaceholderIndex != -1 {
		t.Fatalf("PlaceholderValue().PlaceholderIndex = %d, want -1", got.PlaceholderIndex)
	}
	if got.Any() != nil {
		t.Fatalf("PlaceholderValue().Any() = %#v, want nil", got.Any())
	}
}

func TestParseLiteralValueBool(t *testing.T) {
	tests := []struct {
		name  string
		token string
		want  Value
	}{
		{name: "true upper", token: "TRUE", want: BoolValue(true)},
		{name: "false upper", token: "FALSE", want: BoolValue(false)},
		{name: "true lower", token: "true", want: BoolValue(true)},
		{name: "false lower", token: "false", want: BoolValue(false)},
		{name: "true title", token: "True", want: BoolValue(true)},
		{name: "false title", token: "False", want: BoolValue(false)},
		{name: "quoted true text", token: "'true'", want: StringValue("true")},
		{name: "quoted false text", token: "'false'", want: StringValue("false")},
		{name: "zero remains int", token: "0", want: Int64Value(0)},
		{name: "one remains int", token: "1", want: Int64Value(1)},
		{name: "null regression", token: "NULL", want: NullValue()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseLiteralValue(tc.token)
			if !ok {
				t.Fatalf("parseLiteralValue(%q) ok = false, want true", tc.token)
			}
			if got != tc.want {
				t.Fatalf("parseLiteralValue(%q) = %#v, want %#v", tc.token, got, tc.want)
			}
		})
	}
}

func TestParseLiteralValueReal(t *testing.T) {
	tests := []struct {
		name  string
		token string
		want  Value
	}{
		{name: "zero point zero", token: "0.0", want: RealValue(0.0)},
		{name: "pi-ish", token: "3.14", want: RealValue(3.14)},
		{name: "negative", token: "-2.5", want: RealValue(-2.5)},
		{name: "ten point twenty five", token: "10.25", want: RealValue(10.25)},
		{name: "integer regression", token: "42", want: Int64Value(42)},
		{name: "quoted decimal text", token: "'3.14'", want: StringValue("3.14")},
		{name: "bool regression", token: "TRUE", want: BoolValue(true)},
		{name: "null regression", token: "NULL", want: NullValue()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseLiteralValue(tc.token)
			if !ok {
				t.Fatalf("parseLiteralValue(%q) ok = false, want true", tc.token)
			}
			if got != tc.want {
				t.Fatalf("parseLiteralValue(%q) = %#v, want %#v", tc.token, got, tc.want)
			}
		})
	}
}

func TestParseLiteralValueRealInvalid(t *testing.T) {
	tests := []string{"1.", ".5", "1e3", "-2.5e-1"}

	for _, token := range tests {
		t.Run(token, func(t *testing.T) {
			if got, ok := parseLiteralValue(token); ok {
				t.Fatalf("parseLiteralValue(%q) = %#v, want failure", token, got)
			}
		})
	}
}

func TestParseLiteralValuePlaceholder(t *testing.T) {
	got, ok := parseLiteralValue("?")
	if !ok {
		t.Fatal("parseLiteralValue(?) ok = false, want true")
	}
	if got != PlaceholderValue() {
		t.Fatalf("parseLiteralValue(?) = %#v, want %#v", got, PlaceholderValue())
	}
}

func TestParseSelectExprValueKinds(t *testing.T) {
	intSel, ok := ParseSelectExpr("SELECT 1")
	if !ok {
		t.Fatal("ParseSelectExpr(SELECT 1) ok = false, want true")
	}
	if intSel.Expr == nil {
		t.Fatal("ParseSelectExpr(SELECT 1).Expr = nil, want value")
	}
	if intSel.Expr.Kind != ExprKindInt64Literal {
		t.Fatalf("ParseSelectExpr(SELECT 1).Expr.Kind = %v, want %v", intSel.Expr.Kind, ExprKindInt64Literal)
	}
	if intSel.Expr.I64 != 1 {
		t.Fatalf("ParseSelectExpr(SELECT 1).Expr.I64 = %d, want 1", intSel.Expr.I64)
	}

	realSel, ok := ParseSelectExpr("SELECT 3.14")
	if !ok {
		t.Fatal("ParseSelectExpr(SELECT 3.14) ok = false, want true")
	}
	if realSel.Expr == nil {
		t.Fatal("ParseSelectExpr(SELECT 3.14).Expr = nil, want value")
	}
	if realSel.Expr.Kind != ExprKindRealLiteral {
		t.Fatalf("ParseSelectExpr(SELECT 3.14).Expr.Kind = %v, want %v", realSel.Expr.Kind, ExprKindRealLiteral)
	}
	if realSel.Expr.F64 != 3.14 {
		t.Fatalf("ParseSelectExpr(SELECT 3.14).Expr.F64 = %v, want 3.14", realSel.Expr.F64)
	}

	strSel, ok := ParseSelectExpr("SELECT 'hi'")
	if !ok {
		t.Fatal("ParseSelectExpr(SELECT 'hi') ok = false, want true")
	}
	if strSel.Expr == nil {
		t.Fatal("ParseSelectExpr(SELECT 'hi').Expr = nil, want value")
	}
	if strSel.Expr.Kind != ExprKindStringLiteral {
		t.Fatalf("ParseSelectExpr(SELECT 'hi').Expr.Kind = %v, want %v", strSel.Expr.Kind, ExprKindStringLiteral)
	}
	if strSel.Expr.Str != "hi" {
		t.Fatalf("ParseSelectExpr(SELECT 'hi').Expr.Str = %q, want %q", strSel.Expr.Str, "hi")
	}

	trueSel, ok := ParseSelectExpr("SELECT TRUE")
	if !ok {
		t.Fatal("ParseSelectExpr(SELECT TRUE) ok = false, want true")
	}
	if trueSel.Expr == nil {
		t.Fatal("ParseSelectExpr(SELECT TRUE).Expr = nil, want value")
	}
	if trueSel.Expr.Kind != ExprKindBoolLiteral {
		t.Fatalf("ParseSelectExpr(SELECT TRUE).Expr.Kind = %v, want %v", trueSel.Expr.Kind, ExprKindBoolLiteral)
	}
	if !trueSel.Expr.Bool {
		t.Fatalf("ParseSelectExpr(SELECT TRUE).Expr.Bool = %v, want true", trueSel.Expr.Bool)
	}

	falseSel, ok := ParseSelectExpr("SELECT False")
	if !ok {
		t.Fatal("ParseSelectExpr(SELECT False) ok = false, want true")
	}
	if falseSel.Expr == nil {
		t.Fatal("ParseSelectExpr(SELECT False).Expr = nil, want value")
	}
	if falseSel.Expr.Kind != ExprKindBoolLiteral {
		t.Fatalf("ParseSelectExpr(SELECT False).Expr.Kind = %v, want %v", falseSel.Expr.Kind, ExprKindBoolLiteral)
	}
	if falseSel.Expr.Bool {
		t.Fatalf("ParseSelectExpr(SELECT False).Expr.Bool = %v, want false", falseSel.Expr.Bool)
	}

	nullSel, ok := ParseSelectExpr("SELECT * FROM users WHERE name = NULL")
	if !ok {
		t.Fatal("ParseSelectExpr(SELECT * FROM users WHERE name = NULL) ok = false, want true")
	}
	if nullSel.Where == nil || len(nullSel.Where.Items) != 1 {
		t.Fatalf("ParseSelectExpr(...).Where = %#v, want one condition", nullSel.Where)
	}
	if nullSel.Where.Items[0].Condition.Right.Kind != ValueKindNull {
		t.Fatalf("ParseSelectExpr(...).Where.Items[0].Condition.Right.Kind = %v, want %v", nullSel.Where.Items[0].Condition.Right.Kind, ValueKindNull)
	}

	whereRealSel, ok := ParseSelectExpr("SELECT * FROM prices WHERE amount = 10.25")
	if !ok {
		t.Fatal("ParseSelectExpr(SELECT * FROM prices WHERE amount = 10.25) ok = false, want true")
	}
	if whereRealSel.Where == nil || len(whereRealSel.Where.Items) != 1 {
		t.Fatalf("ParseSelectExpr(...).Where = %#v, want one condition", whereRealSel.Where)
	}
	if whereRealSel.Where.Items[0].Condition.Right != RealValue(10.25) {
		t.Fatalf("ParseSelectExpr(...).Where.Items[0].Condition.Right = %#v, want %#v", whereRealSel.Where.Items[0].Condition.Right, RealValue(10.25))
	}

	wherePlaceholderSel, ok := ParseSelectExpr("SELECT * FROM t WHERE id = ?")
	if !ok {
		t.Fatal("ParseSelectExpr(SELECT * FROM t WHERE id = ?) ok = false, want true")
	}
	if wherePlaceholderSel.Where == nil || len(wherePlaceholderSel.Where.Items) != 1 {
		t.Fatalf("ParseSelectExpr(...).Where = %#v, want one condition", wherePlaceholderSel.Where)
	}
	if wherePlaceholderSel.Where.Items[0].Condition.Right.Kind != ValueKindPlaceholder {
		t.Fatalf("ParseSelectExpr(...).Where.Items[0].Condition.Right.Kind = %v, want %v", wherePlaceholderSel.Where.Items[0].Condition.Right.Kind, ValueKindPlaceholder)
	}
	if wherePlaceholderSel.Where.Items[0].Condition.Right.PlaceholderIndex != -1 {
		t.Fatalf("ParseSelectExpr(...).Where.Items[0].Condition.Right.PlaceholderIndex = %d, want -1", wherePlaceholderSel.Where.Items[0].Condition.Right.PlaceholderIndex)
	}
}

func TestParseSelectExprRealInvalid(t *testing.T) {
	tests := []string{
		"SELECT 1.",
		"SELECT .5",
		"SELECT 1e3",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			if got, ok := ParseSelectExpr(sql); ok {
				t.Fatalf("ParseSelectExpr(%q) = %#v, want failure", sql, got)
			}
		})
	}
}

func TestParseRejectsPlaceholderInCreateTableColumnDefinition(t *testing.T) {
	if stmt, err := Parse("CREATE TABLE t (? INT)"); err == nil {
		t.Fatalf("Parse(CREATE TABLE t (? INT)) = %#v, want error", stmt)
	}
}
