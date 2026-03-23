package planner

import (
	"reflect"
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestBasicIndexRebuildPopulatesExpectedRows(t *testing.T) {
	idx := NewBasicIndex("users", "name")

	err := idx.Rebuild(
		[]string{"id", "name"},
		[][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(2), parser.StringValue("bob")},
			{parser.Int64Value(3), parser.StringValue("alice")},
		},
	)
	if err != nil {
		t.Fatalf("Rebuild() error = %v", err)
	}

	if got := idx.LookupEqual(parser.StringValue("alice")); !reflect.DeepEqual(got, []int{0, 2}) {
		t.Fatalf("LookupEqual(alice) = %#v, want []int{0, 2}", got)
	}
	if got := idx.LookupEqual(parser.StringValue("bob")); !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("LookupEqual(bob) = %#v, want []int{1}", got)
	}
}

func TestBasicIndexNullValuesAreDistinct(t *testing.T) {
	idx := NewBasicIndex("users", "name")

	err := idx.Rebuild(
		[]string{"id", "name"},
		[][]parser.Value{
			{parser.Int64Value(1), parser.NullValue()},
			{parser.Int64Value(2), parser.StringValue("")},
			{parser.Int64Value(3), parser.NullValue()},
		},
	)
	if err != nil {
		t.Fatalf("Rebuild() error = %v", err)
	}

	if got := idx.LookupEqual(parser.NullValue()); !reflect.DeepEqual(got, []int{0, 2}) {
		t.Fatalf("LookupEqual(NULL) = %#v, want []int{0, 2}", got)
	}
	if got := idx.LookupEqual(parser.StringValue("")); !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("LookupEqual(empty string) = %#v, want []int{1}", got)
	}
}

func TestBasicIndexMissingColumnReturnsError(t *testing.T) {
	idx := NewBasicIndex("users", "email")

	err := idx.Rebuild(
		[]string{"id", "name"},
		[][]parser.Value{{parser.Int64Value(1), parser.StringValue("alice")}},
	)
	if err != errIndexColumnDoesNotExist {
		t.Fatalf("Rebuild() error = %v, want %v", err, errIndexColumnDoesNotExist)
	}
}

func TestBasicIndexLookupEqualReturnsCopy(t *testing.T) {
	idx := NewBasicIndex("users", "id")

	err := idx.Rebuild(
		[]string{"id", "name"},
		[][]parser.Value{
			{parser.Int64Value(1), parser.StringValue("alice")},
			{parser.Int64Value(1), parser.StringValue("bob")},
		},
	)
	if err != nil {
		t.Fatalf("Rebuild() error = %v", err)
	}

	got := idx.LookupEqual(parser.Int64Value(1))
	if !reflect.DeepEqual(got, []int{0, 1}) {
		t.Fatalf("LookupEqual(1) = %#v, want []int{0, 1}", got)
	}

	got[0] = 99
	if after := idx.LookupEqual(parser.Int64Value(1)); !reflect.DeepEqual(after, []int{0, 1}) {
		t.Fatalf("LookupEqual(1) after caller mutation = %#v, want []int{0, 1}", after)
	}
}
