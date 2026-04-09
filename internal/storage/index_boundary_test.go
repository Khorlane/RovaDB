package storage

import (
	"errors"
	"testing"
)

func TestValidateIndexRootAcceptsLeafAndInternalPages(t *testing.T) {
	leaf := InitIndexLeafPage(1)
	if err := ValidateIndexRoot(mapPageReader(map[uint32][]byte{1: leaf}), 1); err != nil {
		t.Fatalf("ValidateIndexRoot(leaf) error = %v", err)
	}

	internal := InitIndexInternalPage(2)
	insertInternalEntry(t, internal, "m", 3)
	if err := ValidateIndexRoot(mapPageReader(map[uint32][]byte{
		2: internal,
		3: InitIndexLeafPage(3),
	}), 2); err != nil {
		t.Fatalf("ValidateIndexRoot(internal) error = %v", err)
	}
}

func TestValidateIndexRootRejectsNonIndexPage(t *testing.T) {
	err := ValidateIndexRoot(mapPageReader(map[uint32][]byte{1: InitializeTablePage(1)}), 1)
	if !errors.Is(err, errCorruptedIndexPage) {
		t.Fatalf("ValidateIndexRoot() error = %v, want %v", err, errCorruptedIndexPage)
	}
}

func TestLookupSimpleIndexExactEncodesSingleValueSearchKey(t *testing.T) {
	page := InitIndexLeafPage(1)
	searchKey, err := EncodeIndexKey([]Value{StringValue("alice")})
	if err != nil {
		t.Fatalf("EncodeIndexKey() error = %v", err)
	}
	payload, err := EncodeIndexLeafEntry(searchKey, RowLocator{PageID: 10, SlotID: 2})
	if err != nil {
		t.Fatalf("EncodeIndexLeafEntry() error = %v", err)
	}
	if _, err := InsertIndexEntry(page, payload); err != nil {
		t.Fatalf("InsertIndexEntry() error = %v", err)
	}

	locators, err := LookupSimpleIndexExact(mapPageReader(map[uint32][]byte{1: page}), 1, StringValue("alice"))
	if err != nil {
		t.Fatalf("LookupSimpleIndexExact() error = %v", err)
	}
	if len(locators) != 1 || locators[0] != (RowLocator{PageID: 10, SlotID: 2}) {
		t.Fatalf("LookupSimpleIndexExact() = %#v, want [(10,2)]", locators)
	}
}

func TestReadAllSimpleIndexValuesInOrderDecodesSingleColumnKeys(t *testing.T) {
	page, err := BuildIndexLeafPageData(1, []IndexLeafRecord{
		{Key: mustEncodeSimpleIndexKey(t, Int64Value(1)), Locator: RowLocator{PageID: 10, SlotID: 0}},
		{Key: mustEncodeSimpleIndexKey(t, Int64Value(2)), Locator: RowLocator{PageID: 10, SlotID: 1}},
		{Key: mustEncodeSimpleIndexKey(t, Int64Value(3)), Locator: RowLocator{PageID: 10, SlotID: 2}},
	}, 0)
	if err != nil {
		t.Fatalf("BuildIndexLeafPageData() error = %v", err)
	}

	values, err := ReadAllSimpleIndexValuesInOrder(mapPageReader(map[uint32][]byte{1: page}), 1)
	if err != nil {
		t.Fatalf("ReadAllSimpleIndexValuesInOrder() error = %v", err)
	}
	if len(values) != 3 || values[0] != Int64Value(1) || values[1] != Int64Value(2) || values[2] != Int64Value(3) {
		t.Fatalf("ReadAllSimpleIndexValuesInOrder() = %#v, want [1 2 3]", values)
	}
}

func TestReadAllSimpleIndexValuesInOrderRejectsCompositeKeys(t *testing.T) {
	page, err := BuildIndexLeafPageData(1, []IndexLeafRecord{
		{Key: mustEncodeIndexKey(t, []Value{StringValue("alice"), Int64Value(1)}), Locator: RowLocator{PageID: 10, SlotID: 0}},
	}, 0)
	if err != nil {
		t.Fatalf("BuildIndexLeafPageData() error = %v", err)
	}

	_, err = ReadAllSimpleIndexValuesInOrder(mapPageReader(map[uint32][]byte{1: page}), 1)
	if !errors.Is(err, errCorruptedIndexPage) {
		t.Fatalf("ReadAllSimpleIndexValuesInOrder() error = %v, want %v", err, errCorruptedIndexPage)
	}
}

func mustEncodeSimpleIndexKey(t *testing.T, value Value) []byte {
	t.Helper()
	return mustEncodeIndexKey(t, []Value{value})
}

func mustEncodeIndexKey(t *testing.T, values []Value) []byte {
	t.Helper()
	key, err := EncodeIndexKey(values)
	if err != nil {
		t.Fatalf("EncodeIndexKey() error = %v", err)
	}
	return key
}
