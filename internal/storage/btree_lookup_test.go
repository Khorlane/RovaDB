package storage

import (
	"errors"
	"testing"
)

func TestLookupIndexExactOnRootLeaf(t *testing.T) {
	page := InitIndexLeafPage(1)
	insertLeafEntry(t, page, "alice", RowLocator{PageID: 10, SlotID: 0})
	insertLeafEntry(t, page, "bob", RowLocator{PageID: 10, SlotID: 1})

	locators, err := LookupIndexExact(mapPageReader(map[uint32][]byte{1: page}), 1, []byte("alice"))
	if err != nil {
		t.Fatalf("LookupIndexExact() error = %v", err)
	}
	if len(locators) != 1 || locators[0] != (RowLocator{PageID: 10, SlotID: 0}) {
		t.Fatalf("LookupIndexExact() = %#v, want [(10,0)]", locators)
	}
}

func TestLookupIndexExactOnRootLeafReturnsEmptyForMiss(t *testing.T) {
	page := InitIndexLeafPage(1)
	insertLeafEntry(t, page, "alice", RowLocator{PageID: 10, SlotID: 0})

	locators, err := LookupIndexExact(mapPageReader(map[uint32][]byte{1: page}), 1, []byte("bob"))
	if err != nil {
		t.Fatalf("LookupIndexExact() error = %v", err)
	}
	if len(locators) != 0 {
		t.Fatalf("LookupIndexExact() = %#v, want empty result", locators)
	}
}

func TestFindIndexLeafPageDescendsLeftChild(t *testing.T) {
	left := InitIndexLeafPage(2)
	right := InitIndexLeafPage(3)
	insertLeafEntry(t, left, "alice", RowLocator{PageID: 10, SlotID: 0})
	insertLeafEntry(t, right, "zebra", RowLocator{PageID: 11, SlotID: 0})

	root := InitIndexInternalPage(1)
	insertInternalEntry(t, root, "m", 2)
	insertInternalEntry(t, root, "z", 3)

	leafPageID, _, err := FindIndexLeafPage(mapPageReader(map[uint32][]byte{
		1: root,
		2: left,
		3: right,
	}), 1, []byte("apple"))
	if err != nil {
		t.Fatalf("FindIndexLeafPage() error = %v", err)
	}
	if leafPageID != 2 {
		t.Fatalf("FindIndexLeafPage() leafPageID = %d, want 2", leafPageID)
	}
}

func TestFindIndexLeafPageDescendsRightmostChild(t *testing.T) {
	left := InitIndexLeafPage(2)
	right := InitIndexLeafPage(3)
	insertLeafEntry(t, left, "alice", RowLocator{PageID: 10, SlotID: 0})
	insertLeafEntry(t, right, "zebra", RowLocator{PageID: 11, SlotID: 0})

	root := InitIndexInternalPage(1)
	insertInternalEntry(t, root, "m", 2)
	insertInternalEntry(t, root, "z", 3)

	leafPageID, _, err := FindIndexLeafPage(mapPageReader(map[uint32][]byte{
		1: root,
		2: left,
		3: right,
	}), 1, []byte("zzz"))
	if err != nil {
		t.Fatalf("FindIndexLeafPage() error = %v", err)
	}
	if leafPageID != 3 {
		t.Fatalf("FindIndexLeafPage() leafPageID = %d, want 3", leafPageID)
	}
}

func TestLookupIndexLeafExactReturnsDuplicateLocatorsInOrder(t *testing.T) {
	page := InitIndexLeafPage(1)
	insertLeafEntry(t, page, "alice", RowLocator{PageID: 10, SlotID: 0})
	insertLeafEntry(t, page, "alice", RowLocator{PageID: 10, SlotID: 1})
	insertLeafEntry(t, page, "bob", RowLocator{PageID: 10, SlotID: 2})

	locators, err := LookupIndexLeafExact(page, []byte("alice"))
	if err != nil {
		t.Fatalf("LookupIndexLeafExact() error = %v", err)
	}
	want := []RowLocator{{PageID: 10, SlotID: 0}, {PageID: 10, SlotID: 1}}
	if len(locators) != len(want) {
		t.Fatalf("LookupIndexLeafExact() = %#v, want %#v", locators, want)
	}
	for i := range want {
		if locators[i] != want[i] {
			t.Fatalf("LookupIndexLeafExact()[%d] = %#v, want %#v", i, locators[i], want[i])
		}
	}
}

func TestLookupIndexExactRejectsMalformedInternalEntry(t *testing.T) {
	root := InitIndexInternalPage(1)
	if _, err := InsertIndexEntry(root, []byte{1}); err != nil {
		t.Fatalf("InsertIndexEntry() error = %v", err)
	}

	_, err := LookupIndexExact(mapPageReader(map[uint32][]byte{1: root}), 1, []byte("a"))
	if !errors.Is(err, errCorruptedIndexPage) {
		t.Fatalf("LookupIndexExact() error = %v, want %v", err, errCorruptedIndexPage)
	}
}

func TestLookupIndexLeafExactRejectsMalformedLeafEntry(t *testing.T) {
	page := InitIndexLeafPage(1)
	if _, err := InsertIndexEntry(page, []byte{1}); err != nil {
		t.Fatalf("InsertIndexEntry() error = %v", err)
	}

	_, err := LookupIndexLeafExact(page, []byte("a"))
	if !errors.Is(err, errCorruptedIndexPage) {
		t.Fatalf("LookupIndexLeafExact() error = %v, want %v", err, errCorruptedIndexPage)
	}
}

func TestFindIndexLeafPageRejectsWrongPageTypeDuringDescent(t *testing.T) {
	root := InitIndexInternalPage(1)
	insertInternalEntry(t, root, "m", 2)
	tablePage := InitializeTablePage(2)

	_, _, err := FindIndexLeafPage(mapPageReader(map[uint32][]byte{
		1: root,
		2: tablePage,
	}), 1, []byte("a"))
	if !errors.Is(err, errCorruptedIndexPage) {
		t.Fatalf("FindIndexLeafPage() error = %v, want %v", err, errCorruptedIndexPage)
	}
}

func TestFindIndexLeafPageReturnsMissingChildError(t *testing.T) {
	root := InitIndexInternalPage(1)
	insertInternalEntry(t, root, "m", 2)
	wantErr := errors.New("missing page")

	_, _, err := FindIndexLeafPage(func(pageID uint32) ([]byte, error) {
		if pageID == 1 {
			return root, nil
		}
		return nil, wantErr
	}, 1, []byte("a"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("FindIndexLeafPage() error = %v, want %v", err, wantErr)
	}
}

func TestLookupIndexExactRejectsInvalidRootPageID(t *testing.T) {
	_, err := LookupIndexExact(mapPageReader(nil), 0, []byte("a"))
	if !errors.Is(err, errCorruptedIndexPage) {
		t.Fatalf("LookupIndexExact() error = %v, want %v", err, errCorruptedIndexPage)
	}
}

func TestLookupIndexExactAcrossSplitLeafTree(t *testing.T) {
	left, err := BuildIndexLeafPageData(2, []IndexLeafRecord{
		{Key: []byte("alice"), Locator: RowLocator{PageID: 10, SlotID: 0}},
		{Key: []byte("bob"), Locator: RowLocator{PageID: 10, SlotID: 1}},
	}, 3)
	if err != nil {
		t.Fatalf("BuildIndexLeafPageData(left) error = %v", err)
	}
	right, err := BuildIndexLeafPageData(3, []IndexLeafRecord{
		{Key: []byte("cara"), Locator: RowLocator{PageID: 10, SlotID: 2}},
		{Key: []byte("dora"), Locator: RowLocator{PageID: 10, SlotID: 3}},
	}, 0)
	if err != nil {
		t.Fatalf("BuildIndexLeafPageData(right) error = %v", err)
	}
	root, err := BuildIndexInternalPageData(1, []IndexInternalRecord{
		{Key: []byte("cara"), ChildPageID: 2},
		{Key: []byte("cara"), ChildPageID: 3},
	})
	if err != nil {
		t.Fatalf("BuildIndexInternalPageData(root) error = %v", err)
	}

	locators, err := LookupIndexExact(mapPageReader(map[uint32][]byte{
		1: root,
		2: left,
		3: right,
	}), 1, []byte("dora"))
	if err != nil {
		t.Fatalf("LookupIndexExact() error = %v", err)
	}
	if len(locators) != 1 || locators[0] != (RowLocator{PageID: 10, SlotID: 3}) {
		t.Fatalf("LookupIndexExact() = %#v, want [(10,3)]", locators)
	}
}

func insertLeafEntry(t *testing.T, page []byte, key string, locator RowLocator) {
	t.Helper()

	payload, err := EncodeIndexLeafEntry([]byte(key), locator)
	if err != nil {
		t.Fatalf("EncodeIndexLeafEntry() error = %v", err)
	}
	if _, err := InsertIndexEntry(page, payload); err != nil {
		t.Fatalf("InsertIndexEntry() error = %v", err)
	}
}

func insertInternalEntry(t *testing.T, page []byte, key string, childPageID uint32) {
	t.Helper()

	payload, err := EncodeIndexInternalEntry([]byte(key), childPageID)
	if err != nil {
		t.Fatalf("EncodeIndexInternalEntry() error = %v", err)
	}
	if _, err := InsertIndexEntry(page, payload); err != nil {
		t.Fatalf("InsertIndexEntry() error = %v", err)
	}
}

func mapPageReader(pages map[uint32][]byte) IndexPageReader {
	return func(pageID uint32) ([]byte, error) {
		page, ok := pages[pageID]
		if !ok {
			return nil, errCorruptedIndexPage
		}
		return append([]byte(nil), page...), nil
	}
}
