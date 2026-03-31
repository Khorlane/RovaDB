package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestInitIndexLeafPage(t *testing.T) {
	page := InitIndexLeafPage(7)

	if got := len(page); got != PageSize {
		t.Fatalf("len(page) = %d, want %d", got, PageSize)
	}
	if got := binary.LittleEndian.Uint32(page[indexPageHeaderOffsetPageID : indexPageHeaderOffsetPageID+4]); got != 7 {
		t.Fatalf("pageID = %d, want 7", got)
	}
	if got := PageType(binary.LittleEndian.Uint16(page[indexPageHeaderOffsetPageType : indexPageHeaderOffsetPageType+2])); got != PageTypeIndexLeaf {
		t.Fatalf("pageType = %d, want %d", got, PageTypeIndexLeaf)
	}
	count, err := IndexPageEntryCount(page)
	if err != nil {
		t.Fatalf("IndexPageEntryCount() error = %v", err)
	}
	if count != 0 {
		t.Fatalf("IndexPageEntryCount() = %d, want 0", count)
	}
	freeStart, err := IndexPageFreeStart(page)
	if err != nil {
		t.Fatalf("IndexPageFreeStart() error = %v", err)
	}
	if freeStart != indexPageBodyStart {
		t.Fatalf("IndexPageFreeStart() = %d, want %d", freeStart, indexPageBodyStart)
	}
	freeEnd, err := IndexPageFreeEnd(page)
	if err != nil {
		t.Fatalf("IndexPageFreeEnd() error = %v", err)
	}
	if freeEnd != PageSize {
		t.Fatalf("IndexPageFreeEnd() = %d, want %d", freeEnd, PageSize)
	}
	sibling, err := IndexLeafRightSibling(page)
	if err != nil {
		t.Fatalf("IndexLeafRightSibling() error = %v", err)
	}
	if sibling != 0 {
		t.Fatalf("IndexLeafRightSibling() = %d, want 0", sibling)
	}
}

func TestInitIndexInternalPage(t *testing.T) {
	page := InitIndexInternalPage(8)

	if got := PageType(binary.LittleEndian.Uint16(page[indexPageHeaderOffsetPageType : indexPageHeaderOffsetPageType+2])); got != PageTypeIndexInternal {
		t.Fatalf("pageType = %d, want %d", got, PageTypeIndexInternal)
	}
	count, err := IndexPageEntryCount(page)
	if err != nil {
		t.Fatalf("IndexPageEntryCount() error = %v", err)
	}
	if count != 0 {
		t.Fatalf("IndexPageEntryCount() = %d, want 0", count)
	}
	freeSpace, err := IndexPageFreeSpace(page)
	if err != nil {
		t.Fatalf("IndexPageFreeSpace() error = %v", err)
	}
	if freeSpace != PageSize-indexPageBodyStart {
		t.Fatalf("IndexPageFreeSpace() = %d, want %d", freeSpace, PageSize-indexPageBodyStart)
	}
}

func TestInsertIndexEntry(t *testing.T) {
	page := InitIndexLeafPage(1)
	payload := []byte{0xAA, 0xBB, 0xCC}

	entryID, err := InsertIndexEntry(page, payload)
	if err != nil {
		t.Fatalf("InsertIndexEntry() error = %v", err)
	}
	if entryID != 0 {
		t.Fatalf("entryID = %d, want 0", entryID)
	}
	count, err := IndexPageEntryCount(page)
	if err != nil {
		t.Fatalf("IndexPageEntryCount() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("IndexPageEntryCount() = %d, want 1", count)
	}
	freeStart, err := IndexPageFreeStart(page)
	if err != nil {
		t.Fatalf("IndexPageFreeStart() error = %v", err)
	}
	if freeStart != indexPageBodyStart+indexPageEntrySize {
		t.Fatalf("IndexPageFreeStart() = %d, want %d", freeStart, indexPageBodyStart+indexPageEntrySize)
	}
	freeEnd, err := IndexPageFreeEnd(page)
	if err != nil {
		t.Fatalf("IndexPageFreeEnd() error = %v", err)
	}
	if freeEnd != PageSize-len(payload) {
		t.Fatalf("IndexPageFreeEnd() = %d, want %d", freeEnd, PageSize-len(payload))
	}
	offset, length, err := IndexPageEntry(page, 0)
	if err != nil {
		t.Fatalf("IndexPageEntry() error = %v", err)
	}
	if length != len(payload) {
		t.Fatalf("entry length = %d, want %d", length, len(payload))
	}
	if !bytes.Equal(page[offset:offset+length], payload) {
		t.Fatalf("payload bytes mismatch")
	}
}

func TestInsertMultipleIndexEntries(t *testing.T) {
	page := InitIndexInternalPage(2)
	payload1 := []byte{0x10, 0x11}
	payload2 := []byte{0x20, 0x21, 0x22}

	entry1, err := InsertIndexEntry(page, payload1)
	if err != nil {
		t.Fatalf("InsertIndexEntry(payload1) error = %v", err)
	}
	entry2, err := InsertIndexEntry(page, payload2)
	if err != nil {
		t.Fatalf("InsertIndexEntry(payload2) error = %v", err)
	}
	if entry1 != 0 || entry2 != 1 {
		t.Fatalf("entries = (%d, %d), want (0, 1)", entry1, entry2)
	}
	offset1, _, err := IndexPageEntry(page, entry1)
	if err != nil {
		t.Fatalf("IndexPageEntry(entry1) error = %v", err)
	}
	offset2, _, err := IndexPageEntry(page, entry2)
	if err != nil {
		t.Fatalf("IndexPageEntry(entry2) error = %v", err)
	}
	if offset2 >= offset1 {
		t.Fatalf("offset2 = %d, want less than offset1 = %d", offset2, offset1)
	}
}

func TestIndexLeafRightSibling(t *testing.T) {
	page := InitIndexLeafPage(3)

	if err := SetIndexLeafRightSibling(page, 99); err != nil {
		t.Fatalf("SetIndexLeafRightSibling() error = %v", err)
	}
	got, err := IndexLeafRightSibling(page)
	if err != nil {
		t.Fatalf("IndexLeafRightSibling() error = %v", err)
	}
	if got != 99 {
		t.Fatalf("IndexLeafRightSibling() = %d, want 99", got)
	}
}

func TestIndexLeafSiblingHelpersRejectInternalPage(t *testing.T) {
	page := InitIndexInternalPage(4)

	if _, err := IndexLeafRightSibling(page); !errors.Is(err, errCorruptedIndexPage) {
		t.Fatalf("IndexLeafRightSibling() error = %v, want %v", err, errCorruptedIndexPage)
	}
	if err := SetIndexLeafRightSibling(page, 5); !errors.Is(err, errCorruptedIndexPage) {
		t.Fatalf("SetIndexLeafRightSibling() error = %v, want %v", err, errCorruptedIndexPage)
	}
}

func TestIndexPageRejectsWrongPageType(t *testing.T) {
	page := InitializeTablePage(5)

	if _, err := IndexPageEntryCount(page); !errors.Is(err, errCorruptedIndexPage) {
		t.Fatalf("IndexPageEntryCount() error = %v, want %v", err, errCorruptedIndexPage)
	}
}

func TestIndexPageRejectsInvalidPageSize(t *testing.T) {
	page := make([]byte, PageSize-1)

	if _, err := IndexPageEntryCount(page); !errors.Is(err, errCorruptedIndexPage) {
		t.Fatalf("IndexPageEntryCount() error = %v, want %v", err, errCorruptedIndexPage)
	}
}

func TestIndexPageEntryRejectsOutOfRange(t *testing.T) {
	page := InitIndexLeafPage(6)

	if _, _, err := IndexPageEntry(page, 0); !errors.Is(err, errCorruptedIndexPage) {
		t.Fatalf("IndexPageEntry() error = %v, want %v", err, errCorruptedIndexPage)
	}
}

func TestInsertIndexEntryRejectsInsufficientSpace(t *testing.T) {
	page := InitIndexLeafPage(7)
	payload := bytes.Repeat([]byte("x"), PageSize)

	_, err := InsertIndexEntry(page, payload)
	if !errors.Is(err, errIndexPageFull) {
		t.Fatalf("InsertIndexEntry() error = %v, want %v", err, errIndexPageFull)
	}
}

func TestEncodeDecodeIndexInternalEntry(t *testing.T) {
	payload, err := EncodeIndexInternalEntry([]byte("m"), 42)
	if err != nil {
		t.Fatalf("EncodeIndexInternalEntry() error = %v", err)
	}

	key, childPageID, err := DecodeIndexInternalEntry(payload)
	if err != nil {
		t.Fatalf("DecodeIndexInternalEntry() error = %v", err)
	}
	if string(key) != "m" {
		t.Fatalf("key = %q, want %q", key, "m")
	}
	if childPageID != 42 {
		t.Fatalf("childPageID = %d, want 42", childPageID)
	}
}

func TestEncodeDecodeIndexLeafEntry(t *testing.T) {
	payload, err := EncodeIndexLeafEntry([]byte("alice"), RowLocator{PageID: 7, SlotID: 3})
	if err != nil {
		t.Fatalf("EncodeIndexLeafEntry() error = %v", err)
	}

	key, locator, err := DecodeIndexLeafEntry(payload)
	if err != nil {
		t.Fatalf("DecodeIndexLeafEntry() error = %v", err)
	}
	if string(key) != "alice" {
		t.Fatalf("key = %q, want %q", key, "alice")
	}
	if locator != (RowLocator{PageID: 7, SlotID: 3}) {
		t.Fatalf("locator = %#v, want (7,3)", locator)
	}
}

func TestIndexPageEntryPayload(t *testing.T) {
	page := InitIndexLeafPage(8)
	payload, err := EncodeIndexLeafEntry([]byte("a"), RowLocator{PageID: 1, SlotID: 2})
	if err != nil {
		t.Fatalf("EncodeIndexLeafEntry() error = %v", err)
	}
	if _, err := InsertIndexEntry(page, payload); err != nil {
		t.Fatalf("InsertIndexEntry() error = %v", err)
	}

	got, err := IndexPageEntryPayload(page, 0)
	if err != nil {
		t.Fatalf("IndexPageEntryPayload() error = %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("IndexPageEntryPayload() = %#v, want %#v", got, payload)
	}
}

func TestIndexLeafEntry(t *testing.T) {
	page := InitIndexLeafPage(9)
	payload, err := EncodeIndexLeafEntry([]byte("alice"), RowLocator{PageID: 5, SlotID: 1})
	if err != nil {
		t.Fatalf("EncodeIndexLeafEntry() error = %v", err)
	}
	if _, err := InsertIndexEntry(page, payload); err != nil {
		t.Fatalf("InsertIndexEntry() error = %v", err)
	}

	key, locator, err := IndexLeafEntry(page, 0)
	if err != nil {
		t.Fatalf("IndexLeafEntry() error = %v", err)
	}
	if string(key) != "alice" {
		t.Fatalf("key = %q, want %q", key, "alice")
	}
	if locator != (RowLocator{PageID: 5, SlotID: 1}) {
		t.Fatalf("locator = %#v, want (5,1)", locator)
	}
}

func TestIndexInternalEntry(t *testing.T) {
	page := InitIndexInternalPage(10)
	payload, err := EncodeIndexInternalEntry([]byte("m"), 11)
	if err != nil {
		t.Fatalf("EncodeIndexInternalEntry() error = %v", err)
	}
	if _, err := InsertIndexEntry(page, payload); err != nil {
		t.Fatalf("InsertIndexEntry() error = %v", err)
	}

	key, childPageID, err := IndexInternalEntry(page, 0)
	if err != nil {
		t.Fatalf("IndexInternalEntry() error = %v", err)
	}
	if string(key) != "m" {
		t.Fatalf("key = %q, want %q", key, "m")
	}
	if childPageID != 11 {
		t.Fatalf("childPageID = %d, want 11", childPageID)
	}
}

func TestDecodeIndexEntriesRejectMalformedPayload(t *testing.T) {
	if _, _, err := DecodeIndexInternalEntry([]byte{1}); !errors.Is(err, errCorruptedIndexPage) {
		t.Fatalf("DecodeIndexInternalEntry() error = %v, want %v", err, errCorruptedIndexPage)
	}
	if _, _, err := DecodeIndexLeafEntry([]byte{1}); !errors.Is(err, errCorruptedIndexPage) {
		t.Fatalf("DecodeIndexLeafEntry() error = %v, want %v", err, errCorruptedIndexPage)
	}
}

func TestInsertIndexLeafRecordSortedMaintainsOrder(t *testing.T) {
	page := InitIndexLeafPage(11)

	var err error
	page, err = InsertIndexLeafRecordSorted(page, []byte("bob"), RowLocator{PageID: 1, SlotID: 1})
	if err != nil {
		t.Fatalf("InsertIndexLeafRecordSorted(bob) error = %v", err)
	}
	page, err = InsertIndexLeafRecordSorted(page, []byte("alice"), RowLocator{PageID: 1, SlotID: 0})
	if err != nil {
		t.Fatalf("InsertIndexLeafRecordSorted(alice) error = %v", err)
	}
	page, err = InsertIndexLeafRecordSorted(page, []byte("alice"), RowLocator{PageID: 1, SlotID: 2})
	if err != nil {
		t.Fatalf("InsertIndexLeafRecordSorted(alice duplicate) error = %v", err)
	}

	records, err := ReadIndexLeafRecords(page)
	if err != nil {
		t.Fatalf("ReadIndexLeafRecords() error = %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("len(records) = %d, want 3", len(records))
	}
	if string(records[0].Key) != "alice" || records[0].Locator != (RowLocator{PageID: 1, SlotID: 0}) {
		t.Fatalf("records[0] = %#v, want alice -> (1,0)", records[0])
	}
	if string(records[1].Key) != "alice" || records[1].Locator != (RowLocator{PageID: 1, SlotID: 2}) {
		t.Fatalf("records[1] = %#v, want alice -> (1,2)", records[1])
	}
	if string(records[2].Key) != "bob" || records[2].Locator != (RowLocator{PageID: 1, SlotID: 1}) {
		t.Fatalf("records[2] = %#v, want bob -> (1,1)", records[2])
	}
}

func TestInsertIndexLeafRecordSortedRejectsFullPage(t *testing.T) {
	page := InitIndexLeafPage(12)
	largeKey := make([]byte, 512)
	for i := range largeKey {
		largeKey[i] = 'x'
	}

	for {
		next, err := InsertIndexLeafRecordSorted(page, largeKey, RowLocator{PageID: 1, SlotID: 1})
		if errors.Is(err, errIndexPageFull) {
			return
		}
		if err != nil {
			t.Fatalf("InsertIndexLeafRecordSorted() error = %v, want %v", err, errIndexPageFull)
		}
		page = next
	}
}

func TestDeleteIndexLeafRecordRemovesOneMatchingDuplicate(t *testing.T) {
	records := []IndexLeafRecord{
		{Key: []byte("alice"), Locator: RowLocator{PageID: 1, SlotID: 0}},
		{Key: []byte("alice"), Locator: RowLocator{PageID: 1, SlotID: 1}},
		{Key: []byte("bob"), Locator: RowLocator{PageID: 1, SlotID: 2}},
	}

	updated, removed := DeleteIndexLeafRecord(records, []byte("alice"), RowLocator{PageID: 1, SlotID: 1})
	if !removed {
		t.Fatal("DeleteIndexLeafRecord() removed = false, want true")
	}
	if len(updated) != 2 {
		t.Fatalf("len(updated) = %d, want 2", len(updated))
	}
	if string(updated[0].Key) != "alice" || updated[0].Locator != (RowLocator{PageID: 1, SlotID: 0}) {
		t.Fatalf("updated[0] = %#v, want alice -> (1,0)", updated[0])
	}
	if string(updated[1].Key) != "bob" || updated[1].Locator != (RowLocator{PageID: 1, SlotID: 2}) {
		t.Fatalf("updated[1] = %#v, want bob -> (1,2)", updated[1])
	}
}

func TestDeleteIndexLeafRecordReturnsCloneWhenMissing(t *testing.T) {
	records := []IndexLeafRecord{
		{Key: []byte("alice"), Locator: RowLocator{PageID: 1, SlotID: 0}},
	}

	updated, removed := DeleteIndexLeafRecord(records, []byte("bob"), RowLocator{PageID: 1, SlotID: 1})
	if removed {
		t.Fatal("DeleteIndexLeafRecord() removed = true, want false")
	}
	if len(updated) != 1 || string(updated[0].Key) != "alice" {
		t.Fatalf("updated = %#v, want unchanged alice record", updated)
	}
	updated[0].Key[0] = 'z'
	if string(records[0].Key) != "alice" {
		t.Fatalf("records mutated to %#v, want original preserved", records)
	}
}

func TestBuildIndexLeafPageData(t *testing.T) {
	page, err := BuildIndexLeafPageData(13, []IndexLeafRecord{
		{Key: []byte("alice"), Locator: RowLocator{PageID: 1, SlotID: 0}},
		{Key: []byte("bob"), Locator: RowLocator{PageID: 1, SlotID: 1}},
	}, 14)
	if err != nil {
		t.Fatalf("BuildIndexLeafPageData() error = %v", err)
	}
	sibling, err := IndexLeafRightSibling(page)
	if err != nil {
		t.Fatalf("IndexLeafRightSibling() error = %v", err)
	}
	if sibling != 14 {
		t.Fatalf("IndexLeafRightSibling() = %d, want 14", sibling)
	}
	records, err := ReadAllIndexLeafRecords(page)
	if err != nil {
		t.Fatalf("ReadAllIndexLeafRecords() error = %v", err)
	}
	if len(records) != 2 || string(records[0].Key) != "alice" || string(records[1].Key) != "bob" {
		t.Fatalf("records = %#v, want [alice bob]", records)
	}
}

func TestBuildIndexInternalPageData(t *testing.T) {
	page, err := BuildIndexInternalPageData(15, []IndexInternalRecord{
		{Key: []byte("m"), ChildPageID: 2},
		{Key: []byte("m"), ChildPageID: 3},
	})
	if err != nil {
		t.Fatalf("BuildIndexInternalPageData() error = %v", err)
	}
	records, err := ReadAllIndexInternalRecords(page)
	if err != nil {
		t.Fatalf("ReadAllIndexInternalRecords() error = %v", err)
	}
	if len(records) != 2 || string(records[0].Key) != "m" || records[0].ChildPageID != 2 || records[1].ChildPageID != 3 {
		t.Fatalf("records = %#v, want two m records pointing to 2 and 3", records)
	}
}

func TestSplitIndexLeafRecords(t *testing.T) {
	left, right, separatorKey, err := SplitIndexLeafRecords([]IndexLeafRecord{
		{Key: []byte("a"), Locator: RowLocator{PageID: 1, SlotID: 0}},
		{Key: []byte("b"), Locator: RowLocator{PageID: 1, SlotID: 1}},
		{Key: []byte("c"), Locator: RowLocator{PageID: 1, SlotID: 2}},
		{Key: []byte("d"), Locator: RowLocator{PageID: 1, SlotID: 3}},
	})
	if err != nil {
		t.Fatalf("SplitIndexLeafRecords() error = %v", err)
	}
	if len(left) != 2 || len(right) != 2 {
		t.Fatalf("split lens = (%d,%d), want (2,2)", len(left), len(right))
	}
	if string(separatorKey) != "c" {
		t.Fatalf("separatorKey = %q, want %q", separatorKey, "c")
	}
}

func TestSplitIndexInternalRecords(t *testing.T) {
	left, right, separatorKey, err := SplitIndexInternalRecords([]IndexInternalRecord{
		{Key: []byte("m"), ChildPageID: 2},
		{Key: []byte("t"), ChildPageID: 3},
		{Key: []byte("z"), ChildPageID: 4},
		{Key: []byte("z"), ChildPageID: 5},
	})
	if err != nil {
		t.Fatalf("SplitIndexInternalRecords() error = %v", err)
	}
	if len(left) != 2 || len(right) != 2 {
		t.Fatalf("split lens = (%d,%d), want (2,2)", len(left), len(right))
	}
	if string(separatorKey) != "z" {
		t.Fatalf("separatorKey = %q, want %q", separatorKey, "z")
	}
}
