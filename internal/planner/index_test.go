package planner

import "testing"

func TestNewBasicIndexBuildsMetadataOnlyShell(t *testing.T) {
	idx := NewBasicIndex("users", "name")
	if idx == nil {
		t.Fatal("NewBasicIndex() = nil, want value")
	}
	if idx.TableName != "users" || idx.ColumnName != "name" {
		t.Fatalf("NewBasicIndex() = %#v, want users.name metadata", idx)
	}
	if idx.IndexID != 0 || idx.RootPageID != 0 {
		t.Fatalf("NewBasicIndex() = %#v, want zero IDs until hydrated", idx)
	}
}

func TestBasicIndexRetainsLogicalMetadata(t *testing.T) {
	idx := NewBasicIndex("users", "name")
	idx.IndexID = 7
	idx.RootPageID = 99

	if idx.TableName != "users" || idx.ColumnName != "name" || idx.IndexID != 7 || idx.RootPageID != 99 {
		t.Fatalf("BasicIndex metadata = %#v, want users.name ids 7/99", idx)
	}
}
