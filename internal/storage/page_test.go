package storage

import "testing"

func TestNewPage(t *testing.T) {
	page := NewPage(7)
	if page == nil {
		t.Fatal("NewPage() = nil")
	}
	if got := page.ID(); got != 7 {
		t.Fatalf("page.ID() = %d, want 7", got)
	}
	if got := len(page.Data()); got != PageSize {
		t.Fatalf("len(page.Data()) = %d, want %d", got, PageSize)
	}
	if page.Dirty() {
		t.Fatal("page.Dirty() = true, want false")
	}
}

func TestPageMarkDirty(t *testing.T) {
	page := NewPage(1)
	page.MarkDirty()
	if !page.Dirty() {
		t.Fatal("page.Dirty() = false, want true")
	}
}

func TestPageOffset(t *testing.T) {
	tests := []struct {
		id   PageID
		want int64
	}{
		{id: 0, want: HeaderSize},
		{id: 1, want: HeaderSize + PageSize},
		{id: 2, want: HeaderSize + 2*PageSize},
	}

	for _, tt := range tests {
		if got := pageOffset(tt.id); got != tt.want {
			t.Fatalf("pageOffset(%d) = %d, want %d", tt.id, got, tt.want)
		}
	}
}
