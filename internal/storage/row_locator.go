package storage

import "fmt"

// RowLocator identifies one physical row by page and slot.
type RowLocator struct {
	PageID uint32
	SlotID uint16
}

func (r RowLocator) String() string {
	return fmt.Sprintf("(%d,%d)", r.PageID, r.SlotID)
}
