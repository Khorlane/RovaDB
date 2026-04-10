package storage

const (
	CatalogForeignKeyDeleteActionRestrict uint8 = 1 + iota
	CatalogForeignKeyDeleteActionCascade
)

// CatalogPrimaryKey is persisted primary-key metadata owned by a table.
type CatalogPrimaryKey struct {
	Name       string
	TableID    uint32
	Columns    []string
	IndexID    uint32
	ImplicitNN bool
}

// CatalogForeignKey is persisted foreign-key metadata owned by a child table.
type CatalogForeignKey struct {
	Name                 string
	ChildTableID         uint32
	ChildColumns         []string
	ParentTableID        uint32
	ParentColumns        []string
	ParentPrimaryKeyName string
	ChildIndexID         uint32
	OnDeleteAction       uint8
}
