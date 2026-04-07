# Physical Storage Layer Design

## Purpose

This note locks the target architecture for the Physical Storage Layer milestone.

It is intentionally a design decision document, not an implementation document.
It does not mean the model described here is already implemented in the current
engine.

## Current Implemented State

Today, RovaDB already has:

- one single database file
- a global `4096`-byte page size
- page 0 as the durable database header / directory control page
- CAT/DIR metadata persisted in page 0 or CAT/DIR overflow pages
- durable page-backed indexes

Today, table row storage still has the important limiting assumption that one
table's row storage is effectively rooted in a single table page. This note
defines the architecture that should replace that limitation.

## Locked Target Architecture

The Physical Storage Layer milestone will keep one single database file backed
by one global 4 KB page heap.

The physical page model is locked to exactly three physical page types:

- `Header`
- `SpaceMap`
- `Data`

These are physical storage roles. They do not replace CAT/DIR's logical schema
authority.

## Header Roles

The `Header` physical page type has exactly two logical roles:

- `DatabaseHeader`
- `TableHeader`

### DatabaseHeader

- page 0 remains the `DatabaseHeader`
- page 0 continues to own durable database-level control metadata
- CAT/DIR remains authoritative for logical metadata such as table identity,
  schema metadata, and logical object mappings

### TableHeader

- each table has exactly one `TableHeader`
- `TableHeader` is authoritative for that table's physical root metadata
- `TableHeader` points to the first `SpaceMap` page for the table

## Table Ownership Model

Each table owns its own `SpaceMap` pages and `Data` pages.

That ownership is logical and metadata-driven, not physically contiguous in the
database file. A table's pages do not need to be adjacent on disk.

The intended authority split is:

- CAT/DIR is authoritative for logical metadata
- `TableHeader` is authoritative for physical table root metadata
- `SpaceMap` is authoritative for the table-local `Data`-page inventory and
  free-space classification
- `Data` pages are authoritative only for row and slot contents

## Data Pages And Row Addressing

Rows are physically addressed by:

- `PageID`
- `SlotID`

Within a `Data` page:

- row payload grows upward
- slot metadata grows downward

This row locator contract remains the physical row identity model for phase one
of the Physical Storage Layer milestone.

## SpaceMap Model

Each table's `SpaceMap` is the authoritative inventory for that table's owned
`Data` pages.

The `SpaceMap` design is locked to these properties:

- `TableHeader` points to the first `SpaceMap` page
- `SpaceMap` may grow through a linked metadata chain
- `SpaceMap` entries track explicit `Data PageID` values
- `SpaceMap` does not track physical extents

The free-space classification buckets are locked to:

- `Full`
- `Low`
- `Medium`
- `High`

The insert path is locked to be `SpaceMap`-driven. Inserts should consult the
table's `SpaceMap` inventory and free-space classification rather than walking a
linked list of `Data` pages looking for room.

## Mutation Model

For the first implementation phase of this architecture, row-growth updates are
locked to the simpler correctness-first rule:

- if an update cannot fit in the current row location, it relocates through
  delete plus reinsert semantics

This avoids prematurely locking in an in-place row-growth strategy before the
multi-page storage model is proven correct.

## Recovery And Durability Invariants

The Physical Storage Layer milestone must preserve these core recovery
invariants:

- no orphan owned pages
- no false references
- no row visibility ahead of ownership
- authoritative counts must match reality if stored

These invariants apply across normal execution, commit, crash recovery, reopen,
and corruption-detection paths.

## Byte Layout Still Intentionally Undefined

This note intentionally does not yet define exact byte-level layouts for:

- `DatabaseHeader`
- `TableHeader`
- `SpaceMap`
- `Data`
- `SpaceMap` entry encoding
- bucket thresholds
- chain-pointer field positions
- per-page counters
- exact WAL/journal record shapes needed for this model

Those details belong to later implementation slices once the architecture itself
is stable and accepted.

## Non-Goals Of This Note

This note does not claim that:

- `TableHeader` pages already exist in code
- `SpaceMap` pages already exist in code
- current tables already use the new ownership model
- current on-disk page-type enums already expose these new types
- the multi-page table-storage implementation is already complete

It only locks the target architecture so implementation slices can proceed
without re-deciding the storage model.
