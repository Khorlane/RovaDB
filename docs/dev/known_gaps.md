# RovaDB Known Gaps

This document tracks concrete identified gaps in current RovaDB behavior.

It is intended to be a lightweight developer backlog for things that should be fixed, clarified, or explored.

Prefix values:

- kg - known gap
- dx - design exploration

Status values:

- `pending`
- `in progress`
- `done`

## Summary

- [kg002] Engine `done` Review text comparison / collation behavior `(commit: 2edb9e1)`
- [kg015] Engine `done` Expose catalog/schema introspection in the public API `(commit: 97c726c)`
- [kg024] Engine `done` Make `CREATE INDEX` executable and durable
- [kg025] Engine `done` Make `DROP INDEX` executable and durable
- [kg026] Engine `done` Make `DROP TABLE` executable and durable
- [kg022] Engine `done` Realign `INT` semantics to 32-bit
- [kg023] Engine `done` Enforce a bounded indexable TEXT size
- [dx005] Physical Storage Layer milestone `done`
- [dx006] Physical Storage Polish milestone
- [dx001] Explore `NOT NULL`, `NOT NULL WITH DEFAULT`, etc
- [dx002] Explore planner usage for multi-column indexes
- [dx003] Explore primary key as an explicit table-definition contract
- [dx004] Explore table-level foreign key constraints

## Engine

### `done` Review text comparison / collation behavior [kg002] `(commit: 2edb9e1)`

Observed gap:

```sql
SELECT * FROM users WHERE name < 'bab'
```

Observed behavior in testing:

- rows such as `Charlie` may compare as less than `'bab'`

Expected direction:

- document and/or refine string comparison semantics so text predicates match the intended SQL behavior and user expectations

### `done` Expose catalog/schema introspection in the public API [kg015] `(commit: 97c726c)`

Observed gap:

- the CLI currently reaches into `internal/storage` to implement `tables` and `schema`
- a Go program embedding RovaDB may also need access to table and column metadata

Expected direction:

- provide a supported public API for catalog/schema inspection
- the CLI and Go callers should be able to discover table names and column definitions without depending on internal storage packages

### `done` Make `CREATE INDEX` executable and durable [kg024]

Resolved direction:

- `CREATE INDEX` and `CREATE UNIQUE INDEX` are now executable through `Exec`
- the full SQL-visible index definition is persisted and reloaded durably
- non-unique and unique index rules are enforced on later writes
- planner usage remains intentionally narrow for now:
  - compatible single-column ascending non-unique equality indexes participate in the current planner path
  - broader multi-column planner behavior remains tracked under `dx002`

### `done` Make `DROP INDEX` executable and durable [kg025]

Resolved direction:

- `DROP INDEX` is now executable through `Exec`
- dropped indexes are removed from runtime and durable catalog state
- planner state falls back cleanly when a compatible index is removed
- reopen and rollback-journal recovery preserve the correct dropped or pre-drop committed state

### `done` Make `DROP TABLE` executable and durable [kg026]

Resolved direction:

- `DROP TABLE` is now executable through `Exec`
- dropped tables and their dependent indexes are removed from runtime and durable catalog state
- queries against dropped tables fail deterministically as table-not-found
- reopen and rollback-journal recovery preserve the correct dropped or pre-drop committed state

### `done` Realign `INT` semantics to 32-bit [kg022]

Resolved direction:

- `INT` remains the only public integer schema type for now
- `INT` now follows signed 32-bit semantics consistently across parser, runtime, storage, comparison, and scan paths
- out-of-range values fail deterministically rather than widening silently
- future direction remains to add explicit multi-width integer types later:
  - `SMALLINT`
  - `INT`
  - `BIGINT`

### `done` Enforce a bounded indexable TEXT size [kg023]

Resolved direction:

- plain `TEXT` values may still be larger, subject to normal row/page storage limits
- indexed `TEXT` column values are now limited to `<= 512` bytes
- the limit is measured in bytes, not characters
- the limit is enforced on:
  - `INSERT`
  - `UPDATE`
  - `CREATE INDEX`
  - `CREATE UNIQUE INDEX`
- violations fail with:
  - `execution: indexed TEXT column value exceeds 512-byte limit`

## Design Explorations

### `done` Physical Storage Layer milestone [dx005] `(commit: 54f7828)`

Resolved direction:

- tables now use a durable multi-page physical storage model
- each table has an authoritative `TableHeader`
- `SpaceMap` pages enumerate owned `Data` pages
- normal inserts, reads, scans, and locator-based fetches operate on owned
  `Data` pages
- row-growth updates relocate through delete plus reinsert when the new version
  no longer fits in place
- open, reopen, and corruption detection strictly validate physical ownership
  invariants with no backward-compatibility path for pre-physstore table
  storage

Implemented architecture:

- one single database file with one global 4 KB page heap
- exactly three physical page types:
  - `Header`
  - `SpaceMap`
  - `Data`
- `Header` has two logical roles:
  - `DatabaseHeader`
  - `TableHeader`
- page 0 remains the `DatabaseHeader`
- each table owns one `TableHeader`, its own `SpaceMap` pages, and its own
  `Data` pages
- ownership is logical and metadata-driven rather than physically contiguous
- CAT/DIR remains authoritative for logical metadata
- `TableHeader` is authoritative for physical table root metadata
- `SpaceMap` is authoritative for table-local `Data` page inventory and
  free-space classification
- rows remain addressed by `PageID + SlotID`

Guiding doc:

- `docs/dev/PHYSICAL_STORAGE_LAYER_design.md`

### Physical Storage Polish milestone [dx006]

Planned milestone:

- `v0.38.0-physical-storage-polish`

Planned scope:

- reclaim and free-list truth for dropped tables, emptied pages, and ownership
  cleanup
- multi-page mutation and index interaction hardening on top of the completed
  `TableHeader` / `SpaceMap` / owned-`Data` model
- crash, reopen, and WAL confidence around physical storage transitions
- diagnostics and consistency reporting for physical storage state
- stale wording cleanup after the completed `v0.37.0` milestone

Non-goals:

- no new storage architecture
- no compatibility or migration work for pre-physstore formats
- no planner/query feature expansion
- no major WAL redesign
- no unrelated API growth
- no broad refactors just for elegance

Truthfulness:

- this polish milestone is not implemented yet
- it builds on the completed `v0.37.0-physical-storage-layer` baseline
- it does not reintroduce support for pre-physstore table storage

### Explore `NOT NULL`, `NOT NULL WITH DEFAULT`, etc [dx001]

Exploration scope:

- whether RovaDB should add column-level
  - `NOT NULL`
  - `NOT NULL WITH DEFAULT`
  - `NOT NULL WITH DEFAULT <value>`
- whether those clauses should be supported in:
  - `CREATE TABLE`
  - `ALTER TABLE ... ADD COLUMN`

Questions to resolve:

- what syntax should be accepted
- what runtime enforcement should occur on `INSERT` and `UPDATE`
- how `ALTER TABLE ... ADD COLUMN` should behave for existing rows
- whether `NOT NULL` without `DEFAULT` is allowed when adding a column to a non-empty table
- how defaults should be represented in catalog metadata
- whether defaults are limited to literals or can include expressions
- what error wording and compatibility rules should be standardized

Current context:

- current column definitions only carry `name` and `type`
- current `ALTER TABLE ... ADD COLUMN` behavior is catalog-only and pads existing rows with `NULL`
- there is no current parser or executor support for `NOT NULL` or `DEFAULT`

### Explore planner usage for multi-column indexes [dx002]

Exploration scope:

- what planner/query shapes should be able to use a persisted multi-column index
- how planner support for multi-column indexes should grow beyond the initial executable baseline
- how multi-column indexes should relate to future key/constraint features

Questions to resolve:

- what ordering metadata should mean for multi-column indexes
- what planner/query shapes should be able to use a multi-column index
- whether left-prefix matching should be part of the initial design or deferred
- how multi-column index keys should be encoded and compared
- how planner behavior should interact with multi-column unique indexes and future primary-key backing

Current context:

- the SQL language spec and parser already allow more than one column in `CREATE INDEX (...)`
- V1 executable index work is expected to persist and enforce multi-column index definitions
- planner support may remain narrower than the full persisted index-definition space at first

### Explore primary key as an explicit table-definition contract [dx003]

Exploration scope:

- whether RovaDB should support an optional `PRIMARY KEY` clause in `CREATE TABLE`
- how primary key should relate to unique indexes and future foreign-key support

Current intended direction:

- `PRIMARY KEY` in `CREATE TABLE` is optional
- if present, it may be multi-column
- primary-key columns must be `NOT NULL`
- a table may have multiple unique indexes, but only one unique index may serve as the primary key
- RovaDB does not auto-create the backing unique index
- the matching unique index must be created explicitly
- until that matching unique index exists, the table is considered inconsistent / unusable

Questions to resolve:

- exact `CREATE TABLE` syntax for single-column and multi-column primary keys
- how the engine records the declared primary-key intent before the matching unique index exists
- what operations should be rejected while the table is in the inconsistent / unusable state
- how the engine determines that a later unique index is the required primary-key backing index
- what error wording should be used for tables whose declared primary key is not yet backed by the required unique index
- whether future foreign keys should target only the primary key or also any qualifying unique key

Current context:

- unique indexes are already part of the SQL language spec
- parser/spec direction is broader than the current executable index implementation
- this exploration is partly in preparation for future foreign-key support

### Explore table-level foreign key constraints [dx004]

Exploration scope:

- whether RovaDB should support named table-level foreign key constraints in `CREATE TABLE`
- how foreign keys should relate to primary keys, unique indexes, and referential actions

Canonical syntax direction:

```sql
CONSTRAINT fk_name
  FOREIGN KEY (col1, col2)
  REFERENCES parent_table (pk_col1, pk_col2)
  ON DELETE RESTRICT
```

Current intended direction:

- foreign keys are declared as table-level constraints in `CREATE TABLE`
- multi-column foreign keys are part of the intended design
- constraint names are included in the design
- referenced columns are explicit
- child and parent column counts must match
- corresponding child and parent column data types must match exactly
- type matching is strict, with no coercion
- `ON DELETE RESTRICT` is the preferred first referential action
- `ON DELETE CASCADE` may be a later expansion
- `SET NULL` is not in scope

Questions to resolve:

- whether foreign keys should be allowed to reference only the declared primary key or also other qualifying unique keys
- what operations should validate referential integrity and when
- what error wording should be standardized for invalid foreign key definitions and violating writes/deletes
- how foreign key metadata should be represented in catalog storage
- how foreign keys should interact with tables whose declared primary key is still in the inconsistent / unusable state described in `dx003`

Current context:

- this exploration is downstream of primary-key and unique-index design
- current executable index support is still narrower than the longer-term SQL language direction
