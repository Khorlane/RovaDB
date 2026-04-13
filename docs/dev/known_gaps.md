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
- [dx006] Physical Storage Polish milestone `done`
- [dx001] Column nullability and literal defaults `done`
- [dx002] Explore planner usage for multi-column indexes
- [dx003] Primary key support `done`
- [dx004] Foreign key support `done`

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
- `TableHeader` is authoritative for physical table root and owned-page
  metadata
- `SpaceMap` is authoritative for table-local `Data` page inventory and
  free-space classification
- rows remain addressed by `PageID + SlotID`

Guiding doc:

- `docs/dev/PHYSICAL_STORAGE_LAYER_design.md`

### `done` Physical Storage Polish milestone [dx006]

Completed milestone:

- `v0.38.0-physical-storage-polish`

Completed scope:

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

- this polish milestone is now implemented in the repo
- it builds on the completed `v0.37.0-physical-storage-layer` baseline
- it does not reintroduce support for pre-physstore table storage

### `done` Column nullability and literal defaults [dx001]

Resolved direction:

- column definitions now persist `NOT NULL` and literal `DEFAULT` metadata
- `CREATE TABLE` and `ALTER TABLE ... ADD COLUMN` accept the documented column forms
- `DEFAULT` applies on `INSERT` omission only
- explicit `NULL` is rejected for `NOT NULL` columns on `INSERT` and `UPDATE`
- `ALTER TABLE ... ADD COLUMN <col> <type> NOT NULL` is rejected for non-empty tables unless a literal default is present
- existing rows observe `NULL` or the literal default after `ALTER TABLE ... ADD COLUMN`, and reopen preserves the same logical row behavior

Reference:

- `docs/dev/COLUMN_NULLABILITY_DEFAULTS_design.md`

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

### `done` Primary key support [dx003]

Resolved direction:

- primary keys are supported in `CREATE TABLE` and `ALTER TABLE ... ADD CONSTRAINT`
- primary keys are named only
- at most one primary key may exist per table
- primary keys may be single-column or composite
- primary-key columns are implicitly `NOT NULL`
- `USING INDEX <name>` is required
- the supporting index must be `UNIQUE` and match the primary-key column list exactly
- primary-key values cannot be modified
- `ALTER TABLE ... DROP PRIMARY KEY` removes dependent foreign keys and keeps indexes

### `done` Foreign key support [dx004]

Resolved direction:

- foreign keys are supported in `CREATE TABLE` and `ALTER TABLE ... ADD CONSTRAINT`
- foreign keys are named only
- foreign keys may be single-column or composite
- foreign keys reference the parent primary key only
- `USING INDEX <name>` is required
- `ON DELETE RESTRICT` and `ON DELETE CASCADE` are supported
- foreign-key columns may not be `NULL`
- foreign-key updates are allowed subject to final-state validation
- the child supporting index must use the foreign-key columns as a contiguous leftmost prefix
- enforcement is immediate and statement-atomic
- legal self-reference is supported
- all-`CASCADE` cycles and multiple cascade paths are rejected at DDL time
- `ALTER TABLE ... DROP FOREIGN KEY <name>` removes the constraint and keeps the supporting index
