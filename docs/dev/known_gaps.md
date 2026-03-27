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
- [kg026] Engine `pending` Make `DROP TABLE` executable and durable
- [kg022] Engine `done` Realign `INT` semantics to 32-bit
- [kg023] Engine `pending` Enforce a bounded indexable TEXT size
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

### `pending` Make `DROP TABLE` executable and durable [kg026]

Observed gap:

- the parser already recognizes `DROP TABLE`, but the statement is not executable through the public SQL surface
- practical schema lifecycle in V1 requires users to be able to remove mistaken tables

Expected direction:

- make `DROP TABLE` executable through `Exec`
- remove dropped tables and dependent indexes from runtime and durable catalog state
- preserve reopen and recovery correctness
- follow `docs/dev/SCHEMA_LIFECYCLE_design.md`

### `done` Realign `INT` semantics to 32-bit [kg022]

Resolved direction:

- `INT` remains the only public integer schema type for now
- `INT` now follows signed 32-bit semantics consistently across parser, runtime, storage, comparison, and scan paths
- out-of-range values fail deterministically rather than widening silently
- future direction remains to add explicit multi-width integer types later:
  - `SMALLINT`
  - `INT`
  - `BIGINT`

### `pending` Enforce a bounded indexable TEXT size [kg023]

Observed gap:

- plain `TEXT` values can be much larger than is comfortable for a simple predictable index implementation
- if RovaDB adds executable index support for `TEXT` columns, it should define a clear bound for indexed text values rather than treating all text sizes as equally indexable

Expected direction:

- allow plain `TEXT` values to remain larger, subject to normal row/page storage limits
- define a specific byte-length cap for indexed `TEXT` values:
  - indexed `TEXT` column values must be `<= 512` bytes
- measure the limit in bytes, not characters
- enforce the limit whenever a write would touch an indexed `TEXT` column:
  - `INSERT`
  - `UPDATE`
- also enforce the limit when creating an index over existing `TEXT` data, so `CREATE INDEX` fails cleanly if existing rows violate the rule
- use a specific error message:
  - `indexed TEXT column value exceeds 512-byte limit`

## Design Explorations

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
