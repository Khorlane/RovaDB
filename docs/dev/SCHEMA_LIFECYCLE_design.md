# Schema Lifecycle Design

This document defines the intended design for executable schema-object lifecycle operations in RovaDB V1.

It focuses on:

- `DROP TABLE`
- `DROP INDEX`

It complements `docs/dev/CREATE_INDEX_design.md`, which covers index creation and enforcement. SQL syntax and user-visible statement semantics remain defined in `docs/dev/RovaDB_SQL_Language_Spec.md`.

## Purpose

- define what usable schema lifecycle means for RovaDB V1
- make table and index removal behavior explicit across engine layers
- prevent ad hoc or inconsistent drop behavior
- preserve deterministic, durable, and recoverable schema changes

## V1 Goal

RovaDB V1 should support practical schema lifecycle operations across:

- `CREATE TABLE`
- `DROP TABLE`
- `CREATE INDEX`
- `DROP INDEX`

The create path and drop path should feel symmetric enough that a user can revise mistaken schema objects without abandoning the database.

## Non-Goals

This document does not require V1 support for:

- `ALTER TABLE ... DROP COLUMN`
- index rename
- table rename
- cascading foreign-key behavior
- public `COMMIT` / `ROLLBACK`
- soft delete, tombstoning, or schema-history retention

## SQL Contract Assumptions

This design assumes the current SQL language spec decisions:

- `DROP INDEX` identifies indexes by database-wide name
- `DROP INDEX` fails if the named index does not exist
- `DROP INDEX` removes only the named index
- `DROP TABLE` fails if the named table does not exist
- `DROP TABLE` removes the named table and all indexes defined on that table
- `DROP TABLE` removes both table schema metadata and table row data

If the SQL language spec changes, this document must be kept aligned.

## Definition of Usable Schema Lifecycle

RovaDB V1 schema lifecycle should be considered usable only when:

- a created table can later be dropped
- a created index can later be dropped
- dropping one table or index does not corrupt unrelated schema objects
- drop operations are durable across close / reopen
- rollback/recovery never leaves partially removed schema objects visible
- planner and executor state remain consistent after schema removal

## Current Gap

The current repository already has:

- executable `CREATE TABLE`
- parser recognition for `DROP TABLE` and `DROP INDEX`
- durable table catalog metadata
- internal persisted index metadata in a narrow form

The current repository does not yet have:

- executable public `DROP TABLE`
- executable public `DROP INDEX`
- a complete object-lifecycle design across catalog, storage, planner, and recovery

## Object Model

For V1 schema lifecycle, the relevant durable object types are:

- table definitions
- table row-storage roots
- index definitions
- runtime index state associated with a table

Relationships:

- a table may have zero or more indexes
- an index belongs to exactly one table
- dropping a table removes its dependent indexes
- dropping an index does not affect the owning table's rows

## DROP INDEX Design

`DROP INDEX` should:

1. Resolve the index by database-wide name.
2. Fail if it does not exist.
3. Remove only that index definition and its runtime/persisted index state.
4. Leave table schema and table rows intact.

Required outcomes:

- catalog metadata no longer lists the index
- runtime table/index state no longer includes it
- planner no longer considers it
- reopen does not reload it

## DROP TABLE Design

`DROP TABLE` should:

1. Resolve the table by name.
2. Fail if it does not exist.
3. Remove the table definition.
4. Remove all index definitions belonging to that table.
5. Remove table row-storage ownership from the active catalog/runtime state.

Required outcomes:

- catalog metadata no longer lists the table
- catalog metadata no longer lists indexes belonging to the table
- runtime table map no longer includes the table
- planner/executor can no longer resolve the table
- reopen does not reload the table or its indexes

## Dependency Rule

For V1, table-owned indexes are dependent objects.

That means:

- dropping an index does not drop a table
- dropping a table implicitly drops all indexes on that table

This should be implemented directly rather than as a later cleanup step, so schema state is always self-consistent after `DROP TABLE`.

## Catalog and Storage Design

Drop operations must update durable catalog metadata, not just in-memory state.

Required behavior:

- dropped indexes are removed from persisted index metadata
- dropped tables are removed from persisted table metadata
- dropped tables no longer claim durable root-page ownership in catalog state

This document does not require V1 physical space reuse as part of `DROP TABLE`.

V1-acceptable behavior:

- dropping a table may remove it from logical metadata while leaving its old page space unreused internally for now

What matters for V1:

- dropped objects are no longer visible
- reopen does not resurrect them
- new operations cannot target them successfully

## Runtime State Design

Drop behavior must update in-memory runtime state immediately after a successful statement.

Required runtime effects:

- dropped table entries are removed from the `DB` table registry
- dropped index definitions are removed from the owning table's runtime index set
- planner metadata derived from runtime table/index state no longer exposes dropped objects

There should be no "dangling but ignored" runtime objects after a successful drop.

## Planner and Executor Expectations

After `DROP INDEX`:

- indexed lookup planning must stop considering the removed index
- queries against the owning table must continue to work via fallback scan paths where appropriate

After `DROP TABLE`:

- planning/execution for that table must fail deterministically as table-not-found
- unrelated tables and indexes must continue to function unchanged

## Durability and Transaction Expectations

`DROP TABLE` and `DROP INDEX` must participate in the same statement-level transaction model used by other mutating statements.

Expected properties:

- a successful drop is atomic at the statement level
- a failed drop leaves prior schema state intact
- reopen after commit reflects the dropped state
- rollback/recovery must not expose half-dropped schema objects

## Recovery and Corruption Expectations

Drop behavior must preserve the existing recovery and corruption-detection posture.

Required properties:

- rollback journal behavior remains sufficient for schema drops
- open/recovery must not silently heal partially corrupted dropped-object metadata
- corrupted catalog/index metadata still fails explicitly

## Error Surface Expectations

Exact wording can be finalized in tests, but stable failure classes should include:

- table not found
- index not found
- invalid transaction state
- storage/corruption failures during catalog mutation or reload

Errors should remain deterministic and consistent with the existing public error style.

## Test Plan

V1 completion should include API/integration coverage for at least:

- drop an existing index
- dropping a missing index fails
- drop one index while leaving another index on the same table intact
- reopen after dropping an index
- planner no longer uses a dropped index
- drop an existing table
- dropping a missing table fails
- dropping a table removes its indexes implicitly
- dropping one table leaves unrelated tables intact
- reopen after dropping a table
- table queries fail after drop
- rollback/recovery keeps schema state consistent after drop operations

## Suggested Implementation Slices

Keep slices small and vertical.

Suggested order:

1. Introduce durable named-index metadata sufficient for `DROP INDEX` resolution.
2. Execute `DROP INDEX` against in-memory/runtime state and persisted catalog metadata.
3. Add reopen and planner verification for dropped indexes.
4. Execute `DROP TABLE` against runtime and persisted catalog state.
5. Remove dependent indexes as part of `DROP TABLE`.
6. Add reopen, recovery, and unaffected-object verification for dropped tables.

## Acceptance Criteria

Schema lifecycle V1 is complete when:

- `DROP INDEX` is executable and durable
- `DROP TABLE` is executable and durable
- dropping a table removes dependent indexes
- unrelated schema objects remain intact
- reopen and recovery preserve the dropped state correctly
- planner/executor behavior stays deterministic after drops
- tests cover the committed scope

## Notes

- This design doc is intentionally implementation-oriented.
- SQL syntax and SQL-visible semantics remain defined in `docs/dev/RovaDB_SQL_Language_Spec.md`.
- This doc does not require V1 physical page reuse for dropped tables unless that later becomes an explicit product requirement.
