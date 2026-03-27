# CREATE INDEX Design

This document defines the intended design for making `CREATE INDEX` fully executable in RovaDB.

It is a cross-layer design note for parser, planner, executor, storage, transaction, and test work. It is not the SQL language-definition source of truth; SQL syntax and user-visible statement semantics remain defined in `docs/dev/RovaDB_SQL_Language_Spec.md`.

## Purpose

- define what "fully functional `CREATE INDEX`" means for RovaDB V1
- align the engine design with the existing SQL language spec
- prevent partial implementations that silently discard index-definition meaning
- provide a staged implementation guide across engine layers

## V1 Goal

RovaDB V1 should support executable, durable `CREATE INDEX` and `CREATE UNIQUE INDEX` statements that:

- validate the target table and indexed columns
- persist the full index definition
- validate existing table rows before index creation succeeds
- enforce the created index on later writes
- survive close / reopen
- integrate with the current planner where supported

## Non-Goals

The following are not required for the initial `CREATE INDEX` implementation unless explicitly added later:

- `DROP INDEX`
- `DROP TABLE`
- public `COMMIT` / `ROLLBACK`
- `GROUP BY` / `HAVING`
- subqueries
- joins over more than two tables
- advanced query optimization
- every possible planner use of every created index

## SQL Contract Assumptions

This design assumes the current SQL language spec decisions for `CREATE INDEX`:

- index names must be unique across the database
- equivalent index definitions on the same table are rejected
- duplicate column names within one index definition are rejected
- `ASC` is the default when omitted
- `ASC` / `DESC` are semantically significant
- multi-column index column order is significant
- `CREATE UNIQUE INDEX` enforces uniqueness across the full indexed key
- `CREATE UNIQUE INDEX` fails if duplicate indexed key values already exist in the target table
- for V1, `CREATE UNIQUE INDEX` also fails if any existing row has `NULL` in any indexed column

If the language spec changes, this document must be kept aligned.

## Definition of Fully Functional

For RovaDB V1, `CREATE INDEX` should be considered fully functional only when all of the following are true:

- `Exec("CREATE INDEX ...")` succeeds for valid statements
- `Exec("CREATE UNIQUE INDEX ...")` succeeds for valid statements
- invalid definitions fail deterministically
- index definitions are durably persisted and reloaded on `Open()`
- created indexes are enforced and maintained across:
  - `INSERT`
  - `UPDATE`
  - `DELETE`
- reopen and recovery behavior remain correct
- planner behavior remains deterministic for the currently supported indexed query shapes

## Current Gap

The current repository already has:

- parser support for `CREATE INDEX` and `CREATE UNIQUE INDEX`
- single-column in-memory equality indexes for planner/executor use
- persisted catalog metadata for a single indexed column name

The current repository does not yet have:

- executable public `CREATE INDEX`
- durable storage for the full index definition
- support for index names
- support for unique indexes
- support for multi-column index definitions in storage/runtime
- support for per-column sort direction in storage/runtime
- full write-path validation and maintenance for public created indexes

This means the current internal index path is a useful starting point, but not the final V1 design.

## Required Design Decisions

The following must be represented explicitly in the engine:

- index name
- target table
- whether the index is `UNIQUE`
- ordered index column list
- per-column `ASC` / `DESC`

Any implementation that persists only a single column name is insufficient for the current SQL contract.

## Proposed Metadata Shape

RovaDB should introduce a storage/runtime index-definition shape that preserves the full SQL-visible definition.

Suggested fields:

- index name
- table name
- unique flag
- ordered list of index columns
- per-column direction

Suggested per-column fields:

- column name
- descending flag

The metadata should be durable, reloadable, and validated on open.

## Catalog and Storage Design

The current catalog metadata only persists a single indexed column name per table. V1 `CREATE INDEX` requires a richer catalog representation.

The storage design should support:

- multiple indexes per table
- named indexes
- multi-column definitions
- uniqueness metadata
- direction metadata

The catalog codec should reject corrupted or incomplete index metadata at open time using the existing corruption-detection discipline.

Storage-specific requirements:

- preserve deterministic encoding order
- preserve compatibility rules intentionally rather than accidentally
- version catalog metadata explicitly when the on-disk shape changes

## Runtime Representation

RovaDB should have a runtime representation of index definitions separate from any particular planner optimization path.

That runtime representation should be able to answer:

- what table the index belongs to
- what columns participate
- whether it is unique
- what order/direction each key part uses
- whether a write touches the index

The runtime representation does not need to force the planner to exploit every index shape immediately.

## CREATE INDEX Execution Behavior

Executing `CREATE INDEX` should perform the following high-level steps:

1. Validate the target table exists.
2. Validate all indexed columns exist.
3. Validate the index name is unused.
4. Validate the definition is not semantically equivalent to an existing index on the same table.
5. Validate duplicate columns do not appear in the indexed column list.
6. Scan existing rows and build candidate index state.
7. If the index is unique:
   - fail if any indexed row contains `NULL` in any indexed column
   - fail if duplicate indexed key values already exist
8. Persist the index definition in the catalog.
9. Make the created index active in memory for later query planning and write enforcement.

Creation should be atomic at the statement level under the existing transaction model.

## UNIQUE Enforcement

For V1, uniqueness rules are intentionally strict:

- every indexed key part must be non-`NULL`
- duplicate full key tuples are rejected

This applies both:

- when creating a unique index over existing rows
- when later writes would produce a conflicting key

This rule is intentionally stricter than some SQL systems and is chosen for simpler V1 semantics.

## Write-Path Maintenance

After an index exists, the engine must maintain it on all row-changing operations.

Required write paths:

- `INSERT`
- `UPDATE`
- `DELETE`

Required behavior:

- non-unique indexes stay synchronized with table contents
- unique indexes reject writes that would create a duplicate indexed key
- unique indexes reject writes that would create `NULL` in any indexed column in V1

Write maintenance must remain correct across reopen and rollback/recovery paths.

## Planner Scope for V1

RovaDB does not need to exploit every created index shape immediately in order for `CREATE INDEX` to count as fully functional.

For V1, planner expectations should be intentionally narrow and explicit.

Minimum acceptable planner support:

- continue to support the current deterministic single-table equality lookup path when a compatible index exists

Acceptable V1 limitation:

- some created indexes may be durable and enforced but not yet used by the planner for all logically compatible query shapes

However, the planner must not:

- silently ignore the semantic meaning of index definitions during persistence or enforcement
- behave nondeterministically based on definition ordering

## Multi-Column Indexes in V1

Because the SQL language spec already includes multi-column index definitions, V1 must persist and enforce them correctly.

V1 does not necessarily need full left-prefix or ordered-range optimization behavior from the planner on day one.

But V1 does need:

- durable storage of the full ordered column list
- uniqueness enforcement over the full ordered key tuple
- deterministic duplicate-definition handling using ordered columns and directions
- correct write maintenance

## Direction Semantics in V1

`ASC` / `DESC` are part of the SQL contract and must be preserved durably.

V1 does not necessarily need to use every direction combination for query planning immediately.

But V1 does need:

- to store direction as part of the definition
- to treat direction as part of semantic equality/duplication checks
- to preserve the declared definition across reopen

## Error Surface Expectations

Exact wording can be finalized in tests, but the implementation should preserve stable failure classes for:

- duplicate index name
- equivalent index definition already exists
- invalid indexed column list
- duplicate indexed columns in one definition
- duplicate indexed key values already exist
- `NULL` exists in a would-be unique indexed key
- target table or column not found

Errors should remain deterministic and consistent with the existing public error style.

## Durability and Transaction Expectations

`CREATE INDEX` must participate in the same statement-level transactional model used by other mutating statements.

Expected properties:

- index creation is atomic at the statement level
- persisted catalog/index state survives close/reopen
- rollback/recovery must not leave partially created index definitions visible
- corruption detection remains explicit; no silent repair

## Test Plan

V1 completion should include API/integration coverage for at least:

- create a non-unique single-column index
- create a unique single-column index
- create a multi-column index
- create a multi-column unique index
- reopen after index creation
- planner still chooses compatible index scans where expected
- duplicate index name is rejected
- equivalent index definition is rejected
- duplicate indexed columns are rejected
- unknown table is rejected
- unknown column is rejected
- unique creation fails when duplicate indexed key values already exist
- unique creation fails when any indexed key part is `NULL`
- later `INSERT` violates unique index
- later `UPDATE` violates unique index
- later `UPDATE` introduces `NULL` into a unique indexed key
- `DELETE` keeps index state consistent
- rollback/recovery keeps catalog/index state consistent

## Suggested Implementation Slices

Keep slices small and vertical.

Suggested order:

1. Expand catalog metadata to persist full index definitions.
2. Reload and validate full index definitions on `Open()`.
3. Introduce runtime index-definition representation separate from the current narrow planner index path.
4. Execute non-unique single-column `CREATE INDEX`.
5. Enforce and maintain non-unique indexes on writes.
6. Execute `CREATE UNIQUE INDEX` with duplicate and `NULL` validation.
7. Enforce unique indexes on writes.
8. Persist and enforce multi-column index definitions.
9. Expand planner usage where intentionally supported.
10. Add reopen, rollback, and recovery validation across the full feature.

## Acceptance Criteria

`CREATE INDEX` V1 is complete when:

- the public SQL statement is executable through `Exec`
- the full SQL-visible definition is persisted and reloaded
- uniqueness and `NULL` rules are enforced as designed
- created indexes are maintained on later writes
- reopen and recovery behavior remain correct
- planner behavior stays deterministic for the supported indexed query subset
- tests cover the committed scope

## Notes

- This design doc is intentionally implementation-oriented.
- The SQL language contract remains defined in `docs/dev/RovaDB_SQL_Language_Spec.md`.
- If V2 later introduces `NOT NULL` and `NOT NULL WITH DEFAULT`, that should complement the V1 unique-index `NULL` rule rather than replace this document retroactively.
