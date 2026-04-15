# Storage Engine Design

This document defines the authoritative storage-engine design for RovaDB.

It is the production storage reference for durable data representation, physical page ownership, catalog and directory persistence, index-definition persistence and maintenance, schema-object lifecycle behavior, storage-facing exact-width integer and temporal type rules, and recovery-oriented invariants.

It replaces narrower storage and lifecycle-oriented design notes by consolidating the current durable truth into one document.

## Purpose

- define the locked storage-engine model for RovaDB
- preserve the durable truths needed to prevent storage drift over time
- keep physical storage, catalog persistence, index persistence, and schema lifecycle behavior aligned
- document the storage-facing contract for exact-width integers and temporal types
- define the recovery, reopen, and corruption-detection expectations that preserve correctness

## Scope

This document covers:

- physical storage structure
- catalog and directory persistence
- durable index-definition storage and runtime maintenance
- schema-object lifecycle behavior for dropped tables and indexes
- storage-facing integer width, temporal type identity, and durable-format rules
- recovery, reopen, and corruption-detection invariants

This document does not define:

- SQL syntax and user-visible SQL semantics
- parser, planner, execution, or root-package ownership boundaries
- byte-for-byte implementation listings or helper-function layouts
- milestone planning

Those topics belong in the SQL language specification, database architecture document, source code, and planning documents.

## Storage Engine Overview

RovaDB is a single-file embedded database engine with page-based storage.

The storage engine is responsible for:

- durable table and index persistence
- physical page ownership and page inventory truth
- durable catalog and directory metadata
- schema-object durability across open, close, reopen, and recovery
- corruption detection with explicit failure rather than silent repair
- preserving exact-width integer identity and temporal type identity where physical layout depends on declared schema type

The storage engine is correctness-first. Where there is tension between flexibility and durable truth, durable truth wins.

## 1. Physical Storage Layer

### 1.1 Implemented Physical Baseline

The engine currently uses:

- one single database file
- one global 4096-byte page size
- page 0 as the durable database header and directory control page
- CAT/DIR metadata stored in page 0 or CAT/DIR overflow pages
- durable page-backed indexes
- authoritative `TableHeader` roots for tables
- `SpaceMap` chains enumerating table-owned `Data` pages
- normal inserts, reads, scans, and row-growth updates routed through this physical model
- strict open/reopen validation of physical ownership invariants

### 1.2 Locked Physical Page Types

The physical page model is locked to exactly three physical page types:

- `Header`
- `SpaceMap`
- `Data`

These are physical storage roles. They do not replace the catalog and directory as the authority for logical schema metadata.

### 1.3 Header Roles

The `Header` page type has exactly two logical roles:

- `DatabaseHeader`
- `TableHeader`

#### DatabaseHeader

- page 0 remains the `DatabaseHeader`
- page 0 owns durable database-level control metadata
- CAT/DIR remains authoritative for logical metadata such as table identity, schema metadata, and logical object mappings

#### TableHeader

- each table has exactly one `TableHeader`
- `TableHeader` is authoritative for that table's physical root metadata
- `TableHeader` points to the first `SpaceMap` page for the table

### 1.4 Table Ownership Model

Each table owns its own `SpaceMap` pages and `Data` pages.

That ownership is:

- logical
- metadata-driven
- not dependent on physical contiguity in the database file

A table's pages do not need to be adjacent on disk.

The authority split is locked as follows:

- CAT/DIR is authoritative for logical metadata
- `TableHeader` is authoritative for physical table-root and owned-page metadata
- `SpaceMap` is authoritative for the table-local `Data` page inventory and free-space classification
- `Data` pages are authoritative only for row and slot contents

### 1.5 Data Pages and Row Addressing

Rows are physically addressed by:

- `PageID`
- `SlotID`

Within a `Data` page:

- row payload grows upward
- slot metadata grows downward

This row locator contract is the physical row identity model for the current storage engine.

### 1.6 SpaceMap Model

Each table's `SpaceMap` is the authoritative inventory for that table's owned `Data` pages.

The locked `SpaceMap` properties are:

- `TableHeader` points to the first `SpaceMap` page
- `SpaceMap` may grow through a linked metadata chain
- `SpaceMap` entries track explicit `Data PageID` values
- `SpaceMap` does not track physical extents

The free-space classification buckets are locked to:

- `Full`
- `Low`
- `Medium`
- `High`

The insert path is `SpaceMap`-driven. Inserts consult the table's `SpaceMap` inventory and free-space classification rather than walking a linked list of `Data` pages searching for room.

### 1.7 Mutation Model

The current row-growth update rule is:

- if an update cannot fit in the current row location, it relocates through delete-plus-reinsert semantics

This keeps growth handling explicit and correct without introducing forwarding pointers, overflow fragments, or linked-row chains.

### 1.8 Recovery and Durability Invariants

The physical storage layer preserves these core invariants:

- no orphan owned pages
- no false references
- no row visibility ahead of ownership
- authoritative counts must match reality if stored

These invariants apply across:

- normal execution
- commit
- crash recovery
- reopen
- corruption detection

### 1.9 Byte-Level Detail Boundary

This document intentionally does not serve as the byte-level format specification for:

- bucket thresholds
- chain-pointer field positions
- per-page counters
- exact WAL or journal record layouts
- helper-function implementations

The source code remains the low-level implementation authority. This document defines the stable durable meaning, not the full byte listing.

## 2. Catalog and Directory Persistence

### 2.1 Problem Boundary

Page 0 contains control metadata plus CAT/DIR payload. That creates a size ceiling. Larger schemas, more indexes, and more root mappings can exceed embedded page-0 capacity.

### 2.2 Two Storage Modes

RovaDB defines exactly two CAT/DIR storage modes.

#### Embedded Mode

- all CAT/DIR payload is stored in page 0
- this is the preferred committed form when the full payload fits

#### Overflow Mode

- all CAT/DIR payload is stored outside page 0 in a chained sequence of dedicated CAT/DIR overflow pages
- page 0 stores control metadata plus enough information to locate and validate the overflow chain

### 2.3 Core Invariant

CAT/DIR payload must never be split between embedded page-0 storage and overflow pages at the same time.

At any committed point, catalog and directory metadata is either:

- entirely embedded
- entirely overflow-backed

Mixed-mode committed metadata is not allowed.

### 2.4 Promotion and Demotion Rules

Promotion rule:

- if the newly encoded CAT/DIR payload does not fit in embedded page-0 capacity, persistence promotes to overflow mode

Demotion rule:

- if the newly encoded CAT/DIR payload fits in embedded page-0 capacity, persistence demotes back to embedded mode even if the prior committed state used overflow

### 2.5 Representation Preference

The storage engine prefers the smallest valid committed representation.

That means:

- embedded mode is preferred whenever the payload fits
- overflow mode is used only when necessary

### 2.6 Rewrite Strategy

Metadata-changing commits choose representation fresh from the newly encoded CAT/DIR payload.

Rules:

- representation is chosen at commit/write time based on fit
- persistence is performed as a full representation rewrite
- partial mixed-mode edits are not allowed
- superseded overflow chains are reclaimed through the normal free-page path

### 2.7 Open and Validation Behavior

Open supports both embedded and overflow representations.

Malformed state is rejected deterministically, including:

- malformed CAT/DIR control metadata
- malformed CAT/DIR overflow chains

There is no compatibility fallback path for malformed committed CAT/DIR state.

## 3. Exact-Width Integer Storage Contract

RovaDB supports these SQL integer types:

- `SMALLINT`
- `INT`
- `BIGINT`

These are distinct declared schema types. They are not aliases for one generic storage or API integer family.

### 3.1 SQL-to-Go Identity

The public mapping is exact:

- `SMALLINT` -> `int16`
- `INT` -> `int32`
- `BIGINT` -> `int64`

That mapping applies across:

- binding
- writes
- storage decode
- query materialization
- `Scan`

### 3.2 Binding Contract

Placeholder binding for SQL integer values is exact-width:

- placeholders accept only Go `int16`, `int32`, and `int64` for SQL integer binding
- Go `int` is rejected
- other integer-like Go types are rejected

Binding must not blur SQL integer widths by accepting whichever Go integer happens to fit numerically.

### 3.3 Write Contract

Typed integer writes are exact by declared column type:

- `SMALLINT` columns accept only `int16`
- `INT` columns accept only `int32`
- `BIGINT` columns accept only `int64`

Allowed narrow exception:

- untyped SQL integer literals may resolve to the target width if they fit

Rejected behavior:

- implicit widening
- implicit narrowing
- generic integer interchange
- “fits so allow it” for wrong-width typed Go integers
- use of Go `int` for typed SQL integer columns

### 3.4 Locked Physical Widths

Declared schema type drives fixed on-page width:

- `SMALLINT` = 2 bytes
- `INT` = 4 bytes
- `BIGINT` = 8 bytes

Physical width is determined by declared column type, not by runtime value magnitude.

### 3.5 Encoding Semantics

Integer values are encoded as:

- little-endian
- signed values preserved through their two's-complement bit pattern

The durable meaning is locked to those semantics, regardless of helper-function arrangement.

### 3.6 Storage Round-Trip Contract

Schema-aware row storage preserves exact width on encode and decode:

- `SMALLINT` rows round-trip as `SMALLINT`
- `INT` rows round-trip as `INT`
- `BIGINT` rows round-trip as `BIGINT`

Storage must not:

- collapse typed integer columns into one generic integer identity
- decode typed integer columns onto the wrong width
- silently reinterpret or repair durable bytes that violate declared width

Corruption or durable-format mismatch must fail clearly.

### 3.7 Evaluation and Materialization Contract

Typed integer arithmetic preserves exact width:

- `SMALLINT +/- SMALLINT -> SMALLINT`
- `INT +/- INT -> INT`
- `BIGINT +/- BIGINT -> BIGINT`

Required behavior:

- overflow fails immediately
- mixed-width typed arithmetic is rejected
- typed integer plus untyped integer literal resolves to the typed width if it fits
- typed integer plus untyped integer literal fails if it does not fit

Typed integer results materialize to exact Go widths:

- typed `SMALLINT` -> `int16`
- typed `INT` -> `int32`
- typed `BIGINT` -> `int64`

### 3.8 Scan Contract

Scanning is exact-width for typed integer results:

- `SMALLINT` scans only into `*int16`
- `INT` scans only into `*int32`
- `BIGINT` scans only into `*int64`

Untyped integer results scan only into `*int64`.

Not allowed for typed integer results:

- widening during scan
- narrowing during scan
- scanning into `*int`

### 3.9 Catalog and Schema Identity

Declared integer identity must be preserved exactly in schema and catalog metadata.

That means:

- `SMALLINT`, `INT`, and `BIGINT` remain distinct declared types
- catalog persistence must not collapse them into one generic integer identity
- reopen must preserve the original declared type exactly

### 3.10 Intentional Narrow Untyped Seams

The storage and execution model still retains a narrow untyped integer-result path for expression-only or schema-less results that do not resolve against a typed integer column.

Current examples include:

- `SELECT 1`
- `SELECT 1 + 2`
- `COUNT(*)`
- `LENGTH(text)`

These results materialize as `int64` and scan only into `*int64`.

This seam is intentional and narrow. It does not weaken exact-width schema-aware storage or typed result behavior.

### 3.11 Temporal Storage and Catalog Contract

RovaDB also supports these distinct temporal schema types:

- `DATE`
- `TIME`
- `TIMESTAMP`

The storage engine must preserve their declared identity exactly across:

- writes
- physical storage
- catalog persistence
- reopen
- query materialization

Required behavior:

- temporal values remain family-distinct; `DATE`, `TIME`, and `TIMESTAMP` are not interchangeable
- canonical temporal values persist and round-trip without being collapsed into `TEXT`
- temporal writes enforce exact family matching rather than implicit coercion
- catalog metadata must preserve the original declared temporal type exactly
- reopen must preserve temporal values and declared temporal schema identity exactly

Current non-goals remain:

- fractional seconds
- alternate temporal literal formats
- temporal arithmetic
- built-in temporal functions
- implicit temporal casting or coercion

## 4. Durable Index Design

### 4.1 Scope

RovaDB supports executable, durable `CREATE INDEX` and `CREATE UNIQUE INDEX` statements within a correctness-first, deterministic storage model.

The storage engine owns the durable persistence, validation, maintenance, and reload behavior of created indexes.

### 4.2 Required Durable Index Identity

The storage engine must preserve the full SQL-visible index definition.

That durable definition includes:

- index name
- target table
- whether the index is `UNIQUE`
- ordered index column list
- per-column sort direction

Persisting only a single indexed column name is insufficient.

### 4.3 Catalog and Storage Requirements

The storage engine must support:

- multiple indexes per table
- named indexes
- multi-column definitions
- uniqueness metadata
- direction metadata

Catalog encoding must:

- preserve deterministic encoding order
- preserve compatibility rules intentionally
- reject corrupted or incomplete index metadata at open time
- version on-disk catalog metadata explicitly when shape changes require it

### 4.4 Runtime Representation

A runtime representation of index definitions must exist separately from any single planner optimization path.

That runtime representation must answer:

- what table the index belongs to
- what columns participate
- whether it is unique
- what order and direction each key part uses
- whether a write touches the index

### 4.5 CREATE INDEX Execution Behavior

Executing `CREATE INDEX` must perform, at a high level:

1. validate the target table exists
2. validate all indexed columns exist
3. validate the index name is unused
4. validate the definition is not semantically equivalent to an existing index on the same table
5. validate duplicate columns do not appear in the indexed column list
6. scan existing rows and build candidate index state
7. if the index is unique:
   - fail if any indexed row contains `NULL` in any indexed column
   - fail if duplicate indexed key values already exist
8. persist the index definition in the catalog
9. make the created index active in memory for later query planning and write enforcement

Creation is atomic at the statement level under the existing transaction model.

### 4.6 UNIQUE Enforcement

For the current product boundary, uniqueness rules are intentionally strict:

- every indexed key part must be non-`NULL`
- duplicate full key tuples are rejected

This applies both:

- when creating a unique index over existing rows
- when later writes would produce a conflicting key

### 4.7 Write-Path Maintenance

After an index exists, the engine must maintain it on all row-changing operations:

- `INSERT`
- `UPDATE`
- `DELETE`

Required behavior:

- non-unique indexes stay synchronized with table contents
- unique indexes reject writes that would create a duplicate indexed key
- unique indexes reject writes that would create `NULL` in any indexed column within the current ruleset

Write maintenance must remain correct across reopen and rollback/recovery paths.

### 4.8 Planner Boundary

The storage engine preserves and exposes index truth regardless of whether every durable index shape is exploited by the planner.

Minimum planner expectation:

- continue to support the deterministic single-table equality lookup path when a compatible index exists

Acceptable limitation:

- some created indexes may be durable and enforced but not yet planner-exploited for every logically compatible query shape

### 4.9 Multi-Column and Direction Semantics

Because the SQL language contract includes multi-column index definitions, the storage engine must preserve and enforce them correctly.

This requires:

- durable storage of the full ordered column list
- uniqueness enforcement over the full ordered key tuple
- deterministic duplicate-definition handling using ordered columns and directions
- correct write maintenance
- durable preservation of `ASC` and `DESC` as part of index identity

## 5. Schema Lifecycle Design

### 5.1 Scope

The storage engine owns the durable behavior of schema-object lifecycle operations for:

- `DROP TABLE`
- `DROP INDEX`

This document covers the storage-facing, runtime-state, reopen, and recovery truth of those operations.

### 5.2 Object Model

Relevant durable object types are:

- table definitions
- table row-storage roots
- index definitions
- runtime index state associated with a table

Relationships:

- a table may have zero or more indexes
- an index belongs to exactly one table
- dropping a table removes its dependent indexes
- dropping an index does not affect the owning table's rows

### 5.3 DROP INDEX Design

`DROP INDEX` must:

1. resolve the index by database-wide name
2. fail if it does not exist
3. remove only that index definition and its runtime and persisted index state
4. leave table schema and table rows intact

Required outcomes:

- catalog metadata no longer lists the index
- runtime table/index state no longer includes it
- planner no longer considers it
- reopen does not reload it

### 5.4 DROP TABLE Design

`DROP TABLE` must:

1. resolve the table by name
2. fail if it does not exist
3. remove the table definition
4. remove all index definitions belonging to that table
5. remove table row-storage ownership from the active catalog and runtime state

Required outcomes:

- catalog metadata no longer lists the table
- catalog metadata no longer lists indexes belonging to the table
- runtime table map no longer includes the table
- planner and executor can no longer resolve the table
- reopen does not reload the table or its indexes

### 5.5 Dependency Rule

Table-owned indexes are dependent objects.

That means:

- dropping an index does not drop a table
- dropping a table implicitly drops all indexes on that table

Schema state must be self-consistent immediately after a successful `DROP TABLE`.

### 5.6 Durable Metadata Rules

Drop operations must update durable catalog metadata, not just in-memory state.

Required behavior:

- dropped indexes are removed from persisted index metadata
- dropped tables are removed from persisted table metadata
- dropped tables no longer claim durable root-page ownership in catalog state

Physical page reuse is not required as part of the current `DROP TABLE` contract.

It is acceptable for dropped tables to disappear from logical metadata while leaving old page space unreused internally, as long as:

- dropped objects are no longer visible
- reopen does not resurrect them
- new operations cannot target them successfully

### 5.7 Runtime State Rules

Drop behavior must update runtime state immediately after a successful statement.

Required runtime effects:

- dropped table entries are removed from the `DB` table registry
- dropped index definitions are removed from the owning table's runtime index set
- planner metadata derived from runtime table/index state no longer exposes dropped objects

No dangling-but-ignored runtime objects are allowed after a successful drop.

### 5.8 Post-Drop Planner and Executor Expectations

After `DROP INDEX`:

- indexed lookup planning must stop considering the removed index
- queries against the owning table must continue to work through fallback scan paths where appropriate

After `DROP TABLE`:

- planning and execution for that table must fail deterministically as table-not-found
- unrelated tables and indexes must continue to function unchanged

## 6. Durability, Reopen, Recovery, and Corruption Rules

### 6.1 Statement Atomicity

The following storage mutations must participate in the same statement-level transactional model used by other mutating statements:

- `CREATE INDEX`
- `DROP INDEX`
- `DROP TABLE`

Expected properties:

- successful mutation is atomic at the statement level
- failed mutation leaves prior committed state intact
- reopen after commit reflects the new committed state
- rollback/recovery must not expose partially created or partially dropped durable objects

### 6.2 Recovery Posture

The storage engine preserves a strict recovery posture:

- rollback-journal behavior remains sufficient for current schema and index lifecycle operations
- recovery must not silently heal partially corrupted metadata
- corruption remains explicit and fail-fast

### 6.3 Reopen Truth

Open and reopen must preserve durable truth across:

- physical page ownership
- catalog and directory representation mode
- exact-width schema identity for typed integers
- index definitions and their enforcement state
- dropped-object invisibility

Reopen must not:

- reinterpret malformed durable metadata as valid
- widen or collapse typed integer identity
- resurrect dropped tables or indexes
- lose direction, uniqueness, or ordered-column identity for indexes

### 6.4 Corruption Expectations

Corruption or durable-format mismatch must fail clearly.

This includes, as applicable:

- malformed CAT/DIR control metadata
- malformed CAT/DIR overflow chains
- incomplete or corrupted index metadata
- storage bytes inconsistent with declared integer width
- partially corrupted dropped-object metadata

No silent repair path is part of the storage-engine contract.

## 7. Error-Surface Expectations

Exact wording may evolve through tests, but the storage engine must preserve deterministic failure classes for at least:

- duplicate index name
- equivalent index definition already exists
- invalid indexed column list
- duplicate indexed columns in one definition
- duplicate indexed key values already exist
- `NULL` exists in a would-be unique indexed key
- target table or column not found
- index not found
- table not found
- invalid transaction state
- storage/corruption failures during catalog mutation, load, or reload

Errors must remain deterministic and consistent with the public error model.

## 8. Non-Goals

The storage-engine design does not require or imply:

- broad SQL-surface expansion
- planner or optimizer redesign
- automatic physical space reuse for dropped tables
- silent compatibility fallback for malformed durable state
- implicit numeric widening or narrowing
- generic integer-family interchange
- unsigned integer support
- `TINYINT`
- speculative numeric-framework redesign
- byte-level helper or field-layout duplication in this document

## 9. Stability Requirements

Future work must preserve these storage-engine truths:

- single-file page-based durable storage
- locked physical page-type roles
- authoritative table ownership through `TableHeader` and `SpaceMap`
- CAT/DIR committed in exactly one representation at a time
- deterministic promotion/demotion between CAT/DIR modes
- explicit corruption detection
- exact-width typed integer storage identity
- durable preservation of full index-definition meaning
- deterministic schema-object lifecycle behavior
- reopen and recovery truth across all supported lifecycle operations

## Summary

RovaDB’s storage engine is a correctness-first, page-based, single-file design.

Its durable truth is defined by:

- `Header`, `SpaceMap`, and `Data` page roles
- `TableHeader` and `SpaceMap` ownership of table storage
- CAT/DIR embedded or overflow representation with no mixed committed mode
- exact-width integer storage tied to declared schema type
- durable preservation and enforcement of full index definitions
- deterministic lifecycle handling for created and dropped schema objects
- explicit corruption detection, atomic statement-level mutation, and truthful reopen/recovery behavior

This document is the authoritative storage-engine contract for keeping RovaDB’s durable behavior coherent as it evolves.
