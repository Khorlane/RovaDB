# Road To V1

This is a temporary development planning note for the remaining path to RovaDB V1.

It is intentionally high-level. It captures the major work buckets after the current docs-and-design stabilization phase.

## Status

Step 1, docs and design stabilization, is close to complete but not fully closed yet.

Current intent:

- finish tightening the core docs and design notes
- then use this document to guide implementation sequencing for the remaining V1 work

## V1 Objective

RovaDB V1 should feel like a small but credible embedded SQL database for Go:

- stable public API
- clear SQL contract
- correct persistence and reopen behavior
- practical schema lifecycle
- practical index lifecycle
- deterministic, predictable behavior

V1 does not require broad SQL coverage. It does require that the supported surface feel real and usable.

## Remaining Path

### 2. Correct `INT` Semantics

Primary target:

- complete `kg022`

Goal:

- realign public `INT` behavior to signed 32-bit semantics
- make parser, runtime, storage, comparison, and API behavior consistent
- leave light groundwork for future `SMALLINT` and `BIGINT`

Guiding doc:

- `docs/dev/INTEGER_design.md`

Why this comes early:

- it is a foundational semantic correction
- later work should build on the intended public type contract, not the accidental current one

### 3. Make `CREATE INDEX` Real

Primary target:

- complete `kg024`

Goal:

- make `CREATE INDEX` and `CREATE UNIQUE INDEX` executable and durable
- persist the full SQL-visible index definition
- enforce created indexes correctly on later writes

Guiding doc:

- `docs/dev/CREATE_INDEX_design.md`

Why this matters:

- indexes must become real schema objects, not parser-only placeholders

### 4. Make `DROP INDEX` Real

Primary target:

- complete `kg025`

Goal:

- make `DROP INDEX` executable and durable
- allow users to remove mistaken indexes cleanly
- keep runtime, catalog, and planner state consistent after removal

Guiding doc:

- `docs/dev/SCHEMA_LIFECYCLE_design.md`

Why this follows `CREATE INDEX`:

- practical index lifecycle requires both creation and removal

### 5. Make `DROP TABLE` Real

Primary target:

- complete `kg026`

Goal:

- make `DROP TABLE` executable and durable
- remove dependent indexes correctly
- preserve reopen and recovery correctness

Guiding doc:

- `docs/dev/SCHEMA_LIFECYCLE_design.md`

Why this matters:

- practical schema lifecycle requires users to revise mistaken tables without abandoning the database

### 6. Enforce Bounded Indexed `TEXT`

Primary target:

- complete `kg023`

Goal:

- define and enforce the bounded indexed `TEXT` rule once executable indexes exist
- keep the index implementation predictable and intentionally bounded

Guiding doc:

- `docs/dev/known_gaps.md`

Why this comes after executable indexes:

- it is a real follow-on enforcement rule for public index support

### 7. Final V1 Hardening and Product Polish

Goal:

- complete internal naming cleanup where it improves maintainability without changing behavior or public/user-facing terminology
- recheck README, examples, and CLI alignment
- verify deterministic error behavior across the supported public surface
- harden reopen, rollback, and recovery coverage around the completed feature set
- confirm the supported V1 surface feels coherent rather than merely implemented

This phase should focus on confidence and usability, not broad new feature expansion.

Reference note:

- `C:\Projects\RovaDB Research\Var Func Name Refactor.txt`

## V1 Must-Haves

The following should be complete before calling the line V1-ready:

- `INT` semantics corrected
- executable durable `CREATE INDEX`
- executable durable `DROP INDEX`
- executable durable `DROP TABLE`
- bounded indexed `TEXT` enforcement
- reopen and recovery confidence across schema/index lifecycle operations

## Not Required For V1

The following are intentionally not required for the initial V1 target:

- `GROUP BY`
- `HAVING`
- subqueries
- public `COMMIT` / `ROLLBACK`
- primary keys
- foreign keys
- broad multi-shape index planner exploitation
- broad SQL completeness

## Sequencing Notes

- keep slices small and explicit
- prefer vertical, test-backed increments
- preserve the existing public API unless a change is intentional and documented
- do not let future-facing architecture delay the committed V1 surface
- use the design docs to keep implementation aligned and avoid partial semantics

## Rough Version Plan

Use `v0.x.0` milestone releases until the project is intentionally judged ready for `v1.0.0`.

Current baseline:

- `v0.11.x`

Compressed milestone plan:

- `v0.12.0`
  - complete `kg022`
  - realign `INT` semantics to signed 32-bit behavior
- `v0.13.0`
  - complete `kg024`
  - make `CREATE INDEX` and `CREATE UNIQUE INDEX` executable and durable
- `v0.14.0`
  - complete `kg025`
  - complete `kg026`
  - make practical schema lifecycle real through executable durable `DROP INDEX` and `DROP TABLE`
- `v0.15.0`
  - complete `kg023`
  - enforce bounded indexed `TEXT`
- `v0.16.0` if needed
  - final hardening, reopen/recovery confidence, docs/examples/CLI alignment, and polish before `v1.0.0` consideration

After the above milestones are complete, reassess intentionally for `v1.0.0` rather than treating it as an automatic next step.

## Temporary Nature

This document is temporary and development-facing.

Once the V1 path is either completed or substantially re-planned, this document can be removed, archived, or replaced by a more durable milestone summary.
