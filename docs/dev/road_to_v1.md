# Road To V1

This is a temporary development planning note for the remaining path to RovaDB
V1.

It is intentionally high-level. It captures the major work buckets after the
current docs-and-design stabilization phase.

## Status

The docs-and-design stabilization phase now includes a locked next storage
milestone:

- `v0.37.0-physical-storage-layer`

Current intent:

- keep the current engine truthful about the still-implemented single-page table
  storage model
- use the locked Physical Storage Layer design as the next storage/runtime
  implementation anchor
- then continue the remaining V1-oriented feature and hardening work in small
  slices

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

### 2. Physical Storage Layer

Primary target:

- `v0.37.0-physical-storage-layer`

Goal:

- implement the locked `TableHeader` / `SpaceMap` / `Data` storage model
- remove the current single-page table storage limit
- preserve truthful ownership and authority boundaries between CAT/DIR,
  table-root physical metadata, and table-local free-space tracking
- carry the storage/runtime/recovery implications through to a coherent durable
  baseline

Guiding doc:

- `docs/dev/PHYSICAL_STORAGE_LAYER_design.md`

Why this comes next:

- it is the next major storage milestone after the current `v0.36.x` baseline
- it addresses the current standout table-storage limitation directly
- later lifecycle and hardening work should build on the intended physical
  storage model rather than the temporary single-page assumption

### 3. Final V1 Hardening and Product Polish

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

- Physical Storage Layer completed to replace the current single-page table
  storage limit
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

Use `v0.x.0` milestone releases until the project is intentionally judged ready
for `v1.0.0`.

Current baseline:

- `v0.36.x`

Next milestone anchor:

- `v0.37.0-physical-storage-layer`
  - `TableHeader` / `SpaceMap` / multi-page `Data` ownership model
  - multi-page table storage beyond the current single-page table limit
  - storage, runtime, and recovery implications for the new model

After that milestone lands, reassess the remaining V1 path intentionally rather
than treating `v1.0.0` as an automatic next step.

## Temporary Nature

This document is temporary and development-facing.

Once the V1 path is either completed or substantially re-planned, this document can be removed, archived, or replaced by a more durable milestone summary.
