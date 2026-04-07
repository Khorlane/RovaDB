# Road To V1

This is a temporary development planning note for the remaining path to RovaDB
V1 after the Physical Storage Layer milestone.

It is intentionally high-level. It captures the major work buckets after the
current docs-and-design stabilization phase.

## Status

The next storage milestone has now been completed:

- `v0.37.0-physical-storage-layer`

Current intent:

- keep docs and examples truthful about the implemented physical storage model
- build the remaining V1-oriented hardening and product polish on top of the
  completed Physical Storage Layer baseline
- continue the remaining work in small explicit slices

Next named milestone:

- `v0.38.0-physical-storage-polish`

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

Implemented outcome:

- tables now use the locked `TableHeader` / `SpaceMap` / `Data` storage model
- normal runtime rows now live on table-owned `Data` pages rather than on a
  single table-row root page
- writes, reads, scans, relocation-on-growth updates, reopen, and corruption
  detection now follow one physical ownership model
- CAT/DIR logical authority and physical table-root authority remain separated

Guiding doc:

- `docs/dev/PHYSICAL_STORAGE_LAYER_design.md`

Why this mattered:

- it was the major storage milestone after the `v0.36.x` baseline
- it removed the standout single-page table-row-storage limitation directly
- later lifecycle and hardening work should now build on this physical storage
  model rather than on temporary assumptions

### 3. Physical Storage Polish

Primary target:

- `v0.38.0-physical-storage-polish`

Goal:

- harden reclaim and free-list truth for dropped tables, emptied pages, and
  ownership cleanup in the already-landed physical storage model
- harden multi-page mutation behavior and index interaction around
  `TableHeader` / `SpaceMap` / owned `Data` pages
- increase crash, reopen, and WAL confidence around physical storage
  transitions without redesigning the WAL architecture
- improve diagnostics and consistency reporting for physical storage state
- clean up stale wording that lags behind the completed `v0.37.0` milestone

This is a stabilization milestone for the implemented storage model, not a new
storage-architecture milestone.

Non-goals:

- no new storage architecture
- no compatibility or migration work for pre-physstore formats
- no planner or query feature expansion
- no major WAL redesign
- no unrelated API growth
- no broad refactors just for elegance

Why this comes next:

- the `v0.37.0` physical storage model is in place and now needs polish-level
  hardening around reclaim, mutation edges, and diagnostics
- the remaining V1 path benefits more from storage confidence than from adding
  new feature surface immediately

### 4. Final V1 Hardening and Product Polish

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

- Physical Storage Layer completed and documented as the current storage truth
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

- `v0.37.x`

Next milestone anchor:

- `v0.38.0-physical-storage-polish`
  - reclaim and free-list truth for dropped tables, emptied pages, and
    ownership cleanup
  - multi-page mutation and index interaction hardening
  - crash, reopen, and WAL confidence around physical storage transitions
  - diagnostics and consistency reporting for physical storage state
  - stale wording cleanup after `v0.37.0`

Completed milestone anchor:

- `v0.37.0-physical-storage-layer`
  - `TableHeader` / `SpaceMap` / multi-page `Data` ownership model
  - normal runtime rows live on table-owned `Data` pages enumerated by
    `SpaceMap`
  - storage, runtime, and recovery implications for the new model

After `v0.38.0`, reassess the remaining V1 path intentionally rather than
treating `v1.0.0` as an automatic next step.

## Temporary Nature

This document is temporary and development-facing.

Once the V1 path is either completed or substantially re-planned, this document can be removed, archived, or replaced by a more durable milestone summary.
