# Architectural Boundaries

This document locks the architectural ownership and dependency rules for the RovaDB engine. It is authoritative for boundary decisions and is intentionally declarative rather than aspirational.

## Layers (Locked)

### Parser

- Owns SQL syntax parsing only.
- Produces AST and parsed statement/value structures.
- Has no execution, storage, page, or durability awareness.

### Planner

- Owns plan construction only.
- Consumes parsed structures from the parser layer.
- Produces execution plans and logical planning decisions.
- Has no storage, page, pager, WAL, or physical-layout awareness.

### Execution

- Owns runtime operators, statement execution, and row flow.
- Executes plans and coordinates logical work.
- Interacts with storage only through narrow explicit interfaces.
- Must not expose storage internals upward.

### Storage

- Owns pages, WAL and journal formats, buffer-pool-facing durable access, and physical layout.
- Owns durable metadata encoding and validation.
- Has no knowledge of SQL syntax, parsed statements, or execution plans.

### Root API

- Owns the public embedded API surface, including `DB`, `Rows`, and `Result`.
- Coordinates public entrypoints and transaction-facing API behavior.
- Stays thin: it coordinates engine packages but does not own parser, planner, execution, or storage logic.
- Does not define engine-layer ownership or durable-format policy.

### Transaction And Buffer Pool Support

- `txn` owns transaction state transitions and durability-oriented orchestration.
- `bufferpool` owns committed/private page mediation for durable page access.
- Neither package changes the one-way ownership of the main engine pipeline.

## Dependency Direction (Locked)

The architectural pipeline is one-way:

`parser -> planner -> execution -> storage`

The following rules are locked:

- No upward imports.
- No sibling-layer imports that bypass the pipeline.
- `storage` never imports `execution`, `planner`, or `parser`.
- `planner` never depends on storage concrete types.
- `execution` must not expose storage internals upward.

## Type Boundaries

- Storage types must not leak outside the storage layer unless an explicit boundary type is intentionally defined for that purpose.
- Storage owns durable value encoding, index structures, and page/layout metadata.
- Planner and execution operate on logical and value-level data, not physical page structures.
- Cross-layer interaction must use small, explicit interfaces and narrow data contracts.

## Current Hardening Focus

- The current boundary hardening focus is planner to execution.
- Planner owns plan construction and stable plan shapes.
- Execution owns runtime flow, runtime state, and executor-owned adapted SELECT query/value shapes.
- This boundary is enforced through a data contract and bridge adaptation, not shared concrete runtime shapes.
- `v0.40` established the planner/executor seam; `v0.41.0-outer-seam-tightening` narrows the remaining outer-shell coupling and special-case handling.
- This milestone is refinement work on the existing seam, not a foundational redesign.

## Enforcement

- Package docs and root-layer comments must describe the same ownership model.
- Architectural guardrail tests enforce dependency direction, the current root/storage boundary contracts, and the planner/executor SELECT boundary seam.

## Non-Goals

- No package count increase as part of this boundary lock.
- No feature expansion.
- No SQL surface changes.
