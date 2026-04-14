# Database Architecture

This document defines the authoritative architectural structure, ownership boundaries, dependency rules, and execution-path seam contracts for RovaDB.

It is the production architectural reference for the engine. It replaces narrower architecture and seam notes by consolidating the locked ownership model, the planner-to-execution boundary, the root-side outer seam, and the approved narrow index-only execution surface into one durable document.

## Purpose

- define the locked architectural shape of the RovaDB engine
- make ownership boundaries explicit across engine layers
- prevent cross-layer drift and shortcut coupling
- define the approved seam contracts between planner, execution, and root orchestration
- keep future implementation work aligned with a stable architectural model

## Scope

This document covers:

- engine layers and ownership
- dependency direction
- type and seam boundaries
- planner-to-execution boundary rules
- root orchestration rules around `SELECT` and index-only execution
- the approved narrow index-only execution surface

This document does not define:

- the SQL language contract
- storage-page byte layouts
- durable metadata encoding formats
- release planning or milestone sequencing

Those topics belong in the SQL language specification, storage-engine documents, and planning documents.

## Architectural Pipeline

RovaDB follows a locked, one-way architectural pipeline:

```text
parser -> planner -> execution -> storage
```

The root package owns the public API surface around that pipeline. Supporting packages such as `txn` and `bufferpool` participate in transaction and durable-page orchestration, but they do not redefine the ownership of the main engine path.

## Layer Ownership

### Parser

The parser layer owns SQL syntax parsing only.

It is responsible for:

- tokenization and statement parsing
- AST and parsed statement construction
- parsed value and expression structures
- SQL-shape recognition within the committed language surface

It is not responsible for:

- execution behavior
- planning decisions
- storage/page knowledge
- durability knowledge
- runtime row materialization

### Planner

The planner layer owns logical planning only.

It is responsible for:

- translating parsed statement structures into planner-owned plan structures
- scan and operator selection
- logical query-shape decisions
- stable plan and scan-shape definitions

It is not responsible for:

- storage/page inspection
- runtime row materialization
- executor-owned runtime state
- durable-format awareness

Planner is the architectural boundary where parser-owned structures stop being the general engine currency.

### Execution

The execution layer owns runtime statement behavior.

It is responsible for:

- consuming planner-produced plans
- runtime operator flow
- row iteration and row materialization
- statement execution
- runtime evaluation state
- coordinating logical work against storage through approved narrow boundaries

It is not responsible for:

- planner decision logic beyond interpreting the plan
- parser ownership
- storage-page/layout policy
- exposing storage internals upward

Execution is the runtime layer. It consumes approved inputs and produces user-visible row and statement results without leaking storage detail into upper layers.

### Storage

The storage layer owns durable physical truth.

It is responsible for:

- pages and durable page access
- journal and WAL formats
- durable metadata encoding and validation
- physical layout and physical ownership invariants
- index-structure persistence and physical lookup support

It is not responsible for:

- SQL syntax
- parsed statements
- planner-owned plans
- executor-owned runtime flow

Storage must remain logically below the query engine. It provides durable capabilities and validation, not SQL-facing semantics.

### Root API

The root package owns the public embedded API surface.

It is responsible for:

- public API types such as `DB`, `Rows`, `Row`, `Result`, and `Tx`
- public entrypoints such as `Open`, `Exec`, `Query`, `QueryRow`, and transaction lifecycle methods
- orchestration across parser, planner, execution, and storage-facing engine packages
- public error surfacing and user-facing coordination behavior

It must remain thin. It does not own parser logic, planning logic, execution logic, or storage policy.

### Transaction and Buffer Pool Support

Supporting packages have bounded responsibilities:

- `txn` owns transaction state transitions and transaction-oriented orchestration
- `bufferpool` owns committed/private page mediation for durable page access

These supporting packages do not redefine the one-way ownership of the core engine pipeline.

## Dependency Direction

The dependency rules are locked.

### Core Rules

- no upward imports
- no sibling-layer imports that bypass the approved pipeline
- `storage` never imports `execution`, `planner`, or `parser`
- `planner` never depends on storage concrete types
- `execution` must not expose storage internals upward
- root orchestration must not reach across seams by peeking into lower-layer implementation detail when an approved boundary contract exists

### Intent

Dependency direction exists to preserve:

- local reasoning within a layer
- testable ownership
- deterministic seam behavior
- freedom to refine internals without widening architectural leakage

## Type and Data Boundaries

Architectural boundaries are enforced not only by imports, but also by the shapes of data that cross seams.

### General Boundary Rules

- storage types must not leak outside storage unless an explicit boundary type exists for that purpose
- planner and execution work on logical and value-level data, not physical page structures
- cross-layer interaction must use small, explicit interfaces and narrow data contracts
- runtime helper state must live in the layer that owns the runtime behavior

### Parser Boundary

Parser-owned statement and value structures should not flow past planner translation unless a narrower explicit boundary intentionally permits it.

The normal architectural rule is:

- parser owns parsed SQL structures
- planner translates them into planner-owned plan structures
- execution consumes planner-owned contracts or executor-owned adapted forms

### Storage Boundary

Storage owns:

- durable value encoding
- page and layout metadata
- durable index structures
- physical validation behavior

Upper layers must not treat storage-owned physical details as general engine currency.

## Planner-to-Execution Boundary

The planner-to-execution boundary is a locked architectural seam.

### Planner Owns

- parsed-statement-to-plan translation
- plan structs and plan enums
- scan and operator selection
- logical query-shape decisions

### Execution Owns

- consuming planner-produced plans
- runtime operator flow
- row iteration and materialization
- runtime evaluation state
- storage calls through approved narrow boundaries

### Boundary Rules

- planner outputs plan data only
- execution consumes plan data only
- planner must not depend on executor concrete structs
- execution must not depend on planner query or expression tree structs in `SELECT` runtime hot paths
- `SELECT` runtime hot paths must run through executor-owned adapted shapes where adaptation is required
- execution-time helper state must not live in planner types
- `SELECT` access-path interpretation in executor must stay centralized through the planner-to-executor bridge rather than scattered raw plan branching

### Enforced End State

The intended and maintained end state is:

- planner translates parsed input into planner-owned plan shapes
- executor consumes an execution-facing handoff or adapted runtime shape
- parser-owned concrete structures do not continue as general runtime payloads
- root orchestration does not bypass the seam by inspecting planner-owned internals directly

## Root-Side Outer Seam

The root-side outer seam around `SELECT` and index-only handling is also locked.

### Root Orchestration Rules

Root orchestration must:

- coordinate through approved handoff and access-path APIs
- avoid peeking into planner-owned index-only payload details
- treat normal `SELECT` and index-only execution as seam-governed entry paths, not ad hoc special cases

### Locked Seam Shape

- executor runtime entry is handoff-first through executor-owned `SELECT` handoffs
- index-only handling is isolated behind `IndexOnlyExecutionHandoff`
- index-only execution has a regular `SELECT` fallback handoff on the same seam
- root orchestrates through handoff and access-path APIs rather than planner payload peeking
- planner translates parser-owned `SELECT` input at `PlanSelect` entry
- later scan-choice helpers operate on planner-owned query, value, and table-reference types

### Architectural Intent

This seam exists to keep:

- root orchestration thin
- planner ownership intact
- execution ownership explicit
- special-case access paths isolated rather than spread through the root package

## Index-Only Access Surface

Index-only execution is intentionally narrow.

### Meaning of Index-Only in RovaDB

A query is index-only only when the correct result can be produced entirely from:

- index contents
- index-structure metadata

without fetching base table rows.

If eligibility is uncertain, incomplete, or would require base-row fetches, the engine must fall back to the existing non-index-only path.

Correctness is more important than aggressiveness.

### Approved Surface

The implemented and approved narrow index-only surface is:

- eligible plain single-table `COUNT(*)`
- eligible plain single-table single-column direct indexed projection
- eligible qualified single-table single-column direct indexed projection

This is a narrow architectural capability, not a general covering-index framework.

### Fallback Discipline

Fallback to existing table-scan, index-scan, or join paths is always acceptable when:

- eligibility is uncertain
- the query shape is unsupported
- row fetches are required for correctness

This fallback discipline is part of the architecture, not a temporary implementation detail.

### Non-Goals of the Current Surface

The current index-only architecture does not imply:

- broad optimizer redesign
- general covering-index exploitation
- outer query rewrites
- SQL-surface expansion
- correctness shortcuts based on guessed eligibility

## Architectural Guardrails

The architecture is intended to be enforceable, not merely descriptive.

### Required Enforcement Posture

- package docs and root-level docs must describe the same ownership model
- dependency direction must be protected by architecture guardrail tests
- the planner-to-execution seam must be protected by architecture-focused tests
- root-side orchestration rules around `SELECT` and index-only handling must remain test-backed where feasible

### Review Standard

Changes that introduce any of the following should be treated as architectural regressions unless explicitly justified and redesign-approved:

- upward imports
- storage-type leakage into upper layers
- root-side peeking into planner internals
- planner dependence on execution-owned runtime state
- execution dependence on planner concrete query trees in hot paths
- broadened index-only behavior without explicit surface definition and fallback rules

## Architectural Non-Goals

The architecture lock does not imply:

- increased package count as a goal in itself
- feature expansion
- optimizer redesign
- join algorithm redesign
- storage redesign through this document
- public API expansion through architecture drift
- refactoring for elegance alone when ownership is already clear

## Stability Requirements

Future work must preserve the following architectural truths:

- one-way engine layering
- thin public root package
- planner-owned logical planning
- executor-owned runtime behavior
- storage-owned durable truth
- narrow, explicit seam contracts
- correctness-first fallback behavior for uncertain special paths
- explicit separation between language semantics, architecture, and storage-engine design

## Summary

RovaDB’s architecture is a layered, one-way, correctness-first design:

- parser owns SQL parsing
- planner owns logical planning
- execution owns runtime behavior
- storage owns durable physical truth
- root owns the public API surface and thin orchestration

The planner-to-execution seam and the root-side outer seam are both locked and intentionally narrow. Index-only execution is allowed only on a small approved surface and must fall back whenever correctness would otherwise be uncertain.

This document is the authoritative architectural contract for keeping the engine coherent as it evolves.
