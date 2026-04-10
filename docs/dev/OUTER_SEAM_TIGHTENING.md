# Outer Seam Tightening

This note records the seam refinements locked for milestone `v0.41.0-outer-seam-tightening`. It is a boundary lock for follow-up slices, not a behavior-change plan.

## Locked Seam Shape

- executor runtime entry is handoff-first through executor-owned SELECT handoffs
- index-only handling is isolated behind `IndexOnlyExecutionHandoff`, with a regular SELECT fallback handoff on the same seam
- DB/root orchestrates through handoff/access-path APIs rather than peeking into planner-owned index-only payload details
- planner translates parser-owned SELECT input at `PlanSelect` entry; later scan-choice helpers operate on planner-owned query/value/table-ref types

## Target End State

- executor entry points consume an executor-facing handoff rather than raw planner select shells
- normal SELECT and index-only paths follow the same seam discipline, with direct index-only execution isolated behind a narrow handoff
- root orchestrates without peeking into planner payload details
- planner may still accept parser AST at entry, but parser-owned structures stop at the earliest honest planning boundary

## Boundary Rules For This Milestone

- no SQL behavior changes
- no package-count increase
- no storage redesign
- no broad planner or executor rewrite
- translation/adaptation should happen once at the seam, not scattered

## Non-Goals

- feature expansion
- optimizer redesign
- join algorithm changes
- public API changes
