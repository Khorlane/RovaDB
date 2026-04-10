# Outer Seam Tightening

This note defines the remaining seam-refinement targets for milestone `v0.41.0-outer-seam-tightening`. It is a boundary lock for follow-up slices, not a behavior-change plan.

## Remaining Outer Seam Pressure Points

- executor still accepts `*planner.SelectPlan` as its external input
- DB/root still contains some direct planner-payload inspection for special cases
- index-only handling remains a seam outlier
- planner still consumes parser AST directly where earlier translation may be possible

## Target End State

- executor entry points consume an executor-facing handoff rather than raw planner select shells
- normal select and index-only paths use the same seam discipline, or are isolated equally cleanly
- root orchestrates without peeking into planner payload details where avoidable
- planner may still consume parser AST, but parser-owned structures should stop at the earliest honest planning boundary

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
