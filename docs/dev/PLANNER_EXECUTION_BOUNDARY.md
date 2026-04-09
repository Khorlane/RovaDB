# Planner-Execution Boundary

This note locks the planner and execution ownership split for milestone `v0.40.0-planner-execution-boundary-separation`. It is authoritative for follow-up cleanup slices and does not itself change runtime behavior.

## Planner Owns

- Parsed-statement-to-plan translation.
- Plan structs and plan enums.
- Scan and operator selection.
- Logical query shape decisions.
- No runtime row materialization.
- No storage or page inspection.
- No executor-owned row, value, or runtime state.

## Execution Owns

- Consuming planner-produced plans.
- Runtime operator flow.
- Row iteration and materialization.
- Runtime evaluation state.
- Storage calls through existing narrow boundaries.
- No planner decision logic beyond interpreting the plan.

## Boundary Rules

- Planner outputs plan data only.
- Execution consumes plan data only.
- Planner must not depend on executor concrete structs.
- Execution must not depend on parser/planner internals beyond stable plan shapes.
- Parser-owned statement and value structures should not flow deeper than necessary.
- Execution-time helper state must not live in planner types.

## Non-Goals For This Milestone

- No SQL feature expansion.
- No storage redesign.
- No package-count increase.
- No broad executor rewrite.

## Likely Pressure Points For Later Slices

- `SelectPlan` and `IndexScan` currently carry parser-owned structures.
- Executor paths currently consume planner/parser-owned shapes directly in several places.
- Some runtime-only concerns are still represented too close to planner-owned structs.
