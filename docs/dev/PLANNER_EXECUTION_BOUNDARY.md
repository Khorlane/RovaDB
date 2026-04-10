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
- Execution must not depend on planner query/expression tree structs in SELECT runtime hot paths; those paths run through executor-owned adapted shapes.
- Parser-owned statement and value structures should not flow past planner translation unless a narrower boundary explicitly requires it.
- Execution-time helper state must not live in planner types.
- SELECT access-path interpretation in executor should stay centralized through the planner-to-executor bridge rather than scattered raw plan-shape branching.

## Non-Goals For This Milestone

- No SQL feature expansion.
- No storage redesign.
- No package-count increase.
- No broad executor rewrite.

## Enforced In This Milestone

- `SelectPlan` no longer carries parser-owned `SelectExpr` payload.
- `IndexScan` no longer carries parser-owned `parser.Value` payload.
- SELECT runtime hot paths adapt planner query/expression trees into executor-owned runtime shapes before evaluation.
- Architectural tests protect the bridge-owned access-path seam and the current planner/executor contract.
