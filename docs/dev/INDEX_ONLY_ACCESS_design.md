# Index-Only Access Design Note

This note locks the narrow intended surface for the `ixonly` milestone line.

It is intentionally a scope note, not an implementation note.

## Meaning in RovaDB

For this milestone line, a query is "index-only" only when the engine can
produce the correct result entirely from:

- index contents
- index-structure metadata

without fetching base table rows.

If eligibility is uncertain, incomplete, or would require base-row fetches, the
planner/runtime must fall back to the existing non-index-only paths.

Correctness is more important than aggressiveness.

## Narrow milestone target

Initial target shapes are intentionally small:

1. `COUNT(*)` for eligible simple single-table queries
   Example:
   `SELECT COUNT(*) FROM customers;`

2. narrow indexed-column projection reads for eligible simple single-table
   queries
   Example:
   `SELECT cust_nbr FROM customers;`

This milestone line does not imply a broad covering-index framework or a broad
optimizer redesign.

## Fallback rule

Non-eligible queries must continue using the current table-scan, index-scan, or
join paths.

Falling back to the current path is always acceptable when:

- eligibility is uncertain
- a query shape is unsupported
- row fetches are still required for correctness

## Out of scope for this milestone line

- outer query rewrites
- broad optimizer redesign
- general covering-index exploitation across arbitrary query shapes
- SQL surface changes
- correctness shortcuts that guess eligibility

## Planner/runtime anchor

The planner package reserves an `index_only` scan shape as a future internal
execution contract.

That placeholder began in `ixonly.1` only as a surface lock-in. By the
`v0.36.0` milestone line, the implemented narrow runtime surface is:

- eligible plain single-table `COUNT(*)`
- eligible plain single-table single-column direct indexed projection
- eligible qualified single-table single-column direct indexed projection

Everything else still falls back to the existing non-index-only paths unless
and until a later milestone expands the supported surface.
