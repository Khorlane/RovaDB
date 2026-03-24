# REAL Design

## Goal

Lock the intended internal design for `REAL` so later slices can implement it against one explicit in-repo reference.

## Locked semantics

- `REAL` is the canonical fractional numeric schema type.
- The public schema type name is exactly `REAL`.
- Runtime Go representation is `float64`.

## Literal classification

- Unquoted decimals are `REAL`.
- Unquoted whole numbers are `INT`.
- Quoted numeric-looking values remain `TEXT`.

## Type enforcement rules

- `REAL` columns accept `REAL` and `NULL` only.
- No implicit coercion from `INT`, `TEXT`, or `BOOL`.
- `REAL` does not auto-convert to `INT`.
- `INT` does not auto-convert to `REAL`.

## Comparison rules

- `REAL`-to-`REAL` comparisons support equality and ordering operators.
- Mixed `INT` vs `REAL` comparisons should remain strict and be treated as type mismatch / rejected consistently with RovaDB's strict model.

## Persistence expectations

- `REAL` values must survive close/reopen and rollback-journal recovery exactly like existing persisted types.

## Non-goals

- Do not add `FLOAT` as a distinct public type.
- Do not add broad parser aliases by default.
- Do not add scientific notation unless it later proves trivially compatible and well-tested.
- Keep this rollout datatype-focused, not a broader SQL-language expansion.

## Planned rollout slices

- Slice 2: enable `REAL` in schema parsing and schema/catalog preservation
- Slice 3: parse decimal literals as `REAL`
- Slice 4: carry `REAL` through runtime values and row materialization
- Slice 5: enforce strict `REAL` column validation in `INSERT` and `UPDATE`
- Slice 6: persist `REAL` row values through storage encode/decode
- Slice 7: support `REAL` in `WHERE` comparisons and filtering
- Slice 8: prove `REAL` durability across reopen and recovery paths
- Slice 9: document and demonstrate `REAL` in the user-facing product surface
