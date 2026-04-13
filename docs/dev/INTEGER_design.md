# INTEGER Design

This document defines the current integer semantics contract in RovaDB.

It originally began as the narrower `INT` correction note. That work is now
landed as part of the broader exact-width integer model, so this document is a
final-state design summary rather than a future-looking implementation note.

For the storage-facing width lock and durable-format emphasis, see
`docs/dev/PHYSICAL_INTEGER_WIDTHS_design.md`.

SQL syntax remains defined in `docs/dev/RovaDB_SQL_Language_Spec.md`.

## Purpose

- describe the exact-width SQL integer model now implemented
- keep public API behavior, runtime semantics, storage semantics, and docs aligned
- record the intentionally remaining narrow untyped integer seams

## Exact-Width Integer Model

RovaDB now exposes three distinct SQL integer types:

- `SMALLINT`
- `INT`
- `BIGINT`

Their locked Go mappings are:

- `SMALLINT` <-> `int16`
- `INT` <-> `int32`
- `BIGINT` <-> `int64`

These are distinct user-visible types. RovaDB does not treat them as one
generic integer family with permissive widening or narrowing.

## Binding Contract

Placeholder binding is exact-width for SQL integer values:

- SQL integer binding accepts only Go `int16`, `int32`, and `int64`
- Go `int` is rejected
- other integer-like Go types are rejected

Placeholder values remain positional and one-shot, but integer binding no
longer allows generic "close enough" interchange.

## Write Contract

Writes are exact by declared column type:

- `SMALLINT` columns accept only `int16`
- `INT` columns accept only `int32`
- `BIGINT` columns accept only `int64`

Untyped SQL integer literals are the narrow exception:

- an untyped integer literal may resolve to the target width if it fits
- if it does not fit, the write fails

Typed wrong-width integers are rejected even when the numeric magnitude would
fit in the target column.

Examples:

- `INSERT INTO t VALUES (1)` may resolve against a typed integer target column
- binding `int64(1)` into an `INT` column fails
- binding `int32(1)` into a `SMALLINT` column fails

## Storage Contract

Schema-aware storage preserves exact width on encode and decode:

- `SMALLINT` rows round-trip as `SMALLINT`
- `INT` rows round-trip as `INT`
- `BIGINT` rows round-trip as `BIGINT`

Declared schema type drives physical width and decoded runtime kind. Durable
storage must not collapse typed integer columns into one generic integer form.

## Evaluation Contract

Typed integer evaluation preserves exact width:

- `SMALLINT +/- SMALLINT -> SMALLINT`
- `INT +/- INT -> INT`
- `BIGINT +/- BIGINT -> BIGINT`

Overflow fails immediately.

Mixed-width typed arithmetic is rejected. RovaDB does not silently widen typed
integer operands to a larger result width.

Untyped integer literals may still resolve against a typed operand:

- typed integer + untyped integer literal resolves to the typed width if it fits
- if it does not fit, evaluation fails

## Materialization and Scan Contract

Typed SQL integer results materialize to their exact Go widths:

- typed `SMALLINT` -> `int16`
- typed `INT` -> `int32`
- typed `BIGINT` -> `int64`

Go `int` is not used on typed SQL integer result paths.

`Scan` is exact-width for typed integer results:

- `SMALLINT` scans only into `*int16`
- `INT` scans only into `*int32`
- `BIGINT` scans only into `*int64`

## Intentional Untyped Integer Seams

RovaDB still keeps a narrow untyped SQL integer-result path for expression-only
or schema-less cases that do not resolve against a typed integer column.

Today that includes:

- standalone integer literal results such as `SELECT 1`
- untyped integer literal arithmetic such as `SELECT 1 + 2`
- `COUNT(*)`-style aggregate results
- other narrow legacy integer-producing helper paths such as `LENGTH(text)`

These untyped integer results materialize as `int64` and scan only into
`*int64`.

This seam is intentional but narrow:

- it is not the normal typed-column path
- it does not weaken exact-width typed column semantics
- it does not imply generic Go `int` support

## Non-Goals

The current contract does not include:

- implicit widening between typed integer widths
- implicit narrowing between typed integer widths
- generic integer-family writes or scans
- using Go `int` as a typed SQL integer interchange type
- unsigned integer types
- arbitrary-precision integer support

## Acceptance Summary

The integer contract is correct only when all user-visible layers agree on the
same meaning:

- exact-width placeholder binding
- exact-width typed writes
- exact-width schema-aware storage round trips
- exact-width typed arithmetic with immediate overflow failure
- exact-width typed result materialization and `Scan`
- narrow, clearly described untyped integer-result seams only where still
  intentionally retained
