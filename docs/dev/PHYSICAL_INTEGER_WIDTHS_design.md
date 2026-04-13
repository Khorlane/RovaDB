# Physical Integer Widths Design

This document defines the locked exact-width integer contract for RovaDB.

It is the authoritative design note for the user-visible, storage-facing, and
durable-format behavior of `SMALLINT`, `INT`, and `BIGINT`.

## Purpose

- lock the exact-width integer model that the implementation now preserves
- keep SQL type identity, Go type identity, and physical width aligned
- prevent drift back toward generic integer handling or permissive widening
- describe the intentionally narrow untyped integer-result seams that still remain

## Locked SQL Integer Types

RovaDB supports these SQL integer types:

- `SMALLINT`
- `INT`
- `BIGINT`

These are distinct declared schema types. They are not aliases for one generic
integer storage or API shape.

## Locked SQL-to-Go Mapping

The public mapping is exact:

- `SMALLINT` -> `int16`
- `INT` -> `int32`
- `BIGINT` -> `int64`

That mapping applies across binding, writes, storage decode, query
materialization, and `Scan`.

## Locked Binding Contract

Placeholder binding for SQL integer values is exact-width:

- placeholders accept only Go `int16`, `int32`, and `int64` for SQL integer binding
- Go `int` is rejected
- other integer-like Go types are rejected

Binding is not allowed to blur the SQL integer widths by accepting whichever Go
integer happens to fit numerically.

## Locked Write Contract

Typed integer writes are exact by declared column type:

- `SMALLINT` columns accept only `int16`
- `INT` columns accept only `int32`
- `BIGINT` columns accept only `int64`

Allowed narrow exception:

- untyped SQL integer literals may resolve to the target width if they fit

Rejected behavior:

- implicit widening
- implicit narrowing
- generic integer interchange
- "fits so allow it" for wrong-width typed Go integers
- using Go `int` for typed SQL integer columns

If the caller supplies the wrong typed integer width for the target SQL column,
the write must fail even when the numeric magnitude would fit.

## Locked Physical Widths

Declared schema type drives fixed on-page width:

- `SMALLINT` = 2 bytes
- `INT` = 4 bytes
- `BIGINT` = 8 bytes

Physical width is determined by declared column type, not by runtime value
magnitude.

## Locked Encoding Semantics

Integer values are encoded as:

- little-endian
- signed values preserved through their two's-complement bit pattern

Representative write forms:

- `binary.LittleEndian.PutUint16(buf, uint16(int16(v)))`
- `binary.LittleEndian.PutUint32(buf, uint32(int32(v)))`
- `binary.LittleEndian.PutUint64(buf, uint64(int64(v)))`

Representative read forms:

- `int16(binary.LittleEndian.Uint16(buf))`
- `int32(binary.LittleEndian.Uint32(buf))`
- `int64(binary.LittleEndian.Uint64(buf))`

These examples lock the durable meaning, not one mandatory helper layout.

## Locked Storage Round-Trip Contract

Schema-aware row storage preserves exact width on encode and decode:

- `SMALLINT` rows round-trip as `SMALLINT`
- `INT` rows round-trip as `INT`
- `BIGINT` rows round-trip as `BIGINT`

Storage must not:

- collapse typed integer columns into one generic integer identity
- decode typed integer columns onto the wrong width
- silently repair or reinterpret durable bytes that violate declared width

Corruption or durable-format mismatch must fail clearly.

## Locked Evaluation Contract

Typed integer arithmetic preserves exact width:

- `SMALLINT +/- SMALLINT -> SMALLINT`
- `INT +/- INT -> INT`
- `BIGINT +/- BIGINT -> BIGINT`

Required behavior:

- overflow fails immediately
- mixed-width typed arithmetic is rejected
- typed integer + untyped integer literal resolves to the typed width if it fits
- typed integer + untyped integer literal fails if it does not fit

The engine must not silently widen typed integer arithmetic results to a larger
width.

## Locked Materialization Contract

Typed integer results materialize to exact Go widths:

- typed `SMALLINT` -> `int16`
- typed `INT` -> `int32`
- typed `BIGINT` -> `int64`

Go `int` is not used on typed SQL integer result paths.

## Locked Scan Contract

Scanning is exact-width for typed integer results:

- `SMALLINT` scans only into `*int16`
- `INT` scans only into `*int32`
- `BIGINT` scans only into `*int64`

Untyped integer results scan only into `*int64`.

The following are not allowed for typed integer results:

- widening during scan
- narrowing during scan
- scanning into `*int`

## Locked Catalog and Schema Identity

Declared integer identity must be preserved exactly in schema and catalog
metadata.

That means:

- `SMALLINT`, `INT`, and `BIGINT` remain distinct declared types
- catalog persistence must not collapse them into one generic integer identity
- reopen must preserve the original declared type exactly

This is required so schema truth, physical width, runtime behavior, and API
behavior stay aligned across restarts.

## Intentional Narrow Untyped Seams

RovaDB still retains a narrow untyped integer-result path for expression-only or
schema-less results that do not resolve against a typed integer column.

Current examples include:

- `SELECT 1`
- `SELECT 1 + 2`
- `COUNT(*)`-style results
- narrow helper results such as `LENGTH(text)`

These results materialize as `int64` and scan only into `*int64`.

This seam is intentional and narrow:

- it is not part of the normal typed column path
- it does not permit generic typed integer interchange
- it does not weaken exact-width schema-aware storage or typed scans

## Non-Goals

The exact-width integer contract does not include:

- `TINYINT`
- unsigned integer types
- implicit numeric coercion between typed integer widths
- generic Go `int` support for typed SQL integer paths
- broad speculative numeric-framework work

## Acceptance Summary

The contract is preserved only if all of the following stay true together:

- exact-width placeholder binding
- exact-width typed writes
- exact physical width by declared schema type
- exact-width schema-aware storage round trips
- exact-width typed arithmetic with immediate overflow failure
- exact-width typed materialization and `Scan`
- narrow, explicitly documented untyped integer-result seams only where they are
  still intentional
