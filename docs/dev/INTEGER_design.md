# INTEGER Design

This document defines the intended design for integer semantics in RovaDB.

It currently focuses on correcting the meaning of the public `INT` type and preparing the codebase for possible future support of additional integer widths.

It is an implementation-oriented design note. SQL syntax and user-visible statement semantics remain defined in `docs/dev/RovaDB_SQL_Language_Spec.md`.

## Purpose

- define what `INT` should mean in RovaDB
- describe the current mismatch between public intent and actual behavior
- guide implementation of `kg022`
- leave light groundwork for possible future `SMALLINT` and `BIGINT` support

## Current Gap

RovaDB currently exposes a single public integer schema type:

- `INT`

But current engine/runtime/storage behavior effectively treats that path as 64-bit integer behavior.

That creates a mismatch between:

- the simplicity of the public type surface
- the intended meaning of `INT`
- the likely future need to distinguish integer widths more intentionally

## Design Goal

For the current public surface, RovaDB should treat:

- `INT` as a 32-bit signed integer type

That meaning should be consistent across:

- parsing
- runtime values
- comparisons
- storage
- query result exposure
- `Scan`

## Non-Goals

This document does not make `SMALLINT` or `BIGINT` public schema types yet.

The following are out of scope for the current change unless explicitly added later:

- public `SMALLINT`
- public `BIGINT`
- arbitrary-precision integers
- implicit widening or narrowing coercions between integer widths

## Public Contract

For the current public surface:

- `INT` is the only public integer schema type
- `INT` means signed 32-bit integer semantics
- values outside the supported `INT` range must fail rather than silently truncating

Future direction will add:

- `SMALLINT`
- `BIGINT`

But the current change should not expose those types yet.

## Affected Layers

Implementing correct `INT` semantics touches:

- parser literal handling
- bound value handling
- runtime value representation
- type checking on `INSERT` and `UPDATE`
- comparison behavior
- storage encoding/decoding
- query materialization
- `Scan`

The implementation should aim for one coherent integer contract rather than layer-by-layer special cases.

## Parser and Binding Expectations

Integer literals and bound values intended for `INT` columns should be validated against 32-bit signed range.

Required behavior:

- values inside signed 32-bit range are accepted
- values outside signed 32-bit range are rejected
- rejection should be deterministic and explicit

The parser/binder path should not silently preserve wider integer semantics just because Go or internal storage currently uses wider numeric containers.

## Runtime Expectations

The runtime should have an explicit notion that current public integer semantics are 32-bit.

That does not necessarily require every internal helper to store raw values as Go `int32`, but it does require:

- explicit range correctness
- explicit conversion rules
- no accidental reliance on implicit `int64` semantics

The important design goal is semantic correctness, not blindly forcing every internal variable to one Go type if a wider temporary carrier remains useful internally.

## Storage Expectations

Storage behavior should remain aligned with the public `INT` contract.

If internal storage encoding currently uses a wider integer carrier, that is acceptable only if:

- persisted and decoded values remain valid 32-bit `INT` values
- values outside the 32-bit range cannot enter durable user-visible `INT` state

The storage layer should not be the place where the public contract silently widens.

## Comparison and Execution Expectations

Integer comparison behavior should assume `INT` values are 32-bit values semantically.

Required outcomes:

- equality and ordering behavior remain deterministic
- out-of-range values do not slip into execution through alternate paths
- arithmetic or function behavior that produces integers remains compatible with the `INT` contract where applicable

## Scan and API Expectations

Go-facing query results should remain unsurprising and internally consistent with the `INT` contract.

The implementation should define clearly:

- what Go type `INT` values are exposed as through query materialization
- what `Scan` destinations are supported for `INT`
- how unsupported or out-of-range scans fail

The important part is a stable, explicit API contract rather than accidental behavior inherited from the current wider implementation.

## Compatibility Considerations

Changing effective `INT` semantics from 64-bit behavior to 32-bit behavior is a compatibility-sensitive change.

That means implementation should be intentional about:

- literal acceptance
- bound value acceptance
- persisted values written by prior behavior
- tests that currently assume wider range

This is not just an internal cleanup; it is a semantic correction.

## Future-Width Groundwork

The current `INT` correction should leave light groundwork for future `SMALLINT` and `BIGINT`.

That means:

- make integer-width semantics explicit internally
- centralize integer range validation and conversion
- avoid hard-coding fresh assumptions that `INT` is the only width the engine will ever know about

That does not mean:

- introducing public multi-width SQL syntax now
- building a large speculative type framework
- over-designing for features not yet committed

The goal is to make future width expansion feel like a contained extension rather than another semantic rewrite.

## Suggested Internal Direction

When implementing `kg022`, prefer a design where:

- integer semantic width is defined intentionally in one place
- validation logic is shared rather than duplicated across layers
- future width variants could plug into the same pattern later

Avoid designs where:

- parser, executor, storage, and scan each reinvent their own integer-width assumptions
- the codebase still effectively treats `INT` as "whatever fits in `int64`"

## Test Plan

Implementation should include coverage for at least:

- `INT` literals within 32-bit range
- `INT` literals outside 32-bit range
- bound values within 32-bit range
- bound values outside 32-bit range
- insert/update validation for `INT` columns
- reopen behavior for persisted `INT` values
- comparison behavior at important signed 32-bit boundaries
- query result and `Scan` behavior for valid `INT` values

## Acceptance Criteria

`kg022` is complete when:

- `INT` behaves as signed 32-bit consistently across user-visible engine behavior
- out-of-range values fail deterministically
- parser/runtime/storage/API behavior are aligned
- tests cover the corrected contract
- the implementation leaves a clean path for future `SMALLINT` and `BIGINT` support without exposing them yet

## Notes

- This document is intentionally smaller than the schema/index lifecycle docs.
- It should guide `kg022`, not replace the lightweight backlog entry.
- If future public multi-width integer types are added, this document should either be expanded or succeeded by a broader numeric-types design note.
