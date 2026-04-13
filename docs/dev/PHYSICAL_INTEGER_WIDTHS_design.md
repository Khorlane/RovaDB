# Physical Integer Widths Design

This document defines the design scope for the physical integer widths milestone in RovaDB.

It is intentionally design-scope only. It locks the user-visible and storage-facing contract that later implementation slices must preserve, without changing parser behavior, storage encoding, scan behavior, or examples in this slice.

## Problem Statement

RovaDB already has an `INT` design note, but future support for multiple integer widths needs a narrower and more explicit contract before implementation begins.

Without that lock:

- later slices could drift on SQL-to-Go mappings
- storage width choices could become inconsistent with declared schema types
- write and `Scan` behavior could silently widen or narrow values
- catalog metadata could collapse distinct declared integer types into one runtime assumption

This milestone exists to define those boundaries before code changes begin.

## Milestone Goal

Lock the exact design scope for physical integer widths so later implementation slices can add support intentionally and testably.

After this design milestone:

- the supported integer type set is explicit
- SQL-to-Go type mapping is explicit
- on-page widths are explicit
- binary encoding expectations are explicit
- write and `Scan` strictness are explicit
- catalog persistence expectations are explicit
- non-goals are explicit

## Locked Decisions

### SQL Types

The physical integer widths milestone covers these SQL types:

- `SMALLINT`
- `INT`
- `BIGINT`

### SQL to Go Type Mapping

The public exact mapping is:

- `SMALLINT` -> `int16`
- `INT` -> `int32`
- `BIGINT` -> `int64`

This mapping is strict and is part of the intended public contract.

### On-Page Physical Widths

Declared integer types drive fixed physical widths:

- `SMALLINT` = 2 bytes
- `INT` = 4 bytes
- `BIGINT` = 8 bytes

Physical width is determined by the declared column type, not by the magnitude of a particular value.

### Encoding

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

These examples lock the intended encoding semantics. They do not require one exact helper shape, but later implementation must preserve the same durable meaning.

## Public Contract

### Write Contract

Writes are strict by declared type:

- `SMALLINT` columns accept only Go `int16`
- `INT` columns accept only Go `int32`
- `BIGINT` columns accept only Go `int64`

The following are not allowed:

- implicit widening
- implicit narrowing
- generic integer interchange
- "fits so allow it"
- using Go `int` for these typed integer columns

If a caller supplies the wrong Go integer type for the declared SQL type, the operation must fail explicitly.

### Scan Contract

Scanning is strict by declared type:

- `SMALLINT` scans only into `*int16`
- `INT` scans only into `*int32`
- `BIGINT` scans only into `*int64`

The following are not allowed:

- widening during scan
- narrowing during scan
- scanning these typed integer columns into `*int`

Later implementation should keep this contract explicit and deterministic rather than relying on permissive reflection behavior.

## Catalog and Schema Identity

Declared integer type identity must be preserved exactly in schema and catalog metadata.

That means:

- `SMALLINT`, `INT`, and `BIGINT` remain distinct declared types
- catalog persistence must not collapse them into one generic integer identity
- reopen must preserve the original declared type exactly

This is required so physical width, API behavior, and schema truth remain aligned across restarts.

## Storage and Runtime Boundary

The storage-facing rule is simple:

- physical width is driven by declared column type

Later implementation slices may choose internal execution representations pragmatically, but they must not weaken the public contract.

This design note intentionally does not over-prescribe internal refactors beyond what is needed to ensure:

- exact public write behavior
- exact public `Scan` behavior
- exact catalog identity
- exact physical width by declared type

Internal helpers may stay as narrow as practical, but the engine must not blur the user-visible distinction between `SMALLINT`, `INT`, and `BIGINT`.

## Storage Implications

Once implemented, adding physical integer widths will require storage paths to treat declared integer width as durable format truth.

Implications for later slices:

- row encoding must reserve the exact fixed width for the declared type
- row decoding must reconstruct the matching signed Go width
- validation must fail clearly if durable bytes do not match the declared type expectations
- durable metadata must remain version-aware and validation-aware

This milestone does not itself change the storage format. It only locks what the later storage work must implement.

## Non-Goals

The following are out of scope for this milestone and should not be smuggled into later implementation slices unless separately planned:

- `TINYINT`
- unsigned integer types
- implicit numeric coercion
- arithmetic or type-promotion redesign
- backward-compatibility migration work unless explicitly planned later

This milestone also does not require broad speculative numeric-framework work.

## Relationship to Existing INT Design

The earlier `INTEGER_design.md` note remains the design reference for the already-locked meaning of `INT` as signed 32-bit.

This document extends the design scope by locking the future multi-width contract before implementation begins. Where multi-width wording matters, this document is the controlling note.

## Likely Implementation Slices

The expected follow-on work is small-slice and implementation-oriented, for example:

- parser/type-name recognition for `SMALLINT` and `BIGINT`
- catalog/schema persistence for distinct integer identities
- storage encode/decode by declared width
- write-path validation for strict Go type acceptance
- query materialization and `Scan` enforcement for strict destination types
- reopen and corruption-focused integration coverage

Those slices should stay narrow and test-backed. This design note should not be read as permission to broaden scope beyond the locked contract above.
