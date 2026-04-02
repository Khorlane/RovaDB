# Multi-Page Catalog/Directory Storage Design

## Problem Statement

Page 0 currently stores control metadata plus the CAT/DIR payload. This creates a hard size ceiling. Larger schemas, more indexes, and more root mappings can exhaust page-0 capacity.

## Storage Model

RovaDB defines exactly two CAT/DIR storage modes.

### Embedded Mode

All CAT/DIR payload is stored in page 0. This is the preferred mode when the full CAT/DIR payload fits in embedded capacity.

### Overflow Mode

All CAT/DIR payload is stored outside page 0 in a chained sequence of dedicated CAT/DIR pages. Page 0 stores control metadata plus enough information to locate and validate the CAT/DIR chain.

## Core Invariant

CAT/DIR payload must never be split between embedded page-0 storage and overflow pages at the same time.

At any committed point, metadata is either entirely embedded or entirely overflow-backed.

## Promotion Rule

If the newly encoded CAT/DIR payload does not fit in page 0 embedded capacity, the engine promotes from embedded mode to overflow mode.

## Demotion Rule

If the newly encoded CAT/DIR payload fits in page 0 embedded capacity, the engine may store it in embedded mode even if the prior committed form used overflow.

Embedded mode is preferred whenever possible.

## Representation Preference

RovaDB should prefer the smallest valid committed metadata representation.

Embedded mode is preferred when the payload fits. Overflow mode is used only when necessary.

## Rewrite Strategy

Metadata-changing commits decide representation fresh from the newly encoded CAT/DIR payload.

Representation is chosen at commit/write time based on fit.

Future implementation should treat CAT/DIR persistence as full representation rewrites, not partial mixed-mode edits.

## Scope Boundaries For This Milestone

This milestone covers:

- storage representation for logical catalog plus directory/root-mapping metadata
- deterministic open/load behavior for either representation
- promotion and demotion between representations
- integrity and validation for the overflow chain format

This slice does not yet do:

- new SQL
- planner or runtime query changes
- user-visible behavior changes
- immediate on-disk format migration
