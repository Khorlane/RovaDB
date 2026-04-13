# Column Nullability and Literal Defaults Design

This document defines the locked design scope for column-level `NOT NULL` and literal `DEFAULT` support in RovaDB.

The behavior described here is now implemented and serves as the milestone contract for parser, catalog, executor, reopen/lifecycle, and `ALTER TABLE ... ADD COLUMN` behavior. It also remains the boundary for rejecting wider default semantics such as expressions, functions, and special forms.

## Goal

- lock the supported design surface for column nullability and literal defaults
- define deterministic behavior for `INSERT`, `UPDATE`, and `ALTER TABLE ... ADD COLUMN`
- prevent follow-on slices from widening the feature beyond literal column defaults

## In Scope

- column-level `NOT NULL`
- column-level `DEFAULT <literal>`
- combined `NOT NULL DEFAULT <literal>`
- behavior for `CREATE TABLE` and `ALTER TABLE ... ADD COLUMN`
- behavior for omitted values on `INSERT`
- behavior for explicit `NULL` on `INSERT` and `UPDATE`
- validation of literal-default compatibility for current datatypes:
  - `INT`
  - `TEXT`
  - `BOOL`
  - `REAL`

## Out of Scope

- expressions in defaults
- function-call defaults
- column-reference defaults
- arithmetic in defaults
- `CURRENT_TIMESTAMP`-style special forms
- implicit or computed defaults of any kind
- new datatypes such as `SMALLINT`, `BIGINT`, `DATE`, `TIME`, and `TIMESTAMP`
- error-message wording standardization beyond the rejection rules in this document

Any widening of default semantics is a separate milestone.

## Supported Column Forms

This milestone family defines only the following logical column forms:

- nullable column with no default
- nullable column with a literal default
- `NOT NULL` column with no default
- `NOT NULL` column with a literal default

The intended SQL spellings are:

- `<name> <type>`
- `<name> <type> DEFAULT <literal>`
- `<name> <type> NOT NULL`
- `<name> <type> NOT NULL DEFAULT <literal>`

This design does not define a separate feature named `WITH DEFAULT`.

## Literal Default Rules

Defaults are literal-only for this design surface.

Allowed literal categories:

- numeric literals
- string literals
- boolean literals
- `NULL`, but only where the column is nullable

Datatype compatibility is checked against the target column's normal assignment rules. Examples using currently supported datatypes:

- `INT DEFAULT 1` is valid
- `REAL DEFAULT 1.25` is valid
- `TEXT DEFAULT 'ready'` is valid
- `BOOL DEFAULT TRUE` is valid
- `TEXT DEFAULT NULL` is valid because the column remains nullable
- `INT DEFAULT 'abc'` is invalid
- `BOOL DEFAULT 1` is invalid
- `NOT NULL DEFAULT NULL` is invalid

## INSERT Semantics

`DEFAULT` applies only when an `INSERT` omits a value for the column.

Locked rules:

- omitted value uses the column default if one is defined
- omitted value without a default yields `NULL` only for nullable columns
- omitted value for a `NOT NULL` column without a default is an error
- explicit `NULL` for a `NOT NULL` column is an error
- explicit `NULL` for a nullable column remains allowed, even if the column also has a non-`NULL` default

Examples:

- `age INT DEFAULT 18`: omitted `age` on `INSERT` yields `18`
- `nickname TEXT`: omitted `nickname` on `INSERT` yields `NULL`
- `active BOOL NOT NULL`: omitted `active` on `INSERT` is an error
- `score REAL NOT NULL DEFAULT 0.0`: omitted `score` on `INSERT` yields `0.0`
- `score REAL NOT NULL DEFAULT 0.0`: explicit `NULL` for `score` on `INSERT` is an error

## UPDATE Semantics

`UPDATE` does not inject defaults automatically.

Locked rules:

- explicit `NULL` assigned to a `NOT NULL` column is an error
- explicit `NULL` assigned to a nullable column is allowed
- no assignment to a column leaves the existing stored value unchanged
- `DEFAULT` is not re-applied during `UPDATE` just because the column is not mentioned

This means `DEFAULT` affects `INSERT` omission semantics only. It is not an automatic fill rule for later `UPDATE` statements.

## ALTER TABLE ADD COLUMN Semantics

The existing shape remains:

- `ALTER TABLE ... ADD COLUMN` appends the new column to the end of the table schema

The intended column-form behavior is:

- nullable column with no default: allowed
- nullable column with literal default: allowed
- `NOT NULL` column with no default:
  - allowed only if the target table is empty
  - rejected if the target table already has rows
- `NOT NULL` column with literal default: allowed

Existing-row behavior is locked as follows:

- adding a nullable column with no default yields `NULL` for pre-existing rows
- adding a nullable column with a literal default yields that default for pre-existing rows as a logical row value
- adding a `NOT NULL` column with a literal default backfills pre-existing rows to that default as a logical row value

For this design slice, existing-row backfill is a required logical effect, not a locked storage strategy. Later implementation slices may satisfy that effect by persisted row rewrite or by another durable mechanism that produces the same user-visible result across read, reopen, and subsequent writes. The implementation approach is deferred, but exposing pre-existing rows as transient `NULL` for these defaulted add-column cases is not allowed.

## Validation and Rejection Rules

The following rules are locked for later implementation:

- a `DEFAULT` literal must satisfy the target column datatype's compatibility rules
- `NOT NULL DEFAULT NULL` is invalid
- invalid default forms remain rejected
- omitted values for `NOT NULL` columns without defaults are rejected on `INSERT`
- explicit `NULL` for `NOT NULL` columns is rejected on both `INSERT` and `UPDATE`
- `ALTER TABLE ... ADD COLUMN <col> <type> NOT NULL` is rejected for non-empty tables

This slice does not widen accepted default syntax beyond literal forms. Future support for richer defaults must be introduced by a separate milestone and design pass.

## Test Placement Guidance

Coverage for this milestone should stay close to the owning layer:

- parser coverage should go into existing parser test files
- catalog persistence coverage should go into existing storage/catalog test files
- runtime enforcement should go into existing executor and integration tests
- lifecycle, reopen, recovery, and storage-format validation should prefer existing `integration_*` coverage
- avoid unnecessary new root-level test files

## Boundary Reminder

This milestone is complete within the surface locked here:

- literal defaults only
- no expression, function, placeholder, column-reference, or special-form defaults
- no widening to new datatypes such as `SMALLINT`, `BIGINT`, `DATE`, `TIME`, or `TIMESTAMP`
