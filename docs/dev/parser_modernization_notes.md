# Parser Modernization Notes

This document captures the current parser modernization plan and any decisions we want to preserve across sessions.

## Working Approach

- Replace the parser incrementally, not in one shot.
- Keep the new parser alongside the old parser for a while.
- Add a lexer first.
- Do not switch the main parse entry path to the new lexer immediately.
- Build and test new parser components in isolation before wiring them into the main parser flow.
- Reuse current AST types at first where possible.
- Introduce new AST forms only when the current AST is no longer sufficient.

## First Migration Target

- First statement family to migrate: `CREATE TABLE`

Reasoning:

- simple grammar
- low ambiguity
- no expression complexity
- good fit for shaking out lexer and parser mechanics

## Testing Policy

- Keep normal lexer and parser correctness tests in the standard test suite.
- Add deeper lexer-specific proof tests behind an opt-in Go build tag.
- Deep lexer tests should not run in normal `go test ./...`.
- Run the deep lexer test suite when the lexer changes.

Suggested command shape:

- normal suite: `go test ./...`
- deep lexer suite: `go test -tags lexerdeep ./internal/parser/...`

## Commit Style

Use the following commit message prefix for this effort:

- `Add Parser Modernization Slice N: ...`

Examples:

- `Add Parser Modernization Slice 1: introduce lexer foundation`
- `Add Parser Modernization Slice 2: add isolated CREATE TABLE parser`
- `Add Parser Modernization Slice 3: wire CREATE TABLE into parser entry path`

## Language Definition

The parser modernization is guided by:

- [RovaDB_SQL_Language_Spec.md](c:\Projects\RovaDB\docs\dev\RovaDB_SQL_Language_Spec.md)

That document defines the SQL subset RovaDB supports and intends to support. It is language-definition-first, not an implementation status tracker.

## Session Update Guidance

At the end of a working session, update this file with:

- any parser design decisions that were settled
- any testing policy changes
- any commit naming decisions
- current slice status
- the next recommended step

## Current Slice Status

- `Parser Modernization Slice 1` started
- added a minimal lexer foundation in `internal/parser`
- lexer scope is currently limited to the token set needed for `CREATE TABLE`
- main parser entry path is unchanged
- deep lexer tests are implemented behind the `lexerdeep` build tag
- `Parser Modernization Slice 2` completed in isolation
- added a token-driven `CREATE TABLE` parser that produces the current `CreateTableStmt` AST
- the modern `CREATE TABLE` parser is tested but is not yet wired into `Parse()`
- `Parser Modernization Slice 3` completed
- `Parse()` now routes `CREATE TABLE` through the token-driven parser path
- legacy string-splitting `CREATE TABLE` parsing has been replaced in the active path
- full repo verification still passes after the integration
- `Parser Modernization Slice 4` completed in isolation
- extended the lexer token set to support `ALTER TABLE ... ADD COLUMN`
- added an isolated token-driven `ALTER TABLE` parser that produces the current `AlterTableAddColumnStmt` AST
- full repo verification still passes after the isolated `ALTER TABLE` slice
- `Parser Modernization Slice 5` completed
- `Parse()` now routes `ALTER TABLE ... ADD COLUMN` through the token-driven parser path
- legacy string-splitting `ALTER TABLE` parsing has been replaced in the active path
- full repo verification still passes after the integration
- `Parser Modernization Slice 6` completed in isolation
- extended the lexer token set to support `DELETE FROM ... [WHERE ...]`
- added an isolated token-driven `DELETE FROM` parser
- the modern `DELETE FROM` parser intentionally reuses the existing `WHERE` parser for now
- full repo verification still passes after the isolated `DELETE FROM` slice
- `Parser Modernization Slice 7` completed
- `Parse()` now routes `DELETE FROM` through the token-driven parser path
- the active `DELETE FROM` path still intentionally reuses the existing `WHERE` parser
- full repo verification still passes after the integration
- `Parser Modernization Slice 8` completed in isolation
- extended the lexer token set to support `UPDATE ... SET ...`
- added an isolated token-driven `UPDATE` parser
- the modern `UPDATE` parser intentionally reuses the existing assignment and `WHERE` parsers for now
- full repo verification still passes after the isolated `UPDATE` slice
- `Parser Modernization Slice 9` completed
- `Parse()` now routes `UPDATE` through the token-driven parser path
- the active `UPDATE` path still intentionally reuses the existing assignment and `WHERE` parsers
- full repo verification still passes after the integration
- `Parser Modernization Slice 10` completed in isolation
- extended the lexer token set to support `INSERT INTO ... VALUES ...`
- added an isolated token-driven `INSERT` parser
- the modern `INSERT` parser intentionally reuses the existing column-list and literal-value helpers for now
- full repo verification still passes after the isolated `INSERT` slice
- `Parser Modernization Slice 11` completed
- `Parse()` now routes `INSERT` through the token-driven parser path
- the active `INSERT` path still intentionally reuses the existing column-list and literal-value helpers
- full repo verification still passes after the integration

## Next Recommended Step

- prepare for `SELECT` modernization as the next major parser step
- expect `SELECT` to be the first statement family that likely needs deeper parser structure work
