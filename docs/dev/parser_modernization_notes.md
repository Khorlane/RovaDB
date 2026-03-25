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
- `Parser Modernization Slice 12` completed as a batched `SELECT` update
- extended the lexer token set to support the basic `SELECT` clause shell
- `SELECT ... FROM ... [WHERE ...] [ORDER BY ...]` now routes through a modern token-driven parser shell
- literal and simple arithmetic `SELECT` forms now route through a modern token-driven `SELECT` shell
- the active `SELECT` path still intentionally reuses the existing `WHERE`, `ORDER BY`, and expression helpers
- full repo verification still passes after the `SELECT` batch
- predicate modernization batch started
- added a richer isolated predicate AST with precedence-aware parsing for `NOT`, `AND`, `OR`, and parenthesized grouping
- extended the lexer with predicate operators, literals, placeholders, and grouping tokens
- added a bridge field so `SELECT`, `UPDATE`, and `DELETE` can carry both the legacy flat `WhereClause` and the new predicate tree
- added a conservative adapter that can flatten compatible predicate trees back into the legacy flat `WhereClause`
- the adapter intentionally rejects grouped, mixed-precedence, and `NOT` shapes when flattening would lose semantics
- binder now walks predicate trees for placeholder collection and binding
- binding also backfills the legacy flat `WhereClause` only when a predicate can be flattened safely
- planner now reads simple equality predicates from the new predicate tree for index-scan selection
- executor now evaluates and validates the new predicate tree directly for `SELECT`, `UPDATE`, and `DELETE`
- active `WHERE` parsing now routes through the precedence-aware predicate parser first
- simple flat chains still populate the legacy `WhereClause` when they can be represented safely
- grouped and `NOT` predicates now parse and execute with their intended semantics
- test expectations have been updated from legacy left-to-right boolean evaluation to `NOT` > `AND` > `OR` precedence
- `Parser Modernization Slice 14` completed
- `ORDER BY` clause parsing now uses tokenized parsing instead of `strings.Fields`
- `SELECT` clause-tail parsing now detects `WHERE` and `ORDER BY` through the lexer instead of string prefix/search logic
- the active `SELECT` path no longer depends on raw substring keyword scanning for clause dispatch
- full repo verification still passes after the `ORDER BY` and clause-tail modernization
- `Parser Modernization Slice 15` completed
- literal `SELECT` expression parsing now uses lexer tokens instead of string splitting and manual parenthesis slicing
- arithmetic `+` and `-` are now tokenized explicitly for the current literal-expression subset
- the supported literal-expression surface is intentionally unchanged; this slice modernized the implementation without broadening SQL support
- full repo verification still passes after the literal-expression modernization
- `Parser Modernization Slice 16` completed
- added a shared token-based literal value helper for placeholders, numbers, strings, booleans, and `NULL`
- predicate comparisons now bind their right-hand values through the shared token helper
- `INSERT ... VALUES (...)` now parses column lists and value lists from tokens instead of substring splitting
- `UPDATE ... SET ...` assignments now parse target/value pairs from tokens instead of string splitting
- the supported value surface is intentionally unchanged; this slice modernized shared value parsing without broadening SQL support
- full repo verification still passes after the shared value-expression modernization
- `Parser Modernization Slice 17` completed
- predicate comparisons now support column-to-column form in addition to column-to-literal form
- the predicate parser records right-hand column references explicitly instead of forcing every comparison into a literal value
- executor validation and evaluation now understand right-hand column references
- planner index-scan selection still intentionally requires equality against a concrete literal value
- legacy flat `WhereClause` backfill still applies only when a predicate comparison can be represented safely
- full repo verification still passes after the comparison-operand modernization
- `Parser Modernization Slice 18` completed as a batched operand-expression milestone
- added a small value-expression AST for predicate operands
- predicate comparisons now carry explicit left and right operand expressions while still backfilling the legacy flat condition shape when possible
- predicate operands now support plain column references, literal values, parenthesized value expressions, and a small scalar-function subset
- runtime predicate evaluation now resolves operand expressions before comparison
- initial scalar-function operand support includes `LOWER`, `UPPER`, `LENGTH`, and `ABS`
- placeholder binding now walks predicate operand expressions, including placeholders nested inside function arguments
- planner index-scan selection remains conservative and still only uses plain column-equality-to-literal predicates
- full repo verification still passes after the operand-expression milestone
- `Parser Modernization Slice 19` completed as a batched projection-expression milestone
- `SELECT ... FROM ...` projections now support shared value expressions in addition to plain column lists
- plain column projections still preserve the existing `Columns` path for compatibility
- expression projections now carry stable labels from the parsed select items
- executor projection now evaluates expression items per row instead of requiring every projection to resolve to a base column index
- `*` and `COUNT(*)` behavior remain unchanged
- full repo verification still passes after the projection-expression milestone
- `Parser Modernization Slice 20` completed as a batched qualified-reference milestone
- lexer now tokenizes `.` so table-qualified references can be parsed explicitly
- shared value expressions now support qualified column references such as `users.id`
- single-table executor validation and evaluation accept qualified references when the qualifier matches the current table name
- planner index-scan selection accepts qualified single-table equality predicates when the qualifier matches the statement table
- plain unqualified references continue to work unchanged
- aliases and multi-table resolution are still deferred; this slice only establishes the join-ready qualified-reference structure
- full repo verification still passes after the qualified-reference milestone
- `Parser Modernization Slice 21` completed as an alias-aware single-table `FROM` milestone
- `SELECT` now carries a small `TableRef` shape in addition to the legacy `TableName` compatibility field
- the modern `SELECT` parser accepts optional single-table aliases in both `FROM users u` and `FROM users AS u` forms
- single-table executor validation, projection evaluation, predicate evaluation, and `ORDER BY` resolution now accept qualifiers that match either the base table name or the parsed alias
- planner index-scan selection now accepts alias-qualified equality predicates for the current single-table `SELECT` shape
- runtime scope is intentionally unchanged: this slice is still single-table only and does not activate joins
- full repo verification still passes after the alias-aware single-table milestone
- `Parser Modernization Slice 22` completed as a multi-table `FROM` structure milestone
- the modern `SELECT` parser now accepts comma-separated table references with optional aliases and records them in `SelectExpr.From`
- the first parsed `FROM` item still backfills `TableName` as the current compatibility field
- parser scope is intentionally ahead of runtime scope: multi-table `SELECT` shapes now parse, but planner/executor still reject them through the normal unsupported-query path
- this preserves a clean join-ready `FROM` structure without prematurely widening execution semantics
- full repo verification still passes after the multi-table `FROM` structure milestone
- `Parser Modernization Slice 23` completed as an explicit join-syntax milestone
- the lexer now recognizes `INNER`, `JOIN`, and `ON`
- `SELECT` now carries explicit `JOIN ... ON ...` clauses in the parsed AST
- the modern `SELECT` parser accepts both `JOIN` and `INNER JOIN` for the current inner-join shape
- join predicates are parsed through the same predicate-expression machinery as `WHERE`
- binder traversal now includes join predicates so placeholder/value walking stays structurally complete
- parser scope remains intentionally ahead of runtime scope: explicit join syntax now parses, but planner/executor still reject joined selects through the normal unsupported-query path
- full repo verification still passes after the explicit join-syntax milestone
- `Parser Modernization Slice 24` completed as the first join-runtime milestone
- planner now recognizes the initial supported join shape: a two-table inner equality join with a single explicit `JOIN ... ON ...` clause
- executor now supports a conservative nested-loop join path for that initial two-table equality shape
- join-time name resolution now works across both tables for projections, `WHERE`, and `ORDER BY`
- public query materialization now reports projected column names correctly for joined selects
- comma-style multi-table `FROM` remains unsupported at runtime; the active join runtime is intentionally limited to the explicit two-table inner-equality path
- full repo verification still passes after the first join-runtime milestone
- `Parser Modernization Slice 25` completed as an aggregate-execution milestone
- shared value expressions now distinguish scalar function calls from aggregate calls in the parser AST
- `SELECT` projection parsing now accepts `COUNT(expr)`, `MIN(expr)`, `MAX(expr)`, `AVG(expr)`, and `SUM(expr)` in addition to the existing `COUNT(*)` form
- executor now runs aggregate-only projection queries for both single-table selects and the active two-table inner-join path
- `COUNT(expr)` counts non-`NULL` evaluated values, while `MIN`, `MAX`, `AVG`, and `SUM` skip `NULL` inputs and return `NULL` when no non-`NULL` rows contribute
- aggregate evaluation currently supports text for `MIN`/`MAX` and numeric inputs for `AVG`/`SUM`
- mixed aggregate and non-aggregate projection queries remain intentionally unsupported without grouping semantics
- planner/index behavior remains conservative; this milestone activates aggregate execution without broadening index-planning rules
- full repo verification still passes after the aggregate-execution milestone
- `Parser Modernization Slice 26` completed as a write-expression milestone
- `INSERT ... VALUES (...)` now carries shared `ValueExpr` nodes in addition to the legacy flattened value list
- `UPDATE ... SET ...` assignments now carry shared `ValueExpr` nodes when the assignment is richer than a plain literal
- parser tokenization for write statements now scans `value-expr` slices directly from the lexer stream instead of assuming every write value is a single literal token
- executor now evaluates spec-aligned scalar value expressions for write statements, including parenthesized expressions and the current scalar-function subset
- `UPDATE` assignments now support row-aware expressions such as `UPPER(name)` and `ABS(score)` within the current single-row update model
- `INSERT` value expressions remain intentionally row-free: scalar functions over literals/placeholders are supported, but column-reference and aggregate expressions are rejected at execution time
- placeholder binding now walks write-statement expression trees, including placeholders nested inside scalar-function arguments
- legacy literal fields are still backfilled where possible so compatibility scaffolding remains intact while the expression path becomes active
- full repo verification still passes after the write-expression milestone

## Next Recommended Step

- future `SELECT` growth should keep using the resolver-based join path for richer expressions and additional join-adjacent features
- stay within the current language spec surface unless the spec is updated first
- the next high-value seam is broadening the current non-date-time expression surface in a spec-aligned way, such as arithmetic expression support on the shared value-expression path
- another good candidate is continued cleanup of legacy compatibility fields once the active expression path is well covered by tests
