# RovaDB SQL Language Spec

This document defines the supported SQL subset, type system, literal rules, and user-visible semantic boundaries for RovaDB.

It is the authoritative language and value-semantics reference for parser, planner, executor, public API behavior where SQL-visible types cross into Go, and future SQL-surface work. It defines what RovaDB accepts, what it rejects, and the deterministic behavior expected within the committed product boundary.

## Purpose

- define exactly what SQL RovaDB supports
- define exactly what SQL RovaDB rejects
- lock datatype, literal, nullability, default, and comparison semantics
- keep parser and executor growth deliberate and testable
- provide a durable reference that prevents semantic drift

## Scope

This document defines the supported SQL language surface and its user-visible semantics.

It does not define:
- parser implementation internals
- planner internals
- storage-page layouts
- package ownership boundaries

Those topics belong in architecture and storage-engine documents.

## 1. Statement Inventory

Supported SQL statements:

- `CREATE TABLE`
- `CREATE INDEX`
- `DROP TABLE`
- `DROP INDEX`
- `ALTER TABLE ... ADD COLUMN`
- `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY`
- `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY`
- `ALTER TABLE ... DROP PRIMARY KEY`
- `ALTER TABLE ... DROP FOREIGN KEY`
- `INSERT INTO ... VALUES`
- `SELECT`
- `UPDATE`
- `DELETE`

Parser-recognized but not executable as part of the SQL product surface:

- `COMMIT`
- `ROLLBACK`

Explicit transactions are supported through the Go API only. SQL `BEGIN`, `COMMIT`, and `ROLLBACK` are not part of the current executable SQL surface.

## 2. Statement Syntax

**CREATE TABLE**
```text
>>-CREATE TABLE--table-name--(table-element--+-------------------+--)--><
                                             '-,--table-element--'
table-element:
>>-column-definition----------------------><
  '-constraint-definition-----------------'
column-definition:
>>-column-name--type-name--><
```

Column definitions also support:

- no trailing clause
- `DEFAULT <literal>`
- `NOT NULL`
- `NOT NULL DEFAULT <literal>`

Constraint definition:
```text
>>-CONSTRAINT--constraint-name--+---------------------------------------------------------------+--><
                                '-PRIMARY KEY--(--column-list--)--USING INDEX--index-name-------'
                                '-FOREIGN KEY--(--column-list--)--REFERENCES--table-name--(--column-list--)--USING INDEX--index-name--ON DELETE--+-RESTRICT-+'
                                                                                                                                                   '-CASCADE--'
```

**CREATE INDEX**
```text
>>-CREATE--+--------+--INDEX--index-name--ON--table-name--(--index-column--+------------------+--)--><
           '-UNIQUE-'                                                      '-,--index-column--'
index-column:
>>-column-name--+----------+--><
                '-ASC|DESC-'
```

**DROP TABLE**
```text
>>-DROP TABLE--table-name--><
```

**DROP INDEX**
```text
>>-DROP INDEX--index-name--><
```

**ALTER TABLE ... ADD COLUMN**
```text
>>-ALTER TABLE--table-name--ADD COLUMN--column-definition--><
```

**ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY**
```text
>>-ALTER TABLE--table-name--ADD CONSTRAINT--constraint-name--PRIMARY KEY--(--column-list--)--USING INDEX--index-name--><
```

**ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY**
```text
>>-ALTER TABLE--table-name--ADD CONSTRAINT--constraint-name--FOREIGN KEY--(--column-list--)--REFERENCES--table-name--(--column-list--)--USING INDEX--index-name--ON DELETE--+-RESTRICT-+--><
                                                                                                                                                                              '-CASCADE--'
```

**ALTER TABLE ... DROP PRIMARY KEY**
```text
>>-ALTER TABLE--table-name--DROP PRIMARY KEY--><
```

**ALTER TABLE ... DROP FOREIGN KEY**
```text
>>-ALTER TABLE--table-name--DROP FOREIGN KEY--constraint-name--><
```

**INSERT INTO ... VALUES**
```text
>>-INSERT INTO--table-name--+------------------+--VALUES--(--value-expr--+---------------+--)--><
                            '-(--column-list-)-'                         '-,--value-expr-'
column-list:
>>-column-name----------------------------------------------><
  |                        .-,-----------.                  |
  '-column-name--+-------+-'             '------------------'
                 '-,--column-name-'
```

If no column list is provided, value-expr items map positionally to the table's column definitions in schema order. Value count and type and constraint compatibility must match the target columns.

**UPDATE**
```text
>>-UPDATE--table-name--SET--assignment--+---------------+--+-------------+--><
                                        '-,--assignment-'  '-WHERE--expr-'
assignment:
>>-column-name--=--value-expr--><
```

**DELETE**
```text
>>-DELETE FROM--table-name--+-------------+--><
                            '-WHERE--expr-'
```

**COMMIT**
```text
>>-COMMIT--><
```

**ROLLBACK**
```text
>>-ROLLBACK--><
```

**SELECT**
```text
>>-SELECT--select-list--FROM--from-clause--+-------------------+--+----------------------+--><
                                           '-WHERE--expr-------'  '-ORDER BY--order-list-'
select-list:
>>-*------------------------------------------------------><
  |                          .-,-------------.            |
  '-select-item--+---------+-'               '------------'
                 '-,--select-item-'
from-clause:
>>-table-ref----------------------------------------------><
  |                       .-,----------.                  |
  '-table-ref--+--------+-'            '------------------'
               '-,--table-ref-'
order-list:
>>-order-item----------------------------------------------><
  |                       .-,-----------.                 |
  '-order-item--+-------+-'             '-----------------'
                '-,--order-item-'
expr:
>>-boolean-term--+---------------------------+--><
                  '-boolean-operator--boolean-term-'
boolean-term:
>>-comparison-expr----------------------------><
  |                     .-------------------.  |
  '-(--expr--)+--------+                   +--)'
             '-NOT--'
comparison-expr:
>>-value-expr--comparison-operator--value-expr--><
boolean-operator:
>>-AND--><
  '-OR-'
```

## 3. Statement Semantics

### CREATE INDEX

- index names are unique across the database
- `CREATE INDEX` fails if an equivalent index definition already exists on the same table
- equivalent index definitions are compared by:
  - target table
  - `UNIQUE` setting
  - ordered indexed column list
  - per-column `ASC` or `DESC`
- the same column must not appear more than once in a single index definition
- omitted sort direction means `ASC`
- `ASC` and `DESC` are semantically significant
- column order is significant for multi-column indexes
- `CREATE UNIQUE INDEX` enforces uniqueness across the full indexed key
- `CREATE UNIQUE INDEX` fails if duplicate indexed key values already exist
- for the current product boundary, `CREATE UNIQUE INDEX` also fails if any existing row has `NULL` in any indexed column

### DROP INDEX

- `DROP INDEX` identifies the target index by database-wide index name
- `DROP INDEX` fails if the named index does not exist
- `DROP INDEX` removes only the named index and does not remove table data
- `DROP INDEX` fails while the index is still required by a primary key or foreign key

### DROP TABLE

- `DROP TABLE` fails if the named table does not exist
- `DROP TABLE` removes the named table and all indexes defined on that table
- `DROP TABLE` removes both table schema metadata and table row data
- `DROP TABLE` tears down dependent foreign keys in surviving child tables rather than blocking

### ALTER TABLE ... ADD COLUMN

- `ALTER TABLE ... ADD COLUMN` appends the new column to the end of the table schema
- column definitions use the same supported `NOT NULL` and literal `DEFAULT` forms as `CREATE TABLE`
- adding a nullable column without a default exposes `NULL` for pre-existing rows
- adding a nullable column with a literal default exposes that default for pre-existing rows
- adding a `NOT NULL` column without a default is allowed only when the target table is empty
- adding a `NOT NULL` column with a literal default is allowed and exposes that default for pre-existing rows
- future inserts use the updated column metadata, including default-on-omission behavior

### PRIMARY KEY

- primary keys are named only
- at most one primary key may exist per table
- primary keys may be single-column or composite
- primary-key columns are implicitly `NOT NULL`
- `USING INDEX <name>` is required
- the supporting index must be `UNIQUE` and match the primary-key column list exactly
- primary-key values cannot be modified with `UPDATE`
- `ALTER TABLE ... DROP PRIMARY KEY` removes dependent foreign keys and keeps the supporting indexes

### FOREIGN KEY

- foreign keys are named only
- foreign keys may be single-column or composite
- foreign keys reference the parent table primary key only
- `USING INDEX <name>` is required
- `ON DELETE RESTRICT` or `ON DELETE CASCADE` is required
- foreign-key columns may not be `NULL`
- foreign-key columns may be updated subject to final-state validation
- the child supporting index must use the foreign-key columns as a contiguous leftmost prefix
- `ALTER TABLE ... DROP FOREIGN KEY <name>` removes the constraint and keeps the supporting index

### Referential Integrity

- enforcement is immediate
- statement execution is atomic
- validation is against the final statement state
- `RESTRICT` and `CASCADE` deletes are supported
- self-reference is allowed subject to cascade-graph legality
- all-`CASCADE` cycles are rejected at DDL time
- multiple cascade paths are rejected at DDL time

## 4. Column Definition Semantics

Supported column forms:

- `<name> <type>`
- `<name> <type> DEFAULT <literal>`
- `<name> <type> NOT NULL`
- `<name> <type> NOT NULL DEFAULT <literal>`

No broader default syntax is supported.

### Literal DEFAULT Rules

Defaults are literal-only.

Allowed literal categories:

- integer literal
- string literal
- boolean literal
- real literal
- canonical date literal
- canonical time literal
- canonical timestamp literal
- `NULL`, but only where the column remains nullable

Datatype compatibility is checked against the target column's normal assignment rules.

Examples:

- `INT DEFAULT 1` is valid
- `REAL DEFAULT 1.25` is valid
- `TEXT DEFAULT 'ready'` is valid
- `BOOL DEFAULT TRUE` is valid
- `DATE DEFAULT '2026-04-10'` is valid
- `TIME DEFAULT '13:45:21'` is valid
- `TIMESTAMP DEFAULT '2026-04-10 13:45:21'` is valid
- `TEXT DEFAULT NULL` is valid if the column is nullable
- `INT DEFAULT 'abc'` is invalid
- `BOOL DEFAULT 1` is invalid
- `NOT NULL DEFAULT NULL` is invalid

### INSERT Default Semantics

`DEFAULT` applies only when an `INSERT` omits a value for the column.

Rules:

- omitted value uses the column default if one is defined
- omitted value without a default yields `NULL` only for nullable columns
- omitted value for a `NOT NULL` column without a default is an error
- explicit `NULL` for a `NOT NULL` column is an error
- explicit `NULL` for a nullable column remains allowed even if the column has a non-`NULL` default

### UPDATE Default Semantics

`UPDATE` does not inject defaults automatically.

Rules:

- explicit `NULL` assigned to a `NOT NULL` column is an error
- explicit `NULL` assigned to a nullable column is allowed
- no assignment leaves the existing stored value unchanged
- `DEFAULT` is not re-applied during `UPDATE` just because the column is omitted

## 5. Datatype Inventory

Supported schema datatypes:

- `SMALLINT`
- `INT`
- `BIGINT`
- `TEXT`
- `BOOL`
- `REAL`
- `DATE`
- `TIME`
- `TIMESTAMP`

`NULL` is a value state, not a standalone schema type.

### Temporal Type Contract

- `DATE`, `TIME`, and `TIMESTAMP` are distinct schema types
- `DATE` and `TIMESTAMP` materialize to Go `time.Time` in the public API
- `TIME` materializes to the public `rovadb.Time` value
- no implicit coercion exists between temporal families or between temporal and non-temporal types

### Temporal Write, Comparison, and Ordering Contract

- `DATE` columns accept only `DATE` or `NULL`
- `TIME` columns accept only `TIME` or `NULL`
- `TIMESTAMP` columns accept only `TIMESTAMP` or `NULL`
- temporal writes enforce exact family matching
- temporal predicates support `=`, `!=`, `<>`, `<`, `<=`, `>`, and `>=` only when both sides are the same temporal family
- temporal `ORDER BY` is supported within the current query subset

### Temporal Public Scan Contract

- `DATE` scans only into `*time.Time`
- `TIMESTAMP` scans only into `*time.Time`
- `TIME` scans only into `*rovadb.Time`

### Temporal Non-Goals

- fractional seconds
- alternate literal formats
- temporal arithmetic
- built-in temporal functions
- implicit casting and implicit coercion

## 6. Integer Semantics

RovaDB supports three distinct SQL integer types:

- `SMALLINT`
- `INT`
- `BIGINT`

These are distinct user-visible types. They are not aliases for one generic integer family.

### SQL-to-Go Mapping

The locked Go mappings are:

- `SMALLINT` <-> `int16`
- `INT` <-> `int32`
- `BIGINT` <-> `int64`

These mappings apply across binding, writes, typed result materialization, and `Scan`.

### Binding Contract

Placeholder binding for SQL integer values is exact-width:

- SQL integer binding accepts only Go `int16`, `int32`, and `int64`
- Go `int` is rejected
- other integer-like Go types are rejected

### Write Contract

Writes are exact by declared column type:

- `SMALLINT` columns accept only `int16`
- `INT` columns accept only `int32`
- `BIGINT` columns accept only `int64`

Allowed narrow exception:

- an untyped SQL integer literal may resolve to the target width if it fits
- if it does not fit, the write fails

Typed wrong-width integers are rejected even when the numeric magnitude would fit.

### Arithmetic Contract

Typed integer arithmetic preserves exact width:

- `SMALLINT +/- SMALLINT -> SMALLINT`
- `INT +/- INT -> INT`
- `BIGINT +/- BIGINT -> BIGINT`

Rules:

- overflow fails immediately
- mixed-width typed arithmetic is rejected
- typed integer plus untyped integer literal resolves to the typed width if it fits
- typed integer plus untyped integer literal fails if it does not fit

### Typed Result Materialization and Scan

Typed SQL integer results materialize to their exact Go widths:

- typed `SMALLINT` -> `int16`
- typed `INT` -> `int32`
- typed `BIGINT` -> `int64`

`Scan` is exact-width for typed integer results:

- `SMALLINT` scans only into `*int16`
- `INT` scans only into `*int32`
- `BIGINT` scans only into `*int64`

Go `int` is not part of the typed SQL integer contract.

### Untyped Integer Result Seams

RovaDB retains a narrow untyped integer-result path for expression-only or schema-less cases that do not resolve against a typed integer column.

Current examples include:

- `SELECT 1`
- `SELECT 1 + 2`
- `COUNT(*)`
- `COUNT(expr)`
- `LENGTH(text)`

These results materialize as `int64` and scan only into `*int64`.

## 7. TEXT Semantics

### Type Contract

- `TEXT` is the canonical string schema type
- string literals are written using single quotes
- quoted numeric-looking values remain `TEXT`, not numeric types
- `TEXT` columns accept `TEXT` or `NULL`
- there is no implicit coercion between `TEXT` and `INT`, `REAL`, or `BOOL`

### Comparison and Ordering Contract

- `TEXT` comparisons are case-insensitive
- comparisons use lowercase normalization
- no locale-aware collation is applied
- no accent-aware collation is applied
- behavior is deterministic across `WHERE` and `ORDER BY`

### Function Contract

Supported scalar text functions:

- `LOWER(text) -> text`
- `UPPER(text) -> text`
- `LENGTH(text) -> untyped integer result`

### Indexed TEXT Limit

For indexed `TEXT` values:

- indexed `TEXT` values are limited to `<= 512` bytes
- the limit is measured in bytes, not characters
- the limit is enforced on `INSERT`, `UPDATE`, `CREATE INDEX`, and `CREATE UNIQUE INDEX`

## 8. BOOL Semantics

### Type Contract

- `BOOL` is the canonical boolean schema type
- runtime Go representation is `bool`
- `BOOL` columns accept only `TRUE`, `FALSE`, or `NULL`

### Literal Classification

- unquoted `TRUE` and `FALSE` are `BOOL`
- quoted `'true'` and `'false'` are `TEXT`
- `1` and `0` are `INT`, not `BOOL`

### Enforcement Rules

- reject `INT` values such as `0` and `1` for `BOOL` columns
- reject `TEXT` values such as `'true'` and `'false'` for `BOOL` columns
- BOOL comparisons are BOOL-to-BOOL only
- there is no implicit coercion between `BOOL` and `INT` or `TEXT`

## 9. REAL Semantics

### Type Contract

- `REAL` is the canonical fractional numeric schema type
- runtime Go representation is `float64`
- `REAL` columns accept only `REAL` or `NULL`

### Literal Classification

- unquoted decimal literals are `REAL`
- unquoted whole numbers remain integer literals
- quoted numeric-looking values remain `TEXT`

### Enforcement Rules

- there is no implicit coercion from `INT`, `TEXT`, or `BOOL` into `REAL`
- `REAL` does not auto-convert to `INT`
- `INT` does not auto-convert to `REAL`

### Comparison Rules

- `REAL`-to-`REAL` comparisons support equality and ordering operators
- mixed `INT` versus `REAL` comparisons are strict mismatches and are rejected consistently with RovaDB's strict model

## 10. Literal Inventory

Supported literal forms:

- integer literal
- string literal
- boolean literal
- real literal
- canonical date literal
- canonical time literal
- canonical timestamp literal
- `NULL`

Classification rules:

- unquoted whole numbers are integer literals
- unquoted decimals are real literals
- quoted canonical `YYYY-MM-DD` payloads are `DATE`
- quoted canonical `HH:MM:SS` payloads are `TIME`
- quoted canonical `YYYY-MM-DD HH:MM:SS` payloads are `TIMESTAMP`
- other quoted text remains string literal `TEXT`
- unquoted `TRUE` and `FALSE` are boolean literals
- `NULL` is the null literal

## 11. Placeholder Semantics

- placeholders use `?`
- placeholders bind positionally from left to right
- placeholders are allowed only in value and expression positions
- placeholder count must match argument count exactly
- each call is one-shot; this is not a prepared statement system

Supported Go argument types:

- `int16`
- `int32`
- `int64`
- `string`
- `bool`
- `float64`
- `nil`

Unsupported argument types are rejected.

String arguments that exactly match the canonical temporal literal payloads bind as temporal values: `YYYY-MM-DD` -> `DATE`, `HH:MM:SS` -> `TIME`, and `YYYY-MM-DD HH:MM:SS` -> `TIMESTAMP`. Other strings bind as `TEXT`.

## 12. Expression Inventory

Expressions are shared across projection, predicates, assignments, and value lists, but each context may apply additional restrictions.

### Core Expression Forms

- literal value
- placeholder value
- column reference
- qualified column reference
- parenthesized expression
- scalar function call
- aggregate function call
- arithmetic expression
- comparison expression
- boolean expression

### Column References

- unqualified column reference
- qualified column reference using `table-name.column-name`
- qualified column reference using `alias-name.column-name`

### Arithmetic Expressions

- arithmetic expressions produce a value
- arithmetic expressions may appear in projection, predicates, assignments, and value lists where type-compatible
- currently supported arithmetic operators are `+` and `-`

### Comparison Expressions

Supported comparison operators:

- `=`
- `!=`
- `<>`
- `<`
- `<=`
- `>`
- `>=`

### Boolean Expressions

- boolean expressions are built from comparison expressions and other boolean expressions
- `NOT`, `AND`, and `OR` are boolean operators
- precedence is `NOT`, then `AND`, then `OR`
- parenthesized grouping is supported

## 13. Function Inventory

Supported scalar functions:

- `LOWER(text) -> text`
- `UPPER(text) -> text`
- `LENGTH(text) -> untyped integer result`
- `ABS(int|real) -> same numeric domain as input`

Supported aggregate functions:

- `MIN(text|int|real) -> same domain as input`
- `MAX(text|int|real) -> same domain as input`
- `AVG(int|real) -> real`
- `SUM(int|real) -> real`
- `COUNT(*) -> untyped integer result`
- `COUNT(expr) -> untyped integer result`

## 14. Join Inventory

Supported join shape:

- inner joins
- equality join predicates
- join predicates compare value expressions, but current intended support is table- or alias-qualified column reference to table- or alias-qualified column reference
- table aliases are supported
- current supported query scope is up to two joined tables

Rejected join forms:

- outer joins
- non-equality join predicates
- natural joins
- `USING (...)`
- subquery joins
- joins over more than two tables

## 15. Aggregate Inventory

Supported aggregate functions:

- `MIN`
- `MAX`
- `AVG`
- `SUM`
- `COUNT`

Aggregate usage rules:

- aggregate functions are allowed in `SELECT` projection
- aggregate functions are not allowed in `WHERE`
- aggregate functions are not allowed in `INSERT ... VALUES (...)`
- aggregate functions are not allowed in `UPDATE ... SET ...` assignments
- mixed aggregate and non-aggregate projection is not supported

## 16. SELECT Semantics

Supported SELECT features:

- literal selects such as `SELECT 1`, `SELECT 'hello'`, `SELECT TRUE`, and arithmetic expressions with `+` and `-`
- projection expressions, column projection, qualified column references, and `AS` aliases
- `ORDER BY` resolution against select-item aliases at the same `SELECT` level
- single-table `FROM`
- two-table inner equi-joins via explicit `INNER JOIN ... ON ...` and comma join plus `WHERE`
- `WHERE` with `NOT`, precedence, and parenthesized grouping
- same-family temporal `WHERE` predicates for `DATE`, `TIME`, and `TIMESTAMP` using `=`, `!=`, `<>`, `<`, `<=`, `>`, and `>=`
- scalar functions: `LOWER`, `UPPER`, `LENGTH`, and `ABS`
- aggregates: `COUNT(*)`, `COUNT(expr)`, `MIN`, `MAX`, `AVG`, and `SUM`
- temporal `ORDER BY` within the supported query subset

### Narrow Index-Only Surface

True index-only means the correct result is produced entirely from:

- index contents
- index-structure metadata

without fetching base table rows.

Supported today only for:

- eligible plain single-table `COUNT(*)`
- eligible plain single-table single-column direct indexed projection such as `SELECT id FROM users`
- eligible qualified single-table single-column direct indexed projection such as `SELECT users.id FROM users`

Unsupported or uncertain shapes fall back to existing table, index, or join paths.

## 17. Not Supported

Not supported today:

- `GROUP BY`
- `HAVING`
- subqueries
- joins over more than two tables
- non-`INNER` joins
- non-equality join predicates
- qualified star projection such as `a.*` or `b.*`
- mixed aggregate and non-aggregate projections
- alias resolution in `WHERE`
- SQL `BEGIN`
- executable SQL `COMMIT`
- executable SQL `ROLLBACK`
- fractional-second temporal literals
- alternate temporal literal formats
- temporal arithmetic
- built-in temporal functions
- implicit casting or implicit coercion across temporal families
- schema changes other than `ALTER TABLE ... ADD COLUMN`, `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY`, `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY`, `ALTER TABLE ... DROP PRIMARY KEY`, and `ALTER TABLE ... DROP FOREIGN KEY`

## 18. Accepted Examples

Representative accepted examples:

- `CREATE TABLE users (id INT NOT NULL, name TEXT DEFAULT 'ready', active BOOL NOT NULL DEFAULT TRUE, score REAL DEFAULT 0.0)`
- `CREATE TABLE metrics (small SMALLINT, normal INT, big BIGINT)`
- `CREATE TABLE events (event_date DATE, event_time TIME, recorded_at TIMESTAMP)`
- `CREATE UNIQUE INDEX idx_users_name ON users (name ASC)`
- `CREATE INDEX idx_users_name_score ON users (name ASC, score DESC)`
- `DROP TABLE users`
- `DROP INDEX idx_users_name`
- `ALTER TABLE users ADD COLUMN created_at TEXT`
- `ALTER TABLE users ADD COLUMN active BOOL NOT NULL DEFAULT TRUE`
- `ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id) USING INDEX idx_users_pk`
- `ALTER TABLE users ADD CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT`
- `ALTER TABLE users DROP PRIMARY KEY`
- `ALTER TABLE users DROP FOREIGN KEY fk_users_team`
- `INSERT INTO users VALUES (1, 'Alice', TRUE, 3.14)`
- `INSERT INTO events VALUES ('2026-04-10', '13:45:21', '2026-04-10 13:45:21')`
- `INSERT INTO users (id, name) VALUES (?, ?)`
- `UPDATE users SET name = 'Bob' WHERE id = 1`
- `UPDATE users SET score = ABS(score) WHERE id = ?`
- `DELETE FROM users WHERE id = ?`
- `SELECT * FROM users`
- `SELECT id, name FROM users WHERE active = TRUE ORDER BY name ASC`
- `SELECT LOWER(name), LENGTH(name) FROM users WHERE id = 1`
- `SELECT id FROM users WHERE id = 1 OR id = 2 AND active = TRUE`
- `SELECT COUNT(*) FROM users WHERE active = TRUE`
- `SELECT AVG(score), SUM(score), MIN(score), MAX(score) FROM users`
- `SELECT u.name, d.name FROM users u JOIN departments d ON u.department_id = d.id`
- `SELECT recorded_at FROM events WHERE recorded_at >= '2026-04-10 00:00:00' ORDER BY recorded_at`

## 19. Rejected Examples

Representative rejected examples:

- `CREATE TABLE users ()`
- `CREATE TABLE users (id INT, id TEXT)`
- `CREATE INDEX idx_users_name ON users`
- `CREATE INDEX idx_users_name ON users ()`
- `CREATE INDEX idx_users_name_twice ON users (name, name)`
- `CREATE INDEX idx_users_name ON missing_users (name)`
- `DROP TABLE`
- `DROP INDEX`
- `ALTER TABLE users DROP COLUMN created_at`
- `CREATE TABLE users (id INT, PRIMARY KEY (id) USING INDEX idx_users_pk)`
- `CREATE TABLE users (team_id INT, CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team)`
- `ALTER TABLE users ADD CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE SET NULL`
- `ALTER TABLE users DROP FOREIGN KEY`
- `INSERT INTO users VALUES (1)`
- `INSERT INTO users VALUES (COUNT(*))`
- `UPDATE users SET score = AVG(score)`
- `DELETE FROM users USING archive_users`
- `SELECT id FROM users GROUP BY id`
- `SELECT id FROM users HAVING id > 1`
- `SELECT * FROM users u LEFT JOIN departments d ON u.department_id = d.id`
- `SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id`
- `SELECT * FROM users WHERE SUM(score) > 10`
- `SELECT * FROM users WHERE id IN (SELECT id FROM archived_users)`
- `SELECT * FROM users NATURAL JOIN departments`
- `SELECT * FROM users JOIN departments USING (department_id)`
- `INSERT INTO events VALUES ('2026/04/10', '13:45:21', '2026-04-10 13:45:21')`
- `SELECT * FROM events WHERE event_date = '2026-04-10 13:45:21'`
- binding Go `int` into a typed SQL integer placeholder
- scanning typed `INT` into `*int64`
- inserting `1` into a `BOOL` column
- comparing `REAL` to `INT` as if implicit coercion existed

## 20. Notes

- boolean expression precedence is `NOT`, then `AND`, then `OR`
- parenthesized boolean grouping is supported
- typed integer semantics are exact-width
- text comparison semantics are case-insensitive and normalization-based, not locale-aware
- defaults are literal-only and apply on `INSERT` omission only
