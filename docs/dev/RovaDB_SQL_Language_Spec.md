# RovaDB SQL Language Spec

This document defines the SQL subset RovaDB supports and the syntax and behavior boundaries for that subset.

It is intended to guide parser, planner, and executor work by making the supported language explicit before implementation.

## Purpose

- define exactly what SQL RovaDB will support
- define what SQL RovaDB will reject
- keep parser growth deliberate and testable
- provide a durable reference for future implementation work

## Scope

This document is for the supported SQL language surface of RovaDB. It is not an implementation design for the parser internals.

## Sections

### 1. Statement Inventory

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
- `COMMIT`
- `ROLLBACK`

### 2. Statement Syntax

**CREATE TABLE**
```
>>-CREATE TABLE--table-name--(table-element--+-------------------+--)--><
                                             '-,--table-element--'
table-element:
>>-column-definition----------------------><
  '-constraint-definition-----------------'
column-definition:
>>-column-name--type-name--><
```
constraint-definition:
>>-CONSTRAINT--constraint-name--+---------------------------------------------------------------+--><
                                '-PRIMARY KEY--(--column-list--)--USING INDEX--index-name-------'
                                '-FOREIGN KEY--(--column-list--)--REFERENCES--table-name--(--column-list--)--USING INDEX--index-name--ON DELETE--+-RESTRICT-+'
                                                                                                                                                   '-CASCADE--'
**CREATE INDEX**
```
>>-CREATE--+--------+--INDEX--index-name--ON--table-name--(--index-column--+------------------+--)--><
           '-UNIQUE-'                                                      '-,--index-column--'
index-column:
>>-column-name--+----------+--><
                '-ASC|DESC-'
```

**DROP TABLE**
```
>>-DROP TABLE--table-name--><
```
**DROP INDEX**
```
>>-DROP INDEX--index-name--><
```
**ALTER TABLE ... ADD COLUMN**
```
>>-ALTER TABLE--table-name--ADD COLUMN--column-definition--><
```
**ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY**
```
>>-ALTER TABLE--table-name--ADD CONSTRAINT--constraint-name--PRIMARY KEY--(--column-list--)--USING INDEX--index-name--><
```
**ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY**
```
>>-ALTER TABLE--table-name--ADD CONSTRAINT--constraint-name--FOREIGN KEY--(--column-list--)--REFERENCES--table-name--(--column-list--)--USING INDEX--index-name--ON DELETE--+-RESTRICT-+--><
                                                                                                                                                                              '-CASCADE--'
```
**ALTER TABLE ... DROP PRIMARY KEY**
```
>>-ALTER TABLE--table-name--DROP PRIMARY KEY--><
```
**ALTER TABLE ... DROP FOREIGN KEY**
```
>>-ALTER TABLE--table-name--DROP FOREIGN KEY--constraint-name--><
```
**INSERT INTO ... VALUES**
```
>>-INSERT INTO--table-name--+------------------+--VALUES--(--value-expr--+---------------+--)--><
                            '-(--column-list-)-'                         '-,--value-expr-'
column-list:
>>-column-name----------------------------------------------><
  |                        .-,-----------.                  |
  '-column-name--+-------+-'             '------------------'
                 '-,--column-name-'
```
If no column-list is provided, value-expr items map positionally to the table's column definitions in schema order. Value count and type/constraint compatibility must match the target columns.

**UPDATE**
```
>>-UPDATE--table-name--SET--assignment--+---------------+--+-------------+--><
                                        '-,--assignment-'  '-WHERE--expr-'
assignment:
>>-column-name--=--value-expr--><
```
**DELETE**
```
>>-DELETE FROM--table-name--+-------------+--><
                            '-WHERE--expr-'
```
**COMMIT**
```
>>-COMMIT--><
```
**ROLLBACK**
```
>>-ROLLBACK--><
```
**SELECT**
```
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

### 3. Statement Semantic Notes

**CREATE INDEX semantics**

- index names must be unique across the database
- `CREATE INDEX` fails if an equivalent index definition already exists on the same table
- equivalent index definitions are compared by:
  - target table
  - `UNIQUE` setting
  - ordered indexed column list
  - per-column `ASC` / `DESC` modifiers
- the same column must not appear more than once in a single index definition
- if omitted, `ASC` is assumed for an indexed column
- `ASC` / `DESC` are part of the index definition and are semantically significant
- column order is significant for multi-column indexes
- an index on `(a, b)` is different from an index on `(b, a)`
- `CREATE UNIQUE INDEX` enforces uniqueness across the full indexed key
- `CREATE UNIQUE INDEX` fails if duplicate indexed key values already exist in the target table
- for V1, `CREATE UNIQUE INDEX` also fails if any existing row has `NULL` in any indexed column

**DROP INDEX semantics**

- `DROP INDEX` identifies the target index by database-wide index name
- `DROP INDEX` fails if the named index does not exist
- `DROP INDEX` removes only the named index and does not remove table data
- `DROP INDEX` fails while the index is still required by a primary key or foreign key

**DROP TABLE semantics**

- `DROP TABLE` fails if the named table does not exist
- `DROP TABLE` removes the named table and all indexes defined on that table
- `DROP TABLE` removes both table schema metadata and table row data
- `DROP TABLE` tears down dependent foreign keys in surviving child tables rather than blocking

**ALTER TABLE ... ADD COLUMN semantics**

- `ALTER TABLE ... ADD COLUMN` appends the new column to the end of the table schema
- existing stored rows are not rewritten when the column is added
- existing rows read after the schema change yield `NULL` for the added column unless and until later writes populate it

**PRIMARY KEY semantics**

- primary keys are named only
- at most one primary key may exist per table
- primary keys may be single-column or composite
- primary-key columns are implicitly `NOT NULL`
- `USING INDEX <name>` is required
- the supporting index must be `UNIQUE` and match the primary-key column list exactly
- primary-key values cannot be modified
- `ALTER TABLE ... DROP PRIMARY KEY` removes dependent foreign keys and keeps indexes

**FOREIGN KEY semantics**

- foreign keys are named only
- foreign keys may be single-column or composite
- foreign keys reference the parent table primary key only
- `USING INDEX <name>` is required
- `ON DELETE RESTRICT` or `ON DELETE CASCADE` is required
- foreign-key columns may not be `NULL`
- foreign-key columns may be updated subject to final-state validation
- the child supporting index must use the foreign-key columns as a contiguous leftmost prefix
- `ALTER TABLE ... DROP FOREIGN KEY <name>` removes the constraint and keeps the supporting index

**Referential-integrity semantics**

- enforcement is immediate
- statement execution is atomic
- validation is against the final statement state
- `RESTRICT` and `CASCADE` deletes are supported
- self-reference is allowed subject to cascade-graph legality
- all-`CASCADE` cycles are rejected at DDL time
- multiple cascade paths are rejected at DDL time

### 4. Expression Inventory

Expressions are shared across projection, predicates, assignments, and value lists, but each context may apply additional restrictions.

**Core Expression Forms**

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

Aggregate function calls are context-restricted and are not valid in every expression position.

**Literal Values**

- integer literal
- string literal
- boolean literal
- real literal
- `NULL`

**Placeholder Values**

- `?`
- placeholders bind positionally from left to right
- placeholders are allowed only in value/expression positions

**Column References**

- unqualified column reference
- qualified column reference using `table-name.column-name`
- qualified column reference using `alias-name.column-name`

**Arithmetic Expressions**

- arithmetic expressions produce a value
- arithmetic expressions may appear in projection, predicates, assignments, and value lists where type-compatible
- exact supported operators will be defined explicitly

**Comparison Expressions**

- comparison expressions compare two value expressions
- comparison operators will be defined explicitly
- comparison expressions produce a boolean result

**Boolean Expressions**

- boolean expressions are built from comparison expressions and other boolean expressions
- `NOT`, `AND`, and `OR` are boolean operators
- precedence is `NOT`, then `AND`, then `OR`

**Function Calls**

- scalar function calls produce a value
- aggregate function calls produce an aggregate result
- supported function names are defined in `Function Inventory` and `Aggregate Inventory`

**Projection Context**

- `SELECT` items may be:
  - `*`
  - column references
  - expressions
  - scalar function calls
  - aggregate function calls

**Predicate Context**

- `WHERE` uses boolean expressions
- boolean expressions may contain:
  - comparison expressions
  - parenthesized boolean expressions
  - scalar function calls inside value expressions
- aggregate function calls are not allowed in `WHERE`

**Assignment Context**

- `UPDATE ... SET column = value-expr`
- assignment expressions must produce a value compatible with the target column

**Value List Context**

- `INSERT ... VALUES (...)`
- each item in a `VALUES` list must be a value expression
- aggregate function calls are not allowed in `VALUES` lists

### 5. Function Inventory

Supported scalar functions:

- `LOWER(text) -> text`
- `UPPER(text) -> text`
- `LENGTH(text) -> int`
- `ABS(int|real) -> same numeric domain as input`

Supported aggregate functions:

- `MIN(text|int|real) -> same domain as input`
- `MAX(text|int|real) -> same domain as input`
- `AVG(int|real) -> real`
- `SUM(int|real) -> real`
- `COUNT(*) -> int`
- `COUNT(expr) -> int`

Function calls may appear only in expression contexts that allow them.

### 6. Join Inventory

Supported join shape:

- inner joins
- equality join predicates
- join predicates compare value expressions, but current intended support is table/alias-qualified column reference to table/alias-qualified column reference
- table aliases are supported
- the initial supported query scope is up to two joined tables

Design intent:

- the language shape should not box future growth into two-table-only syntax
- execution strategy is not part of the language spec

Rejected join forms for now:

- outer joins
- non-equality join predicates
- natural joins
- `USING (...)`
- subquery joins
- joins over more than two tables

### 7. Aggregate Inventory

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

Initial aggregate query scope:

- aggregate support is intended for single-result aggregate queries
- `COUNT(*)` is supported explicitly
- `COUNT(expr)` is supported explicitly
- mixed aggregate and non-aggregate projection rules should be defined explicitly before implementation expands

### 8. Accepted Examples

Representative accepted examples:

- `CREATE TABLE users (id INT, name TEXT, active BOOL, score REAL)`
- `CREATE UNIQUE INDEX idx_users_name ON users (name ASC)`
- `CREATE INDEX idx_users_name_score ON users (name ASC, score DESC)`
- `DROP TABLE users`
- `DROP INDEX idx_users_name`
- `ALTER TABLE users ADD COLUMN created_at TEXT`
- `ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id) USING INDEX idx_users_pk`
- `ALTER TABLE users ADD CONSTRAINT fk_users_team FOREIGN KEY (team_id) REFERENCES teams (id) USING INDEX idx_users_team ON DELETE RESTRICT`
- `ALTER TABLE users DROP PRIMARY KEY`
- `ALTER TABLE users DROP FOREIGN KEY fk_users_team`
- `INSERT INTO users VALUES (1, 'Alice', TRUE, 3.14)`
- `INSERT INTO users (id, name) VALUES (?, ?)`
- `UPDATE users SET name = 'Bob' WHERE id = 1`
- `UPDATE users SET score = ABS(score) WHERE id = ?`
- `DELETE FROM users WHERE id = ?`
- `COMMIT`
- `ROLLBACK`
- `SELECT * FROM users`
- `SELECT id, name FROM users WHERE active = TRUE ORDER BY name ASC`
- `SELECT LOWER(name), LENGTH(name) FROM users WHERE id = 1`
- `SELECT id FROM users WHERE id = 1 OR id = 2 AND active = TRUE`
- `SELECT COUNT(*) FROM users WHERE active = TRUE`
- `SELECT AVG(score), SUM(score), MIN(score), MAX(score) FROM users`
- `SELECT u.name, d.name FROM users u JOIN departments d ON u.department_id = d.id`

### 9. Rejected Examples

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

### 10. Notes

Boolean expression precedence is `NOT`, then `AND`, then `OR`. Parenthesized boolean grouping is supported.
