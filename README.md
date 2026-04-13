# RovaDB [www.rovadb.org](https://www.rovadb.org)

A pure Go embedded SQL database for local applications.

RovaDB is a small relational database engine for Go that aims to be practical, readable, and easy to embed. It is built for single-process applications that want SQL, predictable behavior, and a Go-native implementation without CGO.

## Why RovaDB

- embedded relational database for Go
- pure Go implementation
- no CGO dependency
- small, stable public API
- readable engine internals
- practical SQL support for local apps and tools

## Good Fit For

- local Go applications that need embedded relational storage
- tools, utilities, and prototypes that want SQL instead of ad hoc file formats
- developers who want a Go-native embedded database with understandable internals
- contributors who want to work on a real database engine without a massive codebase

## Not Aimed At

- client/server deployments
- distributed systems
- full SQL compatibility
- advanced query optimization workloads
- replacing mature database servers on breadth or raw performance

## Why RovaDB Instead of SQLite or Key-Value Stores?

Choose RovaDB when you want an embedded relational database that feels native in Go and you care about understandable internals, explicit scope, and a small public API.

Compared with SQLite, RovaDB is not trying to match the full breadth, ecosystem maturity, or performance profile of a long-established engine. Its appeal is different: pure Go implementation, no CGO dependency, a smaller surface area, and a codebase that is easier to study and evolve in Go-first projects.

Compared with key-value stores and simple embedded persistence libraries, RovaDB gives you a relational model, SQL queries, joins, ordering, aggregates, catalog visibility, and a more familiar mental model for structured application data.

> **Status:** Pre-release. RovaDB already provides a practical baseline with a small public API, focused SQL support, explicit transaction control through the Go API, and completed physical storage foundation/polish milestones.

## Quick Start

If you already have Go installed, you can try RovaDB without cloning the repo:

```powershell
go install github.com/Khorlane/RovaDB/cmd/rovadb@latest
rovadb
```

Then create and open the sample database:

```text
rovadb> sample demo.db
  created sample database demo.db
  sample tables: customers, orders
```

Run this query:

```sql
SELECT a.cust_nbr AS customer_number, a.name, a.city, b.order_nbr, b.item, b.total_amt
FROM customers a
INNER JOIN orders b ON a.cust_nbr = b.cust_nbr
WHERE b.total_amt > 7.0
ORDER BY a.name DESC, b.total_amt;
```

Or copy/paste this one-line form directly into the CLI:

```text
SELECT a.cust_nbr AS customer_number, a.name, a.city, b.order_nbr, b.item, b.total_amt FROM customers a INNER JOIN orders b ON a.cust_nbr = b.cust_nbr WHERE b.total_amt > 7.0 ORDER BY a.name DESC, b.total_amt;
```

Equivalent comma-join form:

```text
SELECT a.cust_nbr, a.name, b.order_nbr, b.total_amt FROM customers a, orders b WHERE a.cust_nbr = b.cust_nbr AND b.total_amt > 7.0 ORDER BY a.name DESC, b.total_amt;
```

You should see:

```text
rovadb> SELECT a.cust_nbr AS customer_number, a.name, a.city, b.order_nbr, b.item, b.total_amt FROM customers a INNER JOIN orders b ON a.cust_nbr = b.cust_nbr WHERE b.total_amt > 7.0 ORDER BY a.name DESC, b.total_amt;
  customer_number | a.name       | a.city   | b.order_nbr | b.item   | b.total_amt
  ----------------|--------------|----------|-------------|----------|------------
  2               | Brian Lewis  | Chicago  | 3           | Stapler  | 15
  1               | Alice Carter | Boston   | 2           | Pens     | 8.25
  1               | Alice Carter | Boston   | 1           | Paper    | 12.5
```

If you want to embed RovaDB in a Go program instead, see `examples/basic_usage/main.go`.

## Current State

Current engineering focus is the post-physical-storage-polish path beyond the current storage baseline.

The direction under active exploration is:

- strengthening durability and recovery beyond the current rollback-journal model
- expanding on-disk indexing and transaction foundations to support a more capable long-term engine
- continuing to harden and extend the new `TableHeader` / `SpaceMap` / owned-`Data` page storage model

The completed storage milestones are:

- `v0.37.0-physical-storage-layer`
- `v0.38.0-physical-storage-polish`

The current engine storage truth is:

- tables have authoritative `TableHeader` roots
- `SpaceMap` pages enumerate owned `Data` pages
- normal writes land on owned `Data` pages
- normal reads and scans enumerate rows from owned `Data` pages
- row-growth updates relocate through delete plus reinsert when they no longer fit in place
- open and reopen strictly validate physical ownership invariants with no backward-compatibility path for pre-physstore table storage
- diagnostics and consistency reporting expose per-table physical ownership metadata and inventory truth

This work is about deepening storage and transaction internals while preserving RovaDB's existing goals around clarity, determinism, and a small stable public API.

## Architectural Boundaries

RovaDB follows a locked layered pipeline: parser -> planner -> execution -> storage, with the root package owning only the public API surface. Each layer has strict ownership, dependencies flow one way, storage-owned value and index details stay behind explicit boundaries, and architectural guardrail tests enforce the dependency direction.

See `docs/dev/ARCHITECTURAL_BOUNDARIES.md` for the authoritative boundary contract.

## Product Boundary

### Intended Use

- Embedded database for small applications
- Local, single-process usage
- Simple relational storage for embedded Go applications
- Useful for tools, prototypes, and lightweight local persistence

### Current Limits

- No advanced query optimization
- No full SQL compliance

### Guarantees

- Deterministic query behavior
- Deterministic error surface with stable error messages and types
- Crash-safe writes via the existing transaction and rollback-journal model
- Correctness across open, close, reopen, and recovery lifecycle boundaries
- Explicit corruption detection with no silent recovery
- Simple, predictable execution model

### Supported Features

- `CREATE TABLE`
- `CREATE INDEX` / `CREATE UNIQUE INDEX`
- `DROP INDEX`
- `DROP TABLE`
- `ALTER TABLE ... ADD COLUMN`
- `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY`
- `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY`
- `ALTER TABLE ... DROP PRIMARY KEY`
- `ALTER TABLE ... DROP FOREIGN KEY`
- named `PRIMARY KEY` constraints with explicit supporting indexes
- named `FOREIGN KEY` constraints with immediate referential integrity enforcement
- `INSERT INTO ... VALUES`
- `SELECT` with projection expressions, `WHERE`, `ORDER BY`, joins, and the current supported aggregate set
- `UPDATE`
- `DELETE`
- positional args via `?` in `Exec`, `Query`, and `QueryRow`
- explicit public transactions via `Begin`, `Tx.Exec`, `Tx.Query`, `Tx.QueryRow`, `Commit`, and `Rollback`
- public catalog introspection via `ListTables` and `GetTableSchema`
- shared product version via `Version()`
- strict value support for `SMALLINT`, `INT`, `BIGINT`, `TEXT`, `BOOL`, `REAL`, and `NULL`
- narrow index-only access for eligible plain single-table `COUNT(*)` and single-column indexed projection queries

### Scope Discipline

The current repo baseline is the practical target for this intended use case. Future changes should prioritize correctness, determinism, durability, and API stability over feature expansion, and any new feature should justify crossing this boundary.

## Supported SQL

Only the following SQL forms are supported today.

### Statements

- `CREATE TABLE`
- `CREATE INDEX ...`
- `DROP INDEX <name>`
- `DROP TABLE <name>`
- `ALTER TABLE <table> ADD COLUMN <column-definition>`
- `ALTER TABLE <table> ADD CONSTRAINT <name> PRIMARY KEY (...) USING INDEX <index>`
- `ALTER TABLE <table> ADD CONSTRAINT <name> FOREIGN KEY (...) REFERENCES <parent> (...) USING INDEX <index> ON DELETE RESTRICT|CASCADE`
- `ALTER TABLE <table> DROP PRIMARY KEY`
- `ALTER TABLE <table> DROP FOREIGN KEY <name>`
- `INSERT INTO ... VALUES (...)`
- `SELECT ...`
- `UPDATE ... SET ...`
- `DELETE FROM ...`

Parser-recognized but not executable today:

- `BEGIN`
- `COMMIT`
- `ROLLBACK`

Explicit transactions are supported through the Go API only. SQL `BEGIN` / `COMMIT` / `ROLLBACK` statements are not part of the current public product surface.

### Primary and foreign key support

Supported `PRIMARY KEY` contract:

- keys are named only
- at most one primary key may exist per table
- primary keys may be single-column or composite
- primary-key columns are implicitly `NOT NULL`
- `USING INDEX <name>` is required
- the supporting index must be `UNIQUE` and match the primary-key column list exactly
- primary-key values cannot be modified with `UPDATE`
- `ALTER TABLE ... DROP PRIMARY KEY` removes dependent foreign keys and keeps the underlying indexes

Supported `FOREIGN KEY` contract:

- keys are named only
- tables may define zero or more foreign keys
- foreign keys reference the parent table primary key only
- foreign keys may be single-column or composite
- `USING INDEX <name>` is required
- `ON DELETE RESTRICT` or `ON DELETE CASCADE` is required
- child foreign-key columns may not be `NULL`
- foreign-key columns may be updated, but the final row state must remain valid
- the child supporting index must use the foreign-key columns as a contiguous leftmost prefix
- `ALTER TABLE ... DROP FOREIGN KEY <name>` removes the constraint and keeps the supporting index

Referential-integrity behavior:

- enforcement is immediate
- statement execution is atomic
- validation is against the final statement state
- both `RESTRICT` and `CASCADE` deletes are supported
- self-references are allowed when the cascade graph is legal
- all-`CASCADE` cycles are rejected at DDL time
- multiple cascade paths are rejected at DDL time

Destructive DDL behavior:

- `DROP PRIMARY KEY` and `DROP TABLE` tear down dependent foreign keys instead of blocking
- `DROP INDEX` is the dependency-blocking exception and fails while an index is still required by a primary key or foreign key

### SELECT support

- literal selects such as `SELECT 1`, `SELECT 'hello'`, `SELECT TRUE`, `SELECT FALSE`, and arithmetic expressions with `+` and `-`
- projection expressions, column projection, qualified column references, and `AS` aliases
- `ORDER BY` resolution against select-item aliases at the same `SELECT` level
- single-table `FROM`
- two-table inner equi-joins via explicit `INNER JOIN ... ON ...` and comma join + `WHERE`
- `WHERE` with `NOT`, precedence, and parenthesized grouping
- comparison operators: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`
- `ORDER BY` with one or more items
- scalar functions: `LOWER`, `UPPER`, `LENGTH`, and `ABS`
- aggregates: `COUNT(*)`, `COUNT(expr)`, `MIN`, `MAX`, `AVG`, and `SUM`

### Narrow index-only support

- true index-only means the result is satisfied from index contents plus index-structure metadata without fetching base table rows
- supported today only for eligible plain single-table `COUNT(*)`
- supported today only for eligible plain single-table single-column direct indexed projection such as `SELECT id FROM users`
- qualified single-table direct indexed projection such as `SELECT users.id FROM users` is also supported when eligible
- unsupported or uncertain shapes fall back to the existing table/index/join paths
- current fallback examples include aliased projection, `ORDER BY` on projection, `WHERE`-bearing projection, multi-column projection, non-column expressions, non-indexed projection, and joins

### Not supported today

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
- public `COMMIT` / `ROLLBACK` SQL
- schema changes other than `ALTER TABLE ... ADD COLUMN`, `ALTER TABLE ... ADD|DROP PRIMARY KEY`, and `ALTER TABLE ... ADD|DROP FOREIGN KEY`

### Public API

- `Open(path string) (*DB, error)`
- `(*DB).Close() error`
- `(*DB).Exec(query string, args ...any) (Result, error)`
- `(*DB).Query(query string, args ...any) (*Rows, error)`
- `(*DB).QueryRow(query string, args ...any) *Row`
- `(*DB).Begin() (*Tx, error)`
- `(*DB).ListTables() ([]TableInfo, error)`
- `(*DB).GetTableSchema(table string) (TableInfo, error)`
- `(*Tx).Exec(query string, args ...any) (Result, error)`
- `(*Tx).Query(query string, args ...any) (*Rows, error)`
- `(*Tx).QueryRow(query string, args ...any) *Row`
- `(*Tx).Commit() error`
- `(*Tx).Rollback() error`
- `Version() string`

### Supported data types

- `SMALLINT`
- `INT`
- `BIGINT`
- `TEXT`
- `BOOL`
- `REAL`

### Column nullability and literal defaults

- supported column forms are `<name> <type>`, `<name> <type> DEFAULT <literal>`, `<name> <type> NOT NULL`, and `<name> <type> NOT NULL DEFAULT <literal>`
- `DEFAULT` is literal-only for this milestone and applies only when an `INSERT` omits the column
- explicit `NULL` is rejected for `NOT NULL` columns on both `INSERT` and `UPDATE`
- `ALTER TABLE ... ADD COLUMN` uses the same supported column forms
- adding `NOT NULL` without a default is rejected for non-empty tables
- adding a defaulted column exposes that default for existing rows and for future inserts, including after reopen

Examples:

- `CREATE TABLE users (id INT NOT NULL, name TEXT DEFAULT 'ready', active BOOL NOT NULL DEFAULT TRUE, score REAL DEFAULT 0.0)`
- `INSERT INTO users (id) VALUES (1)` stores `name='ready'`, `active=TRUE`, and `score=0.0`
- `INSERT INTO users VALUES (2, 'sam', NULL, 1.25)` fails because `active` is `NOT NULL`
- `ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'new'` exposes `'new'` for existing rows and later omitted inserts
- `ALTER TABLE users ADD COLUMN active BOOL NOT NULL DEFAULT TRUE` backfills existing rows logically and keeps future omitted inserts valid

### BOOL semantics

- BOOL literals are unquoted `TRUE` and `FALSE`
- quoted `'true'` and `'false'` are `TEXT`, not `BOOL`
- `1` and `0` are `INT`, not `BOOL`
- BOOL columns accept only `TRUE`, `FALSE`, or `NULL`
- no implicit type coercion
- BOOL works in `CREATE TABLE`, strict `INSERT` / `UPDATE` validation, `SELECT` result values, and `WHERE` equality / inequality within the supported query subset

### REAL semantics

- REAL values are exposed to Go as `float64`
- decimal literals such as `3.14` and `-2.5` are `REAL`
- unquoted whole numbers such as `1` remain `INT`
- quoted numeric-looking values such as `'3.14'` remain `TEXT`
- REAL columns accept only `REAL` or `NULL`
- there is no implicit coercion between `INT` and `REAL`
- REAL-to-REAL `WHERE` comparisons support `=`, `!=`, `<`, `<=`, `>`, and `>=`
- mixed `INT` / `REAL`, `TEXT` / `REAL`, and `BOOL` / `REAL` comparisons remain strict type mismatches

### Text comparison semantics

- TEXT comparisons are case-insensitive
- comparisons are performed using lowercase normalization
- no locale-aware or accent-aware collation is applied
- behavior is deterministic and consistent across `WHERE` and `ORDER BY`

### Indexed TEXT limit

- indexed `TEXT` values are limited to `<= 512` bytes
- the limit is measured in bytes, not characters
- the limit is enforced on `INSERT`, `UPDATE`, and `CREATE INDEX` / `CREATE UNIQUE INDEX`
- oversized indexed `TEXT` values fail with:
  - `execution: indexed TEXT column value exceeds 512-byte limit`

## Positional Arguments

The public API supports one-shot positional argument binding on:

- `Exec(query string, args ...any)`
- `Query(query string, args ...any)`
- `QueryRow(query string, args ...any)`

Use `?` placeholders only. Binding is positional, left-to-right, and the arg count must match exactly. Placeholders are allowed only in value positions, and binding happens after parse but before planning and execution.

This is not a prepared statement system. Each call parses, binds, plans, and executes once.

Supported Go argument types are:

- `int16`
- `int32`
- `int64`
- `string`
- `bool`
- `float64`
- `nil`

Unsupported argument types fail with an error.

## Catalog Introspection

RovaDB exposes a small public catalog API for listing tables and reading table schema metadata:

- `ListTables()`
- `GetTableSchema(table)`

`ListTables()` returns table names and column definitions for all tables in the open database. `GetTableSchema(table)` returns the same metadata for one table. Column definitions include the declared type plus persisted `NOT NULL` and literal `DEFAULT` metadata when present.

## Canonical Example

```go
db, err := rovadb.Open("app.db")
if err != nil {
	log.Fatal(err)
}
defer db.Close()

if _, err := db.Exec("CREATE TABLE users (id INT NOT NULL, name TEXT DEFAULT 'ready', active BOOL NOT NULL DEFAULT TRUE, score REAL DEFAULT 0.0)"); err != nil {
	log.Fatal(err)
}

tx, err := db.Begin()
if err != nil {
	log.Fatal(err)
}
if _, err := tx.Exec("INSERT INTO users VALUES (?, ?, ?, ?)", int32(1), "Alice", false, 7.5); err != nil {
	_ = tx.Rollback()
	log.Fatal(err)
}
if _, err := tx.Exec("INSERT INTO users (id) VALUES (?)", int32(2)); err != nil {
	_ = tx.Rollback()
	log.Fatal(err)
}

var activeCount int64
if err := tx.QueryRow("SELECT COUNT(*) FROM users WHERE active = ?", true).Scan(&activeCount); err != nil {
	_ = tx.Rollback()
	log.Fatal(err)
}
fmt.Println("active users in tx:", activeCount)

if err := tx.Commit(); err != nil {
	log.Fatal(err)
}

rows, err := db.Query("SELECT id, name FROM users WHERE active = ? ORDER BY id", true)
if err != nil {
	log.Fatal(err)
}
defer rows.Close()

fmt.Println(rows.Columns())
for rows.Next() {
	var id int32
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		log.Fatal(err)
	}
	fmt.Println(id, name)
}
if err := rows.Err(); err != nil {
	log.Fatal(err)
}
```

This stores `Alice` exactly as provided, while the second row uses the schema defaults because the `INSERT` omits `name`, `active`, and `score`.

For typed integer columns, public writes and `Scan` destinations must match the declared SQL width exactly: `SMALLINT` <-> `int16`, `INT` <-> `int32`, and `BIGINT` <-> `int64`. There is no generic Go `int` interchange for these typed integer columns.

Explicit transactions are opt-in through the Go API. Plain `DB.Exec`, `DB.Query`, and `DB.QueryRow` keep their existing autocommit behavior when you do not call `Begin()`.

See `examples/basic_usage/main.go` for a complete open -> begin -> commit -> close -> reopen -> query flow.

## Primary/Foreign Key Example

For a compact example of the supported PK/FK surface, including named constraints, explicit supporting indexes, and `ON DELETE CASCADE`, see `examples/primary_foreign_keys/main.go`.

## Development Note

This project is developed using AI-assisted tooling, with all design, architecture, and validation decisions owned and reviewed by the author.

## Why RovaDB exists

RovaDB exists to provide a pure Go embedded relational database with practical SQL support.

- embedded database
- SQL interface
- Go-native implementation
- readable internals
- extensible architecture
- no dependency on CGO

The project is not trying to outcompete mature databases on breadth or raw performance. The goal is to offer a practical embedded SQL engine for Go that is straightforward to understand, realistic to adopt, and structured to grow carefully over time.

## Project goals

RovaDB is being built to:

- feel natural to Go developers
- embed cleanly into Go applications
- provide a focused SQL feature set first
- keep the codebase understandable
- separate major engine layers cleanly
- allow future growth toward a broader SQL engine

## Design principles

### 1. Go-first

RovaDB should look and feel like a Go project. The code, API, and documentation should serve Go developers directly.

### 2. Small, but not boxed in

The first versions should stay intentionally small while preserving the seams needed for later growth.

### 3. Clear layers

The engine should maintain clean boundaries between parsing, planning, execution, storage, and transactions.

### 4. Readability over cleverness

If contributors cannot understand the code in a reasonable amount of time, the project gives up one of its main advantages.

### 5. Stable direction

RovaDB should grow through deliberate scope expansion, not feature sprawl.

## Intended audience

RovaDB is aimed at two groups:

### Adopters

Go developers who want an embedded SQL database with a straightforward mental model and a clean integration story.

### Contributors

Engineers who want to work on a real database engine in Go without needing to wade into an enormous, opaque codebase.

## Architectural direction

RovaDB grows around these locked major layers:

- **root API**
- **parser**
- **planner**
- **execution**
- **storage**

The intended engine path is:

```text
SQL -> parser -> planner -> execution -> storage
```

`txn` and `bufferpool` support transaction and durable-page orchestration around that path, but they do not redefine layer ownership. The root package stays thin and public-facing, while storage continues to own durable metadata, value encoding, and index/page details behind narrow contracts.

## Storage direction

The storage layer is meant to support long-term growth without turning into a throwaway prototype. That means avoiding shortcuts that would make future evolution painful, including:

- baking SQL behavior directly into storage code
- tying internal row representation to one fixed on-disk layout
- skipping transaction boundaries entirely
- letting one-off statement logic grow without common planning/execution paths

The direction is a page-based storage engine with clear internal abstractions and explicit transaction boundaries.

## Long-Term Direction

RovaDB is intended to be a real embedded relational engine with a deliberately narrow starting point. The long-term ambition is to broaden capability over time while keeping its Go-first identity and readable implementation.

## Documentation

Current documentation in the repository includes:

- `README.md`
- `docs/dev/BOOL_design.md`
- `docs/dev/CREATE_INDEX_design.md`
- `docs/dev/INTEGER_design.md`
- `docs/dev/known_gaps.md`
- `docs/dev/REAL_design.md`
- `docs/dev/RovaDB_SQL_Language_Spec.md`
- `docs/dev/SCHEMA_LIFECYCLE_design.md`
- `docs/dev/road_to_v1.md`
- `docs/dev/workflows.md`
- `examples/basic_usage/main.go`

## Blog

- [Introducing RovaDB](docs/blog/intro.md)

## Contributing

Contributions are welcome, but they should align with RovaDB's established scope and design direction.

The project prioritizes:

- clear issue definitions
- straightforward build and test steps
- readable code review
- stable naming and layering
- small, composable changes

A formal `CONTRIBUTING.md` can be added once the repository is ready for outside participation.

## Name Origin

RovaDB takes its name from the “rova,” a historic fortified complex in Madagascar—built as a central, elevated stronghold. The idea reflects a database as a reliable, foundational place where data is securely held and organized.
