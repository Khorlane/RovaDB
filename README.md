# RovaDB

A small, idiomatic embedded SQL database for Go.

RovaDB is a Go-first embedded relational database engine designed for clarity, portability, and long-term extensibility. It is intended to feel natural to Go developers, remain understandable to contributors, and grow without boxing itself into a dead-end architecture.

> **Status:** Pre-release. The current `v0.37.x` line reflects a practical, durable baseline with a small public API, focused SQL support, explicit public transaction control through the Go API, narrow index-only access for eligible query shapes, and the completed Physical Storage Layer milestone.

## In Progress

Current engineering focus is the post-Physical-Storage-Layer path beyond the current `v0.37.x` baseline.

The direction under active exploration is:
- strengthening durability and recovery beyond the current rollback-journal model
- expanding on-disk indexing and transaction foundations to support a more capable long-term engine
- continuing to harden and extend the new `TableHeader` / `SpaceMap` / owned-`Data` page storage model

The completed storage milestone is:
- `v0.37.0-physical-storage-layer`

The next named milestone is:
- `v0.38.0-physical-storage-polish`

That milestone is now implemented in the engine:
- tables have authoritative `TableHeader` roots
- `SpaceMap` pages enumerate owned `Data` pages
- normal writes land on owned `Data` pages
- normal reads and scans enumerate rows from owned `Data` pages
- row-growth updates relocate through delete plus reinsert when they no longer fit in place
- open and reopen strictly validate physical ownership invariants with no backward-compatibility path for pre-physstore table storage

This work is about deepening storage and transaction internals while preserving RovaDB's existing goals around clarity, determinism, and a small stable public API.

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
WHERE b.total_amt > 7
ORDER BY a.name DESC, b.total_amt;
```

Or copy/paste this one-line form directly into the CLI:

```text
SELECT a.cust_nbr AS customer_number, a.name, a.city, b.order_nbr, b.item, b.total_amt FROM customers a INNER JOIN orders b ON a.cust_nbr = b.cust_nbr WHERE b.total_amt > 7 ORDER BY a.name DESC, b.total_amt;
```

Equivalent comma-join form:

```text
SELECT a.cust_nbr, a.name, b.order_nbr, b.total_amt FROM customers a, orders b WHERE a.cust_nbr = b.cust_nbr AND b.total_amt > 7 ORDER BY a.name DESC, b.total_amt;
```

You should see:

```text
rovadb> SELECT a.cust_nbr AS customer_number, a.name, a.city, b.order_nbr, b.item, b.total_amt FROM customers a INNER JOIN orders b ON a.cust_nbr = b.cust_nbr WHERE b.total_amt > 7 ORDER BY a.name DESC, b.total_amt;
  customer_number | a.name       | a.city   | b.order_nbr | b.item   | b.total_amt
  ----------------|--------------|----------|-------------|----------|------------
  2               | Brian Lewis  | Chicago  | 3           | Stapler  | 15
  1               | Alice Carter | Boston   | 2           | Pens     | 8.25
  1               | Alice Carter | Boston   | 1           | Paper    | 12.5
```

If you want to embed RovaDB in a Go program instead, see `examples/basic_usage/main.go`.

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
- `INSERT INTO ... VALUES`
- `SELECT` with projection expressions, `WHERE`, `ORDER BY`, joins, and the current supported aggregate set
- `UPDATE`
- `DELETE`
- `ALTER TABLE ... ADD COLUMN`
- positional args via `?` in `Exec`, `Query`, and `QueryRow`
- explicit public transactions via `Begin`, `Tx.Exec`, `Tx.Query`, `Tx.QueryRow`, `Commit`, and `Rollback`
- public catalog introspection via `ListTables` and `GetTableSchema`
- shared product version via `Version()`
- strict value support for `INT`, `TEXT`, `BOOL`, `REAL`, and `NULL`
- narrow index-only access for eligible plain single-table `COUNT(*)` and single-column indexed projection queries

### Scope Discipline

The current `v0.34.x` line is the practical baseline for this intended use case. Future changes should prioritize correctness, determinism, durability, and API stability over feature expansion, and any new feature should justify crossing this boundary.

## Supported SQL

Only the following SQL forms are supported today.

### Statements

- `CREATE TABLE`
- `CREATE INDEX ...`
- `DROP INDEX <name>`
- `DROP TABLE <name>`
- `ALTER TABLE <table> ADD COLUMN <column> <type>`
- `INSERT INTO ... VALUES (...)`
- `SELECT ...`
- `UPDATE ... SET ...`
- `DELETE FROM ...`

Parser-recognized but not executable today:

- `BEGIN`
- `COMMIT`
- `ROLLBACK`

Explicit transactions are supported through the Go API only. SQL `BEGIN` / `COMMIT` / `ROLLBACK` statements are not part of the current public product surface.

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
- schema changes other than `ALTER TABLE ... ADD COLUMN`

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

- `INT`
- `TEXT`
- `BOOL`
- `REAL`

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

- `int`
- `string`
- `bool`
- `float64`
- `nil`

Unsupported argument types fail with an error.

## Catalog Introspection

RovaDB exposes a small public catalog API for listing tables and reading table schema metadata:

- `ListTables()`
- `GetTableSchema(table)`

`ListTables()` returns table names and column definitions for all tables in the open database. `GetTableSchema(table)` returns the same metadata for one table.

## Canonical Example

```go
db, err := rovadb.Open("app.db")
if err != nil {
	log.Fatal(err)
}
defer db.Close()

if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, active BOOL)"); err != nil {
	log.Fatal(err)
}

tx, err := db.Begin()
if err != nil {
	log.Fatal(err)
}
if _, err := tx.Exec("INSERT INTO users VALUES (?, ?, ?)", 1, "Alice", true); err != nil {
	_ = tx.Rollback()
	log.Fatal(err)
}
if _, err := tx.Exec("INSERT INTO users VALUES (?, ?, ?)", 2, "Bob", false); err != nil {
	_ = tx.Rollback()
	log.Fatal(err)
}

var activeCount int
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
	var id int
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

Explicit transactions are opt-in through the Go API. Plain `DB.Exec`, `DB.Query`, and `DB.QueryRow` keep their existing autocommit behavior when you do not call `Begin()`.

See `examples/basic_usage/main.go` for a complete open -> begin -> commit -> close -> reopen -> query flow.

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

RovaDB grows around these major layers:

- **parser**
- **binder / catalog**
- **planner**
- **executor**
- **storage**
- **transaction / pager**
- **SQL function and operator registry**

The intended execution path is:

```text
SQL -> AST -> Bound AST -> Logical Plan -> Physical Plan -> Executor
```

Even in a deliberately small engine, those boundaries matter. They reduce coupling and make it possible to add features later without tearing the engine apart.

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
