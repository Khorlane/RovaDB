# RovaDB

A small, idiomatic embedded SQL database for Go.

RovaDB is a Go-first embedded relational database engine designed for clarity, portability, and long-term extensibility. It is intended to feel natural to Go developers, remain understandable to contributors, and grow without boxing itself into a dead-end architecture.

> **Status:** Pre-release. The current `v0.10.x` line establishes a practical, durable baseline with a small public API and focused SQL support.

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
- `INSERT INTO ... VALUES`
- `SELECT` over a single table with projection, `WHERE`, `ORDER BY`, and `COUNT(*)`
- `UPDATE`
- `DELETE`
- `ALTER TABLE ... ADD COLUMN`
- positional args via `?` in `Exec`, `Query`, and `QueryRow`
- strict value support for `INT`, `TEXT`, `BOOL`, `REAL`, and `NULL`

### Scope Discipline

The current `v0.10.x` line is the practical baseline for this intended use case. Future changes should prioritize correctness, determinism, durability, and API stability over feature expansion, and any new feature should justify crossing this boundary.

## Supported SQL

Only the following SQL forms are supported today.

### Statements

- `CREATE TABLE`
- `ALTER TABLE <table> ADD COLUMN <column> <type>`
- `INSERT INTO ... VALUES (...)`
- `SELECT ...`
- `UPDATE ... SET ...`
- `DELETE FROM ...`

### SELECT support

- literal selects such as `SELECT 1`, `SELECT 'hello'`, `SELECT TRUE`, `SELECT FALSE`, and the current simple integer arithmetic forms
- column projection such as `SELECT id` and `SELECT id, name`
- single-table `FROM`
- `WHERE` with flat `AND` / `OR` evaluation from left to right
- comparison operators: `=`, `!=`, `<`, `<=`, `>`, `>=`
- `ORDER BY <column> [ASC|DESC]`
- `COUNT(*)`

### Not supported today

- `JOIN`
- `GROUP BY`
- `HAVING`
- subqueries
- multi-table queries
- aggregate forms other than `COUNT(*)`
- expression precedence in `WHERE` beyond the current flat left-to-right chain
- public `CREATE INDEX` SQL
- schema changes other than `ALTER TABLE ... ADD COLUMN`

### Supported schema types

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
for _, user := range []struct {
	id     int
	name   string
	active bool
}{
	{id: 1, name: "Alice", active: true},
	{id: 2, name: "Bob", active: false},
} {
	if _, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", user.id, user.name, user.active); err != nil {
		log.Fatal(err)
	}
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

var name string
var active bool
if err := db.QueryRow("SELECT name, active FROM users WHERE id = ?", 2).Scan(&name, &active); err != nil {
	log.Fatal(err)
}
fmt.Println(name, active)
```

See [examples/basic_usage/main.go](/c:/Projects/RovaDB/examples/basic_usage/main.go) for a complete open -> write -> close -> reopen -> query flow.

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
- `docs/dev/REAL_design.md`
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
