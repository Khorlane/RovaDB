# RovaDB

A small, idiomatic embedded SQL database for Go.

RovaDB is a Go-first embedded relational database engine designed for clarity, portability, and long-term extensibility. It is intended to feel natural to Go developers, remain understandable to contributors, and grow without boxing itself into a dead-end architecture.

> **Status:** Early design / pre-release. The goal is to build a real, usable foundation first, then expand carefully.

## Product Boundary

### Intended Use

- Embedded database for small applications
- Local, single-process usage
- Simple CRUD over a small number of tables
- Deterministic behavior prioritized over feature breadth
- Suitable for tooling, prototypes, and lightweight persistence needs

### Non-Goals

- No JOIN support
- No multi-table queries
- No advanced query optimization
- No `GROUP BY` / `HAVING` / aggregates beyond `COUNT(*)`
- No subqueries
- No concurrent multi-writer support
- No distributed usage
- No large-scale performance tuning
- No full SQL compliance

### Guarantees

- Deterministic query behavior
- Deterministic error surface with stable error messages and types
- Crash-safe writes via the existing transaction and rollback-journal model
- Correctness across open, close, reopen, and recovery lifecycle boundaries
- Explicit corruption detection with no silent recovery
- Simple, predictable execution model

### Supported Features (Stage 8)

- `CREATE TABLE`
- `INSERT INTO ... VALUES`
- `SELECT` over a single table with projection, `WHERE`, `ORDER BY`, and `COUNT(*)`
- `UPDATE`
- `DELETE`
- `ALTER TABLE ... ADD COLUMN`
- Indexed equality query support when index metadata already exists

### Feature Freeze

Stage 8 is the feature-complete baseline for this intended use case. Future changes should prioritize correctness and clarity over expansion, and any new feature should justify crossing this boundary.

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

- literal selects such as `SELECT 1`, `SELECT 'hello'`, `SELECT TRUE`, `SELECT FALSE`, and simple integer arithmetic forms already supported by the engine
- column projection such as `SELECT id` and `SELECT id, name`
- single-table `FROM`
- `WHERE` with flat `AND` / `OR` evaluation from left to right
- comparison operators: `=`, `!=`, `<`, `<=`, `>`, `>=`
- `ORDER BY <column> [ASC|DESC]`
- `COUNT(*)`
- equality lookups may use an index when index metadata exists
- broader query optimization is not guaranteed; table scan remains the fallback outside the current indexed-equality shape

### Not supported

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

### BOOL semantics

- BOOL literals are unquoted `TRUE` and `FALSE`
- quoted `'true'` and `'false'` are `TEXT`, not `BOOL`
- `1` and `0` are `INT`, not `BOOL`
- BOOL columns accept only `TRUE`, `FALSE`, or `NULL`
- no implicit type coercion
- BOOL works in `CREATE TABLE`, strict `INSERT` / `UPDATE` validation, `SELECT` result values, and `WHERE` equality / inequality within the supported query subset

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
for _, sql := range []string{
	"INSERT INTO users VALUES (1, 'Alice', TRUE)",
	"INSERT INTO users VALUES (2, 'Bob', FALSE)",
} {
	if _, err := db.Exec(sql); err != nil {
		log.Fatal(err)
	}
}

rows, err := db.Query("SELECT id, name FROM users WHERE active = TRUE ORDER BY id")
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

var active any
row := db.QueryRow("SELECT active FROM users WHERE id = 2")
if err := row.Scan(&active); err != nil {
	log.Fatal(err)
}
fmt.Println(active)
```

See [examples/basic_usage/main.go](/c:/Projects/RovaDB/examples/basic_usage/main.go) for a complete open -> write -> close -> reopen -> query flow.

## Indexed Equality Note

RovaDB can execute indexed equality queries such as:

```sql
SELECT id FROM users WHERE name = 'alice'
```

Index-backed equality scans are supported when index metadata already exists, but there is not yet a public `CREATE INDEX` SQL statement in the user-facing API. The query shape above is supported; index definition remains an internal capability for now.

## Why RovaDB exists

RovaDB exists to explore a different point in the design space:

- embedded database
- SQL interface
- Go-native implementation
- readable internals
- extensible architecture
- no dependency on CGO

The project is **not** trying to beat mature databases on completeness or performance. The goal is to create a practical embedded SQL engine for Go that is small enough to understand, useful enough to adopt, and structured well enough to grow over time.

## Project goals

RovaDB is being designed to:

- feel natural to Go developers
- embed cleanly into Go applications
- provide a focused SQL feature set first
- keep the codebase understandable
- separate major engine layers cleanly
- allow future growth toward a broader SQL engine without forcing a rewrite

## Design principles

### 1. Go-first
RovaDB should look and feel like a Go project. The code, API, and documentation should serve Go developers first.

### 2. Small, but not boxed in
The first versions should stay intentionally small, while preserving architectural seams that allow the engine to grow later.

### 3. Clear layers
The engine should maintain clean boundaries between parsing, planning, execution, storage, and transactions.

### 4. Readability over cleverness
If contributors cannot understand the code in a reasonable amount of time, the project loses one of its main advantages.

### 5. Stable direction
RovaDB should grow through deliberate scope expansion, not feature sprawl.

## Intended audience

RovaDB is aimed at two groups:

### Adopters
Go developers who want an embedded SQL database with a straightforward mental model and a clean integration story.

### Contributors
Engineers who want to work on a real database engine in Go without needing to wade into an enormous, opaque codebase.

## Architectural direction

RovaDB is intended to grow around these major layers:

- **parser**
- **binder / catalog**
- **planner**
- **executor**
- **storage**
- **transaction / pager**
- **SQL function and operator registry**

The current design direction is:

```text
SQL -> AST -> Bound AST -> Logical Plan -> Physical Plan -> Executor
```

Even if the first versions are minimal, those boundaries matter. They reduce coupling and make it possible to add features later without tearing the engine apart.

## Storage direction

The project is being designed with long-term growth in mind.

That means avoiding shortcuts that would make future evolution painful, including:

- baking SQL behavior directly into storage code
- tying internal row representation to one fixed on-disk layout
- skipping transaction boundaries entirely
- letting one-off statement logic grow without common planning/execution paths

The storage layer is expected to evolve toward a page-based design with clear internal abstractions rather than a throwaway prototype format.

## What RovaDB is trying to become

RovaDB is not being designed as a toy. It is being designed as a real embedded relational engine with a deliberately narrow starting point.

The long-term ambition is to let RovaDB grow into a broader SQL engine over time, while keeping its Go-first identity and readable implementation.

## What success looks like

RovaDB will be successful if it becomes:

- useful in real Go projects
- understandable by motivated contributors
- stable enough to build on
- extensible without architectural regret

## Documentation philosophy

RovaDB documentation should be practical, layered, and contributor-friendly.

The project is expected to maintain documentation such as:

- `README.md`
- `docs/architecture.md`
- `docs/storage.md`
- `docs/query-engine.md`
- `docs/roadmap.md`
- small working examples

The README should stay durable: clear enough for newcomers, honest about current scope, and stable enough to remain useful as the project evolves.

## Contributing

Early contributors will help shape the project heavily. The project will prioritize:

- clear issue definitions
- straightforward build/test steps
- readable code review
- stable naming and layering
- small, composable changes

A formal `CONTRIBUTING.md` can be added once the repository is ready for outside participation.

## Naming

RovaDB takes its name from the “rova,” a historic fortified complex in Madagascar—built as a central, elevated stronghold. The idea reflects a database as a reliable, foundational place where data is securely held and organized.

## Roadmap direction

Near-term work is expected to focus on:

1. defining the V1 architecture clearly
2. locking the public API shape
3. implementing a minimal SQL path end to end
4. adding durable storage foundations
5. expanding features carefully only after the core is solid

## Project stance

RovaDB is being built with ambition, but also with restraint.

The project would rather be:
- small and solid
- clear and dependable
- understandable and extensible

than broad, rushed, and difficult to reason about.

---

RovaDB is intended to earn trust by being useful, readable, and well-structured.
