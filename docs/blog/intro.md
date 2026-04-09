# RovaDB: A Pure Go Embedded SQL Database

RovaDB is a pure Go embedded relational database for local, single-process applications.

It is designed to be practical, readable, and easy to embed. The goal is to provide a useful SQL database for Go developers without requiring CGO, without a massive surface area, and without sacrificing clarity.

---

## What RovaDB Is

RovaDB is:

- a pure Go embedded SQL database
- designed for local, single-process applications
- built around a small, stable API
- focused on clarity, determinism, and correctness

It supports a practical subset of SQL, including:

- `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`
- the current supported join, ordering, and aggregate features
- explicit transactions via the Go API
- strict typing: `INT`, `TEXT`, `BOOL`, `REAL`, and `NULL`

The engine is structured around clear layers:

```
SQL → AST → Plan → Execution → Storage
```

---

## What It Is Not

RovaDB is not trying to be:

- a replacement for mature database servers
- a distributed system
- a fully SQL-compliant engine
- a performance-focused competitor to SQLite

Instead, it is intentionally scoped as:

> a practical embedded relational database for Go with a clear boundary and understandable internals

---

## Why RovaDB Exists

There are already strong options in the Go ecosystem:

- SQLite (typically via CGO)
- key-value stores such as Bolt, Badger, and BuntDB

RovaDB sits in a different space:

- pure Go (no CGO dependency)
- relational model with SQL
- small, understandable codebase
- explicit product boundaries

If you want SQL without introducing a C dependency, and you value clarity over breadth, this is the niche RovaDB targets.

---

## Example

You can install and try RovaDB immediately:

```bash
go install github.com/Khorlane/RovaDB/cmd/rovadb@latest
rovadb
sample demo.db
```

Then run a query against the sample `customers` and `orders` tables:

```sql
SELECT a.cust_nbr, a.name, b.order_nbr, b.total_amt
FROM customers a
INNER JOIN orders b ON a.cust_nbr = b.cust_nbr
WHERE b.total_amt > 7
ORDER BY a.name DESC, b.total_amt;
```

---

## Current Status

RovaDB is **pre-release**, but already at a practical baseline:

- small, stable public API
- focused SQL support
- explicit transaction model through the Go API
- completed physical storage foundation and polish milestones

Current work is focused on:

- durability and recovery improvements
- expanding indexing and transaction foundations
- continued hardening of the storage layer

---

## Who It’s For

RovaDB is a good fit if you are:

- building a local Go application
- writing tools or utilities
- prototyping with structured data
- looking for a Go-native embedded SQL database

It is also intended for contributors who want to work on a real database engine without navigating a large or opaque codebase.

---

## Links

- GitHub: https://github.com/Khorlane/RovaDB
- Website: https://www.rovadb.org

---

## Closing

RovaDB is intentionally small, focused, and evolving carefully.

If you are looking for a Go-native embedded SQL database with a clear direction and readable internals, it may be a good fit.
