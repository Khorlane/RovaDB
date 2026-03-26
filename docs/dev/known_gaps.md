# RovaDB Known Gaps

This document tracks concrete identified gaps in current RovaDB behavior.

It is intended to be a lightweight developer backlog for things that should be fixed or clarified, based on observed behavior in the engine or CLI.

Status values:

- `pending`
- `in progress`
- `done`

## Summary

- [kg002] Engine `pending` Review text comparison / collation behavior
- [kg011] CLI `pending` Improve CLI result formatting for wider query output
- [kg013] CLI `pending` Distinguish obvious non-SQL input from SQL passthrough

## Engine

### `pending` Review text comparison / collation behavior [kg002]

Observed gap:

```sql
SELECT * FROM users WHERE name < 'bab'
```

Observed behavior in testing:

- rows such as `Charlie` may compare as less than `'bab'`

Expected direction:

- document and/or refine string comparison semantics so text predicates match the intended SQL behavior and user expectations

## CLI

### `pending` Improve CLI result formatting for wider query output [kg011]

Observed gap:

- current query-result printing is intentionally minimal and does not try to align or format wider result sets

Expected direction:

- improve output readability for broader result shapes while keeping the CLI lightweight

### `pending` Distinguish obvious non-SQL input from SQL passthrough [kg013]

Observed gap:

```text
rovadb> look
  exec error: parse: unsupported query form
```

Expected direction:

- obvious non-SQL input should be treated as an unknown CLI command rather than being passed through to SQL execution
- the CLI should respond with a clearer message, such as:
  - `unknown command: look`
  - `type help for commands`
