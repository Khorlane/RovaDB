# RovaDB Workflows

This document defines short-trigger repeatable workflows that can be run on demand.

The goal is to let us invoke a known task with a short prompt, while keeping the task definitions easy to review, edit, add, or remove over time.

## Trigger Convention

Use a short prompt in this shape:

```text
workflow <name>
```

Examples:

- `workflow snapshot`
- `workflow archive_kgs`
- `workflow update_context`

## Workflow Format

Each workflow should define:

- `Name`
- `Trigger`
- `Mode`
- `Purpose`
- `Steps`
- `Outputs`
- `Notes`

### Field Meanings

- `Name`
  Human-readable workflow name.
- `Trigger`
  The exact short name used after `workflow`.
- `Mode`
  One of:
  - `safe routine`
  - `pause before commit/push`
  - `high impact`
- `Purpose`
  Short description of why the workflow exists.
- `Steps`
  The repeatable actions to perform.
- `Outputs`
  Files, branches, or other artifacts the workflow updates.
- `Notes`
  Clarifications, guardrails, or assumptions.

## Workflows

### Snapshot Repo History

- `Name`: Snapshot Repo History
- `Trigger`: `snapshot`
- `Mode`: `safe routine`
- `Purpose`: Capture a full repo-history snapshot for continuity/reference in the sister research folder.

Steps:

1. List all git commits in the repository.
2. Overwrite `C:\Projects\RovaDB Research\snapshot.txt` with that commit list.
3. List all git tags in the repository.
4. Append result to `C:\Projects\RovaDB Research\snapshot.txt`
5. Produce a concise project folder/file list excluding Git internals.
6. Append result to `C:\Projects\RovaDB Research\snapshot.txt`
7. Produce a concise inventory of function and method signatures for the whole repo, grouped by source file, with each source file listed once as a heading using its full absolute path.
8. Append result to `C:\Projects\RovaDB Research\snapshot.txt`
9. Report completion.

Outputs:

- `C:\Projects\RovaDB Research\snapshot.txt`

Notes:

- This workflow overwrites the target file at the start, then appends the later sections.
- The snapshot should include both:
  - full commit history
  - full tag list
- The snapshot should also include a concise project folder/file list excluding Git internals.
- The snapshot should also include a concise function/method signature inventory grouped by source file.
- This workflow defines the process only; it is not run automatically.

## Maintenance Rules

- Workflows may be added, edited, or removed as project needs change.
- Prefer stable short trigger names once a workflow starts being used regularly.
- Keep workflows concrete and operational rather than aspirational.
- If a workflow becomes ambiguous or too broad, split it into smaller workflows.
