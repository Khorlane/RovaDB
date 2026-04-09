# RovaDB Workflows

This document defines short-trigger repeatable workflows that can be run on demand.

The goal is to let us invoke a known task with a short prompt, while keeping the task definitions easy to review, edit, add, or remove over time.

----------------------------------------------------------------------
TRIGGER CONVENTION
----------------------------------------------------------------------

Use a short prompt in this shape:

workflow <name>

Examples:
- workflow snapshot
- workflow archive_kgs
- workflow update_context

----------------------------------------------------------------------
WORKFLOW FORMAT
----------------------------------------------------------------------

Each workflow defines:

- Name
- Trigger
- Mode
- Purpose
- Steps
- Outputs
- Notes

Mode values:
- safe routine
- pause before commit/push
- high impact

----------------------------------------------------------------------
WORKFLOWS
----------------------------------------------------------------------

======================================================================
SNAPSHOT REPO (AUTHORITATIVE HISTORY + ARCHITECTURE SNAPSHOT)
======================================================================

Name: Snapshot Repo (History + Architecture)
Trigger: snapshot
Mode: safe routine
Purpose:
Produce a single authoritative snapshot that captures BOTH:
1) repository history
2) current architectural reality

This snapshot is used for:
- architecture review
- continuity across chats
- regression detection
- design validation

----------------------------------------------------------------------
STEPS
----------------------------------------------------------------------

0) Initialize output

- Overwrite:
  C:\Projects\RovaDB Research\snapshot.txt

All following sections append to this file.

----------------------------------------------------------------------
SECTION 1 — GIT HISTORY
----------------------------------------------------------------------

1. List full git commit history:
   git log --decorate --date=iso --pretty=format:"%h %ad %d %s"

2. Append to snapshot.

----------------------------------------------------------------------
SECTION 2 — GIT TAGS
----------------------------------------------------------------------

3. List all tags:
   git tag -l

4. Append to snapshot.

----------------------------------------------------------------------
SECTION 3 — REPO STRUCTURE
----------------------------------------------------------------------

5. Produce concise directory tree (depth ≤ 3):
   - exclude .git
   - exclude build artifacts

6. Append to snapshot.

----------------------------------------------------------------------
SECTION 4 — PACKAGE DEPENDENCY MAP (CRITICAL)
----------------------------------------------------------------------

7. For each package, list DIRECT imports only:

- root package
- internal/parser
- internal/planner
- internal/executor
- internal/storage
- internal/txn
- internal/bufferpool

Format:

[package]
imports:
  - pkgA
  - pkgB

Rules:
- include ONLY internal project packages (ignore stdlib unless useful)
- no recursion
- keep deterministic order

8. Append to snapshot.

----------------------------------------------------------------------
SECTION 5 — EXPORTED API SURFACE
----------------------------------------------------------------------

9. For each package, list EXPORTED symbols only:

- root package (DB, Rows, Result, Tx, etc.)
- each internal package

Format:

[package]
exports:
  - TypeName
  - FuncName(...)
  - MethodName(...)

Goal:
- verify root remains thin
- detect leakage from internal packages

10. Append to snapshot.

----------------------------------------------------------------------
SECTION 6 — KEY BOUNDARY TYPES
----------------------------------------------------------------------

11. Extract and list core cross-layer types:

- planner:
  - plan structs (SelectPlan, IndexScan, etc.)

- executor:
  - row/value representations
  - execution structs

- storage:
  - value representation
  - page structs
  - row encoding types
  - index key types

Format:
[group]
type definitions (names + short shape)

Goal:
- detect type leakage across layers
- verify boundary ownership

12. Append to snapshot.

----------------------------------------------------------------------
SECTION 7 — FUNCTION / METHOD SIGNATURE INVENTORY
----------------------------------------------------------------------

13. Produce full signature inventory grouped by file:

- each file listed once (absolute path)
- list functions + methods only

14. Append to snapshot.

----------------------------------------------------------------------
SECTION 8 — CRITICAL CALL PATHS (FLOW)
----------------------------------------------------------------------

15. Produce concise call chains (function → function):

A) Query path
   DB.Query → ... → storage

B) Mutation path
   INSERT / UPDATE / DELETE flow

C) Index-only path
   planner → execution → storage boundary helpers

Rules:
- keep each path short (5–12 steps max)
- no prose, just flow

16. Append to snapshot.

----------------------------------------------------------------------
SECTION 9 — ARCHITECTURAL GUARDRAILS
----------------------------------------------------------------------

17. Summarize what arch_guardrails_test.go enforces:

- forbidden import directions
- storage isolation guarantees
- root boundary restrictions
- index-only boundary constraints

18. List any obvious gaps (if detectable).

19. Append to snapshot.

----------------------------------------------------------------------
SECTION 10 — LARGE / CENTRAL FILES
----------------------------------------------------------------------

20. Identify top ~10 files by size (lines of code).

21. Format:

file path — line count

Goal:
- identify orchestration hotspots
- detect boundary pressure zones

22. Append to snapshot.

----------------------------------------------------------------------
SECTION 11 — SUMMARY SIGNAL (SHORT)
----------------------------------------------------------------------

23. Produce a very short summary:

- boundary direction status
- root thickness status
- main remaining pressure points (if obvious)

Limit: 5–10 lines max

24. Append to snapshot.

----------------------------------------------------------------------
FINAL STEP
----------------------------------------------------------------------

25. Report completion.

----------------------------------------------------------------------
OUTPUTS
----------------------------------------------------------------------

- C:\Projects\RovaDB Research\snapshot.txt

----------------------------------------------------------------------
NOTES
----------------------------------------------------------------------

- This is the SINGLE authoritative snapshot.
- Do not split into multiple files.
- Keep output structured and labeled by section.
- Keep output concise and signal-focused.
- Avoid duplication across sections.
- Do not include Git internals in structure.
- Do not include raw code bodies (signatures/types only).
- Deterministic output is required.

======================================================================
LOAD FILE INTO CONTEXT
======================================================================

Name: Load File Into Context
Trigger: load <file>
Mode: safe routine
Purpose:
Read a specified file into chat context without echoing or analyzing it.

Steps:

1. Read the specified file into context.
2. Do NOT print, quote, summarize, or transform it.
3. Respond with exactly:

OK

Outputs:
- None

Notes:
- Context ingestion only.
- File path must be explicitly provided.
- This workflow intentionally uses a parameterized trigger rather than the general `workflow <name>` shape.

----------------------------------------------------------------------
MAINTENANCE RULES
----------------------------------------------------------------------

- Workflows may be added, edited, or removed as needed.
- Keep triggers stable once adopted.
- Prefer precise, operational steps.
- Split workflows if they become too broad.
