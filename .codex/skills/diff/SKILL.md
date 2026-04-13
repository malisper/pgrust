---
name: diff
description: Explain the differences from a regression test run for a specific file. Use when the user provides a regression test name, a `.diff` file, or actual vs expected output from `scripts/run_regression.sh` and wants a concise diagnosis of what changed and which code area is most likely responsible.
---

# Regression Diff Analysis

Use this skill when the user wants help understanding a single regression failure rather than rerunning the whole suite.

## Inputs

Accept any of:

- a regression test name like `select` or `json`
- a diff file under `/tmp/pgrust_regress/diff/`
- actual output under `/tmp/pgrust_regress/output/`
- pasted unified diff or expected/actual snippets

If the user names only the test, default to:

- SQL: `/Users/malisper/workspace/work/postgres-rewrite/postgres/src/test/regress/sql/<name>.sql`
- expected: `/Users/malisper/workspace/work/postgres-rewrite/postgres/src/test/regress/expected/<name>.out`
- actual: `/tmp/pgrust_regress/output/<name>.out`
- diff: `/tmp/pgrust_regress/diff/<name>.diff`

## Workflow

1. Load the SQL file, expected output, actual output, and diff for the named test if they exist.
2. Identify the first real mismatch, not just the first hunk header.
3. Map that mismatch back to the SQL statement that produced it.
4. Explain the difference in PostgreSQL terms:
   - wrong rows
   - wrong order
   - wrong type coercion
   - wrong formatting or display
   - wrong error text or SQLSTATE
   - missing unsupported feature
5. Give the narrowest likely code area in pgrust.

## Heuristics

- If the echoed SQL matches but row contents differ, suspect binder, executor, storage, casts, or output formatting.
- If row order differs with no `ORDER BY`, call out that the test may rely on planner or heap order and say whether the change looks semantically safe or not.
- If the output changes around headers, `(N rows)`, whitespace-insensitive formatting, or error caret text, suspect protocol formatting in `src/backend/libpq` or `src/backend/tcop`.
- If the failure starts after DDL or setup statements, inspect earlier setup output before blaming the visible query.
- If the mismatch is around unsupported syntax or type names, compare against the upstream PostgreSQL source tree in `../postgres` and point to the missing parser/analyzer/executor slice.

## Output Format

Use short sections:

- `Summary`
- `First mismatch`
- `Likely cause`
- `Next files to inspect`

Keep the explanation concrete. Quote only the smallest expected/actual fragment needed to show the difference.

## Good file targets

- Parser and binding: `src/backend/parser/analyze/*`, `src/backend/parser/gram.*`
- Runtime semantics: `src/backend/executor/*`
- SQL-visible formatting and errors: `src/backend/tcop/postgres.rs`, `src/backend/libpq/*`
- Upstream reference: `../postgres/src/backend`, `../postgres/src/test/regress`
