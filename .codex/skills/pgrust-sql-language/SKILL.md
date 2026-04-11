---
name: pgrust-sql-language
description: Add or change SQL syntax and semantics in pgrust. Use when changing grammar, AST, binder/planner behavior, statement support, SQL-visible errors, or regression behavior for SELECT/VALUES/DML/DDL features.
---

# pgrust SQL Language Changes

Use this when a change touches the SQL pipeline rather than just executor math or storage internals.

## Workflow

1. Find the narrowest layer that is actually missing behavior.
- Syntax only: `src/backend/parser/gram.pest`, `src/backend/parser/gram.rs`, `src/include/nodes/parsenodes.rs`
- Binding/planning: `src/backend/parser/analyze/*`
- Runtime semantics: `src/backend/executor/*`
- SQL-visible errors or protocol output: `src/backend/tcop/postgres.rs`, `src/backend/libpq/pqformat.rs`

2. Preserve the layer split.
- Parser produces `parsenodes`
- Analyzer produces `plannodes`
- Executor runs plans
- Do not hide language changes in `tcop/postgres.rs` unless the work is explicitly a temporary rewrite/shim and the user asked for that tradeoff

3. Add tests in the same slice.
- Parser test for the new surface syntax
- Planner/binder test if name resolution, type inference, or plan shape changes
- Executor test if runtime behavior changes
- Regression rerun for the affected file when SQL behavior is user-visible

4. Commit in slices.
- Syntax/AST
- planner/binder
- executor/protocol
- cleanup/removal of temporary rewrites

## Usual Touch Points

### Grammar and AST
- `src/backend/parser/gram.pest`
- `src/backend/parser/gram.rs`
- `src/include/nodes/parsenodes.rs`

Add new syntax here first. Keep raw parse structures small and dumb.

### Binder and Planner
- `src/backend/parser/analyze/mod.rs`
- `src/backend/parser/analyze/scope.rs`
- `src/backend/parser/analyze/expr.rs`
- `src/backend/parser/analyze/infer.rs`
- `src/backend/parser/analyze/functions.rs`
- `src/backend/parser/analyze/agg*.rs`

Common patterns:
- thread new statement-local environments through planner entrypoints instead of using globals
- resolve names before catalog fallbacks when local SQL objects should shadow tables
- reuse existing plan nodes when possible before inventing a new one

### Executor
- `src/backend/executor/driver.rs`
- `src/backend/executor/startup.rs`
- `src/backend/executor/nodes.rs`
- `src/backend/executor/exec_expr.rs`
- focused helpers like `expr_ops.rs`, `expr_casts.rs`, `expr_string.rs`, `expr_bool.rs`, `value_io.rs`

Put type-specific or feature-specific behavior in the focused helper, not back into facade files.

### Protocol and SQL-visible Errors
- `src/backend/tcop/postgres.rs`
- `src/backend/libpq/pqformat.rs`

Touch these only when PostgreSQL-visible message text, SQLSTATE, caret position, row description, or command tags change.

## Typical Patterns

### New query syntax with existing semantics
- Parse new syntax into an existing AST form if possible
- Bind to an existing plan shape
- Prefer lowering over adding a new executor node

Examples:
- `POSITION(a IN b)` lowered to a normal builtin call
- `IS TRUE/FALSE/UNKNOWN` lowered to existing distinct/null forms

### New statement form
- Add a new `Statement` variant in `parsenodes`
- Add parser entrypoint
- Add planner/binder entrypoint in `analyze/mod.rs`
- Add executor routing in `executor/driver.rs`
- Update any exhaustive matches in session/database/repl paths

### Statement-local environments
- CTEs, aliases, grouped scopes, outer scopes, and temp masking belong in binder/planner state
- Thread them explicitly through recursive binding paths
- Do not special-case them in the protocol layer unless it is an intentional temporary shim

## Temporary Rewrite Rule

If you must add a rewrite/shim:
- keep it tightly scoped
- document it in `deferred/`
- add a real planner/binder path later
- remove the shim in its own commit once the real path is green

## Validation

Default:
- `cargo check`
- targeted `cargo test --lib ...`

When SQL behavior changes:
- rerun the specific regression file with `.codex/skills/pgrust-regression/scripts/run_regression.sh`

Before finishing:
- check for exhaustive matches in:
  - `src/backend/executor/driver.rs`
  - `src/pgrust/session.rs`
  - `src/pgrust/database.rs`
  - `src/bin/query_repl.rs`

## Common Failure Modes

- Parser accepts syntax but AST forgot a field
- Binder resolves against catalog before local scope/CTE/alias
- Subquery/inference path missed new environment threading
- Row names are right in scope but wrong in `Plan::column_names()`
- Regression harness passes because of an old tcop rewrite still masking missing planner support
