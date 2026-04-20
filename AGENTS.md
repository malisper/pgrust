# pgrust — project guide for AI agents

## Overview

`pgrust` is a PostgreSQL-style database prototype implemented in Rust. The repo is structured around the same broad layers PostgreSQL uses:

- `src/backend/parser`: SQL grammar, parse tree handling, semantic analysis, and logical plan construction.
- `src/backend/executor`: expression evaluation, plan startup, runtime plan-node execution, tuple/value I/O, and aggregates.
- `src/backend/catalog`: table/type metadata and catalog mutations.
- `src/backend/access` and `src/backend/storage`: heap access, page layout, buffer/storage concerns.
- `src/backend/tcop` and `src/backend/libpq`: protocol entry points, error mapping, and frontend/backend message handling.
- `src/include/nodes`: shared node/value/plan/runtime data structures.
- `src/pgrust`: server/session/database orchestration outside the PostgreSQL-style backend tree.

The current codebase was recently refactored to separate parser, logical plan, and executor-runtime responsibilities more cleanly. Prefer extending those boundaries instead of reintroducing cross-layer dependencies.

## Shared Node Layers

The canonical shared types live under `src/include/nodes`:

- [src/include/nodes/parsenodes.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/include/nodes/parsenodes.rs): raw SQL AST produced by the parser.
- [src/include/nodes/datum.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/include/nodes/datum.rs): logical scalar values like `Value` and `NumericValue`.
- [src/include/nodes/plannodes.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/include/nodes/plannodes.rs): bound expressions, logical plans, column metadata, aggregates, and scalar-function identifiers.
- [src/include/nodes/execnodes.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/include/nodes/execnodes.rs): executor runtime state such as tuple slots and concrete `*State` plan-node structs.

Rules:

- Parser code should depend on `parsenodes`, `datum`, and `plannodes`, not executor implementation files.
- Executor code may depend on `datum`, `plannodes`, and `execnodes`.
- Runtime behavior should not live in `src/include/nodes`; keep behavior in `src/backend/*`.

## Parser Structure

Top-level parser entry points are in [src/backend/parser/mod.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/mod.rs). This module should stay thin: grammar entry points, public parser API, and re-exports.

Grammar files:

- [src/backend/parser/gram.pest](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/gram.pest)
- [src/backend/parser/gram.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/gram.rs)

Semantic analysis lives in `src/backend/parser/analyze`:

- [src/backend/parser/analyze/mod.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/analyze/mod.rs): statement-level orchestration, DDL/DML binding entry points, and top-level `SELECT` planning flow.
- [src/backend/parser/analyze/scope.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/analyze/scope.rs): relation binding, scope construction, column resolution, outer-scope lookup.
- [src/backend/parser/analyze/coerce.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/analyze/coerce.rs): coercion helpers, type-family logic, and common-type selection.
- [src/backend/parser/analyze/functions.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/analyze/functions.rs): builtin scalar-function and aggregate lookup plus arity validation.
- [src/backend/parser/analyze/expr.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/analyze/expr.rs): normal expression binding.
- [src/backend/parser/analyze/infer.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/analyze/infer.rs): SQL expression type inference.
- [src/backend/parser/analyze/agg.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/analyze/agg.rs): aggregate discovery and grouped-column validation.
- [src/backend/parser/analyze/agg_output.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/analyze/agg_output.rs): binding grouped aggregate output expressions.
- [src/backend/parser/analyze/agg_output_special.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/analyze/agg_output_special.rs): grouped subquery/function/array helper paths.

Guidance:

- Add new raw syntax in `gram.pest`/`gram.rs`, then map it into `parsenodes`.
- Add new semantic binding in the narrowest `analyze/*` module that matches the responsibility.
- Avoid growing `analyze/mod.rs` back into a catch-all file.

## Executor Structure

The executor facade is [src/backend/executor/mod.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/mod.rs). It owns public executor entry points, shared executor error types, and exports, but most production logic should live in submodules.

Execution modules:

- [src/backend/executor/startup.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/startup.rs): plan startup and plan-state construction.
- [src/backend/executor/driver.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/driver.rs): top-level execution flow and tuple production.
- [src/backend/executor/nodes.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/nodes.rs): runtime behavior for concrete plan-node state structs.
- [src/backend/executor/agg.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/agg.rs): aggregate transition and finalize logic.

Expression and value handling:

- [src/backend/executor/exec_expr.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/exec_expr.rs): high-level expression evaluation entry points.
- [src/backend/executor/expr_ops.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/expr_ops.rs): arithmetic, comparison, ordering, and boolean operator helpers.
- [src/backend/executor/expr_casts.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/expr_casts.rs): cast and coercion behavior during execution.
- [src/backend/executor/expr_compile.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/expr_compile.rs): predicate compilation and fixed-layout fast paths.
- [src/backend/executor/expr_json.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/expr_json.rs): JSON operator and builder behavior.
- [src/backend/executor/value_io.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/value_io.rs): tuple encoding/decoding and value serialization helpers.
- [src/backend/executor/exec_tuples.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/exec_tuples.rs): tuple decoding/deformation helpers.
- [src/backend/executor/jsonb.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/jsonb.rs) and [src/backend/executor/jsonpath.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/jsonpath.rs): JSONB and JSONPath support.

Guidance:

- Do not move logical plan or `Value` definitions back into executor files.
- Keep type-specific I/O in focused helper modules instead of growing `exec_expr.rs`.
- Keep runtime node behavior in `nodes.rs`, not `execnodes.rs`.

## Catalog, Access, Storage, and Protocol

- [src/backend/catalog/catalog.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/catalog/catalog.rs): catalog state and metadata operations.
- [src/backend/commands/tablecmds.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/commands/tablecmds.rs): DDL-heavy command handling.
- [src/backend/commands/copyfrom.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/commands/copyfrom.rs): `COPY FROM`.
- [src/backend/commands/explain.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/commands/explain.rs): `EXPLAIN` formatting and explain-only behavior.
- `src/backend/access/*`: heap access methods and transaction-visible tuple handling.
- `src/backend/storage/*`: page layout and storage primitives.
- [src/backend/libpq/pqcomm.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/libpq/pqcomm.rs) and [src/backend/libpq/pqformat.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/libpq/pqformat.rs): wire protocol messaging and error formatting.
- [src/backend/tcop/postgres.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/tcop/postgres.rs): SQL execution entry flow and SQLSTATE mapping.

## Server Layer

The PostgreSQL-like backend modules sit under `src/backend`, but process/session orchestration is in `src/pgrust`:

- [src/pgrust/server.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/pgrust/server.rs): TCP server loop.
- [src/pgrust/session.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/pgrust/session.rs): per-client session behavior.
- [src/pgrust/database.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/pgrust/database.rs): database-level shared state and temp-object/session interactions.

If a change is about SQL semantics, planning, or execution, it usually belongs under `src/backend`, not `src/pgrust`.

## Working Rules

- Before adding code to a large file, check whether there is already a responsibility-specific sibling module.
- Prefer moving logic outward into narrow modules rather than adding another broad helper section to `mod.rs`.
- Keep parser analysis, logical plan construction, and executor runtime concerns separate.
- Keep tests close to the module they validate when practical. The executor facade still has a large test block; shrinking that is still a good follow-up.
- Avoid adding new parser dependencies on executor implementation modules.
- When you introduce a narrow workaround, compatibility shim, or intentionally temporary shortcut, add a nearby `:HACK:` comment explaining what is being worked around and what the preferred long-term shape should be.

## Validation

For structural refactors, the default verification loop is:

- `cargo check`
- `cargo test --lib --quiet`

If a change affects SQL behavior more broadly, run the regression harness afterward.

## Profiling Output

When the user asks for a profile or profiling analysis:

- Present the results in a clean, readable format.
- Include the profile source or file path, a short summary of the main hotspots, and a compact list of the most important syscall or caller chains when relevant.
- Prefer concise sections such as `Summary`, `Top Hotspots`, and `Key Call Paths` over dumping raw profiler output without interpretation.


<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
