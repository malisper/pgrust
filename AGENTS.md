# pgrust Agent Guide

This file is always loaded. Keep it compact: route agents to the right code and
prevent common mistakes. Put detailed, task-specific workflows in skills or
docs, not here.

## Core Rules

- Use PostgreSQL in `../postgres` as the behavioral reference for SQL semantics,
  planner/executor behavior, catalog details, protocol behavior, and regression
  diffs.
- Keep parser, logical-plan, executor-runtime, catalog, storage, and server
  responsibilities separated.
- Prefer narrow sibling modules over adding broad helper sections to large
  `mod.rs` files.
- Do not move logical plan or `Value` definitions into executor modules.
- Do not put runtime behavior in `src/include/nodes`; keep behavior under
  `src/backend/*`.
- Add a nearby `:HACK:` comment for intentional compatibility shims or temporary
  shortcuts, with the preferred long-term shape.

## Task Routing

| Task | Start Here | Usually Also Touches |
| --- | --- | --- |
| SQL syntax or new statement shape | `src/backend/parser/gram.pest`, `src/backend/parser/gram.rs`, `src/include/nodes/parsenodes.rs` | `src/backend/parser/analyze/*`; skill: `pgrust-sql-language` |
| Name/type binding, SELECT planning | `src/backend/parser/analyze/mod.rs`, `scope.rs`, `expr.rs`, `infer.rs`, `coerce.rs` | `src/include/nodes/plannodes.rs` |
| Builtin functions/operators | `src/backend/parser/analyze/functions.rs`, `expr.rs`, `infer.rs` | `src/backend/executor/exec_expr.rs`, `expr_ops.rs`, `expr_casts.rs` |
| Aggregates or grouping | `src/backend/parser/analyze/agg*.rs` | `src/backend/executor/agg.rs`, `src/backend/executor/nodes.rs` |
| New SQL type or type I/O | `src/include/nodes/datum.rs`, `src/backend/catalog/catalog.rs` | `src/backend/executor/value_io.rs`, `expr_casts.rs`, protocol files; skill: `pgrust-add-type` |
| DDL/table metadata | `src/backend/commands/tablecmds.rs` | `src/backend/catalog/catalog.rs`, storage/access modules |
| COPY | `src/backend/commands/copyfrom.rs` | executor value I/O, regression tests |
| EXPLAIN | `src/backend/commands/explain.rs` | plan nodes and planner output |
| Wire protocol/errors | `src/backend/libpq/*`, `src/backend/tcop/postgres.rs` | executor/commands error mapping |
| Heap/storage/visibility | `src/backend/access/*`, `src/backend/storage/*` | catalog metadata and executor scan nodes |
| Server/session orchestration | `src/pgrust/server.rs`, `session.rs`, `database.rs` | usually not SQL semantics |
| Regression diff diagnosis | failing `.diff` file and expected/actual snippets | skill: `diff` |
| Profiling | benchmark/repro script and profile output | skill: `pgrust-profile` |

## PostgreSQL Source Map

Use `../postgres` for behavior checks. Start with the narrowest matching area,
then follow includes in `../postgres/src/include/*` when structs or macros are
needed.

| Need | PostgreSQL Reference |
| --- | --- |
| SQL grammar | `../postgres/src/backend/parser/gram.y`, `scan.l` |
| Raw parse nodes | `../postgres/src/include/nodes/parsenodes.h`, `nodes.h` |
| Parse analysis / binding | `../postgres/src/backend/parser/analyze.c`, `parse_expr.c`, `parse_clause.c`, `parse_relation.c`, `parse_target.c`, `parse_type.c`, `parse_func.c`, `parse_oper.c` |
| Type coercion | `../postgres/src/backend/parser/parse_coerce.c`, `parse_type.c` |
| Function/operator lookup | `../postgres/src/backend/parser/parse_func.c`, `parse_oper.c`; catalogs in `src/include/catalog/pg_proc.dat`, `pg_operator.dat` |
| Planner entry | `../postgres/src/backend/optimizer/plan/planner.c`, `src/backend/optimizer/prep/*`, `src/backend/optimizer/path/*` |
| Plan node structs | `../postgres/src/include/nodes/pathnodes.h`, `plannodes.h`, `primnodes.h` |
| Executor entry | `../postgres/src/backend/executor/execMain.c`, `execProcnode.c`, `execExpr.c` |
| Executor plan nodes | `../postgres/src/backend/executor/node*.c` |
| Aggregates | `../postgres/src/backend/executor/nodeAgg.c`, `src/backend/parser/parse_agg.c`, `src/include/catalog/pg_aggregate.dat` |
| DDL / utility commands | `../postgres/src/backend/tcop/utility.c`, `src/backend/commands/*` |
| Table DDL | `../postgres/src/backend/commands/tablecmds.c` |
| COPY | `../postgres/src/backend/commands/copy*.c` |
| EXPLAIN | `../postgres/src/backend/commands/explain.c` |
| Catalog metadata | `../postgres/src/backend/catalog/*`, `src/include/catalog/*.h`, `src/include/catalog/*.dat` |
| Type implementation | `../postgres/src/backend/utils/adt/*`, `src/include/utils/*`, relevant catalog `.dat` files |
| Heap/index access | `../postgres/src/backend/access/heap/*`, `index/*`, `nbtree/*`, `hash/*`, `gist/*`, `gin/*`, `brin/*` |
| Transactions / WAL / MVCC | `../postgres/src/backend/access/transam/*`, `src/include/access/*` |
| Buffers/storage/locks | `../postgres/src/backend/storage/buffer/*`, `storage/smgr/*`, `storage/lmgr/*`, `storage/page/*`, `storage/ipc/*` |
| Rewriter/rules/views | `../postgres/src/backend/rewrite/*`, `src/include/rewrite/*` |
| Wire protocol | `../postgres/src/backend/libpq/*`, `src/include/libpq/*`, `src/backend/tcop/postgres.c`, `pquery.c` |
| Errors/GUC/fmgr/cache | `../postgres/src/backend/utils/error/*`, `misc/guc.c`, `fmgr/*`, `cache/*` |
| Regression tests | `../postgres/src/test/regress/sql/*`, `expected/*` |
| Isolation tests | `../postgres/src/test/isolation/specs/*` |
| PL/pgSQL | `../postgres/src/pl/plpgsql/src/*` |
| psql/client behavior | `../postgres/src/bin/psql/*`, `src/interfaces/libpq/*` |

## Shared Node Layers

- `src/include/nodes/parsenodes.rs`: raw SQL AST from parser.
- `src/include/nodes/datum.rs`: logical scalar values such as `Value`.
- `src/include/nodes/plannodes.rs`: bound expressions, logical plans, metadata,
  aggregates, scalar-function ids.
- `src/include/nodes/execnodes.rs`: executor runtime state structs only.

Parser code may depend on `parsenodes`, `datum`, and `plannodes`, not executor
implementation modules. Executor code may depend on `datum`, `plannodes`, and
`execnodes`.

## Token Budget Rules

- Start with `rg` and small `sed -n` ranges. Do not dump whole large files.
- Keep tool output small. Default `max_output_tokens` to `4000` or less unless
  there is a concrete reason to raise it.
- For `write_stdin` polling, use `max_output_tokens <= 2000` and wait at least
  30s unless actively debugging an interactive failure.
- Do not run broad `rg` over `/tmp/diffs`, `/tmp`, `.`, `~/.cargo/registry`,
  or `target`. Narrow searches to specific files or subdirectories.
- Do not run `rg --files /tmp/diffs`; inspect the task-specific result
  directory and only the relevant `.diff` or `.out` file.
- Use `git diff --stat` before full `git diff`; inspect only relevant hunks.
- Save large logs to `/tmp` and summarize the failing lines.
- For CI logs, save raw logs to `/tmp` when needed and filter summaries with
  bounded commands such as `rg -m 100 -C 3`.
- For regression reruns, copy useful `.diff` artifacts to `/tmp/diffs`.
- After broad exploration, write a short task note in
  `.codex/task-notes/<task>.md`:

  ```md
  Goal:
  Key decisions:
  Files touched:
  Tests run:
  Remaining:
  ```

- Prefer fresh sessions for unrelated tasks. Resume from the task note instead
  of replaying long chat history.
- Restart or hand off after noisy test, CI, or log-debugging loops instead of
  continuing inside a polluted session.
- Use subagents only for narrow read-only exploration or disjoint file ownership.
  Give them explicit output limits and owned files.

## Worktrees

Use a separate worktree when work will produce a commit/PR or spans multiple
turns of edits. Skip this for pure reading or one-off inspection.

- Base: `perf-optimization`.
- Path: `../pgrust-worktrees/<short-name>/`.
- Branch: follow `<owner>/<short-description>` when practical.
- Create:

  ```sh
  git worktree add ../pgrust-worktrees/<name> -b <branch> perf-optimization
  ```

Conductor workspaces live under `~/conductor/pgrust/<city>/`; use
`perf-optimization` as base there too.

## Formatting

- Rust formatting is pinned by `rust-toolchain.toml` and `rustfmt.toml`.
- Run `cargo fmt` after editing any `*.rs` file.
- Do not reformat unrelated files.
- Enable hooks per clone/worktree with:

  ```sh
  bash scripts/setup-dev.sh
  ```

- Do not bypass pre-commit hooks with `git commit --no-verify`.

## Validation

Run focused validation for the files/features changed.

- Structural Rust changes: `cargo check`.
- Module behavior: targeted `cargo test --lib --quiet <test-or-module>`.
- SQL behavior: relevant regression file, not the full harness unless asked.
- Do not run the full test suite unless the user asks; CI covers it.

On macOS, full local suites may need:

```sh
ulimit -n 65536
cargo test --lib --quiet
```

## Finish

When the user says `Finish`: mark the work finished in the todo list, commit it,
merge it, and list next related features as a numbered list. Do not push unless
the user explicitly asks.
