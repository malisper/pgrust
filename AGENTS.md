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

## Worktrees

Run code changes inside a git worktree instead of the primary checkout. Multiple agents and humans frequently run against this repo in parallel on the same machine; sharing one working copy produces merge races, stale-index bugs, and half-applied edits across unrelated tasks.

Use a worktree when:

- The work will produce at least one commit or PR.
- The work spans more than one turn of file edits.

Skip the worktree for:

- Pure exploration, reading, or answering questions without file changes.
- One-off shell/inspection commands.

Conventions:

- Path: `../pgrust-worktrees/<short-descriptive-name>/` (sibling to this repo).
- Branch: follow the existing `<owner>/<short-description>` pattern where it applies (e.g. `malisper/alter-table-todo`, `jason/isolation-tests-setup`).
- Base: `perf-optimization`.
- Create: `git worktree add ../pgrust-worktrees/<name> -b <branch> perf-optimization`.
- Remove when merged or abandoned: `git worktree remove ../pgrust-worktrees/<name>`.

If you use Conductor (conductor.build), its workspaces live at `~/conductor/pgrust/<city>/` instead — no collision with the path above. Pick `perf-optimization` as the base in the New Workspace dialog, and rename the auto-generated city branch to match our `<owner>/<short-description>` pattern once you know what you're working on.

## Formatting

Rust formatting is pinned: `rust-toolchain.toml` fixes the rustc/rustfmt version and `rustfmt.toml` fixes the style edition. CI fails any PR that is not formatted with that exact rustfmt.

The repo ships a versioned pre-commit hook in `.githooks/pre-commit` that runs `cargo fmt -- --check`. Activate it with one command per clone (and per worktree, since worktrees have their own git config):

```sh
bash scripts/setup-dev.sh
```

This sets `core.hooksPath` to `.githooks` for that clone. After it runs, every `git commit` is rejected in <1 second if anything is unformatted. The setup is idempotent and safe to re-run.

- After editing any `*.rs` file, run `cargo fmt` before considering the task done. Running from inside the repo uses the pinned toolchain automatically.
- Do not reformat files you did not otherwise touch. If you notice unrelated drift, leave it — a separate fmt-only PR is the right cleanup path. Agents mixing stray reformatting into feature PRs is the exact churn this policy exists to prevent.
- Do not bypass the pre-commit hook with `git commit --no-verify`, and do not disable the `cargo-fmt-check` CI job. If the hook rejects you, run `cargo fmt`, re-stage, and re-commit.

### Optional: agent-side / editor-side auto-format

For an even tighter feedback loop, configure your tool to run `cargo fmt` immediately after every file edit so the pre-commit hook never even has to fire. These configs are per-user, not versioned in the repo:

- **Claude Code** — Add a `PostToolUse` hook in `~/.claude/settings.json`:

  ```json
  {
    "hooks": {
      "PostToolUse": [
        {
          "matcher": "Edit|Write|MultiEdit",
          "hooks": [{ "type": "command", "command": "cargo fmt" }]
        }
      ]
    }
  }
  ```

  Docs: https://code.claude.com/docs/en/hooks-guide

- **Codex CLI** — Hooks are still under development as of early 2026 (off by default). Enable with `codex_hooks = true` in the `[features]` section of your Codex config, then add a `PostToolUse` hook in `hooks.json` next to your config. Docs: https://developers.openai.com/codex/hooks

- **Cursor** — Enable VS Code's `editor.formatOnSave` setting and rust-analyzer will run rustfmt on every save. Settings → search "Format On Save" → check the box.

- **Conductor** (parallel agent runner) — Uses Claude Code under the hood, so the Claude Code `PostToolUse` hook above applies automatically.

## PostgreSQL Reference Repo

A sibling PostgreSQL source checkout is available at `../postgres`.

Guidance:

- Refer to the PostgreSQL repo as much as necessary when implementing SQL behavior, planner/executor semantics, catalog behavior, protocol details, or PostgreSQL-compatible errors.
- Treat the PostgreSQL repo as the primary behavioral reference when pgrust behavior is unclear or when regression results need explanation.
- Prefer checking the matching PostgreSQL area before introducing new semantics or compatibility shims in pgrust.

## Shared Node Layers

The canonical shared types live under `src/include/nodes`:

- [src/include/nodes/parsenodes.rs](/src/include/nodes/parsenodes.rs): raw SQL AST produced by the parser.
- [src/include/nodes/datum.rs](/src/include/nodes/datum.rs): logical scalar values like `Value` and `NumericValue`.
- [src/include/nodes/plannodes.rs](/src/include/nodes/plannodes.rs): bound expressions, logical plans, column metadata, aggregates, and scalar-function identifiers.
- [src/include/nodes/execnodes.rs](/src/include/nodes/execnodes.rs): executor runtime state such as tuple slots and concrete `*State` plan-node structs.

Rules:

- Parser code should depend on `parsenodes`, `datum`, and `plannodes`, not executor implementation files.
- Executor code may depend on `datum`, `plannodes`, and `execnodes`.
- Runtime behavior should not live in `src/include/nodes`; keep behavior in `src/backend/*`.

## Parser Structure

Top-level parser entry points are in [src/backend/parser/mod.rs](/src/backend/parser/mod.rs). This module should stay thin: grammar entry points, public parser API, and re-exports.

Grammar files:

- [src/backend/parser/gram.pest](/src/backend/parser/gram.pest)
- [src/backend/parser/gram.rs](/src/backend/parser/gram.rs)

Semantic analysis lives in `src/backend/parser/analyze`:

- [src/backend/parser/analyze/mod.rs](/src/backend/parser/analyze/mod.rs): statement-level orchestration, DDL/DML binding entry points, and top-level `SELECT` planning flow.
- [src/backend/parser/analyze/scope.rs](/src/backend/parser/analyze/scope.rs): relation binding, scope construction, column resolution, outer-scope lookup.
- [src/backend/parser/analyze/coerce.rs](/src/backend/parser/analyze/coerce.rs): coercion helpers, type-family logic, and common-type selection.
- [src/backend/parser/analyze/functions.rs](/src/backend/parser/analyze/functions.rs): builtin scalar-function and aggregate lookup plus arity validation.
- [src/backend/parser/analyze/expr.rs](/src/backend/parser/analyze/expr.rs): normal expression binding.
- [src/backend/parser/analyze/infer.rs](/src/backend/parser/analyze/infer.rs): SQL expression type inference.
- [src/backend/parser/analyze/agg.rs](/src/backend/parser/analyze/agg.rs): aggregate discovery and grouped-column validation.
- [src/backend/parser/analyze/agg_output.rs](/src/backend/parser/analyze/agg_output.rs): binding grouped aggregate output expressions.
- [src/backend/parser/analyze/agg_output_special.rs](/src/backend/parser/analyze/agg_output_special.rs): grouped subquery/function/array helper paths.

Guidance:

- Add new raw syntax in `gram.pest`/`gram.rs`, then map it into `parsenodes`.
- Add new semantic binding in the narrowest `analyze/*` module that matches the responsibility.
- Avoid growing `analyze/mod.rs` back into a catch-all file.

## Executor Structure

The executor facade is [src/backend/executor/mod.rs](/src/backend/executor/mod.rs). It owns public executor entry points, shared executor error types, and exports, but most production logic should live in submodules.

Execution modules:

- [src/backend/executor/startup.rs](/src/backend/executor/startup.rs): plan startup and plan-state construction.
- [src/backend/executor/driver.rs](/src/backend/executor/driver.rs): top-level execution flow and tuple production.
- [src/backend/executor/nodes.rs](/src/backend/executor/nodes.rs): runtime behavior for concrete plan-node state structs.
- [src/backend/executor/agg.rs](/src/backend/executor/agg.rs): aggregate transition and finalize logic.

Expression and value handling:

- [src/backend/executor/exec_expr.rs](/src/backend/executor/exec_expr.rs): high-level expression evaluation entry points.
- [src/backend/executor/expr_ops.rs](/src/backend/executor/expr_ops.rs): arithmetic, comparison, ordering, and boolean operator helpers.
- [src/backend/executor/expr_casts.rs](/src/backend/executor/expr_casts.rs): cast and coercion behavior during execution.
- [src/backend/executor/expr_compile.rs](/src/backend/executor/expr_compile.rs): predicate compilation and fixed-layout fast paths.
- [src/backend/executor/expr_json.rs](/src/backend/executor/expr_json.rs): JSON operator and builder behavior.
- [src/backend/executor/value_io.rs](/src/backend/executor/value_io.rs): tuple encoding/decoding and value serialization helpers.
- [src/backend/executor/exec_tuples.rs](/src/backend/executor/exec_tuples.rs): tuple decoding/deformation helpers.
- [src/backend/executor/jsonb.rs](/src/backend/executor/jsonb.rs) and [src/backend/executor/jsonpath.rs](/src/backend/executor/jsonpath.rs): JSONB and JSONPath support.

Guidance:

- Do not move logical plan or `Value` definitions back into executor files.
- Keep type-specific I/O in focused helper modules instead of growing `exec_expr.rs`.
- Keep runtime node behavior in `nodes.rs`, not `execnodes.rs`.

## Catalog, Access, Storage, and Protocol

- [src/backend/catalog/catalog.rs](/src/backend/catalog/catalog.rs): catalog state and metadata operations.
- [src/backend/commands/tablecmds.rs](/src/backend/commands/tablecmds.rs): DDL-heavy command handling.
- [src/backend/commands/copyfrom.rs](/src/backend/commands/copyfrom.rs): `COPY FROM`.
- [src/backend/commands/explain.rs](/src/backend/commands/explain.rs): `EXPLAIN` formatting and explain-only behavior.
- `src/backend/access/*`: heap access methods and transaction-visible tuple handling.
- `src/backend/storage/*`: page layout and storage primitives.
- [src/backend/libpq/pqcomm.rs](/src/backend/libpq/pqcomm.rs) and [src/backend/libpq/pqformat.rs](/src/backend/libpq/pqformat.rs): wire protocol messaging and error formatting.
- [src/backend/tcop/postgres.rs](/src/backend/tcop/postgres.rs): SQL execution entry flow and SQLSTATE mapping.

## Server Layer

The PostgreSQL-like backend modules sit under `src/backend`, but process/session orchestration is in `src/pgrust`:

- [src/pgrust/server.rs](/src/pgrust/server.rs): TCP server loop.
- [src/pgrust/session.rs](/src/pgrust/session.rs): per-client session behavior.
- [src/pgrust/database.rs](/src/pgrust/database.rs): database-level shared state and temp-object/session interactions.

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
- `cargo test --lib --quiet` (only tests relevant to the changed module, not the full suite)

If a change affects SQL behavior more broadly, run the regression harness afterward.

When rerunning an individual regression file, copy the resulting `.diff` artifact into `/tmp/diffs` so it is easy to inspect outside the results directory.

**Testing guidance for agents:** Run only the specialized tests directly related to your change (e.g., tests in the module you modified, integration tests for the feature you're working on). Do not run the full `cargo test` suite unless specifically asked. The full test suite runs in CI, so focus your effort on validating that your specific change works correctly. This keeps iteration fast and avoids redundant testing.

## Finish

When finishing work in this repo:

- When the user says `Finish`, mark the work as finished in the todo list, commit it, merge it, and then list the next related features to work on as a numbered list.
- Do not push new code unless the user explicitly asks for a push.

## Profiling Output

When the user asks for a profile or profiling analysis:

- Present the results in a clean, readable format.
- Include the profile source or file path, a short summary of the main hotspots, and a compact list of the most important syscall or caller chains when relevant.
- Prefer concise sections such as `Summary`, `Top Hotspots`, and `Key Call Paths` over dumping raw profiler output without interpretation.
