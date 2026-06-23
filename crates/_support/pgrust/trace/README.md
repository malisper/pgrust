# pgrust-trace

A lightweight, env-gated TRACE facility for debugging the single-user query
pipeline of the pgrust PostgreSQL port. This is **infrastructure**, not a C
port. When unset it costs one relaxed atomic load per trace site; when set it
prints faithful, minimal, high-signal output to stderr.

It has **zero non-std dependencies**, so any crate — even the low-level
`seam-core` — can depend on it without creating a dependency cycle.

## Environment usage

```sh
# Enable specific categories (comma-separated, case-insensitive):
PGRUST_TRACE=seam,heaptuple ./target/debug/postgres --single ...

# Enable everything:
PGRUST_TRACE=all ./target/debug/postgres --single ...
# (PGRUST_TRACE=* works too)

# Also emit a backtrace after each line for selected categories:
PGRUST_TRACE=exec PGRUST_TRACE_BT=mcx ./target/debug/postgres --single ...
```

Unknown category names produce one warning on stderr listing the known names.

## Categories

`seam`, `heaptuple`, `slot`, `exec`, `catcache`, `syscache`, `relcache`,
`planner`, `xact`, `mcx`, `smgr`, `bufmgr`.

The list is extensible: add a `Category` variant, append it to `ALL`, and add
its name in `Category::name`.

## Macro API

| Macro | Purpose |
|-------|---------|
| `trace!(Category::X, "fmt {}", args...)` | Conditional formatted print. Format args evaluated only when the category is enabled. Emits a backtrace too if the category is in `PGRUST_TRACE_BT`. |
| `trace_enabled!(Category::X)` | `bool` — guard expensive value construction. |
| `trace_bt!(Category::X, "fmt", args...)` | Like `trace!` but always captures a backtrace (ignores `PGRUST_TRACE_BT`). |
| `trace_scope!(Category::X, "label {}", args...)` | Returns an RAII guard: logs `>> label` on creation and `<< label` on drop, with thread-local depth indentation. No-op when off. |

### Output format

```
[seam] crates/seam-core/src/lib.rs:71: heap_insert
[exec]   >> ExecProcNode SeqScan
[exec]   << ExecProcNode SeqScan
```

`trace!` / `trace_bt!` lines are prefixed `[category] file:line:`.
`trace_scope!` lines use `>>`/`<<` with two-space-per-depth indentation.

## Instrumented chokepoints

* **seam-core `seam!`**: every `call(...)` emits `[seam] <seam path>`; an
  uninstalled `call(...)` emits `trace_bt!` `MISS <path>` (full caller stack)
  before panicking.
* **heaptuple** (feature `trace`, default on): per-attr decode in getattr/deform
  (attnum, isnull, byval/byref, attlen).
* **execTuples**: slot deform (nvalid transitions), `ExecStore*Tuple`,
  `ExecClearTuple`.
* **execProcnode**: `trace_scope!` enter/exit around the dispatch boundary with
  the node tag.
* **catcache / syscache**: search hit/miss, cacheid + keys, release.
* **relcache**: `RelationBuildDesc(relid)` enter/exit.
