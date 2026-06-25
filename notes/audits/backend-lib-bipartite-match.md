# Audit: backend-lib-bipartite-match

- C source: `src/backend/lib/bipartite_match.c` (+ `src/include/lib/bipartite_match.h`), PostgreSQL 18.3
- c2rust reference: `c2rust-runs/backend-lib-all/src/bipartite_match.rs` (also in `backend-lib-algorithms` / `backend-lib-no-ilist`, identical)
- Port: `crates/backend-lib-bipartite-match/src/lib.rs` (+ `src/tests.rs`)
- Method: independent re-derivation from C + c2rust; the port's comments/build were not trusted.

## Function inventory (complete — 4 defs in the C TU)

| # | C function (loc) | Port (loc) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `BipartiteMatch` (bipartite_match.c:38-71) | `BipartiteMatch` (lib.rs:108-167) | MATCH | see below |
| 2 | `BipartiteMatchFree` (bipartite_match.c:77-86) | `BipartiteMatchFree` (lib.rs:175) | MATCH | see below |
| 3 | `hk_breadth_search` (static, bipartite_match.c:92-139) | `hk_breadth_search` (lib.rs:hk_breadth_search) | MATCH | see below |
| 4 | `hk_depth_search` (static, bipartite_match.c:145-180) | `hk_depth_search` (lib.rs:hk_depth_search) | MATCH | see below |

Private Rust helpers with no C counterpart (factored-out access/alloc, audited as
part of their callers, no independent logic): `adjacency_values`, `zeroed_vec`,
`charged_zeroed`, `oom`, and the `Scratch`/`BipartiteMatchState` structs.

## Constants

| Constant | C value | Port | Verdict |
|---|---|---|---|
| `HK_INFINITY` | `SHRT_MAX` = 32767 (c2rust line 38/40) | `i16::MAX` = 32767 (lib.rs:HK_INFINITY) | MATCH |
| size-check bound | `SHRT_MAX` = 32767 | `i16::MAX as i32` = 32767 | MATCH |
| error message | `"invalid set size for BipartiteMatch"` (c2rust:67) | identical string | MATCH |
| error level/SQLSTATE | `elog(ERROR)` → ERRCODE_INTERNAL_ERROR / XX000 (default for `elog`/`errmsg_internal` with no errcode) | `PgError::error(...)` (level ERROR, default sqlstate XX000) — asserted in `invalid_set_size_matches_c_message` | MATCH |

## Per-function detail

### 1. BipartiteMatch — MATCH
- Size guard `u_size < 0 || u_size >= SHRT_MAX || v_size < 0 || v_size >= SHRT_MAX`
  → `elog(ERROR)` (c2rust:50-80): port replicates the exact disjunction and
  returns `Err(PgError::error("invalid set size for BipartiteMatch"))`. Real C
  `elog(ERROR)` is a non-local longjmp, so the field assignments after the guard
  are never reached on the error path; the port's early `Err` return is the exact
  semantic (c2rust's fall-through after the guard is a translation artifact — it
  does not model longjmp — and is *not* the C behavior).
- State init: `u_size`/`v_size`/`adjacency`/`matching=0` (c2rust:81-84). Port sets
  `u_size`, `v_size`, `matching=0`; `adjacency` is threaded as the `&[&[i16]]`
  parameter rather than stored in the struct (safe-slice model, see Design notes).
- `pair_uv = palloc0((u_size+1))`, `pair_vu = palloc0((v_size+1))` (c2rust:85-92):
  port `zeroed_vec(u_size+1)` / `zeroed_vec(v_size+1)` — exact lengths, zero-init
  matching `palloc0`.
- `distance = palloc((u_size+1))`, `queue = palloc((u_size+2))` (c2rust:93-100):
  port `charged_zeroed(u_size+1)` / `charged_zeroed(u_size+2)` — exact lengths.
  `palloc` is uninitialized in C but every slot is written before read; zero-init
  is observably identical and avoids any uninit read. Off-by-one (`+1` / `+2`)
  preserved exactly.
- Outer loop `while (hk_breadth_search(state)) { for u in 1..=u_size {
  if pair_uv[u]==0 && hk_depth_search(state,u) matching++ } CHECK_FOR_INTERRUPTS }`
  (c2rust:101-119): port's `drive()` replicates the while/for, the `pair_uv[u]==0`
  short-circuit, `matching += 1`, and ends each iteration with
  `check_for_interrupts::call()?`. `CHECK_FOR_INTERRUPTS()` expands to
  `if (InterruptPending) ProcessInterrupts()` (c2rust:114-117); the seam owner
  (`backend-utils-init-miscinit`) holds that body — correct delegation.

### 2. BipartiteMatchFree — MATCH
- C `pfree`s pair_uv/pair_vu/distance/queue/state, leaving the caller-owned
  adjacency alone (c2rust:123-129). In the port, distance/queue live in a private
  `mcx` context already dropped at the end of `BipartiteMatch`; pair_uv/pair_vu and
  the state are owned `Vec`/value released by `drop`. `BipartiteMatchFree` consumes
  the value (final drop) and does not touch the adjacency (the parameter is gone).
  Behavior identical: all owned memory released, adjacency untouched. Memory-model
  divergence only (no `pfree`/longjmp model in the pilot allocator), no logic change.

### 3. hk_breadth_search — MATCH
- `distance[0] = HK_INFINITY` (c2rust:137-138): MATCH.
- BFS seed loop `for u in 1..=usize: if pair_uv[u]==0 { distance[u]=0;
  queue[qhead++]=u } else distance[u]=HK_INFINITY` (c2rust:139-152): MATCH,
  including the post-increment enqueue.
- Drain loop `while qtail<qhead: u=queue[qtail++]; if distance[u]<distance[0] {...}`
  (c2rust:153-159): MATCH.
- Edge scan `u_adj = adjacency[u]; i = u_adj? u_adj[0]:0; while i>0 { u_next =
  pair_vu[u_adj[i]]; if distance[u_next]==HK_INFINITY { distance[u_next]=1+distance[u];
  queue[qhead++]=u_next } i-- }` (c2rust:160-182): the descending `i>0; i--` scan
  is reproduced by `adjacency_values(...).iter().rev()` (count, then v_count..v_1).
  The `u_adj ? u_adj[0] : 0` NULL/zero-row guard is `adjacency_values` returning
  `&[]` for an empty row. `Assert(qhead < usize+2)` → `debug_assert!`. MATCH.
- `1 + distance[u]`: c2rust computes in `c_int` then truncates to `short`; port
  computes in `i16`. No overflow: reachable only when `distance[u] < distance[0]`
  with `distance[0] <= HK_INFINITY`, so `distance[u] <= HK_INFINITY-1` and `+1`
  is in range. MATCH.
- Return `distance[0] != HK_INFINITY` (c2rust:185-186): MATCH.

### 4. hk_depth_search — MATCH
- Guards `if u==0 return true; if distance[u]==HK_INFINITY return false;`
  (c2rust:202-207): MATCH. (c2rust reads `u_adj`/`i` before the guards; they are
  only *used* after, so the port computing the row after the guards is identical.)
- `nextdist = distance[u] + 1` (c2rust:208-209): MATCH; no overflow — the
  `distance[u]==HK_INFINITY` early return guarantees `distance[u] < SHRT_MAX`.
- `check_stack_depth()` (c2rust:210): port `check_stack_depth::call()?` — owner is
  `backend-utils-misc-stack-depth`. Correct delegation (the C recursion guard).
- Edge scan `while i>0 { v=u_adj[i]; if distance[pair_vu[v]]==nextdist {
  if hk_depth_search(state, pair_vu[v]) { pair_vu[v]=u; pair_uv[u]=v; return true } }
  i-- }` (c2rust:211-226): port snapshots the row into a local `Vec` (so the
  recursive `&mut` borrow does not alias the iterator) and iterates `.rev()`,
  matching the descending `i>0; i--`. C never mutates a row, so the snapshot is
  value-identical. The `distance[pair_vu[v]]==nextdist` test, recursive call,
  and the `pair_vu[v]=u; pair_uv[u]=v; return true` augment are reproduced
  exactly. MATCH.
- `distance[u] = HK_INFINITY; return false` (c2rust:227-228): MATCH.

## Safe-slice / range checks (port-only, design-justified)

C does unchecked pointer arithmetic on a *trusted* `short **` (`pair_vu[u_adj[i]]`,
`u_adj[1..=count]`). A safe slice cannot reproduce unchecked indexing without risk
of a panic, so the port adds explicit fallible checks routed through `types_error`:
`adjacency.len() <= u_size`, `count < 0 || row.len() < count+1`,
`v == 0 || v > v_size`. These fire only on inputs the C contract forbids and the
sole in-tree caller (`extract_rollup_sets`, planner.c) never produces, so observable
behavior is identical for every valid input. Documented in the crate header. This is
the standard safe-Rust analogue of C trusting the caller — not absent or simplified
logic.

## Seam audit

- Owned seam crates: enumerating `crates/X-seams` for every C file in this unit's
  `c_sources` (`bipartite_match.c` only) yields **none** — there is no
  `backend-lib-bipartite-match-seams` and no per-file seam crate for this TU. This
  is a pure leaf data structure that owns no inward seams. An empty/absent
  `init_seams()` is therefore correct (nothing to install), and no `seams-init`
  wiring is required. PASS.
- Outward seams consumed (both real cross-crate calls justified by ownership
  elsewhere, both thin "one call, propagate `?`" sites), both from
  `backend-tcop-postgres-seams` — the canonical install target whose owner
  (`backend-tcop-postgres`, status `todo`) holds `ProcessInterrupts` /
  `check_stack_depth`; this is the same seam crate the executor scan nodes and the
  parallel runtime consume `CHECK_FOR_INTERRUPTS` from:
  - `backend_tcop_postgres_seams::check_for_interrupts` — the
    `CHECK_FOR_INTERRUPTS()` body (`if InterruptPending ProcessInterrupts()`).
    Thin delegate. OK.
  - `backend_tcop_postgres_seams::check_stack_depth` — `check_stack_depth()`.
    Thin delegate. OK.
  No branching/node-construction/computation occurs on either seam path. (An
  earlier draft routed these through `backend-utils-init-miscinit-seams` /
  `backend-utils-misc-stack-depth-seams`; the miscinit `check_for_interrupts`
  declaration is owned by a *merged* crate that does not install it, which the
  `seams-init` recurrence_guard correctly flags as a latent runtime panic, so the
  consumer was moved to the `todo`-owner tcop seam crate that nothing-yet-installs
  legitimately, matching the repo convention.)

## Design conformance

- Opacity: no invented handles; `BipartiteMatchState` is the real C struct's
  caller-facing fields (scratch fields correctly dropped to the private context,
  mirroring that the C consumer only reads pair_uv/pair_vu/matching). No
  opacity introduced.
- `Mcx` + `PgResult`: the allocating path is fallible (`PgResult`), scratch arrays
  charged to an `mcx::MemoryContext`; OOM maps to `PgError`. Compliant.
- No shared statics, no ambient-global seams, no locks across `?`, no registry side
  tables.
- `#![forbid(unsafe_code)]`; zero `extern "C"`; zero `todo!()`/`unimplemented!()`.

## Gate

- `cargo check -p backend-lib-bipartite-match`: clean.
- `cargo test -p backend-lib-bipartite-match`: 12 passed (units + groupingsets.out
  golden parity).

## Verdict: PASS

Every C function MATCH; constants verified against the C/c2rust SHRT_MAX (32767);
no MISSING/PARTIAL/DIVERGES; no seam findings (pure leaf, owns none); design rules
satisfied.
