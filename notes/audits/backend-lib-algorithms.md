# Audit: backend-lib-algorithms

- **Unit:** backend-lib-algorithms
- **Effective C sources (per task + crate Cargo.toml):**
  `src/common/binaryheap.c` (+ `src/include/lib/binaryheap.h`) and
  `src/backend/lib/ilist.c` (+ `src/include/lib/ilist.h`).
- **c2rust:**
  - binaryheap: `c2rust-runs/common-extra-srv-batch5/src/binaryheap.rs`
  - ilist: `c2rust-runs/backend-lib-all/src/ilist.rs`
- **Port:**
  - aggregator: `crates/backend-lib-algorithms/src/lib.rs`
  - binaryheap leaf: `crates/backend-lib-binaryheap/src/lib.rs`
  - ilist leaf: `crates/backend-lib-ilist/src/lib.rs`
- **Verdict: PASS** (one DIVERGES finding found and fixed during this audit; re-audited clean.)

## Scope note

The `backend-lib-algorithms` crate in this repo is an **aggregator**: its
`lib.rs` is `#![no_std]` and contains only `pub mod binaryheap { pub use
backend_lib_binaryheap::*; } pub mod ilist { pub use backend_lib_ilist::*; }`
plus crate-root globs. It holds **zero algorithm logic** — to avoid two
near-identical copies it re-exports the canonical single ports living in the
dedicated leaf crates `backend-lib-binaryheap` and `backend-lib-ilist`. So the
function-by-function audit below targets those two leaf crates; the aggregator
itself has nothing to diverge.

(The unrelated stale `backend-lib-algorithms` CATALOG.tsv row listing
`bipartite_match/bloomfilter/integerset/knapsack/rbtree` is a different,
abandoned bundling; those files are each owned by their own audited leaf crates
— `backend-lib-bloomfilter`, `backend-lib-integerset`, `backend-lib-rbtree`,
etc. The task and the actual crate Cargo.toml both scope this unit to
binaryheap.c + ilist.c.)

## binaryheap.c — function inventory

binaryheap.c defines 11 functions: 8 extern (header `binaryheap.h:52-63`) + 3
`static inline` offset helpers + 2 file-static sift routines = 13 total entities.
Header macros `binaryheap_empty`/`binaryheap_size`/`binaryheap_get_node` are
also mirrored. The header-declared `bh_node_type`/`binaryheap_comparator`/`arg`
ABI maps to a generic payload `T` + a stored closure `C: FnMut(&T,&T)->i32`
preserving the three-way `>0/==0/<0` comparator contract verbatim.

| # | C function | C loc | Port loc | Verdict | Notes |
|---|-----------|-------|----------|---------|-------|
| 1 | `binaryheap_allocate` | binaryheap.c:38-54 | `allocate` | MATCH | `palloc(offsetof+sizeof*cap)` → `Vec::try_reserve_exact(cap)`, OOM→`PgError(ERRCODE_OUT_OF_MEMORY)`; space/has_heap_property=true/size=0. mctx-charge dropped (leaf convention, same as rbtree/pairingheap/bloomfilter). |
| 2 | `binaryheap_reset` | binaryheap.c:62-67 | `reset` | MATCH | size=0 (`nodes.clear()`, keeps capacity = C keeps palloc'd slots), has_heap_property=true. |
| 3 | `binaryheap_free` | binaryheap.c:74-78 | `free` | MATCH | `pfree` → consume + drop. |
| 4 | `left_offset` | binaryheap.c:89-93 | `left_offset` | MATCH | `2*i+1`. |
| 5 | `right_offset` | binaryheap.c:95-99 | `right_offset` | MATCH | `2*i+2`. |
| 6 | `parent_offset` | binaryheap.c:101-105 | `parent_offset` | MATCH | `(i-1)/2`. |
| 7 | `binaryheap_add_unordered` | binaryheap.c:115-129 | `add_unordered` | MATCH | size>=space → `out_of_slots` Err (C `elog(ERROR)`/`pg_fatal`); has_heap_property=false; push. |
| 8 | `binaryheap_build` | binaryheap.c:137-145 | `build` | MATCH | `for i=parent_offset(size-1); i>=0; i--` sift_down; has_heap_property=true. |
| 9 | `binaryheap_add` | binaryheap.c:153-167 | `add` | MATCH | capacity check; push; sift_up(size-1). |
| 10 | `binaryheap_first` | binaryheap.c:176-181 | `first` | MATCH | returns nodes[0]; `Assert(!empty && has_heap_property)` → `debug_assert!`. |
| 11 | `binaryheap_remove_first` | binaryheap.c:191-216 | `remove_first` | MATCH | size==1 fast path (pop); else result=nodes[0], nodes[0]=nodes[--size], sift_down(0). Realised with `pop` + `mem::replace` — identical final array. |
| 12 | `binaryheap_remove_node` | binaryheap.c:224-245 | `remove_node` | MATCH (after fix) | see detail below. |
| 13 | `binaryheap_replace_first` | binaryheap.c:254-263 | `replace_first` | MATCH | nodes[0]=d; if size>1 sift_down(0). |
| 14 | `sift_up` (static) | binaryheap.c:269-306 | `sift_up` | MATCH | hole-copy realised with `nodes.swap`; final array identical (proof below). |
| 15 | `sift_down` (static) | binaryheap.c:312-354 | `sift_down` | MATCH | hole-copy realised with `nodes.swap`; final array identical (proof below). |
| H | `binaryheap_empty/size/get_node` macros | binaryheap.h:65-67 | `is_empty/size/get_node` | MATCH | |

### sift_up / sift_down hole-vs-swap equivalence (re-derived)

C holds `node_val` in a register and shuffles a notional "hole": each step
copies the parent/child value into the hole slot and advances `node_off`,
filling `node_off` with `node_val` only at the end. The port uses
`nodes.swap(node_off, other)`. After a swap, `nodes[node_off]` holds the value
that C would have copied down, **and** `nodes[other]` holds `node_val` — exactly
the value the next iteration's comparisons use (the loop reads
`nodes[node_off]` as the sifted node, and C's cached `node_val` equals that
slot's value after the swap). The comparison operands on the other slots
(`parent_off`, `left_off`, `right_off`) are untouched by the swap, so every
branch decision matches C, and the final array is identical on every input.
Verified against the c2rust raw-pointer rendering line-by-line.

### binaryheap_remove_node — DIVERGES → fixed → MATCH

C: `cmp = bh_compare(bh_nodes[--bh_size], bh_nodes[n], bh_arg)` then
`bh_nodes[n] = bh_nodes[bh_size]`, then sift up/down by sign of cmp.
`Assert(n >= 0 && n < bh_size)` (pre-decrement) makes `n == size-1` a **valid
input** (remove the last node): after `--bh_size`, `bh_nodes[n]` aliases the
just-vacated `bh_nodes[bh_size]`, so `cmp == compare(x, x) == 0`, no sift fires,
and the write is a self-assignment into a vacated slot — net effect: drop the
last element.

The port as originally written did `let last = self.nodes.pop(); let cmp =
compare(&last, &self.nodes[n])`. On `n == size-1`, after `pop()` index `n`
equals the new `len`, so `self.nodes[n]` is **out of bounds → panic** — a
behavioral divergence on a valid C input.

**Fix applied** (`crates/backend-lib-binaryheap/src/lib.rs` `remove_node`):
compute `cmp` by index *before* the pop (`let last_idx = size-1; cmp =
compare(&nodes[last_idx], &nodes[n])`), then pop, and guard the
"place last into vacated entry" assignment with `if n != last_idx` (when they
alias, C's write is a no-op into a vacated slot and cmp==0 means no sift). For
all `n < size-1` the behavior is byte-identical to before/to C. Added a
regression test `remove_node_last_index`. Re-audited: now MATCH.

## ilist.c — function inventory

ilist.c defines exactly 4 functions (no statics, no in-TU inline helpers; the
init/push/insert/delete-current/iteration inline functions live in `ilist.h`
and are emitted into consumer crates, out of scope here). c2rust (built without
`ILIST_DEBUG`) kept only `slist_delete`, confirming the other three are
`#ifdef ILIST_DEBUG`-gated.

| # | C function | C loc | Gate | Port loc | Verdict | Notes |
|---|-----------|-------|------|----------|---------|-------|
| 1 | `slist_delete` | ilist.c:30-52 | always | `slist_delete` | MATCH | `&raw mut (*head).head` walk, NULL-break, pointer-equality splice, `last=cur`; `found` under `#[cfg(debug_assertions)]`+`debug_assert!`; trailing `slist_check` via `abort_on_corruption` (no-op unless `ilist_debug`). C-faithful raw-pointer intrusive port. |
| 2 | `dlist_member_check` | ilist.c:59-71 | `ILIST_DEBUG` | `dlist_member_check` | MATCH | open-coded const walk head.next..&head; found→Ok; miss→`elog(ERROR,"double linked list member check failure")`→Err. No-op Ok when feature off. |
| 3 | `dlist_check` | ilist.c:76-108 | `ILIST_DEBUG` | `dlist_check` | MATCH | NULL-head err; zeroed-head early Ok; forward + backward walks with the exact 5-way corruption predicate; errs match elog text. No-op Ok when feature off. |
| 4 | `slist_check` | ilist.c:113-127 | `ILIST_DEBUG` | `slist_check` | MATCH | NULL-head err; walk-to-end cycle check. No-op Ok when feature off. |

The struct types (`dlist_node`/`dlist_head`/`dclist_head`/`slist_node`/
`slist_head`) are `#[repr(C)]` field-for-field mirrors of `ilist.h`, byte-layout
verified by the `struct_layouts_match_c` test.

## Seam / wiring audit

- The aggregator and both leaf crates are **pure leaves owning no inward seams**.
  No `crates/*binaryheap*-seams`, `*ilist*-seams`, or `*algorithms*-seams` crate
  exists (verified). binaryheap.c (allocation) surfaces OOM/overflow as
  `PgResult` errors in-crate; ilist.c never allocates and has no error path on
  `slist_delete`.
- Correctly, none of the three crates define `init_seams()`, and none are
  referenced by `seams-init::init_all()` (verified). The `seams-init` recurrence
  guard test passes — a leaf owning no inward seam crates must not appear there.
- No outward seam calls, no `todo!`/`unimplemented!`, no own-logic stubs, no
  deferred/SEAMED-equivalent escapes. The aggregator does not even introduce
  opacity; it re-exports the canonical types.

## Design conformance

- Allocating routine (`binaryheap_allocate`) returns `PgResult` and uses
  fallible `try_reserve_exact`; OOM → `ERRCODE_OUT_OF_MEMORY` (no `Mcx` needed —
  leaf convention matching rbtree/pairingheap/bloomfilter; mctx-charge model
  intentionally dropped, no mctx leaf in this dependency cone).
- `binaryheap` impl is `#![forbid(unsafe_code)]`, 0 unsafe. `ilist` is a
  sanctioned C-faithful raw-pointer intrusive port (same class as
  `backend-lib-dshash`/`dynahash`): the intrusive shared-aliasing link graph is
  inherent to the C contract, opacity is inherited not invented.
- No shared statics for per-backend globals, no ambient-global seams, no
  registry side tables, no locks across `?`, no unledgered divergence markers.

## Gates

- `cargo check --workspace` — clean (only pre-existing unrelated warnings).
- `cargo test -p backend-lib-algorithms` — pass (re-exports; 0 own tests).
- `cargo test -p backend-lib-binaryheap` — 8 pass (added `remove_node_last_index`).
- `cargo test -p backend-lib-ilist` — 6 pass.
- `cargo test -p seams-init` — 2 pass (recurrence/wiring guards green).

## Verdict: PASS
