# Audit: backend-lib-rbtree (safe arena rewrite)

C source: `src/backend/lib/rbtree.c` (+ `src/include/lib/rbtree.h`)
c2rust reference: `../pgrust/c2rust-runs/backend-lib-all/src/rbtree.rs`
Port: `crates/backend-lib-rbtree/src/lib.rs`

Independent re-derivation from C + headers. This port **replaces** the earlier
C-faithful raw-pointer transcription (which SIGABRTed/null-deref'd under the
concurrent stress test). It is a memory-safe **index-based (arena) tree**:
`RBTree<T, C>` owns a `Vec<Node<T>>` slab; `parent`/`left`/`right` are `usize`
slot indices; slot 0 (`SENTINEL`) is the single shared `RBTNIL`, used both for
"NULL parent of root" and "RBTNIL child" exactly as C uses its mutable
`sentinel` static for both roles. The comparator is a closure (C's
`rbt_comparator` fn ptr); the combiner is a per-insert closure (C's `combiner`).
`#![forbid(unsafe_code)]`; zero `unsafe`, `*mut`, `*const`, `Box::into_raw`,
`extern "C"`.

## Constants (verified against headers)

| Constant | C value | Port | Verdict |
| --- | --- | --- | --- |
| `RBTBLACK` | `0` (rbtree.c:35) | `Color::Black` | MATCH |
| `RBTRED` | `1` (rbtree.c:36) | `Color::Red` | MATCH |
| `LeftRightWalk` | enum ord 0 (rbtree.h) | `RBTOrderControl::LeftRightWalk` | MATCH |
| `RightLeftWalk` | enum ord 1 (rbtree.h) | `RBTOrderControl::RightLeftWalk` | MATCH |
| sentinel init | color=BLACK, left/right=RBTNIL, parent=NULL (rbtree.c:63-66) | slot 0: Black, left/right/parent = SENTINEL | MATCH |

## Function inventory & verdicts

| C function (line) | Port location | Verdict | Notes |
| --- | --- | --- | --- |
| `sentinel` static (63) | `rbt_create_with` slot-0 push | MATCH | One shared sentinel = slot 0; black, self-pointing links. |
| `rbt_create` (102) | `rbt_create`/`rbt_create_with` | MATCH | root=SENTINEL, comparator stored. `palloc`(control)→owned struct; `node_size`/allocfunc/freefunc/arg dropped (closure/arena model). Sentinel-alloc OOM panics, mirroring C palloc-fatal. |
| `rbt_copy_data` (127) | inline in `delete_node` (`if y != z`) | MATCH | C `memcpy(dest+1, src+1, node_size-sizeof(RBTNode))` → owned-value move `nodes[z].value = nodes[y].value.take()`; no clone, no double-drop. |
| `rbt_find` (145) | `find_node` + `rbt_find` | MATCH | cmp==0 return; <0 left; >0 right. NULL→`None`. |
| `rbt_find_great` (172) | `find_great_node` + `rbt_find_great` | MATCH | equal_match short-circuit; <0 record greater+go left; else right. |
| `rbt_find_less` (203) | `find_less_node` + `rbt_find_less` | MATCH | equal_match short-circuit; >0 record lesser+go right; else left. |
| `rbt_leftmost` (235) | `leftmost_node` + `rbt_leftmost` | MATCH | descend left; RBTNIL→None. |
| `rbt_rotate_left` (263) | `rotate_left` | MATCH | line-for-line; `if (x->parent)` → `parent(x) != SENTINEL`; root reassign on NULL parent. |
| `rbt_rotate_right` (300) | `rotate_right` | MATCH | mirror, line-for-line. |
| `rbt_insert_fixup` (344) | `insert_fixup` | MATCH | both uncle-red/uncle-black branches + mirror image; final root→BLACK. |
| `rbt_insert` (453) | `insert` + `rbt_insert` | MATCH | descend; cmp==0 → combiner + `Ok(false)` (*isNew=false); else alloc RED (links/parent=NIL), copy_data, link to parent or root, fixup, `Ok(true)`. |
| `rbt_delete_fixup` (521) | `delete_fixup` | MATCH | both left/right symmetric branches, sibling-recolor/rotate, line-for-line; final `x`→BLACK. |
| `rbt_delete_node` (619) | `delete_node` | MATCH | z/successor selection, x=y's only child, splice y out (writes `parent(x)` even when x==SENTINEL, mirroring C writing the shared sentinel), `y!=z` payload move, black→delete_fixup, free/recycle y. See note 1. |
| `rbt_delete` (695) | `rbt_delete` | MATCH | C void wrapper over delete_node; port is find+delete_node returning the removed value (note 1). NULL/RBTNIL guard preserved (find miss → `Ok(None)`). |
| `rbt_left_right_iterator` (704) | `left_right_iterator` | MATCH | NULL-last_visited start vs `!started`; descend-left, right-subtree, up-until-from-left; sets is_over. |
| `rbt_right_left_iterator` (746) | `right_left_iterator` | MATCH | mirror. |
| `rbt_begin_iterate` (802) | `rbt_begin_iterate` | MATCH | last_visited=NULL→`started=false`; is_over = root==RBTNIL; dispatch by order. default/elog arm: note 2. |
| `rbt_iterate` (826) | `rbt_iterate` | MATCH | is_over→NULL/None; else dispatch; NIL→None. |

### Note 1 — delete return value (typed-wrapper divergence, behavior-equivalent)

C `rbt_delete` returns `void`; the caller already holds the node and its payload
lives in caller-owned storage. The Rust API owns the payloads, so `rbt_delete`
returns the removed `T` by move: `delete_node` `take()`s the logical value at
`z` and (when `y != z`) moves `y`'s value into `z`'s slot — the exact analog of
C's `rbt_copy_data` memcpy + `freefunc`-never-runs-a-destructor contract. No
clone, no double-drop, no leak (proven by
`delete_moves_payload_without_clone_or_double_drop`).

### Note 2 — `rbt_begin_iterate` default/`elog(ERROR)` arm (unrepresentable, not MISSING)

C's `switch (ctrl)` has a `default: elog(ERROR, "unrecognized rbtree iteration
order: %d", ctrl)` reachable only when an out-of-range `RBTOrderControl` int is
passed. The port models `RBTOrderControl` as a closed 2-variant Rust enum, so
the `match` is exhaustive and the error condition is unrepresentable at the type
level — the C error fires under a predicate (invalid enum int) that cannot occur
here. This is the sanctioned enum-narrowing analog, not absent logic. (The prior
raw-pointer port kept an `elog(ERROR)?` arm because it took the order as a wider
type; the safe rewrite's typed enum subsumes it.)

## Sanctioned divergences (audit against these)

1. **Raw-pointer intrusive tree → safe arena.** C threads aliasing `RBTNode *`
   through caller storage with a shared mutable `RBTNIL` static. The port owns a
   `Vec<Node<T>>` slab and uses `usize` slot links with slot 0 as the shared
   sentinel. Identical algorithm; memory-safe by construction.
2. **`palloc`/`allocfunc`/`freefunc`/`arg`/`node_size` → owned arena + closures.**
   No caller-supplied allocator; the arena's `Vec` owns nodes, `free` recycles
   slots. Node-alloc OOM is surfaced as `PgResult` via `Vec::try_reserve`
   (modelling C `allocfunc`'s `palloc`-ereport), so insert/alloc keep their C
   failure surface. Sentinel-alloc OOM in `rbt_create` panics (C palloc-of-control
   is fatal).
3. **`backend-utils-mctx` charge/free model dropped.** The upstream src-idiomatic
   tree charges its slabs to a crate-internal `MemoryContext`. This repo has no
   `backend-utils-mctx` leaf and its mcx owner (`mcx`) differs; following the same
   decision recorded for `backend-utils-activity-waitevent`, the slabs are plain
   `alloc::vec::Vec`s. `try_reserve` preserves the OOM-fallible signatures.
   `types-core` dependency removed (now unused).
4. **`rbt_delete` returns the removed value** (note 1).
5. **`elog(ERROR)` iteration-order arm eliminated by typed enum** (note 2).

## Seam audit

Pure leaf. Owns no `crates/*-seams` mapping to `rbtree.c`. No outward seam calls
(only `backend_utils_error::elog`/`types_error` direct deps for the alloc-OOM
`PgResult`). `init_seams()` not applicable; `seams-init::init_all()` does not
reference this crate. `seams-init` recurrence guards
(`every_seam_installing_crate_is_wired_into_init_all`,
`every_declared_seam_is_installed_by_its_owner`) pass. No findings.

## Memory-safety / design conformance

- `#![forbid(unsafe_code)]` present and holds: zero `unsafe`/`*mut`/`*const`/
  `Box::into_raw`/`extern "C"` in `src/` (grep clean; only doc-comment mentions).
- Allocating paths return `PgResult` (no allocating fn without a fallible
  surface). No shared statics, no ambient-global seams, no locks across `?`.
- SIGABRT regression: the concurrent `stress_insert_delete_keeps_order` (and the
  larger `large_stress_matches_sorted_reference`) pass; 30 release runs at
  `--test-threads=16` all green. The null-deref is gone (no raw pointers remain).

## Verdict: PASS

Every C function MATCH (with notes 1-2 documenting behavior-equivalent
typed-wrapper / enum-narrowing divergences); no MISSING/PARTIAL/DIVERGES; zero
seam findings; `forbid(unsafe_code)` holds. 10/10 tests pass, including the
concurrency stress and payload-ownership (no clone / no double-drop / no leak)
tests. Ready to merge.
