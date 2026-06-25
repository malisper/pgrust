# Audit: backend-lib-pairingheap

Independent function-by-function audit of the Rust port against the original
PostgreSQL 18.3 C (`src/backend/lib/pairingheap.c` + `src/include/lib/pairingheap.h`)
and the c2rust rendering (`c2rust-runs/backend-lib-all/src/pairingheap.rs`).

## Function inventory & verdicts

| # | C function | C loc | Port location (lib.rs) | Verdict | Notes |
|---|------------|-------|------------------------|---------|-------|
| 1 | `pairingheap_allocate` | pairingheap.c:42 | `pairingheap_allocate` | MATCH | C `palloc`s the control struct and sets `ph_compare`/`ph_arg`/`ph_root=NULL`. Port constructs the struct with `root = NONE` (== `NULL`), the comparator closure standing in for `ph_compare`+`ph_arg` (the closure captures what C passes via `arg`). The arena spines start empty. Idiomatic-allocation difference only; behaviour identical. |
| 2 | `pairingheap_free` | pairingheap.c:63 | `Drop for PairingHeap` (implicit) | MATCH | C `pfree`s only the control struct ("the nodes in the heap are not freed"). Port's arena `Vec`s free themselves on drop; payloads in live slots drop with the `Vec`. Same control-struct teardown; node storage is caller-owned in C, arena-owned here (the documented arena adaptation). |
| 3 | `merge` (static) | pairingheap.c:79 | `merge` | MATCH | Line-for-line: NULL short-circuits (`a==NONE`/`b==NONE`), `ph_compare(a,b,arg) < 0` → `(comparator)(value(a),value(b)) == Less` swap, then the exact 4-step child-link splice (`a->first_child->prev_or_parent=b`; `b->prev_or_parent=a`; `b->next_sibling=a->first_child`; `a->first_child=b`). Returned node's `next_sibling`/`prev_or_parent` left garbage, as in C. |
| 4 | `pairingheap_add` | pairingheap.c:112 | `add` + `add_node` | MATCH | `node->first_child=NULL`; `ph_root=merge(ph_root,node)`; root's `prev_or_parent`/`next_sibling`=NULL — all transcribed. `add` allocates the slot (the C caller's node storage) then runs the C body in `add_node`. Allocation failure surfaces `ERRCODE_OUT_OF_MEMORY` (C `palloc` analogue). |
| 5 | `pairingheap_first` | pairingheap.c:130 | `first` | MATCH | C asserts non-empty and returns `ph_root`. Port returns `Some(&value(root))` when `root != NONE`, `None` otherwise (the safe analogue of the C Assert+deref). O(1). |
| 6 | `pairingheap_remove_first` | pairingheap.c:145 | `remove_first` + `remove_first_node` | MATCH | `result=ph_root`; `children=result->first_child`; `ph_root=merge_children(children)`; if non-NULL clear root's `prev_or_parent`/`next_sibling`; return old root. Transcribed exactly; wrapper guards empty heap (C Assert) and recycles the old slot, returning the moved-out payload. |
| 7 | `pairingheap_remove` | pairingheap.c:170 | `remove` + `remove_node` | MATCH | Root case delegates to `remove_first`. Else: capture `children`/`next_sibling`; locate the back-pointer (`prev_or_parent->first_child==node ? first_child : next_sibling`) via `prev_is_first_child`; the `if (children)` branch (merge_children replacement, splice `prev_ptr`, fix `next_sibling->prev_or_parent`) and the `else` unlink branch both transcribed exactly. C `Assert(*prev_ptr==node)` mirrored by `debug_assert_eq!`. Stale-handle guard is the safe-API addition for C's UB-on-double-remove. |
| 8 | `merge_children` (static) | pairingheap.c:234 | `merge_children` | MATCH | Early return for 0/1 element (`children==NULL || children->next_sibling==NULL`). First pass: `for(;;)` pairing loop with the odd-tail special case (`curr->next_sibling==NULL` → prepend to `pairs`, break), `next=curr->next_sibling->next_sibling`, `curr=merge(curr,curr->next_sibling)`, prepend to `pairs`. Second pass: walk `pairs` merging into `newroot`. Loop bounds, ordering, and the two-pass strategy match line-for-line. |
| 9 | `pairingheap_dump_recurse` | pairingheap.c:296 | — | NOT BUILT | Inside `#ifdef PAIRINGHEAP_DEBUG` (header line 17 leaves it undefined). Absent from the c2rust post-preprocessor output, confirming it is not in any normal build. Correctly omitted. |
| 10 | `pairingheap_dump` | pairingheap.c:318 | — | NOT BUILT | Same `#ifdef PAIRINGHEAP_DEBUG` guard; absent from c2rust. Correctly omitted. |

### Macros (pairingheap.h)

| Macro | Header loc | Port | Verdict |
|-------|-----------|------|---------|
| `pairingheap_reset(h)` | :93 (`(h)->ph_root = NULL`) | `reset` | MATCH — sets `root = NONE`; additionally frees arena-owned payloads (C's caller owns node storage, so its macro only nulls the root; the arena must free to avoid leaking). |
| `pairingheap_is_empty(h)` | :96 | `is_empty` | MATCH — `root == NONE` ≡ `ph_root == NULL`. |
| `pairingheap_is_singular(h)` | :99 | `is_singular` | MATCH — `root != NONE && first_child(root) == NONE` ≡ `ph_root && ph_root->first_child == NULL`. |

## Constants

The file defines no OIDs, NodeTags, flag bits, or magic numbers. The sole
sentinel is `NONE = usize::MAX`, the index analogue of C's `NULL` link — used
uniformly and never compared as a real index. No transcribed tables. Nothing to
mis-copy.

## Seam audit

This unit's `c_sources` is `*/pairingheap.c` only. No `crates/pairingheap-seams`
(or any per-file seam crate for this unit) exists — correct: a pure leaf data
structure owns no inward seams, and the port makes zero outward seam calls (its
only dependency is `types-error` for the `PgResult`/`PgError`/`ERRCODE_OUT_OF_MEMORY`
vocabulary, a direct dep with no cycle). `init_seams()` is therefore correctly
absent and nothing needs wiring into `seams-init`. No seam findings.

## Design conformance

- No invented opacity: `PairingHeap`/`Node`/`PairingHeapHandle` are real concrete
  types; `PairingHeapHandle` is the safe analogue of a `pairingheap_node *`, not a
  stand-in for an unported type.
- The single allocating operation (`add`) returns `PgResult`; OOM is raised with
  `ERRCODE_OUT_OF_MEMORY` via `try_reserve`, faithfully modelling C's `palloc`
  failure / `ereport(ERROR)` non-local exit. No allocating path lacks the error
  return.
- `#![forbid(unsafe_code)]`; no raw pointers, no `extern "C"`, no shared statics,
  no ambient-global seams, no locks.
- No `todo!`/`unimplemented!`/`unreachable!`; no logic replaced by a seam call.

## Verdict: PASS

Every built C function is MATCH; the two un-built `PAIRINGHEAP_DEBUG` functions
are correctly omitted (confirmed absent from c2rust). No seam findings, no design
violations. 12 crate tests pass.
