# Audit: backend-lib-integerset

Unit: `backend-lib-integerset`
C source: `src/backend/lib/integerset.c` (+ `src/include/lib/integerset.h`)
c2rust reference: `c2rust-runs/backend-lib-all/src/integerset.rs`
Port: `crates/backend-lib-integerset/src/lib.rs`

Independent audit: function inventory re-derived from the C source and
cross-checked against the c2rust rendering; each function compared C / c2rust /
port. This is a self-contained leaf data structure (no SQL, no shared memory,
no catalog).

## Constants (verified against C header + .c, not from memory)

| Constant | C value | Port value | Verdict |
|---|---|---|---|
| `SIMPLE8B_MAX_VALUES_PER_CODEWORD` | 240 | 240 | MATCH |
| `MAX_INTERNAL_ITEMS` | 64 | 64 | MATCH |
| `MAX_LEAF_ITEMS` | 64 | 64 | MATCH |
| `MAX_TREE_LEVELS` | 11 | 11 | MATCH |
| `MAX_VALUES_PER_LEAF_ITEM` | 1+240 = 241 | 1+240 | MATCH |
| `MAX_BUFFERED_VALUES` | 241*2 = 482 | *2 | MATCH |
| `EMPTY_CODEWORD` | 0x0FFFFFFFFFFFFFFF | 0x0FFF_FFFF_FFFF_FFFF | MATCH |
| `simple8b_modes[17]` | see C lines 826-847 | SIMPLE8B_MODES | MATCH (all 17 rows: bits_per_int & num_ints identical, incl. sentinel {0,0}) |

## Function inventory & verdicts

| C fn (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `intset_create` (282) | `IntegerSet::create` + `intset_create()` | MATCH | C palloc → owned `Self`; `mem_used = sizeof(IntegerSet)` mirrors `GetMemoryChunkSpace(intset)`. Field init order/values identical. |
| `intset_new_internal_node` (314) | `IntegerSet::intset_new_internal_node` | MATCH | level=0 (caller sets), num_items=0; `mem_used += sizeof(InternalNode)`. OOM via `context.oom` → real PgError (models palloc ereport). |
| `intset_new_leaf_node` (329) | `IntegerSet::intset_new_leaf_node` | MATCH | level field elided (NodeRef tags arena); num_items=0, next=NULL→None; mem_used charged. |
| `intset_num_entries` (348) | `num_entries()` / `intset_num_entries` | MATCH | returns field. |
| `intset_memory_usage` (357) | `memory_usage()` / `intset_memory_usage` | MATCH | returns `mem_used`. |
| `intset_add_member` (368) | `add_member` / `intset_add_member` | MATCH | both elog(ERROR) guards (iter_active; out-of-order with num_entries>0) → PgError::error, same predicates/order/text. Buffer flush threshold, buffer append, counters all identical. |
| `intset_flush_buffered_values` (394) | `intset_flush_buffered_values` | MATCH | first-leaf-as-root creation, `while num_values-num_packed >= MAX_VALUES_PER_LEAF_ITEM`, leaf-full new-leaf+link+update_upper, item append, `num_packed += 1+num_encoded`, memmove→`copy_within`, `num_buffered_values -= num_packed`. Arena-index links replace pointers; semantics identical. |
| `intset_update_upper` (479) | `intset_update_upper` | MATCH | `level >= num_levels` root-grow; MAX_TREE_LEVELS guard → same elog text/SQLSTATE; downlink_key from leaf.items[0].first vs internal.values[0] (C branches on `root->level==0`); parent-fits vs new-parent recursion identical. The `oldroot` None case is impossible in C (root set before reaching here); port returns an internal PgError instead of deref-NULL — behavior-preserving on all reachable inputs. |
| `intset_is_member` (552) | `is_member` / `intset_is_member` | MATCH | buffer fast-path (binsrch nextkey=false, ==x), root-NULL→false, descent loop `level=num_levels-1; level>0` with binsrch nextkey=true and itemno==0→false, leaf binsrch nextkey=true, first==x, simple8b_contains. All branches/returns mirrored. |
| `intset_begin_iterate` (622) | `begin_iterate` / `intset_begin_iterate` | MATCH | iter_active=true, iter_node=leftmost_leaf, itemno/valueno/num_values=0, iter_values→buf modeled by IterSource::DecodeBuf. |
| `intset_iterate_next` (641) | `iterate_next` / `intset_iterate_next` | MATCH | the C `for(;;)` with 4 stages reproduced exactly: emit pending; decode next leaf item (num_decoded+1); step to next leaf; flip to buffered_values once (DecodeBuf→Buffered); break→iter_active=false. `*next=0` on end preserved in free-fn wrapper. The C "step to next node" guard `if (iter_node)` vs the decode guard is faithfully split (decode when itemno in range; else step). |
| `intset_binsrch_uint64` (712) | `intset_binsrch_uint64` | MATCH | identical low/high/mid loop, nextkey >=/> on `arr[mid]`. |
| `intset_binsrch_leaf` (745) | `intset_binsrch_leaf` | MATCH | same as above on `arr[mid].first`. |
| `simple8b_encode` (871) | `simple8b_encode` | MATCH | mode-selection loop (selector/nints/bits/diff/last_val/i) byte-for-byte; `nints==0`→(EMPTY_CODEWORD,0); reverse-shift pack when bits>0; `selector<<60`. Returns (codeword, nints) ≡ C return + `*num_encoded`. |
| `simple8b_decode` (973) | `simple8b_decode` | MATCH | selector/nints/bits/mask computed before EMPTY check (selector=0 for EMPTY → harmless), early return 0; loop curr_value += 1+diff, decoded[i], shift. `mask` via wrapping_sub equals `(1<<bits)-1` for all bits in [0,60]. |
| `simple8b_contains` (1002) | `simple8b_contains` | MATCH | EMPTY→false; bits==0 special `(key-base)<=nints`; else loop with `curr_value>=key` ⇒ `==key`. Identical short-circuit. |
| `simple8b_modes[]` table (820) | `SIMPLE8B_MODES` | MATCH | see constants table. |
| `IntegerSet`, `intset_node`/`intset_internal_node`/`intset_leaf_node`, `leaf_item` structs | `IntegerSet`, `InternalNode`/`LeafNode`/`NodeRef`, `LeafItem` | MATCH | raw `intset_node*` links → tagged arena indices (`NodeRef`, `Option<LeafIdx>`); common-header level/num_items kept per-node; no invented opacity. |

## Seam audit

No owned seam crate. The unit's only C file is `integerset.c`; there is no
`crates/integerset-seams` / `backend-lib-integerset-seams`. A pure leaf data
structure owns no inward seams. No outward seam calls: the crate depends only
on `mcx` (MemoryContext) and `types-error` (PgError/PgResult), both ported
leaves — no dependency cycle, so direct deps are correct. Consequently there is
no `init_seams()` and the crate is correctly NOT wired into `seams-init`
(`grep` confirms absence). This matches the documented "pure leaf owns none —
empty/none is correct" rule.

## Design conformance

- Opacity: inherited only. Raw pointers became real `InternalNode`/`LeafNode`
  structs in owned arenas with tagged-index links; no invented handles. (types.md 6-7 OK.)
- Mcx + PgResult: every allocating/erroring fn returns `PgResult`; OOM routed
  through the owning `MemoryContext` (`context.oom`) as a real `PgError`,
  modelling palloc's `ereport(ERROR)`. No allocating seam.
- No shared statics for per-backend globals (none exist; the set is a value).
- No `todo!()`/`unimplemented!()`/`unreachable!()`-as-logic: `unreachable!()`
  appears only on arena-tag invariants that hold by construction (mirrors C's
  unconditional pointer downcasts after a level check), not as deferred logic.
- Error text & SQLSTATE: all three elog(ERROR) sites reproduced verbatim;
  `PgError::error` defaults to ERRCODE_INTERNAL_ERROR (XX000) = C `elog(ERROR)`.
  Verified by test `out_of_order_error_uses_internal_sqlstate`.

## Memory-accounting note (ledgered divergence, behavior-preserving)

The src-idiomatic counterpart used a context-coupled `PgVec` (push-charges-context).
The fabled `mcx::PgVec<'mcx,T>` is lifetime-bound and cannot back an owned-by-value
`IntegerSet` without `McxOwned` plumbing that would change the public API shape.
The arenas are therefore plain `alloc::vec::Vec` grown via `try_reserve` (OOM →
`context.oom`). The externally-observable figure `intset_memory_usage()`
(`mem_used`) is computed exactly as C does (`GetMemoryChunkSpace` analog: struct
sizes). The context's internal `used()` counter is not fed by arena growth, but
no C caller reads it and integerset.c itself "doesn't do anything with mem_used"
beyond exposing it. Behavior on every documented input is identical.

## Verdict

**PASS** — every function MATCH; constants verified against the C header/.c;
no MISSING/PARTIAL/DIVERGES; no seam findings (pure leaf, no owned seams, no
outward seams, correctly absent from seams-init). `cargo check --workspace`,
`cargo test -p backend-lib-integerset` (10/10), `cargo test -p seams-init`
all green.
