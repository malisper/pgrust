# Audit: backend-access-heap-hio

- **Unit:** `backend-access-heap-hio`
- **C source:** `src/backend/access/heap/hio.c` (885 lines)
- **c2rust:** `../pgrust/c2rust-runs/backend-access-heap-hio/src/hio.rs`
- **Port:** `crates/backend-access-heap-hio/src/lib.rs`
- **Seams:** `crates/backend-access-heap-hio-seams/src/lib.rs`
- **Branch:** `port/backend-access-heap-hio`
- **Verdict:** **PASS**

This audit was derived independently from the C and the c2rust rendering; the
port's own comments were not trusted.

## 1. Function inventory

`hio.c` defines exactly 5 functions. All 5 are present in the c2rust run and in
the port. No statics/inline helpers exist beyond these.

| # | C function (line) | C linkage | Port location | Verdict |
|---|---|---|---|---|
| 1 | `RelationPutHeapTuple` (35) | extern | `lib.rs:124` | MATCH |
| 2 | `ReadBufferBI` (87) | static | `lib.rs:177` | MATCH |
| 3 | `GetVisibilityMapPins` (139) | static | `lib.rs:231` | MATCH |
| 4 | `RelationAddBlocks` (237) | static | `lib.rs:327` | MATCH |
| 5 | `RelationGetBufferForTuple` (501) | extern | `lib.rs:494` | MATCH |

## 2. Per-function comparison

### RelationPutHeapTuple — MATCH
- Both `Assert`s ported as `debug_assert!` (token⇒speculative; the
  `HEAP_XMAX_COMMITTED & HEAP_XMAX_IS_MULTI` corruption guard). Flag values
  verified against `htup_details.h`: `HEAP_XMAX_COMMITTED=0x0400`,
  `HEAP_XMAX_IS_MULTI=0x1000` (`types-tuple` matches).
- `PageAddItem(... InvalidOffsetNumber, false, true)` → `page_add_item` seam;
  failure (`offnum == InvalidOffsetNumber`) → `elog(PANIC, "failed to add tuple
  to page")` mapped to `ereport(PANIC).errmsg_internal(...)`. Severity and text
  match. `!!! EREPORT(ERROR) DISALLOWED !!!` honored — PANIC, not ERROR.
- `ItemPointerSet(&t_self, BufferGetBlockNumber(buffer), offnum)` and the
  non-speculative `item->t_ctid = t_self` write (`set_stored_tuple_ctid`) both
  preserved, gated by `!token`. `SpecTokenOffsetNumber=0xfffe` verified vs
  `itemptr.h`.

### ReadBufferBI — MATCH
- `!bistate` ⇒ `ReadBufferExtended(..., NULL)` early return. Match.
- `current_buf != InvalidBuffer` re-pin path: block-number compare, the
  `RBM_ZERO_AND_LOCK`/`RBM_ZERO_AND_CLEANUP_LOCK` debug-assert, `IncrBufferRefCount`
  + return; else `ReleaseBuffer` + clear. Match. `RBM_*` constants verified vs
  `bufmgr.h` enum order (`RBM_NORMAL=0`, `RBM_ZERO_AND_LOCK=1`,
  `RBM_ZERO_AND_CLEANUP_LOCK=2`; `types-storage` matches).
- Strategy read: `bistate->strategy` (a pointer) crosses as `has_strategy =
  strategy.is_set()`; `ReadBufferExtended` then `IncrBufferRefCount` + store.
  Match.

### GetVisibilityMapPins — MATCH
- The pointer swap (`buffer1↔buffer2`, `vmbuffer1↔vmbuffer2`, `block1↔block2`)
  under `!BufferIsValid(buffer1) || (BufferIsValid(buffer2) && block1 > block2)`
  is modeled with `core::mem::swap` over locals + a `swapped` flag; the
  out-params `*vmbuffer1`/`*vmbuffer2` are copied back with the same aliasing the
  C produces (swapped ⇒ vmbuf2→*vmbuffer1, vmbuf1→*vmbuffer2). Behaviorally
  identical for both the single-buffer and lock-ordering cases.
- The `while(1)` loop: `need_to_pin_buffer1/2` predicates
  (`PageIsAllVisible && !visibilitymap_pin_ok`), the `break` when neither is
  needed, the unlock-both / pin / relock sequence (with the
  `buffer2 != buffer1` guards), and the second-pass `break` condition
  (`buffer2==Invalid || buffer1==buffer2 || (need1 && need2)`) all match
  exactly. `released_locks` returned. The `visibilitymap_pin` seam returns the
  (possibly newly pinned) vm buffer, mirroring the C `&vmbuffer` out-param.

### RelationAddBlocks — MATCH
- `extend_by_pages` computation: `(!bistate && !use_fsm) ⇒ 1`, else
  `num_pages`, `+= ebp*waitcount` (waitcount=0 when `RELATION_IS_LOCAL`),
  `Max(.., already_extended_by)` when bistate, `Min(.., MAX_BUFFERS_TO_EXTEND_BY)`.
  `MAX_BUFFERS_TO_EXTEND_BY=64` matches the local `#define`. Overflow modeled
  with `wrapping_*`/`max`/`min`, matching C unsigned arithmetic.
- `not_in_fsm_pages`: `(num_pages>1 && !bistate) ⇒ 1 else num_pages`. Match.
- bistate `current_buf` release before extending. Match.
- `ExtendBufferedRelBy(BMR_REL, MAIN_FORKNUM, strategy, EB_LOCK_FIRST,
  extend_by, victim_buffers, &extend_by)` → `extend_buffered_rel_by` seam
  returning `ExtendedRelation{first_block, victim_buffers, extended_by}`;
  `buffer = victim_buffers[0]`, `last_block = first_block + (extend_by-1)`. Match.
- `PageIsNew` check → `elog(ERROR, "page %u of relation \"%s\" should be empty
  but is not", first_block, RelationGetRelationName)`. Mapped to `ereport(ERROR)`
  with the same format. (No SQLSTATE in the C → `errmsg_internal`; matches the
  repo convention for `elog(ERROR)`.)
- `PageInit` + `MarkBufferDirty`; FSM-unlock branch sets `*did_unlock`; the
  release-pins loop (`i=1..extend_by`) with the `i>=not_in_fsm_pages`
  `RecordPageWithFreeSpace(freespace = pageSize - SizeOfPageHeaderData)`; the
  `FreeSpaceMapVacuumRange`; the bistate `next_free/last_free/current_buf/
  already_extended_by` bookkeeping — all match. `SizeOfPageHeaderData=24`
  verified.

### RelationGetBufferForTuple — MATCH
- `use_fsm = !(options & HEAP_INSERT_SKIP_FSM)` (`0x0002`); `HEAP_INSERT_FROZEN`
  = `0x0004`; both verified through `heapam.h`→`tableam.h`.
- `len = MAXALIGN(len)` (8-byte), `num_pages<=0 ⇒ 1`, the
  `otherBuffer==Invalid || !bistate` assert.
- Oversize check `len > MaxHeapTupleSize` → `ereport(ERROR,
  errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), "row is too big: size %zu, maximum
  size %zu")`. SQLSTATE + text match. `MaxHeapTupleSize = BLCKSZ -
  MAXALIGN(SizeOfPageHeaderData + sizeof(ItemIdData)) = 8192 - 32 = 8160`
  verified (`ItemIdData` is `u32`, BLCKSZ=8192).
- `saveFreeSpace = RelationGetTargetPageFreeSpace(rel, HEAP_DEFAULT_FILLFACTOR)`
  (`=100`). `nearlyEmptyFreeSpace = MaxHeapTupleSize - (MaxHeapTuplesPerPage/8 *
  sizeof(ItemIdData))`; `MaxHeapTuplesPerPage=291` verified. `targetFreeSpace`
  branch (`Max(len, nearlyEmpty)` vs `len+save`) matches.
- Target-block selection (bistate.current_buf ⇒ its block; else
  `RelationGetTargetBlock`; FSM `GetPageWithFreeSpace`; last-page fallback
  `nblocks-1`) matches. The `nblocks-1` is reached only when `nblocks>0`, so no
  underflow.
- The `loop:`/`while` page-selection body: all four lock-ordering branches
  (otherBuffer invalid / otherBlock==target / otherBlock<target / else), the
  `PageIsAllVisible⇒visibilitymap_pin`, the `HEAP_INSERT_FROZEN && maxoff==0`
  extra pin (invalid-other branch only, as in C), `GetVisibilityMapPins`, the
  `PageIsNew⇒PageInit+MarkBufferDirty`, the `targetFreeSpace<=pageFreeSpace`
  success (`RelationSetTargetBlock` + return), the give-up unlock/release, and
  the three continuation arms (ongoing bulk extension with
  `next_free/last_free` advance; `!use_fsm⇒break`; else
  `RecordAndGetPageWithFreeSpace`) all match branch-for-branch.
- Post-loop extend path: `RelationAddBlocks`, the `HEAP_INSERT_FROZEN` vm-pin
  (with `unlockedTargetBuffer` handling), the reacquire-locks logic
  (`unlocked` ⇒ relock other+target; else otherBuffer valid ⇒
  `ConditionalLockBuffer` with the unlikely fallback), the `recheckVmPins`
  `GetVisibilityMapPins` call, the `len>pageFreeSpace` retry (`goto loop` ⇒
  `continue 'outer`) vs `elog(PANIC, "tuple is too big: size %zu")`, and the
  final `RelationSetTargetBlock` + return. All match; the `goto loop` is a
  faithful `continue 'outer`.

## 3. Seam & wiring audit

Owned seam crate (by C-source coverage): `backend-access-heap-hio-seams` only.
Every declaration in it is **OUTWARD** — it is a function `hio.c` reaches in a
crate across a real dependency cycle:

- bufmgr.c: `lock_buffer`, `conditional_lock_buffer`, `mark_buffer_dirty`,
  `release_buffer`, `unlock_release_buffer`, `incr_buffer_ref_count`,
  `buffer_get_block_number`, `buffer_get_page_size`, `read_buffer_extended`,
  `read_buffer`, `extend_buffered_rel_by`
- bufpage.c: `page_is_all_visible`, `page_is_new`, `page_get_max_offset_number`,
  `page_get_heap_free_space`, `page_init`, `page_add_item`,
  `set_stored_tuple_ctid`
- freespace.c: `get_page_with_free_space`, `record_and_get_page_with_free_space`,
  `record_page_with_free_space`, `free_space_map_vacuum_range`
- lmgr.c: `relation_extension_lock_waiter_count`
- visibilitymap.c: `visibilitymap_pin`, `visibilitymap_pin_ok`
- relcache/rel.h: `relation_get_number_of_blocks`, `relation_get_target_block`,
  `relation_set_target_block`, `relation_get_target_page_free_space`,
  `relation_is_local`, `relation_get_relation_name`

Each is a `seam_core::seam!` loud-panic slot installed by its real owner, not by
this crate. Every seam is a thin marshal+delegate — no branching, node
construction, or computation lives on a seam path (the `set_stored_tuple_ctid`
seam isolates the opaque-page CTID poke; `page_add_item` carries the owned tuple
across so the runtime serializes the on-disk image). The relation crosses as its
bare `Oid` (`RelationGetRelid`), mirroring the `vacuumlazy-seams` precedent.

`hio.c` owns **no inward seam** — nothing calls `RelationPutHeapTuple` /
`RelationGetBufferForTuple` across a dependency cycle yet — so `init_seams()` is
empty and intentionally **unwired** in `seams-init::init_all()` (the established
`functioncmds` precedent: an empty installer with no owned *inward* seam crate is
correct, not a finding). The recurrence guard's two tests both pass with the unit
marked `audited` (the outward seams are imported under the `hio_seam` alias and
are owned by other crates, so neither the "wired-into-init_all" nor the
"declared-seam-installed-by-owner" check flags them).

## 4. Design conformance

- No invented opacity: the relation resolves to a real `Oid` key; buffers are
  the buffer manager's own `Buffer` indices; the tuple crosses as the owned
  `HeapTupleData`. No stand-in handles.
- Seams return `PgResult` (the C can `ereport(ERROR)` / `elog(PANIC)` across
  these surfaces), matching the seam-signature rule.
- No `Mcx`/allocation introduced in this crate (it allocates nothing of its own;
  the FSM/extend allocations live behind owner seams).
- No own-logic stubs, no `todo!()`/`unimplemented!()`, no deferred/SEAMED-
  equivalent escape for any in-crate logic. Every line of `hio.c`'s logic lives
  in this crate; only genuine cross-cycle callees are seamed.

## Gates

- `cargo check --workspace` — pass
- `cargo test -p backend-access-heap-hio` — pass (0 tests; no own logic to unit-test beyond the seam boundary)
- `cargo test -p seams-init` — pass (both recurrence-guard tests)

**PASS.** CATALOG.tsv set to `audited`.
