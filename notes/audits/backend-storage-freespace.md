# Audit: backend-storage-freespace

- Date: 2026-06-13
- Model: Claude Fable 5 (Opus 4.8 1M)
- Branch: port/backend-storage-freespace
- Verdict: **PASS**

Independent function-by-function audit per `.claude/skills/audit-crate/SKILL.md`.
Completeness oracle: `../pgrust/c2rust-runs/backend-storage-freespace/src/`
(freespace.rs, fsmpage.rs, indexfsm.rs). C sources:
`postgres-18.3/src/backend/storage/freespace/{freespace.c,fsmpage.c,indexfsm.c}`.
Port: `crates/backend-storage-freespace/src/lib.rs`.

## Function inventory & verdicts

### fsmpage.c (7 fns)

| C fn | Port location | Verdict | Notes |
|------|---------------|---------|-------|
| rightneighbor | lib.rs:168 | MATCH | x++, then `((x+1)&x)==0 ? parentof(x) : x`. |
| fsm_set_avail | lib.rs:198 | MATCH | early-out on unchanged value; C `do{}while(nodeno>0)` rendered as `loop{...; if nodeno<=0 break}` (post-test, equivalent); corrupt-tree rebuild on `value > fp_nodes[0]`. |
| fsm_get_avail | lib.rs:247 | MATCH | `fp_nodes[NonLeafNodesPerPage+slot]`. |
| fsm_get_max_avail | lib.rs:253 | MATCH | `fp_nodes[0]`. |
| fsm_search_avail | lib.rs:270 | MATCH | root quick-exit; fp_next_slot hint sanity; climb-via-parentof(rightneighbor); descend left-preferring; torn-page DEBUG1 elog + exclusive relock + fsm_rebuild_page + MarkBufferDirtyHint + restart; fp_next_slot update with advancenext. Page round-trips via FSMPageData; re-read after exclusive relock is behaviour-preserving (C operates on live buffer content). |
| fsm_truncate_avail | lib.rs:373 | MATCH | clear leaf nodes >= nslots; rebuild if changed. |
| fsm_rebuild_page | lib.rs:398 | MATCH | backwards over non-leaf nodes, Max of children. |

### freespace.c (22 fns)

| C fn | Port location | Verdict | Notes |
|------|---------------|---------|-------|
| fsm_space_avail_to_cat | lib.rs:435 | MATCH | `>= MaxFSMRequestSize ? 255`; `avail/FSM_CAT_STEP` clamped to 254. |
| fsm_space_cat_to_avail | lib.rs:454 | MATCH | 255 -> MaxFSMRequestSize else `cat*FSM_CAT_STEP`. |
| fsm_space_needed_to_cat | lib.rs:466 | MATCH | `> MaxFSMRequestSize` -> elog(ERROR,"invalid FSM request size %zu"); 0->1; round-up clamped to 255. |
| fsm_logical_to_physical | lib.rs:493 | MATCH | signed-int leafno multiply (wrapping_mul i32), upper-node page count, `-level`, `-1`. |
| fsm_get_location | lib.rs:519 | MATCH | bottom level, div/mod SlotsPerFSMPage. |
| fsm_get_heap_blk | lib.rs:531 | MATCH | `(uint)logpageno * SlotsPerFSMPage + slot`. |
| fsm_get_parent | lib.rs:539 | MATCH | level+1, div/mod. |
| fsm_get_child | lib.rs:553 | MATCH | level-1, `logpageno*SlotsPerFSMPage+slot`. |
| fsm_readbuf | lib.rs:570 | MATCH | cached-nblocks re-derivation (helper `fsm_cached_nblocks`); extend vs InvalidBuffer; PageIsNew double-check init under exclusive lock. |
| fsm_extend | lib.rs:618 | SEAMED | thin delegate to bufmgr `extend_buffered_rel_to_fsm` (ExtendBufferedRelTo + EB flags baked into seam). |
| fsm_cached_nblocks | lib.rs:626 | MATCH | helper modeling SMgrRelation->smgr_cached_nblocks[FSM_FORKNUM] re-derive over smgr seams; returns effective nblocks; preserves `smgrexists ? smgrnblocks : 0`. |
| fsm_set_and_search | lib.rs:646 | MATCH | readbuf(extend=true), exclusive lock, set_avail+dirty_hint, optional search, unlock_release. |
| fsm_search | lib.rs:682 | MATCH | shared-lock search; bottom-level does_block_exist -> return/zero-and-restart; past-EOF resets to root then fsm_get_child; lower-level upper-node fixup via fsm_set_and_search; both 10000-restart valves compare-then-bump (C post-increment). |
| fsm_vacuum_page | lib.rs:787 | MATCH | EOF on invalid buffer; recurse into children, slot-range computation (start/end_slot Ordering), CHECK_FOR_INTERRUPTS, child update under exclusive lock; fp_next_slot reset; release. |
| fsm_does_block_exist | lib.rs:880 | MATCH | cached MAIN_FORKNUM short-circuit OR fresh RelationGetNumberOfBlocks. |
| GetPageWithFreeSpace | lib.rs:908 | MATCH | needed_to_cat -> fsm_search. |
| RecordAndGetPageWithFreeSpace | lib.rs:920 | MATCH | set_and_search then does_block_exist guard then fallback fsm_search. |
| RecordPageWithFreeSpace | lib.rs:954 | MATCH | set_and_search(minValue=0). |
| XLogRecordPageWithFreeSpace | lib.rs:971 | MATCH | xlog_read_buffer_extended_fsm (FSM_FORKNUM/RBM_ZERO_ON_ERROR baked into seam), exclusive lock, PageInit if new, set_avail + conditional dirty_hint. |
| GetRecordedFreeSpace | lib.rs:1003 | MATCH | readbuf(false); InvalidBuffer->0; get_avail -> cat_to_avail. |
| FreeSpaceMapPrepareTruncateRel | lib.rs:1026 | MATCH | smgrexists guard; first_removed_slot>0 path: crit section, truncate_avail, MarkBufferDirty (full, not hint), conditional log_newpage_buffer (`!InRecovery && RelationNeedsWAL && XLogHintBitIsNeeded`); slot==0 path smgrnblocks compare. |
| FreeSpaceMapVacuum | lib.rs:1106 | MATCH | fsm_vacuum_page(root, 0, InvalidBlockNumber). |
| FreeSpaceMapVacuumRange | lib.rs:1120 | MATCH | `end>start` guard. |

### indexfsm.c (4 fns)

| C fn | Port location | Verdict | Notes |
|------|---------------|---------|-------|
| GetFreeIndexPage | lib.rs:1145 | MATCH | GetPageWithFreeSpace(BLCKSZ/2); RecordUsedIndexPage if valid. |
| RecordFreeIndexPage | lib.rs:1156 | MATCH | RecordPageWithFreeSpace(blk, BLCKSZ-1). |
| RecordUsedIndexPage | lib.rs:1161 | MATCH | RecordPageWithFreeSpace(blk, 0). |
| IndexFreeSpaceMapVacuum | lib.rs:1166 | MATCH | FreeSpaceMapVacuum(rel). |

## Constants

types-fsm verified against `src/include/storage/fsm_internals.h`:
NonLeafNodesPerPage = BLCKSZ/2-1, NodesPerPage = BLCKSZ - MAXALIGN(SizeOfPageHeaderData)
- offsetof(fp_nodes)=4, LeafNodesPerPage = NodesPerPage - NonLeafNodesPerPage,
SlotsPerFSMPage = LeafNodesPerPage. In-crate: FSM_CATEGORIES=256, FSM_CAT_STEP=BLCKSZ/256,
MaxFSMRequestSize=MaxHeapTupleSize=BLCKSZ-MAXALIGN(24+4), FSM_TREE_DEPTH=(SlotsPerFSMPage>=1626?3:4),
FSM_ROOT_LEVEL=DEPTH-1, FSM_BOTTOM_LEVEL=0, FSM_ROOT_ADDRESS={ROOT_LEVEL,0}. Compile-time
asserts guard FSM_CATEGORIES==256 and the node-count invariants.

## Seam audit

Owned seam crate (by C-source coverage): `backend-storage-freespace-seams` — declares
`record_free_index_page`, `index_free_space_map_vacuum`. Both installed by
`backend_storage_freespace::init_seams()` (only `set()` calls), which is wired into
`seams-init::init_all()` (lib.rs:101). No other `*-seams` crate maps to these three C files.
Outward seam calls (bufmgr fsm_buffer_get/set_page, lock_buffer, page_init/is_new,
buffer_get_tag, read/extend_buffered_rel_to_fsm, mark_buffer_dirty[_hint], (unlock_)release;
smgr smgrexists/smgrnblocks/smgr_cached_nblocks; xloginsert log_newpage_buffer;
xlogutils xlog_read_buffer_extended_fsm; xlog in_recovery/xlog_hint_bit_is_needed;
relcache relation_needs_wal/relation_get_number_of_blocks; miscadmin crit-section/interrupts)
are all thin marshal+delegate into unported owners; no FSM logic lives in any seam path.

## Gates

- `cargo check --workspace`: PASS (warnings only).
- `cargo test --workspace`: PASS except the 2 known unrelated flakes in
  `backend-utils-misc-timeout` (signal_handler_fires_reached_timeouts,
  periodic_timeout_reschedules) — ignored per audit instructions.
- recurrence_guard (`seams-init` tests): both checks PASS
  (every-seam-installing-crate-wired + every-declared-seam-installed).

No MISSING / PARTIAL / DIVERGES findings; no own-logic stubs; no design-conformance findings.
