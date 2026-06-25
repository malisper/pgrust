# Audit: backend-access-transam-xlogprefetcher

Unit(s): `backend-access-transam-xlogprefetcher`, `backend-access-transam-xlogreader-seams`
C source: `src/backend/access/transam/xlogprefetcher.c` (PostgreSQL 18.3, 1101 lines)
Port: `crates/backend-access-transam-xlogprefetcher/src/lib.rs`
c2rust: `../pgrust/c2rust-runs/backend-access-transam-xlogprefetcher/src/xlogprefetcher.rs`

Audit is independent: inventory built from the C source, cross-checked against
the c2rust rendering; every function compared C ↔ c2rust ↔ port. Constants
verified against headers (not memory).

## Constants verified against headers

| Constant | Port value | Header | Header value |
|---|---|---|---|
| `XLOG_CHECKPOINT_SHUTDOWN` | `0x00` | catalog/pg_control.h | `0x00` |
| `XLOG_END_OF_RECOVERY` | `0x90` | catalog/pg_control.h | `0x90` |
| `XLOG_SMGR_CREATE` | `0x10` | catalog/storage_xlog.h | `0x10` |
| `XLOG_SMGR_TRUNCATE` | `0x20` | catalog/storage_xlog.h | `0x20` |
| `XLOG_DBASE_CREATE_FILE_COPY` | `0x00` | commands/dbcommands_xlog.h | `0x00` |
| `IO_DIRECT_DATA` | `0x01` | storage/fd.h | `0x01` |
| `XLR_INFO_MASK` (types-wal) | `0x0F` | access/xlogrecord.h | `0x0F` |
| `BKPBLOCK_WILL_INIT` (types-wal) | `0x40` | access/xlogrecord.h | `0x40` |
| `RM_XLOG_ID` (types-wal) | `0` | rmgrlist.h | entry 0 |
| `RM_SMGR_ID` (types-wal) | `2` | rmgrlist.h | entry 2 |
| `RM_DBASE_ID` (types-wal) | `4` | rmgrlist.h | entry 4 |
| `RECOVERY_PREFETCH_{OFF,ON,TRY}` | `0,1,2` | access/xlogprefetcher.h (seq enum) | `0,1,2` |
| `XLOGPREFETCHER_SEQ_WINDOW_SIZE` | `4` | xlogprefetcher.c | `4` |
| `XLOGPREFETCHER_DISTANCE_MULTIPLIER` | `4` | xlogprefetcher.c | `4` |
| `XLOGPREFETCHER_STATS_DISTANCE` | `BLCKSZ` | xlogprefetcher.c | `BLCKSZ` |

WAL record struct offsets (used by `from_bytes`) verified against headers:
`xl_smgr_create` rlocator@0/forkNum@12; `xl_smgr_truncate` blkno@0/rlocator@4/flags@16;
`xl_dbase_create_file_copy_rec` db_id@0 (Oid=4B). All correct.

## Per-function table

| # | C function (loc) | Port loc | Verdict | Notes |
|---|---|---|---|---|
| 1 | `lrq_alloc` (201) | lib.rs:198 | MATCH | `size = max_distance+1`; ring vec resized; Assert→debug_assert; palloc OOM→`?`. min size 2 (no `size-1` underflow). |
| 2 | `lrq_free` (226) | (Drop) | MATCH | `pfree`→drop of owned `LsnReadQueue`. |
| 3 | `lrq_inflight` (232) | lib.rs:224 | MATCH | field read. |
| 4 | `lrq_completed` (238) | lib.rs:229 | MATCH | field read. |
| 5 | `lrq_prefetch` (244) | lib.rs:237 | MATCH | admission `inflight<max_inflight && inflight+completed<size-1`; switch on Again/Io/NoIo; head wrap. `next` callback as closure. |
| 6 | `lrq_complete_lsn` (271) | lib.rs:267 | MATCH | tail-drain `tail!=head && queue[tail].lsn<lsn`, dec inflight/completed, tail wrap; then prefetch iff enabled. `enabled` passed by caller (stable across call), C reads `RecoveryPrefetchEnabled()` itself — same value. |
| 7 | `XLogPrefetchShmemSize` (293) | lib.rs:402 | MATCH | `sizeof(XLogPrefetchStats)`. |
| 8 | `XLogPrefetchResetStats` (302) | lib.rs:408 | MATCH | reset_time=GetCurrentTimestamp, others 0. |
| 9 | `XLogPrefetchShmemInit` (314) | lib.rs:425 | MATCH | first call = C `!found` init arm (OnceLock get_or_init); SharedStats is genuine shared memory → process-global. |
| 10 | `XLogPrefetchReconfigure` (339) | lib.rs:444 | MATCH | `count++` thread-local. |
| 11 | `XLogPrefetchIncrement` (350) | lib.rs:452 | MATCH | `*c = read+1` Relaxed; Assert(AmStartup \|\| !IsUnderPostmaster)→debug_assert. |
| 12 | `XLogPrefetcherAllocate` (361) | lib.rs:466 | MATCH | palloc0, filter_table reserve(1024), filter_queue init, reconfigure_count = count-1; SharedStats gauges zeroed. |
| 13 | `XLogPrefetcherFree` (389) | lib.rs:509 | MATCH | consumes self → Drop frees ring + table. |
| 14 | `XLogPrefetcherGetReader` (400) | lib.rs:513 | MATCH | returns reader. |
| 15 | `XLogPrefetcherComputeStats` (409) | lib.rs:519 | MATCH | wal_distance = tail.lsn-head.lsn or 0; io_depth/block_distance/wal_distance stored; next_stats_shm_lsn = ReadRecPtr+STATS_DISTANCE. head expect mirrors C unconditional deref when tail non-null. |
| 16 | `XLogPrefetcherNextBlock` (458) | lib.rs:567 | MATCH | full for(;;): read-ahead (block/nonblock + no_readahead_until guard), disabled→NO_IO, filter ops for RM_XLOG (TLI suppress)/RM_DBASE (file-copy)/RM_SMGR (create main-fork / truncate), block scan: in_use, fork, has_image→skip_fpw, WILL_INIT→skip_init, IsFiltered→skip_new, recent-window→skip_rep, recent ring update, smgrexists→skip_new, blkno>=nblocks→skip_new, PrefetchSharedBuffer hit/io/elog(ERROR). begin_ptr one-record guard, advance. `*lsn` set after `!in_use continue`. |
| 17 | `pg_stat_get_recovery_prefetch` (823) | lib.rs:896 | MATCH | 10 cols; InitMaterializedSRF; reset_time as TimestampTz(int64); putvalues; return (Datum)0. |
| 18 | `XLogPrefetcherAddFilter` (855) | lib.rs:936 | MATCH | HASH_ENTER not-found: insert + push_head; found: extend lsn, Min(from_block), delete+push_head. PgVec front==dlist head; rotate models push_head/delete+push_head. |
| 19 | `XLogPrefetcherCompleteFilters` (893) | lib.rs:994 | MATCH | drain from tail (back/oldest) while until_replayed<replaying_lsn; `>=`→break; delete+HASH_REMOVE. |
| 20 | `XLogPrefetcherIsFiltered` (913) | lib.rs:1017 | MATCH | empty-queue fast path; block-range filter (from_block<=blockno); whole-db filter (spcOid=Invalid, relNumber=Invalid). |
| 21 | `XLogPrefetcherBeginRead` (961) | lib.rs:1050 | MATCH | reconfigure_count--, begin_ptr=recPtr, no_readahead_until=0, XLogBeginRead. |
| 22 | `XLogPrefetcherReadRecord` (980) | lib.rs:1073 | MATCH | reconfigure (free+realloc ring, enabled→inflight=mic/distance=mic*4 else 1/1), ReleasePreviousRecord, CompleteFilters, complete_lsn, prefetch-if-empty (with inflight/completed==0 asserts), NextRecord, drop record ref by LSN identity, ComputeStats when lsn>=next_stats. NULL→NoRecord{errmsg}. |
| 23 | `check_recovery_prefetch` (1080) | lib.rs:1223 | MATCH | `#ifndef USE_PREFETCH` ON rejected with GUC_check_errdetail text; USE_PREFETCH cfg by target_os. |
| 24 | `assign_recovery_prefetch` (1094) | lib.rs:1236 | MATCH | recovery_prefetch=new_value; if AmStartupProcess→Reconfigure. |
| 25 | `RecoveryPrefetchEnabled` (macro, 71) | lib.rs:145 | MATCH | USE_PREFETCH && prefetch!=OFF && mic>0; else false. |

Helpers `lrq_prefetch_self`/`lrq_complete_lsn_self` (lib.rs:1173/1194) are the
Rust bridge that takes the ring out of `self` so the `NextBlock` callback can
borrow the rest — they model the C `lrq_private` back-pointer; no added logic.
`storage_locator` (lib.rs:882) is a field rename between the two trimmed
`RelFileLocator` mirrors, ledgered in DESIGN_DEBT.md.

## Seam audit

Outward seams (all justified by real dependency cycles to unported owners;
panic-until-installed is acceptable per skill §4):

- `backend-access-transam-xlogreader-seams`: 11 thin reader-access seams
  (has_queued_record_or_error, read_ahead, decode_queue_head/tail_lsn,
  read_ahead_record_block/main_data, set_..._prefetch_buffer,
  release_previous_record, next_record, deferred_errmsg, begin_read). Each is
  marshal+delegate; the `ReadAheadRecordInfo` (Copy header facts) + re-read
  pattern is the documented solution to holding `DecodedXLogRecord *` across
  reader calls. No branching/computation in the seam path.
- smgr-seams `smgrexists`/`smgrnblocks`, bufmgr-seams `prefetch_shared_buffer`
  (flattened rlocator+backend, like existing `smgrnblocks`).
- init-small-seams `my_backend_type`/`is_under_postmaster`.
- funcapi-seams `InitMaterializedSRF`/`materialized_srf_putvalues`.
- timestamp-seams `get_current_timestamp`.

No function body was replaced by a seam to "elsewhere" — all logic lives in the
crate. `init_seams()` is empty and correct: this unit declares no inward seam
crate (callers — xlogrecovery, ipci, guc hooks — depend directly, acyclically).

## Design conformance

- Per-backend C globals (`recovery_prefetch`, `XLogPrefetchReconfigureCount`)
  → `thread_local!`. PASS.
- `SharedStats` is genuine shared memory (`ShmemInitStruct`), not a per-backend
  global → process-global `OnceLock` is correct (not a shared-statics
  violation). PASS.
- `maintenance_io_concurrency`/`io_direct_flags` taken as explicit params (no
  ambient-global seams). PASS.
- All allocating functions/paths carry `Mcx` + return `PgResult`
  (`lrq_alloc`, `XLogPrefetcherAllocate`, `XLogPrefetcherAddFilter`). PASS.
- No registry side tables; no locks across `?`; the one cross-type
  `RelFileLocator` conversion is ledgered in DESIGN_DEBT.md. PASS.

## Build / tests

`cargo build -p backend-access-transam-xlogprefetcher`: clean.
`cargo test -p backend-access-transam-xlogprefetcher`: 6/6 pass.

## Verdict: PASS

All 25 functions MATCH; constants verified against headers; zero seam findings;
no design-conformance violations.
