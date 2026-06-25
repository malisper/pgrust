# Audit: backend-access-transam-clog

- Verdict: **PASS**
- Date: 2026-06-13
- Auditor model: Claude Fable 5 (Opus 4.8 1M)
- C source: `src/backend/access/transam/clog.c` (PostgreSQL 18.3, 1162 lines).
- Port: `crates/backend-access-transam-clog/src/lib.rs`.
- c2rust completeness oracle: `../pgrust/c2rust-runs/backend-access-transam-clog/src/clog.rs`.

Independent re-derivation from the C and headers; the port's comments, its
self-review, and the green build were not trusted. Constants verified against
`access/clog.h`, `access/rmgrlist.h`, `access/slru.c`.

## Function inventory (every clog.c definition)

The c2rust run also renders inlined callees from sibling units (`SimpleLru*`,
`LWLock*`, `XLog*`, `pg_atomic_*`, `errstart`, `memcpy`, …); those are the
dependency surface, not clog.c's own functions. The 25 functions actually
*defined* in clog.c (incl. statics, inline helpers, the C2RUST barrier shim,
and the page-arithmetic macros) are below.

| C function (clog.c) | Port location | Verdict | Notes |
|---|---|---|---|
| `c2rust_pg_write_barrier` | `fence(Release)` inline at wakeup loop | MATCH | C2RUST_TRANSPILE no-op shim; real `pg_write_barrier()` modeled by a Release fence in the group wakeup loop. |
| `TransactionIdToPage` (static inline) | `TransactionIdToPage` | MATCH | `xid as i64 / CLOG_XACTS_PER_PAGE`. |
| `TransactionIdToPgIndex` (macro) | `TransactionIdToPgIndex` | MATCH | `xid % CLOG_XACTS_PER_PAGE`. |
| `TransactionIdToByte` (macro) | `TransactionIdToByte` | MATCH | pgindex / 4. |
| `TransactionIdToBIndex` (macro) | `TransactionIdToBIndex` | MATCH | `xid % 4`. |
| `GetLSNIndex` (macro) | `GetLSNIndex` | MATCH | `slotno*CLOG_LSNS_PER_PAGE + (xid%per_page)/32`. |
| `TransactionIdSetTreeStatus` | `TransactionIdSetTreeStatus` | MATCH | same-page scan loop, single-page vs split path (subcommit pass for off-first-page subxids only on COMMITTED, first-page set, remaining pages). `&subxids[nsubxids_on_first_page..]` == C `subxids + i`. |
| `set_status_by_pages` | `set_status_by_pages` | MATCH | per-page chunk loop; `&subxids[offset..]` + `num_on_page` == C `subxids+offset`; `Assert(nsubxids>0)` -> debug_assert. |
| `TransactionIdSetPageStatus` | `TransactionIdSetPageStatus` | MATCH | group-eligibility predicate, conditional-acquire/group-update/lock-fallback. The `xid==MyProc->xid`, `nsubxids==count`, `memcmp(subxids,...)` parts split across proc seams (xid/count via `my_proc_xid`/`my_proc_subxids`) and an in-crate compare (`my_proc_subxids_match`) — comparison logic stays in clog. StaticAssertDecl is a compile-time proc invariant (noted, no runtime code needed). |
| `TransactionIdSetPageStatusInternal` | `TransactionIdSetPageStatusInternal` | MATCH | async-commit `SimpleLruReadPage(..., XLogRecPtrIsInvalid(lsn), ...)`; subcommit-subxids-first-then-main on COMMITTED, then all subxids, then `page_dirty=true`. Per-iteration page_number debug_assert preserved. |
| `TransactionGroupUpdateXidStatus` | `TransactionGroupUpdateXidStatus` | MATCH | full group-commit protocol: enqueue via CAS on `clogGroupFirst`, page-mismatch bail (`clogGroupMember=false`, `clogGroupNext=INVALID`), follower semaphore-wait loop with extraWaits absorb, leader bank-lock acquire + exchange-to-INVALID + walk with bank-lock switching + per-proc `SetPageStatusInternal`, lock release, wakeup loop with Release barrier and self-skip on `PGSemaphoreUnlock`. PGPROC/ProcGlobal fields reached via thin proc seams. |
| `TransactionIdSetStatusBit` | `TransactionIdSetStatusBit` | MATCH | byteno/bshift, curval read, InRecovery+SUB_COMMITTED+already-COMMITTED no-op short-circuit (`xlog_seams::in_recovery`), state-transition debug_assert, RMW bit write, group_lsn max-update on valid lsn. |
| `TransactionIdGetStatus` | `TransactionIdGetStatus` | MATCH | `SimpleLruReadPage_ReadOnly`, status read, group_lsn read, bank-lock release; C `*lsn` out-param returned as tuple `(status, lsn)`. |
| `CLOGShmemBuffers` (static) | `CLOGShmemBuffers` | MATCH | autotune branch `SimpleLruAutotuneBuffers(512,1024)` when `transaction_buffers==0`, else `Min(Max(16, n), CLOG_MAX_ALLOWED_BUFFERS)`. |
| `CLOGShmemSize` | `CLOGShmemSize` | MATCH | `SimpleLruShmemSize(CLOGShmemBuffers(), CLOG_LSNS_PER_PAGE)`. |
| `CLOGShmemInit` | `CLOGShmemInit` | MATCH | GUC autotune (`SetConfigOption` PGC_S_DYNAMIC_DEFAULT then PGC_S_OVERRIDE fallback), `SimpleLruInit`, `PagePrecedes=CLOGPagePrecedes`, `SlruPagePrecedesUnitTests`. PagePrecedes is set *after* SimpleLruInit here vs *before* in C — benign: slru.c:341 documents that SimpleLruInit only "assumes caller set PagePrecedes" and never dereferences it; it is first read by the unit-test call, which still runs after the set. |
| `check_transaction_buffers` | `check_transaction_buffers` | MATCH | delegates to `check_slru_buffers("transaction_buffers", newval)`. |
| `BootStrapCLOG` | `BootStrapCLOG` | MATCH | bank-lock(0), `ZeroCLOGPage(0,false)`, `SimpleLruWritePage`, dirty assert, release. |
| `ZeroCLOGPage` (static) | `ZeroCLOGPage` | MATCH | `SimpleLruZeroPage`; `WriteZeroPageXlogRec` iff writeXlog; returns slotno. |
| `StartupCLOG` | `StartupCLOG` | MATCH | `latest_page_number = TransactionIdToPage(nextXid)` via atomic write; nextXid via `varsup_seams::read_next_transaction_id`. |
| `TrimCLOG` | `TrimCLOG` | MATCH | `TransactionIdToPgIndex(xid)!=0` guard; `*byteptr &= (1<<bshift)-1`; `MemSet(byteptr+1, 0, BLCKSZ-byteno-1)` == `iter.take(BLCKSZ).skip(byteno+1)` (exactly BLCKSZ-byteno-1 bytes); dirty. |
| `CheckPointCLOG` | `CheckPointCLOG` | MATCH | `SimpleLruWriteAll(ctl, true)`; dtrace probes are no-ops. |
| `ExtendCLOG` | `ExtendCLOG` | MATCH | first-XID-of-page-or-FirstNormal guard; lock; `ZeroCLOGPage(pageno,true)`; release. |
| `TruncateCLOG` | `TruncateCLOG` | MATCH | cutoffPage; `SlruScanDirectory(SlruScanDirCbReportPresence)` presence test; early return when absent; `AdvanceOldestClogXid` (varsup seam) *before* WAL; `WriteTruncateXlogRec`; `SimpleLruTruncate`. |
| `CLOGPagePrecedes` (static) | `CLOGPagePrecedes` | MATCH | `xid = page*CLOG_XACTS_PER_PAGE + FirstNormalTransactionId + 1` (wrapping); `Precedes(x1,x2) && Precedes(x1, x2+per_page-1)`. wrapping arithmetic matches C unsigned overflow. |
| `WriteZeroPageXlogRec` (static) | `WriteZeroPageXlogRec` | MATCH | `XLogBeginInsert/RegisterData(&pageno)/XLogInsert(RM_CLOG_ID, CLOG_ZEROPAGE)` collapsed into one `xlog_insert` seam call with the 8-byte pageno payload. |
| `WriteTruncateXlogRec` (static) | `WriteTruncateXlogRec` | MATCH | `xl_clog_truncate` packed as 16 bytes (pageno@0 i64, oldestXact@8 u32, oldestXactDb@12 u32) matching clog.h struct; `XLogInsert(RM_CLOG_ID, CLOG_TRUNCATE)` then `XLogFlush(recptr)`. |
| `clog_redo` | `clog_redo` | MATCH | `info = GetInfo & ~XLR_INFO_MASK`; no-block-refs assert; CLOG_ZEROPAGE (memcpy pageno, lock, ZeroCLOGPage, WritePage, dirty assert, release); CLOG_TRUNCATE (memcpy xlrec, AdvanceOldestClogXid, SimpleLruTruncate); else `elog(PANIC)` -> `PgError::new(PANIC, ...)`. |
| `clogsyncfiletag` | `clogsyncfiletag` | MATCH | `SlruSyncFileTag(XactCtl, ftag)`; C `path` out-param + saved `errno` returned via `FileTagOpResult`. |

No `MISSING` / `PARTIAL` / `DIVERGES`. clog_desc / clog_identify live in
`clogdesc.c` (separate unit `backend-access-rmgrdesc-clogdesc`) and are
correctly out of scope.

## Constants verified (against headers, not memory)

- `CLOG_BITS_PER_XACT=2`, `CLOG_XACTS_PER_BYTE=4`, `CLOG_XACT_BITMASK=0b11`,
  `CLOG_XACTS_PER_LSN_GROUP=32` — match clog.c #defines.
- `TRANSACTION_STATUS_{IN_PROGRESS=0x00,COMMITTED=0x01,ABORTED=0x02,SUB_COMMITTED=0x03}`
  — match clog.h.
- `CLOG_ZEROPAGE=0x00`, `CLOG_TRUNCATE=0x10`, `RM_CLOG_ID=3` (4th rmgrlist
  entry: XLOG=0,XACT=1,SMGR=2,CLOG=3) — match clog.h / rmgrlist.h.
- `WAIT_EVENT_XACT_GROUP_UPDATE = PG_WAIT_IPC + 56` — present in types-pgstat.
- `xl_clog_truncate` field offsets (pageno@0 i64, oldestXact@8 u32,
  oldestXactDb@12 u32) — match clog.h struct layout and the 16-byte packing in
  WriteTruncateXlogRec / from_bytes.

## State model

`static SlruCtlData XactCtlData` (file-static, per-backend shmem-backed) is
mirrored by a `thread_local XACT_CTL: RefCell<Option<SlruCtlData>>`, accessed
through `with_xact_ctl`. This is the per-backend-global pattern, not a shared
static — no design violation. NULL-before-init becomes a panic (vs C
null-deref). SLRU buffer machinery and the LWLock manager are consumed directly
from the ported sibling crates (slru, lwlock), not seamed — correct.

## Seam audit

Owned seam crate (by c_sources = clog.c): **`backend-access-transam-clog-seams`**
only. (The `backend-access-rmgrdesc-clogdesc-seams` crate maps to clogdesc.c, a
different unit, and is correctly untouched here.)

All six declarations in `clog-seams` are installed by this crate's
`init_seams()` and nowhere else: `clog_redo`, `transaction_id_get_status`,
`transaction_id_set_tree_status`, `clogsyncfiletag`, `clog_shmem_size`,
`clog_shmem_init`. `init_seams()` contains nothing but `set()` calls (the two
shmem wrappers are thin `Ok`-adapters for the seam's `PgResult` signature).
`seams-init::init_all()` calls `backend_access_transam_clog::init_seams()`.

Outward seams used by the port are all justified by real dependency cycles
against unported owners and are thin marshal+delegate:
- `xloginsert_seams::xlog_insert`, `xlog_seams::{xlog_flush,in_recovery}` — WAL
  emit/flush/recovery flag (xloginsert/xlog).
- `varsup_seams::{read_next_transaction_id, advance_oldest_clog_xid}` —
  TransamVariables read + oldestClogXid advance (varsup).
- `proc_seams::*` — thin field accessors/mutators on PGPROC/ProcGlobal
  group-commit fields (proc owns the struct, unported). The group-update
  *algorithm* (CAS loop, bank-lock switching, wakeups, the memcmp compare in
  `my_proc_subxids_match`) lives in clog, not in any seam — no logic crossed the
  seam.
- `guc_seams::set_config_option` (autotune write), `waitevent_seams::pgstat_report_wait_{start,end}`.

No branching/node-construction/computation in any seam path. No invented
opacity, no allocating seam without Mcx+PgResult, no ambient-global seam, no
lock held across `?` without a guard (bank locks are acquired/released with the
`ctl` re-borrow protocol slru.c itself uses).

## Wiring / recurrence guard

`cargo test -p seams-init` recurrence_guard:
`every_seam_installing_crate_is_wired_into_init_all` and
`every_declared_seam_is_installed_by_its_owner` both PASS.

## Gate

- `cargo check --workspace` — clean (warnings only).
- clog crate unit tests (5) PASS; `seams-init` tests (2) PASS.
- `cargo test --workspace` — the only failures are in crates clog does not
  touch and does not depend on: `interfaces-libpq-legacy-pqsignal`
  (`handler_round_trips_through_kernel`, a sandbox sigaction round-trip) and
  `backend-utils-adt-arrayfuncs` (`element_iteration_int4`, an in-progress
  unported crate). Pre-existing / environmental, not introduced by this unit.

## Verdict

**PASS.** Every clog.c function MATCH or correctly SEAMED; zero seam findings;
zero design-conformance findings; wiring + recurrence guard green.
