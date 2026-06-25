# Audit: backend-access-transam-slru

- Unit: `backend-access-transam-slru` (`src/backend/access/transam/slru.c`)
- Port: `crates/backend-access-transam-slru`
- C source: `postgres-18.3/src/backend/access/transam/slru.c` (1853 lines)
- c2rust: `c2rust-runs/backend-access-transam-slru/src/slru.rs`
- Auditor: independent re-derivation from C + c2rust, model Opus, 2026-06-12
- Verdict: **PASS**

This is a genuine, independent re-audit. The earlier workflow audit's verdict
was not trusted; the inventory and every verdict below were re-derived from the
C source and cross-checked against the c2rust rendering. (The integration gate
had deferred this unit only because the report was never committed; this commit
supplies it.)

## Function inventory and verdicts

slru.c defines 31 functions (per-build c2rust kept all 31). Every one gets a
row. `SlruFileName` is `static inline`; `SimpleLruGetBankLock` is a `slru.h`
inline used by the port and is listed for completeness.

| # | C function (slru.c line) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `SlruFileName` (static inline, 91) | `SlruFileName` (404) | MATCH | long: `%015X` over `[0,2^60-1]`; short: `%04X` over `[0,2^24-1]` cast `unsigned int`. Asserts → `debug_assert`; `snprintf` → `format!`; both name forms covered by unit tests. |
| 2 | `SimpleLruShmemSize` (198) | `SimpleLruShmemSize` (263) | MATCH | Per-array MAXALIGN accumulation reproduced; `sizeof(SlruSharedData)`=104 verified by tabulation (int+pad, 9 ptrs=72, int+pad, atomic u64=8, int, pad→104); `LWLockPadded`→`LWLOCK_PADDED_SIZE`; `BUFFERALIGN(sz)+BLCKSZ*nslots`. Both `nslots<=MAX`/`%BANK==0` asserts present. Accumulation unit-tested. |
| 3 | `SimpleLruAutotuneBuffers` (231) | `SimpleLruAutotuneBuffers` (289) | MATCH | `Min(max - max%BANK, Max(BANK, NBuffers/divisor - (NBuffers/divisor)%BANK))`; `NBuffers` via `globals::nbuffers` seam (ambient global, justified). Integer truncation identical. |
| 4 | `SimpleLruInit` (252) | `SimpleLruInit` (310) | MATCH (model-adapted) | Owned-`Vec` shmem model (documented; `CreateLWLocks` precedent). C carve-out offsets replaced by `try_reserve_exact`-backed Vecs of identical element counts; `pgstat_get_slru_index` via seam; per-buffer + per-bank `LWLockInitialize` with the two tranche ids; `group_lsn` only when `nlsns>0`. No attach branch (always init) — the `IsUnderPostmaster` else-branch is an attach-only path that only re-validates `num_slots`; behaviorally inert here. `PagePrecedes` left `None` for caller to set (= C "caller set PagePrecedes"). |
| 5 | `check_slru_buffers` (355) | `check_slru_buffers` (386) | MATCH | `newval % BANK == 0`; on failure returns the `GUC_check_errdetail` text `"%s" must be a multiple of 16.` as the second tuple element. Unit-tested. |
| 6 | `SimpleLruZeroPage` (375) | `SimpleLruZeroPage` (431) | MATCH | `SlruSelectLRUPage` → mark VALID+dirty, `SlruRecentlyUsed`, `MemSet(0)`, `SimpleLruZeroLSNs`, `pg_atomic_write_u64(latest_page_number)`, zeroed-page stat. Entry assert (bank lock held exclusive) present. |
| 7 | `SimpleLruZeroLSNs` (static, 428) | `SimpleLruZeroLSNs` (472) | MATCH | Zero `lsn_groups_per_page` entries at `slotno*groups`, only when `groups>0`; `InvalidXLogRecPtr` (bitwise 0). |
| 8 | `SimpleLruWaitIO` (static, 445) | `SimpleLruWaitIO` (487) | MATCH | Release bank → acquire+release buffer SHARED → re-acquire bank EXCLUSIVE; recovery branch on READ/WRITE_IN_PROGRESS using `LWLockConditionalAcquire`, resetting EMPTY / VALID+dirty exactly as C. Bare lock calls (no guards) by design (DESIGN_DEBT). |
| 9 | `SimpleLruReadPage` (502) | `SimpleLruReadPage` (531) | MATCH | Infinite restart loop; in-memory hit (with READ/WRITE-in-progress wait+continue), else mark read-busy, acquire buffer lock, release bank, `SlruPhysicalReadPage`, zero LSNs, re-acquire bank, set VALID/EMPTY from `ok`, release buffer, report on failure, `SlruRecentlyUsed`, read stat. `ok` is the inner `Result<(),SlruIoError>`; failure routed to `SlruReportIOError`. |
| 10 | `SimpleLruReadPage_ReadOnly` (605) | `SimpleLruReadPage_ReadOnly` (627) | MATCH | `bankno = pageno % nbanks`; shared-lock scan of `[bankstart,bankend)` for a non-EMPTY non-READ_IN_PROGRESS slot holding pageno → hit returns with SHARED lock still held; else release shared, acquire exclusive, delegate to `SimpleLruReadPage(.., write_ok=true, ..)`. |
| 11 | `SlruInternalWritePage` (static, 652) | `SlruInternalWritePage` (691) | MATCH | wait-while-WRITE_IN_PROGRESS-same-page; do-nothing predicate (`!dirty || !VALID || page changed`); mark write-busy, clear dirty; acquire buffer, release bank, `SlruPhysicalWritePage`; on failure+fdata close all fdata fds; re-acquire bank; on failure re-dirty; set VALID; release buffer; report on failure; checkpoint counters via `count_ckpt_slru_written` seam + `PendingCheckpointerStats.slru_written++` when fdata present. `fdata` as `Option<&mut>`. |
| 12 | `SimpleLruWritePage` (732) | `SimpleLruWritePage` (776) | MATCH | Entry assert + `SlruInternalWritePage(.., None)`. |
| 13 | `SimpleLruDoesPhysicalPageExist` (746) | `SimpleLruDoesPhysicalPageExist` (785) | MATCH | exists stat; open `O_RDONLY`; `fd<0`: ENOENT→false, else SLRU_OPEN_FAILED report; `lseek SEEK_END`<0→SLRU_SEEK_FAILED report; `result = endpos >= offset+BLCKSZ`; close!=0→record CLOSE_FAILED and return false (no report) — matches C exactly. `PG_BINARY`=0 on Unix, correctly omitted. |
| 14 | `SlruPhysicalReadPage` (static, 804) | `SlruPhysicalReadPage` (847) | MATCH | open `O_RDONLY`; `fd<0`: if `errno!=ENOENT || !InRecovery`→Err(OPEN_FAILED), else `ereport(LOG,"...doesn't exist, reading as zeroes")`, zero buffer, Ok; `errno=0` then `pg_pread`; `!=BLCKSZ`→wait_end, errno(0 if short, else current), close, Err(READ_FAILED); close!=0→Err(CLOSE_FAILED). `InRecovery` via `xlogrecovery_seams::in_recovery`. See note A. |
| 15 | `SlruPhysicalWritePage` (static, 876) | `SlruPhysicalWritePage` (918) | MATCH | written stat; WAL-before-data: max group_lsn scan, if not invalid `START/END_CRIT_SECTION` around `XLogFlush` (crit-section count bumped via `config` so an ERROR promotes to PANIC); reuse-open-fd from fdata; create with `O_RDWR|O_CREAT` else Err(OPEN_FAILED); register fd in fdata or fall back to standalone at MAX_WRITEALL_BUFFERS; `errno=0`+`pg_pwrite`, `!=BLCKSZ`→ENOSPC default, conditional close, Err(WRITE_FAILED); sync-request via seam, on full-queue synchronous `pg_fsync` (FSYNC_FAILED on err); close unless fdata. |
| 16 | `SlruReportIOError` (static, 1048) | `SlruReportIOError` (1064) | MATCH | C file statics `slru_errcause`/`slru_errno` carried as `SlruIoError` value. All six causes mapped to `ereport(ERROR)` with `errcode_for_file_access()` + the exact errdetail strings; READ/WRITE split on `errno!=0` (`%m` vs "too few bytes"); FSYNC uses `data_sync_elevel(ERROR)` via seam. Never returns (every arm >= ERROR) → `unreachable!`. The C `default:`/`elog` arm is the Rust enum-exhaustive impossibility. |
| 17 | `SlruRecentlyUsed` (static inline, 1123) | `SlruRecentlyUsed` (1157) | MATCH | `if (new != page_lru_count[slot]) { bank_cur = ++new; page = new; }`; pre-increment via `wrapping_add(1)`. Concurrency note preserved. |
| 18 | `SlruSelectLRUPage` (static, 1169) | `SlruSelectLRUPage` (1174) | MATCH | restart loop; first scan returns slot already holding pageno; `cur_count = bank_cur++` (post-inc); per-slot: EMPTY→return; `this_delta = cur_count - lru[slot]` (`wrapping_sub`), `<0`→reset lru to cur_count and delta=0; skip latest_page_number; VALID vs invalid best-tracking with PagePrecedes tiebreak; all-busy (`best_valid_delta<0`)→WaitIO+continue; clean victim→return; else WritePage+loop. Tie-break and wraparound arithmetic verified against C lines 1224-1312. |
| 19 | `SimpleLruWriteAll` (1322) | `SimpleLruWriteAll` (1278) | MATCH | flush stat; bank-walk releasing/acquiring locks on bank change; skip EMPTY; `SlruInternalWritePage(.., Some(&mut fdata))`; redirtied assert; release last bank; close all open fdata fds tracking last failing `(errno, pageno=segno*PAGES_PER_SEGMENT)` and report CLOSE_FAILED; `fsync_fname(Dir, true)` when sync_handler != NONE. Last-failure-wins close semantics match C. |
| 20 | `SimpleLruTruncate` (1408) | `SimpleLruTruncate` (1353) | MATCH | truncate stat; `restart:` loop → `'restart` labeled loop; wraparound backstop `PagePrecedes(latest, cutoff)`→LOG+return; bank-walk; skip EMPTY / non-preceding; clean→EMPTY; else VALID→WritePage / WaitIO, release bank, `goto restart`→`continue 'restart`; final `SlruScanDirectory(SlruScanDirCbDeleteCutoff, &cutoffPage)` via closure. |
| 21 | `SlruInternalDeleteSegment` (static, 1503) | `SlruInternalDeleteSegment` (1430) | MATCH | forget sync request when handler != NONE (`SYNC_FORGET_REQUEST`, retryOnError=true); `ereport(DEBUG2, errmsg_internal "removing file ...")`; `unlink(path)` via libc, return ignored as in C. |
| 22 | `SlruDeleteSegment` (1526) | `SlruDeleteSegment` (1451) | MATCH | acquire bank 0; `restart:` (did_write) loop; bank-walk; skip EMPTY; `pagesegno != segno`→skip; clean→EMPTY; else VALID→WritePage / WaitIO, did_write=true; re-loop if did_write; `SlruInternalDeleteSegment`; release bank. |
| 23 | `SlruMayDeleteSegment` (static, 1603) | `SlruMayDeleteSegment` (1515) | MATCH | `seg_last_page = segpage + PAGES_PER_SEGMENT - 1`; assert segpage aligned; `PagePrecedes(segpage,cutoff) && PagePrecedes(seg_last_page,cutoff)`. |
| 24 | `SlruPagePrecedesTestOffset` (static, USE_ASSERT_CHECKING, 1615) | `SlruPagePrecedesTestOffset` (1530) | MATCH | All asserts → `debug_assert`; XID wrap arithmetic via `wrapping_*`; `lhs/per_page` C division reproduced over non-negative operands by i64 division; `(1U<<31)%per_page` guards retained; the two GetNewTransactionId boundary cases (last/first page of 2nd segment) verified. |
| 25 | `SlruPagePrecedesUnitTests` (USE_ASSERT_CHECKING, 1697) | `SlruPagePrecedesUnitTests` (1604) | MATCH | offsets 0, per_page/2, per_page-1; gated on `cfg!(debug_assertions)` ≡ `USE_ASSERT_CHECKING`. |
| 26 | `SlruScanDirCbReportPresence` (1712) | `SlruScanDirCbReportPresence` (1620) | MATCH | `SlruMayDeleteSegment`→true (stop) else false. `void *data`→typed `cutoff_page: i64`. |
| 27 | `SlruScanDirCbDeleteCutoff` (static, 1728) | `SlruScanDirCbDeleteCutoff` (1635) | MATCH | `SlruMayDeleteSegment`→`SlruInternalDeleteSegment(segpage/PAGES_PER_SEGMENT)`; always returns false. |
| 28 | `SlruScanDirCbDeleteAll` (1744) | `SlruScanDirCbDeleteAll` (1650) | MATCH | unconditional `SlruInternalDeleteSegment`; returns false. |
| 29 | `SlruCorrectSegmentFilenameLength` (static inline, 1758) | `SlruCorrectSegmentFilenameLength` (1662) | MATCH | long→`len==15`; short→`len in {4,5,6}`. Unit-tested across 0..20. |
| 30 | `SlruScanDirectory` (1791) | `SlruScanDirectory` (1677) | MATCH | `AllocateDir`/`ReadDir`/`FreeDir` via `with_allocated_dir` seam closure; length + `strspn(...,"0123456789ABCDEF")==len` charset filter; `strtoi64(name,16)`→`i64::from_str_radix`; `segpage=segno*PAGES_PER_SEGMENT`; DEBUG2 elog; callback; stop on true. `void *data` modeled as closure capture. |
| 31 | `SlruSyncFileTag` (1831) | `SlruSyncFileTag` (1712) | MATCH | build path from `ftag->segno`; open `O_RDWR`, `fd<0`→return -1; `pg_fsync` between wait-start/end; save errno; close; restore errno; return `(result, path)` (path is the C out-buffer). |
| — | `SimpleLruGetBankLock` (slru.h inline) | `SimpleLruGetBankLock` (419) | MATCH | `bank_locks[pageno % nbanks]`. Used by entry asserts and the read/write paths. |

## Constants verified against headers (not from memory)

- `SLRU_MAX_ALLOWED_BUFFERS = (1024*1024*1024)/BLCKSZ` — slru.h:24 ✔ (port line 85)
- `SLRU_PAGES_PER_SEGMENT = 32` — slru.h:39 ✔ (port line 88)
- `SLRU_BANK_BITSHIFT = 4`, `SLRU_BANK_SIZE = 1<<4 = 16` — slru.c:142-143 ✔
- `MAX_WRITEALL_BUFFERS = 16` — slru.c:123 ✔
- `SlruPageStatus` discriminants 0..3 (EMPTY/READ_IN_PROGRESS/VALID/WRITE_IN_PROGRESS) — slru.h ✔
- Wait events SLRU_READ/WRITE/SYNC/FLUSH_SYNC — from `types_pgstat::wait_event` ✔
- `INIT_SLRUFILETAG` (zero tag, set handler+segno) — `FileTag::for_slru` ✔

## Seam audit

Ownership is by C-source coverage. The unit's only C file is slru.c, and no
crate declares a `backend-access-transam-slru-seams` crate (nothing cycles back
into slru) — so this unit **owns no seam crate** and correctly has no
`init_seams()`. The `backend-storage-sync-seams` and
`backend-access-transam-xlogrecovery-seams` crates created during this port are
owned by their respective units (sync.c / xlogrecovery.c), not by slru, and are
not this unit's installation responsibility.

Every outward seam call is a thin marshal+delegate for a genuine external
callee, and no slru function body was replaced by a seam ("logic lives
elsewhere" would be MISSING — none found; all 31 bodies are present and
complete):

- `file_seams`: OpenTransientFile / CloseTransientFile / pg_fsync /
  fsync_fname / data_sync_elevel / with_allocated_dir (fd.c, dirent.c) — real dep.
- `sync_seams::register_sync_request` (sync.c) — real dep.
- `xlog_seams`: xlog_flush, count_ckpt_slru_written (xlog.c / CheckpointStats) — real dep.
- `xlogrecovery_seams::in_recovery` (xlogrecovery.c `InRecovery`) — real dep.
- `stat_seams`: pgstat_get_slru_index + the slru page counters (pgstat_slru.c) — real dep.
- `waitevent_seams`: pgstat_report_wait_start/end (wait_event.c) — real dep.
- `globals::nbuffers` (NBuffers ambient global) — real dep.

No branching/node-construction/computation observed inside any seam path; each
is argument conversion + one call + result conversion. `FileTag::for_slru`
lives in `types-storage` (a type constructor), not in a seam path.

## Design conformance

- Allocating path (`SimpleLruInit`) returns `PgResult` and uses
  `try_reserve_exact` for the shmem stand-in — fallible, no infallible alloc. ✔
- No shared statics for per-backend globals; `SlruSharedData` is an owned tree
  on the returned `SlruCtlData`, matching the documented `CreateLWLocks`
  owned-Vec precedent (LICENSE in DESIGN_DEBT / module header). ✔
- The C file statics `slru_errcause`/`slru_errno` are NOT reproduced as shared
  statics; they are a per-call value channel (`SlruIoError`). ✔
- Locks held across `?` without Drop guards: this is the deliberate mirror of
  slru.c's release/re-acquire protocol (callers enter/exit holding the bank
  lock; ERROR unwinds with locks held to be released by `LWLockReleaseAll` at
  abort). Documented in the module header and DESIGN_DEBT.md — ledgered, not an
  unmarked divergence. ✔
- No invented opacity, no registry-shaped side tables, no ambient-global seams
  beyond the justified `NBuffers`. ✔

## Auditor self-check (re-derived MATCH samples)

- `SlruSelectLRUPage` wraparound: C `cur_count = bank_cur++` (post-inc, old
  value to cur_count) vs port `let cur_count = bank[bn]; bank[bn] =
  cur_count.wrapping_add(1)` — identical; `this_delta = cur_count - lru[slot]`
  with `<0` clamp reproduced via `wrapping_sub`; tie-break `this_delta ==
  best && PagePrecedes(...)` matches both VALID and invalid arms.
- `SimpleLruReadPage_ReadOnly` lock state on the hit path: port returns while
  still holding the SHARED bank lock acquired at entry — matches the C contract
  "will be held at exit ... shared or exclusive."
- `SimpleLruWriteAll` close loop: both record the last failing close's
  errno/pageno and report once afterward.
- `SlruReportIOError` errdetail strings compared character-for-character against
  C lines 1059-1116, including the `%m` vs "too few bytes"/"wrote too few
  bytes" split and the FSYNC `data_sync_elevel` elevation.

## Notes (behaviorally MATCH; recorded for completeness)

- Note A — `SlruPhysicalReadPage` short read: C does `errno = 0` before
  `pg_pread` and saves whatever `errno` is afterward; the port forces `errno=0`
  whenever `nread >= 0`. `SlruReportIOError`'s READ branch only distinguishes
  `errno != 0` (with `%m`) from `errno == 0` ("read too few bytes"). A positive
  partial read does not set `errno` in practice, so both paths produce the
  "read too few bytes" message identically; the negative-return path captures
  `current_errno()` the same as C. Equivalent on every realizable input.
- The `IsUnderPostmaster` attach branch of `SimpleLruInit` (re-validate
  `num_slots`, no state change) is intentionally absent in the always-init
  owned model; it produces no observable behavior the port omits.

## Verdict

**PASS** — all 31 functions MATCH (one model-adapted but behavior-identical:
`SimpleLruInit`), no MISSING/PARTIAL/DIVERGES, zero seam findings, zero design
findings. Constants verified against slru.h/slru.c. Crate builds clean. The
unit may merge.
