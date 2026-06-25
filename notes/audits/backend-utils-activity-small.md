# Audit: backend-utils-activity-small

Unit: `backend-utils-activity-small` — the four small `src/backend/utils/activity/`
files of PostgreSQL 18.3: `backend_progress.c` (166 lines), `pgstat_archiver.c`
(120 lines), `pgstat_bgwriter.c` (126 lines), `pgstat_checkpointer.c` (146 lines).
Crates audited: `crates/backend-utils-activity-small`, plus the seam crates it
introduced (`backend-utils-activity-status-seams`,
`backend-utils-activity-pgstat-seams`, `backend-utils-activity-stat-seams`,
`backend-access-transam-parallel-seams`, `backend-libpq-pqformat-seams`,
`backend-storage-lmgr-lwlock-seams`, `backend-utils-adt-timestamp-seams`) and the
types crates `types-storage` / `types-pgstat`.
Cross-checked against `../pgrust/c2rust-runs/backend-utils-activity-small/src/*.rs`.
Auditor: independent re-derivation from the C sources and headers
(`utils/backend_progress.h`, `utils/backend_status.h`, `libpq/protocol.h`,
`access/parallel.h`, `pgstat.h`, `utils/pgstat_internal.h`, `utils/pgstat_kind.h`,
`postmaster/pgarch.h`, `storage/lwlock.h`, `storage/lwlocklist.h`,
`utils/memutils.h`).

## Function inventory

Every function definition in the four C files (statics included; none are static
here), cross-checked against the c2rust rendering. c2rust additionally inlined
header helpers; those are listed at the bottom of the table with their owners.

### backend_progress.c

| # | C function | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `pgstat_progress_start_command` (:26) | `backend_progress.rs::pgstat_progress_start_command` | MATCH | `!beentry \|\| !pgstat_track_activities` early return via `my_be_entry_present`/`track_activities` seams; then begin-write / set command / set target / `MemSet(st_progress_param, 0, ...)` (== `zero_progress_param`) / end-write, in C order. The entry and the write-activity protocol belong to `backend_status.c` (SEAMED field ops). |
| 2 | `pgstat_progress_update_param` (:46) | `backend_progress.rs::pgstat_progress_update_param` | MATCH | `Assert(index >= 0 && index < PGSTAT_NUM_PROGRESS_PARAM)` → `debug_assert`, placed before the beentry/track check as in C; write bracketing + single `st_progress_param[index] = val`. `PGSTAT_NUM_PROGRESS_PARAM` = 20 verified against backend_progress.h:33. |
| 3 | `pgstat_progress_incr_param` (:67) | `backend_progress.rs::pgstat_progress_incr_param` | MATCH | Same shape with `+= incr`. |
| 4 | `pgstat_progress_parallel_incr_param` (:88) | `backend_progress.rs::pgstat_progress_parallel_incr_param` | MATCH | `IsParallelWorker()` (== `ParallelWorkerNumber >= 0`, parallel.h:60, SEAMED to its owner) selects the worker branch: begin message `PqMsg_Progress` (`'P'`, protocol.h:69, verified), `pq_sendint32(index)` (C implicitly converts int→uint32; port casts `index as u32`, bit-identical), `pq_sendint64(incr)`, end message — exact c2rust order. Leader branch calls `pgstat_progress_incr_param` directly. C's per-call `static StringInfoData` + `initStringInfo` (a fresh, leaked buffer each call) is buffer plumbing for the pqformat calls; the seam contract ("beginmessage starts a fresh message, endmessage sends it") preserves the observable behavior — fresh empty message per call, same bytes, same send. |
| 5 | `pgstat_progress_update_multi_param` (:118) | `backend_progress.rs::pgstat_progress_update_multi_param` | MATCH | `(nparam, index*, val*)` → two slices with `nparam == index.len()`; `!beentry \|\| !track \|\| nparam == 0` early return; one write bracket around the whole loop (atomicity preserved); per-iteration `Assert` → `debug_assert`; `st_progress_param[index[i]] = val[i]`. |
| 6 | `pgstat_progress_end_command` (:147) | `backend_progress.rs::pgstat_progress_end_command` | MATCH | beentry/track check, then the unbracketed `st_progress_command == PROGRESS_COMMAND_INVALID` early return (read outside the write window, as in C), then bracketed reset to `PROGRESS_COMMAND_INVALID` / `InvalidOid` (0, verified). |

### pgstat_archiver.c

| # | C function | Port location | Verdict | Notes |
|---|---|---|---|---|
| 7 | `pgstat_report_archiver` (:28) | `pgstat_archiver.rs::pgstat_report_archiver` | MATCH | `&pgStatLocal.shmem->archiver` via `shmem_archiver` seam; `GetCurrentTimestamp()` SEAMED to its owner and taken before the write window, as in C; changecount write bracket (local port of the pgstat_internal.h inlines, see below); failed branch: `++failed_count`, `memcpy(last_failed_wal, xlog, 41)` (== `copy_from_slice(&xlog[..WAL_NAME_LEN])`, `WAL_NAME_LEN` = `MAX_XFN_CHARS + 1` = 41 verified against pgarch.h:26 and the c2rust `[c_char; 41]`), `last_failed_timestamp = now`; success branch symmetric. |
| 8 | `pgstat_fetch_stat_archiver` (:57) | `pgstat_archiver.rs::pgstat_fetch_stat_archiver` | MATCH | `pgstat_snapshot_fixed(PGSTAT_KIND_ARCHIVER)` (kind = 7, pgstat_kind.h:35; `PgStat_Kind` is `uint32`, pgstat_kind.h:17) then returns `&pgStatLocal.snapshot.archiver` via seam. |
| 9 | `pgstat_archiver_init_shmem_cb` (:65) | `pgstat_archiver.rs::pgstat_archiver_init_shmem_cb` | MATCH | `void *stats` → typed `&mut PgStatShared_Archiver`; `LWLockInitialize(&lock, LWTRANCHE_PGSTATS_DATA)` SEAMED. `LWTRANCHE_PGSTATS_DATA` = 81 re-derived from lwlock.h/lwlocklist.h (`NUM_INDIVIDUAL_LWLOCKS` = 54, chain of 27 builtin tranches) and confirmed against the c2rust constant (81). |
| 10 | `pgstat_archiver_reset_all_cb` (:73) | `pgstat_archiver.rs::pgstat_archiver_reset_all_cb` | MATCH | `LWLockAcquire(LW_EXCLUSIVE)` → `pgstat_copy_changecounted_stats(reset_offset ← stats)` → `stat_reset_timestamp = ts` → release. Destructuring split borrow keeps src/dst/cc aliasing C's three pointers into the same struct. |
| 11 | `pgstat_archiver_snapshot_cb` (:88) | `pgstat_archiver.rs::pgstat_archiver_snapshot_cb` | MATCH | Changecounted copy of live stats into the snapshot, `LW_SHARED`-locked copy of `reset_offset`, then compensation exactly as C: `archived_count == reset.archived_count` → `last_archived_wal[0] = 0`, timestamp 0; subtract; same for failed. Port stages the snapshot in a local and writes it back at the end — the snapshot is backend-local in C too, so intermediate states are unobservable; final state identical. |

### pgstat_bgwriter.c

| # | C item | Port location | Verdict | Notes |
|---|---|---|---|---|
| 12 | `PgStat_BgWriterStats PendingBgWriterStats = {0}` (global) | `pgstat_bgwriter.rs::PENDING_BGWRITER_STATS` + `pending_bgwriter_stats()` | MATCH | File-owned, zero-initialized process global; the accessor is the `PGDLLIMPORT` access path for `bufmgr.c`'s direct field bumps. |
| 13 | `pgstat_report_bgwriter` (:30) | `pgstat_bgwriter.rs::pgstat_report_bgwriter` | MATCH | `Assert(!pgStatLocal.shmem->is_shutdown)` → `debug_assert!(!shmem_is_shutdown::call())`; `pgstat_assert_is_up()` SEAMED; `pg_memory_is_all_zeros(&Pending..., sizeof)` → `is_all_zeros()` (field-equality with default; the struct is 4 contiguous int64s with no padding, so all-zero bytes ⇔ all fields zero) with early return **before** touching shmem; changecount bracket; the three `BGWRITER_ACC` fields (`buf_written_clean`, `maxwritten_clean`, `buf_alloc` — order and names verified against pgstat.h); `MemSet(&Pending..., 0, ...)` → `= default()`; `pgstat_flush_io(false)` SEAMED last. |
| 14 | `pgstat_fetch_stat_bgwriter` (:73) | `pgstat_bgwriter.rs::pgstat_fetch_stat_bgwriter` | MATCH | `snapshot_fixed(PGSTAT_KIND_BGWRITER)` (8, verified) + snapshot pointer. |
| 15 | `pgstat_bgwriter_init_shmem_cb` (:81) | `pgstat_bgwriter.rs::pgstat_bgwriter_init_shmem_cb` | MATCH | Same as #9. |
| 16 | `pgstat_bgwriter_reset_all_cb` (:89) | `pgstat_bgwriter.rs::pgstat_bgwriter_reset_all_cb` | MATCH | Same protocol as #10. |
| 17 | `pgstat_bgwriter_snapshot_cb` (:104) | `pgstat_bgwriter.rs::pgstat_bgwriter_snapshot_cb` | MATCH | Changecounted copy, locked reset read, `BGWRITER_COMP` subtraction of exactly the three counter fields (no `stat_reset_timestamp` compensation, as in C). |

### pgstat_checkpointer.c

| # | C item | Port location | Verdict | Notes |
|---|---|---|---|---|
| 18 | `PgStat_CheckpointerStats PendingCheckpointerStats = {0}` (global) | `pgstat_checkpointer.rs::PENDING_CHECKPOINTER_STATS` + `pending_checkpointer_stats()` | MATCH | Same pattern as #12. |
| 19 | `pgstat_report_checkpointer` (:30) | `pgstat_checkpointer.rs::pgstat_report_checkpointer` | MATCH | Same shape as #13 with the ten `CHECKPOINTER_ACC` fields — `num_timed`, `num_requested`, `num_performed`, `restartpoints_timed`, `restartpoints_requested`, `restartpoints_performed`, `write_time`, `sync_time`, `buffers_written`, `slru_written` — names and order verified against pgstat.h (11 contiguous int64s, no padding, so `is_all_zeros` ⇔ `pg_memory_is_all_zeros`). |
| 20 | `pgstat_fetch_stat_checkpointer` (:78) | `pgstat_checkpointer.rs::pgstat_fetch_stat_checkpointer` | MATCH | `snapshot_fixed(PGSTAT_KIND_CHECKPOINTER)` (9, verified) + snapshot pointer. |
| 21 | `pgstat_checkpointer_init_shmem_cb` (:90) | `pgstat_checkpointer.rs::pgstat_checkpointer_init_shmem_cb` | MATCH | Same as #9. |
| 22 | `pgstat_checkpointer_reset_all_cb` (:98) | `pgstat_checkpointer.rs::pgstat_checkpointer_reset_all_cb` | MATCH | Same protocol as #10. |
| 23 | `pgstat_checkpointer_snapshot_cb` (:113) | `pgstat_checkpointer.rs::pgstat_checkpointer_snapshot_cb` | MATCH | Changecounted copy, locked reset read, ten-field `CHECKPOINTER_COMP` subtraction. |

### Header inlines kept by c2rust (owners noted)

| C inline (header) | Disposition | Verdict |
|---|---|---|
| `pgstat_begin_changecount_write` / `pgstat_end_changecount_write` (pgstat_internal.h:798/:808) | `changecount.rs` — local port | MATCH: parity `Assert` → `debug_assert`, wrapping `++`; `START/END_CRIT_SECTION` and `pg_write_barrier` are process-global effects with no file-visible result, noted in the doc comment. |
| `pgstat_begin_changecount_read` / `pgstat_end_changecount_read` (pgstat_internal.h:820/:835) | `changecount.rs` — local port | MATCH: `before_cc` capture; odd-`before_cc` → retry; `before_cc == after_cc` success predicate, exactly pgstat_internal.h. (`CHECK_FOR_INTERRUPTS`/read barriers elided as above.) |
| `pgstat_copy_changecounted_stats` (pgstat_internal.h:859) | `changecount.rs` — local port | MATCH: do/while copy-retry loop; `memcpy(dst, src, len)` == typed `*dst = *src` (callers always pass `sizeof(stats)`). |
| `pg_memory_is_all_zeros` (memutils.h:219) | `is_all_zeros()` on the two pending-stats types | MATCH: byte-zero test ⇔ all-fields-zero for these padding-free all-int64 structs. |
| `PGSTAT_BEGIN/END_WRITE_ACTIVITY` (backend_status.h:211/:217) | `begin_write_activity`/`end_write_activity` seams | SEAMED — protocol and `MyBEEntry` owned by `backend_status.c`. |
| `pq_sendint32`/`pq_sendint64` (+`pq_writeint*`) (pqformat.h) | pqformat seams | SEAMED — owned by `libpq/pqformat.c`. |
| `IsParallelWorker()` (parallel.h:60) | parallel seam | SEAMED — reads `ParallelWorkerNumber`, owned by `parallel.c`. |

Constants verified against headers (not from memory): `PGSTAT_NUM_PROGRESS_PARAM` (20),
`PqMsg_Progress` (`'P'`), `ProgressCommandType` values (INVALID..COPY = 0..6),
`InvalidOid` (0), `MAX_XFN_CHARS` (40) / `WAL_NAME_LEN` (41, == c2rust `[c_char; 41]`),
`PGSTAT_KIND_ARCHIVER/BGWRITER/CHECKPOINTER` (7/8/9), `PgStat_Kind` = `uint32`,
`LW_EXCLUSIVE/LW_SHARED/LW_WAIT_UNTIL_FREE` (0/1/2), `NUM_INDIVIDUAL_LWLOCKS` (54,
last `PG_LWLOCK` id 53), `LWTRANCHE_PGSTATS_DATA` (81, full builtin-tranche chain
re-derived and cross-checked against c2rust), `INVALID_PROC_NUMBER` (-1).

Struct layouts verified against pgstat.h / pgstat_internal.h / lwlock.h:
`PgStat_ArchiverStats`, `PgStat_BgWriterStats`, `PgStat_CheckpointerStats`
(field order exact), `PgStatShared_{Archiver,BgWriter,Checkpointer}`
(`lock`, `changecount`, `stats`, `reset_offset`), `LWLock`
(`tranche`/`state`/`waiters`; `LOCK_DEBUG` fields outside build config).

## Seam audit

- All seven seam crates contain **declarations only** (pure `seam_core::seam!`
  slots); each depends only on `seam-core` plus the `types-*` crates its
  signatures need. No logic, no branching, no `set()` calls anywhere in them.
- Every outward seam targets a real not-yet-ported owner (`backend_status.c`,
  `pgstat.c`, `pgstat_io.c`, `parallel.c`, `pqformat.c`, `lwlock.c`,
  `timestamp.c`) — a direct dependency cannot exist because those units have no
  crates yet; the seams are the loud-panic stand-ins the repo convention
  prescribes, installed by each owner's `init_seams()` when it lands.
- The status seams expose `MyBEEntry` per-field operations
  (`set_progress_param`, `incr_progress_param`, `zero_progress_param`, ...);
  each is a single field read/write on the owner's struct — no computation
  beyond the one assignment the C statement performs. The
  begin/end-write-activity protocol is the owner's (backend_status.h macros).
- The pqformat seams elide the `StringInfo` argument (buffer owned by the
  pqformat side); the message content and call order stay in this crate; noted
  at function #4 — behavior-preserving.
- `backend_utils_activity_small::init_seams()` is empty (the crate declares no
  inward seams; future callers can depend on it directly) and is invoked by
  `seams-init::init_all()`, which contains only one-line `init_seams()` calls.
- No function body in this unit was replaced by a seam — all 21 bodies (plus
  the two file-owned globals) live in `crates/backend-utils-activity-small`.
- `test_seams.rs` installs fixture dispatchers, but only under `#[cfg(test)]`
  inside the crate's own test binary; no production `set()` outside an owner.

## Build / tests

`cargo check --workspace` clean; `cargo test -p backend-utils-activity-small`:
25/25 pass (including the changecount, reset/snapshot compensation, all-zeros
early return, and parallel-worker message-path cases).

## Verdict

**PASS** — every function MATCH (or SEAMED per the seam rules), zero seam
findings, zero fix rounds. Spot-checks re-derived in full detail:
`pgstat_archiver_snapshot_cb` (reset-compensation + staging equivalence),
`pgstat_progress_parallel_incr_param` (message bytes vs c2rust),
`pgstat_report_checkpointer` (all ten ACC fields vs pgstat.h), and the
changecount read/write protocol vs pgstat_internal.h.
