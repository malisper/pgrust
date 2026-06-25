# Audit: backend-utils-activity-pgstat-backend

C source: `src/backend/utils/activity/pgstat_backend.c` (PG 18.3).
Cross-checked against `c2rust-runs` rendering and the C headers.

## Function inventory

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `pgstat_count_backend_io_op_time` (56) | same | MATCH | track_io||track_wal debug_assert; tracks_backend_bktype gate; tracks_io_op debug_assert; INSTR_TIME_ADD into pending_times; have_iostats=true. pgstat_report_fixed not modeled (no-op, per IO precedent). |
| 2 | `pgstat_count_backend_io_op` (74) | same | MATCH | gate; debug_assert; counts += cnt; bytes += bytes; have_iostats=true. |
| 3 | `pgstat_fetch_stat_backend` (93) | same | SEAMED | `pgstat_fetch_entry(PGSTAT_KIND_BACKEND,...)` → `pgstat_fetch_entry_backend` seam; pgstat_fetch_entry (variable-kind snapshot fetch) is genuinely unported in pgstat.c core. |
| 4 | `pgstat_fetch_stat_backend_by_pid` (111) | same | SEAMED (prefix) + MATCH (logic) | bktype out-param init/clear; PID-resolution prefix (BackendPidGetProc/Auxiliary/GetNumberFromPGProc/get_beentry_by_proc_number) → `pgstat_backend_pid_lookup` seam (get_beentry_by_proc_number unported); tracks_backend_bktype + st_procpid==pid checks + entry refetch all ported faithfully. |
| 5 | `pgstat_flush_backend_entry_io` (166) | `pgstat_flush_backend_entry_io` (unsafe) | MATCH | have_iostats short-circuit; cast shared_stats→PgStatShared_Backend; triple loop counts/bytes/times(+=GET_MICROSEC); MemSet pending_io; have=false. |
| 6 | `pgstat_backend_wal_have_pending` (216) | same | MATCH | pgWalUsage().wal_records != prev.wal_records. |
| 7 | `pgstat_flush_backend_entry_wal` (226) | `pgstat_flush_backend_entry_wal` (unsafe) | MATCH | have_pending short-circuit; WalUsageAccumDiff; WALSTAT_ACC 4 fields (buffers_full/records/fpi/bytes); save prev. |
| 8 | `pgstat_flush_backend` (270) | same | MATCH | bktype gate; IO/WAL pending flags; get_entry_ref_locked(nowait)→None→true; flush requested entries under lock; unlock; false. |
| 9 | `pgstat_backend_flush_cb` (313) | same | MATCH | flush_backend(nowait, FLUSH_ALL). |
| 10 | `pgstat_create_backend` (322) | same | MATCH | get_entry_ref_locked(nowait=false) (NULL→error, impossible in C); memset shstatent.stats; unlock; MemSet pending; have=false; prev=pgWalUsage. |
| 11 | `pgstat_tracks_backend_bktype` (365) | same | MATCH | false/true arms verified against C switch (exhaustive, no `_`). |
| 12 | `pgstat_backend_reset_timestamp_cb` (400) | same | MATCH | cast header→PgStatShared_Backend; stats.stat_reset_timestamp = ts. |

## Constants verified
`PGSTAT_BACKEND_FLUSH_IO=1<<0`, `_WAL=1<<1`, `_ALL=IO|WAL` (pgstat_internal.h:617-619). MATCH.
KindInfo: fixed_amount=false, accessed_across_databases=true, write_to_file=false,
flush_static_cb=pgstat_backend_flush_cb, reset_timestamp_cb — matches pgstat.c kind table for BACKEND.

## Seam audit

Owned outward seams installed by `init_seams()`:
- io-seams: `pgstat_count_backend_io_op`, `pgstat_count_backend_io_op_time`, `pgstat_flush_backend_io` (consumed by pgstat_io.c).
- wal-seams: `pgstat_flush_backend_wal` (consumed by pgstat_wal.c).
All four were previously seam-and-panic; now real — closing the pgstat_io/pgstat_wal flush path.

Consumed (seam-and-panic, owner = unported pgstat.c core): `pgstat_fetch_entry_backend`,
`pgstat_backend_pid_lookup` — newly declared in pgstat-seams; correctly NOT installed here
(owner is pgstat.c core / backend_status.c, both unported). No logic in any seam path
beyond marshalling. `init_seams` is `register(...)` + `set(...)` only.

## Design conformance

- Per-backend file-statics (`PendingBackendStats`, `backend_has_iostats`, `prevBackendWalUsage`)
  → `thread_local!`, not shared statics. PASS.
- Shared stats body via the real `*mut PgStatShared_Common` → `*mut PgStatShared_Backend` cast
  (added `#[repr(C)]` to PgStatShared_Backend so `header` is provably first); no opaque handle,
  matching C's `(PgStatShared_Backend *) entry_ref->shared_stats`. PASS (types.md rule 6-7).
- Content lock acquired by `pgstat_get_entry_ref_locked`, released by `pgstat_unlock_entry`
  (C's two-call protocol; the lock spans a deliberate scope, modeled with mem::forget in the
  shmem owner, released explicitly here — the documented exception, not a `?`-across-lock bug). PASS.
- Callback failure surface: flush_backend returns PgResult<bool> (LWLockAcquire ereport);
  the io/wal-seams shapes return bool, so the installers map a genuine error to `true`
  (unflushed) defensively — the error is unreachable on the flush path. PASS.

## Verdict: PASS
