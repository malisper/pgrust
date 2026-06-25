# Audit: backend-utils-activity-pgstat-slru

C source: `src/backend/utils/activity/pgstat_slru.c` (PG 18.3).
Cross-checked against `c2rust-runs` rendering and the C headers.

## Function inventory

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `pgstat_reset_slru` (45) | `pgstat_reset_slru` | MATCH | GetCurrentTimestamp seam; calls index + reset_counter_internal. `Assert(name != NULL)` is implicit (Rust `&str`). |
| 2 | `pgstat_count_slru_page_zeroed` (59) | same | MATCH | `get_slru_entry(idx).blocks_zeroed += 1`. |
| 3 | `pgstat_count_slru_page_hit` (65) | same | MATCH | |
| 4 | `pgstat_count_slru_page_exists` (71) | same | MATCH | |
| 5 | `pgstat_count_slru_page_read` (77) | same | MATCH | |
| 6 | `pgstat_count_slru_page_written` (83) | same | MATCH | |
| 7 | `pgstat_count_slru_flush` (89) | same | MATCH | |
| 8 | `pgstat_count_slru_truncate` (95) | same | MATCH | |
| 9 | `pgstat_fetch_slru` (105) | `pgstat_fetch_slru` | MATCH | snapshot_fixed(PGSTAT_KIND_SLRU) then returns a copy of `snapshot.slru` (C returns the pointer). |
| 10 | `pgstat_get_slru_name` (118) | same | MATCH | `<0 || >= SLRU_NUM_ELEMENTS` → None (C NULL); else SLRU_NAMES[idx]. |
| 11 | `pgstat_get_slru_index` (132) | same | MATCH | linear strcmp over SLRU_NAMES; fallback = last ("other") index. |
| 12 | `pgstat_slru_flush_cb` (156) | same | MATCH | have_slrustats short-circuit; nowait→conditional acquire→true; SLRU_ACC over 7 fields × SLRU_NUM_ELEMENTS; MemSet pending; release; have=false. |
| 13 | `pgstat_slru_init_shmem_cb` (196) | same | MATCH | LWLockInitialize(&ctl.slru.lock, LWTRANCHE_PGSTATS_DATA). |
| 14 | `pgstat_slru_reset_all_cb` (204) | same | MATCH | per-index reset_counter_internal (lock per index, as C). |
| 15 | `pgstat_slru_snapshot_cb` (211) | same | MATCH | LW_SHARED acquire; memcpy stats→snapshot.slru; release. |
| 16 | `get_slru_entry` (228) | `get_slru_entry` (closure form) | MATCH | sets have_slrustats; debug_assert idx bounds; pgstat_report_fixed not modeled (no-op, per IO precedent). |
| 17 | `pgstat_reset_slru_counter_internal` (247) | `pgstat_reset_slru_counter_internal{,_ctl}` | MATCH | LW_EXCLUSIVE; zero stats[i]; stamp reset ts; release. Split into a `_ctl` body so reset_all_cb (handed the control block by the registry adapter) does not re-borrow pgStatLocal. |

## Seam audit

Owned outward seams (the 8 SLRU declarations in the shared `backend-utils-activity-stat-seams`
crate): all installed by `init_seams()` (verified unique installers). Consumed by `slru.c`.
`snapshot_fixed` / `get_current_timestamp` / lwlock seams are inward deps of ported neighbors.
No logic in any seam path. `init_seams` is `register(...)` + `set(...)` only.

## Design conformance

- Per-backend file-statics (`pending_SLRUStats`, `have_slrustats`) modeled as `thread_local!`
  (one backend == one thread), not shared statics. PASS.
- LWLocks held via RAII guards released with `guard.release()?`; no lock across `?` without a guard. PASS.
- No invented opacity, no registry side-tables, no ambient-global seams. PASS.

## Verdict: PASS
