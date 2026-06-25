# Audit — backend-access-heap-vacuumlazy

- **Unit:** `backend-access-heap-vacuumlazy`
- **Verdict: PASS**
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Branch:** `port/backend-access-heap-vacuumlazy`
- **C source:** `src/backend/access/heap/vacuumlazy.c` (PostgreSQL 18.3)
- **c2rust:** `c2rust-runs/backend-access-heap-vacuumlazy/src/vacuumlazy.rs`
- **Port crate:** `crates/backend-access-heap-vacuumlazy/`
- **Owned seam crate:** `crates/backend-access-heap-vacuumlazy-seams/`

This is an independent, from-scratch re-derivation per
`.claude/skills/audit-crate/SKILL.md`. It re-confirms resolution of the
previously-failing finding (commit `88ea829c`,
"Audit … FAIL (reap callback relocated out of crate)").

## 1. Previously-failing finding — RESOLVED

The prior FAIL flagged `vacuum_reap_lp_read_stream_next` (vacuumlazy.c:2682) as
MISSING: its body had been folded into a `read_stream_next_buffer_reap` seam
owned by the not-yet-ported read-stream owner, relocating logic that this C
file owns across the seam boundary.

Fix commit `ee9fc7f8` brings the callback in-crate at
`scan_block.rs:235` (`vacuum_reap_lp_read_stream_next`), symmetric with the
phase-I `heap_vac_scan_next_block` callback:

- It drives only the genuinely-owned `tidstore_iterate_next` seam
  (`TidStoreIterateNext`/`TidStoreGetBlockOffsets` are `access/tidstore.c`,
  another owner — not vacuumlazy.c); returns `Exhausted` on `None`
  (C: `InvalidBlockNumber` on `NULL`), else the saved `ReapBlockInfo`
  (C: `memcpy` of `TidStoreIterResult`).
- The caller `lazy_vacuum_heap_rel` (`heap_vacuum.rs:97-117`) calls it in-crate,
  then reads the chosen block's buffer via `read_buffer_extended` — mirroring
  phase I.
- The fabricated `read_stream_next_buffer_reap` seam declaration is removed;
  `grep` confirms no remaining reference in either crate.

Behavior is identical to C: C's `read_stream_next_buffer` internally invokes the
callback (returning the next blkno, or `InvalidBlockNumber` to terminate) then
reads that block. The port splits the same two steps explicitly. The TID offsets
that C extracts post-read via `TidStoreGetBlockOffsets` are carried inside
`ReapBlockInfo` from the iterate-next seam — equivalent.

## 2. Function inventory (29 functions)

Enumerated from the C file (every `static`/exported definition); cross-checked
against the c2rust render. All present in-crate.

| # | C function (line) | Port location | Verdict |
|---|---|---|---|
| 1 | heap_vacuum_eager_scan_setup (488) | vacuum_rel.rs | MATCH |
| 2 | heap_vacuum_rel (615) | vacuum_rel.rs | MATCH |
| 3 | lazy_scan_heap (1200) | scan.rs | MATCH |
| 4 | heap_vac_scan_next_block (1572) | scan_block.rs | MATCH |
| 5 | find_next_unskippable_block (1677) | scan_block.rs | MATCH |
| 6 | lazy_scan_new_or_empty (1809) | scan_page.rs | MATCH |
| 7 | cmpOffsetNumbers (1919) | scan_page.rs (inlined `sort_by` w/ `pg_cmp_u16`) | MATCH |
| 8 | lazy_scan_prune (1944) | scan_page.rs | MATCH |
| 9 | lazy_scan_noprune (2239) | scan_page.rs | MATCH |
| 10 | lazy_vacuum (2450) | vacuum_phase.rs | MATCH |
| 11 | lazy_vacuum_all_indexes (2575) | vacuum_phase.rs | MATCH |
| 12 | vacuum_reap_lp_read_stream_next (2682) | scan_block.rs | MATCH (was the prior FAIL) |
| 13 | lazy_vacuum_heap_rel (2720) | heap_vacuum.rs | MATCH |
| 14 | lazy_vacuum_heap_page (2838) | heap_vacuum.rs | MATCH |
| 15 | lazy_check_wraparound_failsafe (2950) | vacuum_phase.rs | MATCH |
| 16 | lazy_cleanup_all_indexes (3003) | index.rs | MATCH |
| 17 | lazy_vacuum_one_index (3071) | index.rs | MATCH |
| 18 | lazy_cleanup_one_index (3120) | index.rs | MATCH |
| 19 | should_attempt_truncation (3180) | truncate.rs | MATCH |
| 20 | lazy_truncate_heap (3200) | truncate.rs | MATCH |
| 21 | count_nondeletable_pages (3331) | truncate.rs | MATCH |
| 22 | dead_items_alloc (3473) | dead_items.rs | MATCH |
| 23 | dead_items_add (3538) | dead_items.rs | MATCH |
| 24 | dead_items_reset (3560) | dead_items.rs | MATCH |
| 25 | dead_items_cleanup (3582) | dead_items.rs | MATCH |
| 26 | heap_page_is_all_visible (3607) | heap_vacuum.rs | MATCH |
| 27 | update_relstats_all_indexes (3723) | index.rs | MATCH |
| 28 | vacuum_error_callback (3758) | errcb.rs | MATCH |
| 29 | update_vacuum_error_info (3822) | errcb.rs | MATCH |
| 30 | restore_vacuum_error_info (3841) | errcb.rs | MATCH |

The c2rust render contained no functions absent from this table; the inline
`pg_cmp_u16` comparator and `START/END_CRIT_SECTION` macros are not free
functions.

### Detailed re-derivations (auditor spot-checks)

- **`vacuum_reap_lp_read_stream_next`** — re-derived against C:2682-2700; the
  `None → Exhausted` / `Some → carry ReapBlockInfo` mapping is exact.
- **`lazy_vacuum_heap_rel`** (C:2720-2827) — progress-phase report, error-info
  push, TID-store iterate begin, read-stream begin (same flags
  `READ_STREAM_MAINTENANCE | READ_STREAM_USE_BATCHING`), the reap loop
  (delay-point → next block → read+pin buffer → VM pin → exclusive lock →
  `lazy_vacuum_heap_page` → record free space → unlock/release → count), stream
  end, iterate end, the `vacuumed_pages` accounting asserts, the DEBUG2 log, and
  error-info restore match 1:1.
- **`lazy_vacuum_heap_page`** (C:2838-2935) — LP_DEAD→LP_UNUSED loop, line-pointer
  truncation, mark-dirty, WAL gate (`PRUNE_VACUUM_CLEANUP`, `unused` list),
  all-visible recheck, four-way VM-set with `ALL_VISIBLE`/`ALL_FROZEN`, and the
  `vm_new_visible*` counters match. C `START/END_CRIT_SECTION` are no-ops in this
  model (not logic).
- **`heap_page_is_all_visible`** (C:3607-…) — offset loop, unused/redirect skip,
  dead → not visible, the `HEAPTUPLE_LIVE` xmin-committed / oldest-xmin /
  needs-eventual-freeze sub-logic, the dead/recently-dead/in-progress → not
  visible group, and the `unexpected HeapTupleSatisfiesVacuum result`
  internal `elog(ERROR)` all match.
- **`lazy_check_wraparound_failsafe`** (C:2950-2997) — early `VacuumFailsafeActive`
  return, `vacuum_xid_failsafe_check`, abandon bstrategy, disable
  index-vacuum/cleanup/truncate, zero the two progress counters, the WARNING +
  errdetail + errhint with the exact three-part message text, and the
  `VacuumCostActive=false`/`VacuumCostBalance=0` resets all match.
- **`find_next_unskippable_block` / `heap_vac_scan_next_block`** — the
  three-state skip machine, `SKIP_PAGES_THRESHOLD` jump, eager-scan region reset,
  all-frozen skip, aggressive/eager-fail rules, last-block/`!skipwithvm`
  unskippable rules, and the `VAC_BLK_*` flag writes match. The `wrapping_add`
  on `current_block`/`next_unskippable_block` faithfully reproduces the C
  `InvalidBlockNumber + 1 == 0` overflow idiom.

## 3. Seam audit

**Owned seam crate:** `crates/backend-access-heap-vacuumlazy-seams` (the only
`-seams` crate mapping to this unit's single C file, vacuumlazy.c). 109 seam
declarations.

- **Inward (1):** `heap_vacuum_rel` — the driver's public entry, called by the
  not-yet-ported `commands/vacuum.c` across a real dependency cycle. **Installed**
  by this crate's `init_seams()` (`lib.rs:83`) as a thin marshal+delegate
  (Oid/params/strategy in, `PgResult<()>` out).
- **Outward (108):** every other declaration targets a function owned by a
  *different, not-yet-ported* C file — `access/heapam.c` prune/freeze +
  visibility predicates, `access/tidstore.c`, `access/visibilitymap.c`,
  `storage/buffer/bufmgr.c` + read_stream, `storage/freespace/`,
  `storage/lmgr/` + `catalog/storage.c`, `commands/vacuum.c` cutoff/relstat,
  `commands/vacuumparallel.c`, progress/pgstat, and misc backend infra. None
  names a vacuumlazy.c function. These default to the `seam!` loud panic and are
  installed by their owner when it lands — the established repo inward/outward
  convention (e.g. `backend-access-index-indexam`: 2 decls, 1 installed; AGENTS.md
  has each seam document which crate owns the implementation).

No outward seam path performs branching, node construction, or non-trivial
computation — each is argument conversion + one call + result conversion. No
vacuumlazy.c logic was relocated across any seam (the prior leak is fixed). No
uninstalled inward seam; no `set()` outside the owner.

## 4. Design conformance

- **Opacity (types.md 6-7):** Relations/indexes cross as bare `Oid` (relcache
  identity); buffers as the substrate's `Buffer` integer; dead-TID store,
  parallel-vacuum state, visibility test, read stream, strategy as the small
  handles the seam owner defines (`TidStore`, `ParallelVacuumStateHandle`,
  `GlobalVisStateHandle`, `ReadStreamHandle`, `StrategyHandle`). No invented
  opacity, no stand-in handles, no `void*` layering.
- **Mcx + PgResult on allocators/seams:** every seam returns `PgResult<…>`; a
  thrown `elog(ERROR)` surfaces as `Err`. No `&'static mut`.
- **No shared statics for per-backend globals:** `VacuumFailsafeActive`,
  `VacuumCostActive/Balance`, GUCs, latch, and pgstat accumulators are reached
  through getter/setter seams owned by their real owners, not shadowed by
  crate-level statics.
- **Errors:** `elog`/`ereport` map to `ereport(...)` with matching severity
  (DEBUG2/WARNING/ERROR) and message text; the one internal error preserved.
- Idiomatic surface: no raw pointers, `extern "C"`, `c_void`, `libc`, `CString`.

No design-conformance findings.

## 5. Gate

- `cargo check -p backend-access-heap-vacuumlazy -p backend-access-heap-vacuumlazy-seams` — clean.
- `cargo test -p backend-access-heap-vacuumlazy` — 8 passed, 0 failed.

## Verdict

**PASS.** All 29 functions MATCH; the previously-failing reap callback is
resolved (in-crate, owned-seam-only). One inward seam installed by
`init_seams()`; 108 outward seams correctly owned by unported neighbors. No
logic or design-conformance findings.
