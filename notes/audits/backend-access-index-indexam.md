# Audit: backend-access-index-indexam

- Date: 2026-06-12
- Model: Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- Unit: `backend-access-index-indexam` (`src/backend/access/index/indexam.c`, PG 18.3, 1086 lines)
- Crate: `crates/backend-access-index-indexam` (src/lib.rs, 929 lines)
- Seam crate owned: `crates/backend-access-index-indexam-seams`
- Compared against: C ground truth + `../pgrust/c2rust-runs/backend-access-index-indexam/src/indexam.rs` (4217 lines)
- Branch: `port/backend-access-index-indexam`

This is an independent re-audit, re-derived from the sources (not trusting the
port comments or the prior in-commit audit).

## Top-line verdict: PASS

Every C function is `MATCH` or properly `SEAMED`. Zero seam findings. Zero
design-conformance findings. The crate and its seam dependencies build clean.

## Function inventory (28 functions)

The C file defines 28 functions (one `static` helper `index_beginscan_internal`,
one `static inline` `validate_relation_kind`, 26 external `index_*`). All 28 are
present in c2rust (the build kept everything) and all 28 have a port counterpart.

| # | C function | C loc | Port loc (lib.rs) | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `index_open` | 132 | 91 | MATCH | `relation_open` (seam) → `validate_relation_kind`; allocates in `mcx`, `PgResult`. Also installed as inward seam. |
| 2 | `try_index_open` | 151 | 103 | MATCH | `try_relation_open` (seam) → `None` on missing → validate. |
| 3 | `index_close` | 176 | 123 | MATCH | `RelationClose` + conditional `UnlockRelationId` = `Relation::close(lockmode)`; lock floor assert kept (`debug_assert lockmode >= NoLock`). |
| 4 | `validate_relation_kind` | 196 | 132 | MATCH | relkind != INDEX('i') && != PARTITIONED_INDEX('I') → ERRCODE_WRONG_OBJECT_TYPE. Constants verified vs pg_class.h. |
| 5 | `index_insert` | 212 | 147 | MATCH | RELATION_CHECKS; `!ampredlocks` → `CheckForSerializableConflictIn` (seam); `aminsert` required callback (non-Option). |
| 6 | `index_insert_cleanup` | 240 | 181 | MATCH | RELATION_CHECKS; optional `aminsertcleanup`. |
| 7 | `index_beginscan` | 255 | 199 | MATCH | internal + heap_relation/snapshot/instrument + `table_index_fetch_begin` (direct dep). |
| 8 | `index_beginscan_bitmap` | 288 | 227 | MATCH | internal(norderbys=0) + snapshot/instrument, no heap fetch. |
| 9 | `index_beginscan_internal` (static) | 313 | 246 | MATCH | RELATION_CHECKS; `!ampredlocks` → `PredicateLockRelation` (seam); `RelationIncrementReferenceCount` (seam); `ambeginscan` (required) → parallel_scan + xs_temp_snap. |
| 10 | `index_rescan` | 355 | 278 | MATCH | SCAN_CHECKS; `amrescan` required; nkeys/norderbys asserts; `table_index_fetch_reset` if heapfetch; kill/heap_continue reset → `amrescan`. |
| 11 | `index_endscan` | 381 | 303 | MATCH | `amendscan` required; fetch_end + null; `amendscan`; `RelationDecrementReferenceCount`; temp_snap → `UnregisterSnapshot`; `IndexScanEnd` = drop(Box). |
| 12 | `index_markpos` | 411 | 333 | MATCH | optional `ammarkpos` → error if absent (CHECK_SCAN_PROCEDURE, ERRCODE_INTERNAL_ERROR via elog). |
| 13 | `index_restrpos` | 435 | 342 | MATCH | IsMVCCSnapshot assert; optional `amrestrpos`; fetch_reset; kill/heap_continue reset. |
| 14 | `index_parallelscan_estimate` | 460 | 399 | MATCH | header offset + EstimateSnapshotSpace + MAXALIGN; instrument block; `amestimateparallelscan` optional. Layout math verified (see below). |
| 15 | `index_parallelscan_initialize` | 509 | 447 | MATCH | offset math; locators/offsets; SerializeSnapshot; instrument sharedinfo (owned model in `target.shared_instrument`); `aminitparallelscan` optional (am_specific). |
| 16 | `index_parallelrescan` | 564 | 504 | MATCH | SCAN_CHECKS; fetch_reset; optional `amparallelrescan`. |
| 17 | `index_beginscan_parallel` | 582 | 519 | MATCH | locator-equality asserts; RestoreSnapshot+RegisterSnapshot (seams); internal(pscan, temp=true); heap/snapshot/instrument + fetch_begin. |
| 18 | `index_getnext_tid` | 620 | 561 | MATCH | optional `amgettuple`; RecentXmin assert omitted (see design note); kill/heap_continue reset; `!found` → fetch_reset + None; `pgstat_count_index_tuples(1)`; returns xs_heaptid by value. |
| 19 | `index_fetch_heap` | 678 | 602 | MATCH | `table_index_fetch_tuple` (direct dep); `pgstat_count_heap_fetch` on found; `!xactStartedInRecovery` → kill_prior_tuple = all_dead. |
| 20 | `index_getnext_slot` | 719 | 644 | MATCH | loop: when !xs_heap_continue fetch next TID, break on None; `index_fetch_heap` → return on hit. |
| 21 | `index_getbitmap` | 764 | 675 | MATCH | optional `amgetbitmap`; kill reset; `amgetbitmap`; `pgstat_count_index_tuples(ntids)`. |
| 22 | `index_bulk_delete` | 794 | 699 | MATCH | RELATION_CHECKS; required `ambulkdelete`; callback+state owned by AM. |
| 23 | `index_vacuum_cleanup` | 815 | 710 | MATCH | RELATION_CHECKS; required `amvacuumcleanup`. |
| 24 | `index_can_return` | 834 | 726 | MATCH | RELATION_CHECKS; optional `amcanreturn` → false if absent. |
| 25 | `index_getprocid` | 872 | 742 | MATCH | procindex = nproc*(attnum-1)+(procnum-1); `rd_support[procindex]` via relcache seam (cache is relcache-owned). Arithmetic verified vs c2rust line 3990. |
| 26 | `index_getprocinfo` | 906 | 764 | SEAMED | procindex arithmetic + procnum assert in-crate; the `rd_supportinfo` slot access + lazy `fmgr_info_cxt`/`set_fn_opclass_options` init + "missing support function" elog cross one relcache seam. See seam audit. |
| 27 | `index_store_float8_orderby_distances` | 974 | 794 | MATCH | FLOAT8OID(701)/FLOAT4OID(700) branches; USE_FLOAT8_BYVAL pfree branch correctly compiled out (64-bit); lossy-type elog (ERRCODE_INTERNAL_ERROR) under recheckOrderBy. |
| 28 | `index_opclass_options` | 1042 | 856 | MATCH/SEAMED | amoptsprocnum fetch + procid logic in-crate; no-options error → relcache seam (carries ERRCODE_INVALID_PARAMETER_VALUE); reloptions machinery → reloptions seam. |

## Detail re-derivation (spot checks)

- **Constants**: `RELKIND_INDEX = 'i'`, `RELKIND_PARTITIONED_INDEX = 'I'` (pg_class.h
  168/176) match `types-tuple/src/access.rs`. `FLOAT4OID = 700`, `FLOAT8OID = 701`
  (pg_type) match `types-tuple/src/heaptuple.rs`. OK
- **SQLSTATEs**: reindex guard → ERRCODE_FEATURE_NOT_SUPPORTED; not-an-index →
  ERRCODE_WRONG_OBJECT_TYPE; CHECK_*_PROCEDURE (`elog(ERROR)`) → ERRCODE_INTERNAL_ERROR;
  lossy ORDER BY (`elog(ERROR)`) → ERRCODE_INTERNAL_ERROR; opclass-no-options →
  ERRCODE_INVALID_PARAMETER_VALUE (carried by relcache seam decl, verified). OK
- **index_getprocid arithmetic** vs c2rust line 3990: `nproc*(attnum-1)+(procnum-1)`
  — identical.
- **index_getprocinfo** vs c2rust line 4007: same procindex; lazy-init guarded by
  `fn_oid == InvalidOid`; missing-procId → elog "missing support function". The port
  delegates exactly this block to the relcache seam (cache is relcache-owned).
- **Parallel layout math**: `ParallelIndexScanDescData` header = 2×RelFileLocator +
  2×Size; port computes `2*sizeof(RelFileLocator)+2*sizeof(usize)` from the real Rust
  types (so it reflects actual layout). `SharedIndexScanInstrumentation` header =
  offsetof(winstrument) = sizeof(i32) rounded to align_of(IndexScanInstrumentation);
  port matches. `add_size`/`MAXALIGN(8)` faithful.
- **Required vs optional callbacks**: C uses CHECK_REL/SCAN_PROCEDURE (elog if NULL)
  for aminsert/ambeginscan/amrescan/amendscan/ambulkdelete/amvacuumcleanup/
  ammarkpos/amrestrpos/amgettuple/amgetbitmap; truly-optional (NULL-tolerant)
  callbacks are aminsertcleanup/amcanreturn/amestimateparallelscan/aminitparallelscan/
  amparallelrescan. The port models the always-present required ones as non-Option
  fields, and the NULL-checked-by-macro scan procs (ammarkpos/amrestrpos/amgettuple/
  amgetbitmap) as `Option` with `check_scan_procedure` reproducing the elog. Both
  render the same observable behavior: the elog fires exactly when the C callback
  pointer is NULL. The optional ones are `Option` with the assume-default branch.

## Seam audit

**Owned seam crate** (by C-source coverage — indexam.c is the only c_source):
`backend-access-index-indexam-seams`. It declares exactly one seam, `index_open`,
which `init_seams()` installs (`set(index_open)`), and `seams-init::init_all()`
calls `backend_access_index_indexam::init_seams()` (verified, seams-init/src/lib.rs:13).
`init_seams()` contains nothing but that one `set()`. No uninstalled owned seam.

**Outward seams** (all to other owners; each a thin marshal+delegate, no branching/
node-construction/computation in the seam path):
- `relation_open` / `try_relation_open` (relation-seams) — dep cycle: relcache/relation.
- `relation_rd_indam` / `relation_increment/decrement_reference_count` / `rd_support_at` /
  `index_getprocinfo` / `index_opclass_missing_options_error` (relcache-seams) — the
  rd_indam vtable + rd_support/rd_supportinfo/rd_indexcxt caches are relcache-owned.
- `predicate_lock_relation` / `check_for_serializable_conflict_in` (predicate-seams).
- `unregister_snapshot` / `estimate_snapshot_space` / `serialize_snapshot` /
  `restore_snapshot` / `register_snapshot` (snapmgr-seams).
- `pgstat_count_index_tuples` / `pgstat_count_heap_fetch` (pgstat-seams).
- `reindex_is_processing_index` (catalog-index-seams) — the RELATION_CHECKS guard
  delegates only the ReindexIsProcessingIndex lookup; the ereport itself is in-crate.
- `index_build_local_reloptions` (reloptions-seams).

`index_getprocinfo` SEAMED justification: the procindex arithmetic and the procnum
range assert live in this crate; only the `rd_supportinfo[procindex]` slot read and
its lazy `fmgr_info_cxt`/`set_fn_opclass_options` initialization (which require the
relcache entry's `rd_support`, `rd_supportinfo`, and `rd_indexcxt` memory context)
cross the seam. This is a genuine ownership boundary, not logic relocated arbitrarily
— the cache and its context are owned by relcache.c and unreachable from here. The
"missing support function" elog is part of that lazy-init block, so it is carried on
the seam's `Err` surface (documented in the seam decl). Not MISSING.

`index_opclass_missing_options_error` SEAMED justification: the no-procedure-but-
options-present error needs `indrel->rd_indextuple`'s indclass via
`SysCacheGetAttrNotNull(INDEXRELID, ...)` + `generate_opclass_name` (ruleutils) — both
relcache/syscache/ruleutils-owned. The decision (procid invalid + attoptions != NULL)
is in-crate; only the error construction crosses. ERRCODE_INVALID_PARAMETER_VALUE is
carried.

Direct deps (not seams, correctly): `backend-access-table-tableam`
(`table_index_fetch_begin/_reset/_end/_tuple`) — tableam.c is ported, so a real dep,
not a seam. Verified those functions exist in the tableam crate.

## Design conformance (step 3b)

- **Allocating + fallible**: `index_open` takes `Mcx` and returns `PgResult<Relation<'mcx>>`;
  the relation handle is `mcx`-scoped. No allocating function/seam lacks `Mcx`+`PgResult`.
- **No invented opacity**: `IndexAmRoutine` is a real vtable type (types-tableam/amapi),
  not a stand-in; `IndexInfo`/`TIDBitmap` are opaque-but-real (the C treats them opaquely
  here — opacity inherited, not introduced). `IndexScanDescData` is the real generic
  scan descriptor.
- **No ambient-global seams / shared statics**: the `Assert(TransactionIdIsValid(RecentXmin))`
  in `index_getnext_tid` is a debug-only assert on a per-backend snapmgr global; the port
  correctly omits it rather than introducing a forbidden ambient-global seam. A debug
  assert is not load-bearing logic, and modeling it would require an illegitimate global
  seam.
- **No locks held across `?`**: none taken here.
- **No registry-shaped side tables / unledgered divergence markers**: none.

## Build

`cargo build -p backend-access-index-indexam -p backend-access-index-indexam-seams`
finishes clean (only a pre-existing unrelated `types-storage` warning).

## Conclusion: PASS

All 28 functions MATCH or are properly SEAMED with justified, thin, correctly-owned
seams installed by their owners. No MISSING/PARTIAL/DIVERGES. No seam or
design-conformance findings.
