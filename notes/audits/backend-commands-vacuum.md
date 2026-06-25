# Audit: backend-commands-vacuum

- **Unit:** `backend-commands-vacuum` (`src/backend/commands/vacuum.c`)
- **Date:** 2026-06-15
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Verdict:** **PASS (self-audit)**

Ported from the C (`../pgrust/postgres-18.3/src/backend/commands/vacuum.c`, 2703
LOC) using the c2rust rendering and the src-idiomatic base for logic; reshaped
to the repo's `Oid`-token / `StrategyHandle` / value-`VacuumParams` model (the
src-idiomatic base used the abandoned central-seam + opaque-handle model).

## 1. Function inventory

vacuum.c defines 22 named functions; all 22 are present in `crates/backend-commands-vacuum/src/lib.rs`:

`check_vacuum_buffer_usage_limit`, `ExecVacuum`, `vacuum`,
`vacuum_is_permitted_for_relation`, `vacuum_open_relation`, `expand_vacuum_rel`,
`get_all_vacuum_rels`, `vacuum_get_cutoffs`, `vacuum_xid_failsafe_check`,
`vac_estimate_reltuples`, `vac_update_relstats`, `vac_update_datfrozenxid`,
`vac_truncate_clog`, `vacuum_rel`, `vac_open_indexes`, `vac_close_indexes`,
`vacuum_delay_point`, `compute_parallel_delay`, `get_vacoptval_from_boolean`,
`vac_bulkdel_one_index`, `vac_cleanup_one_index`, `vac_tid_reaped`.

No `todo!`/`unimplemented!`/own-logic stubs (no-todo-guard green).

## 2. Logic spot-checks (re-derived from C)

- **`vacuum_get_cutoffs`** — freeze/cutoff arithmetic verified line-for-line:
  modular `wrapping_sub` for safeOldestXmin/safeOldestMxact/FreezeLimit/
  MultiXactCutoff/aggressive cutoffs; `FirstNormalTransactionId` /
  `FirstMultiXactId` clamping; `min(freeze_min_age, autovacuum_freeze_max_age/2)`;
  `min(freeze_table_age, 0.95*max_age)`; the two "far in the past" WARNINGs;
  `<= relfrozenxid` / `<= relminmxid` aggressive returns. MATCH.
- **`ExecVacuum`** — full option parser: every DefElem name, the BUFFER_USAGE_LIMIT
  range check + hint, the FULL/parallel/ANALYZE/ONLY_DATABASE_STATS sanity errors,
  the freeze-age zero-vs-(-1) assignment, the buffer-strategy creation guard. MATCH.
- **`vacuum`** — recursion guard (`in_vacuum`), use_own_xacts decision tree,
  PG_TRY/PG_FINALLY modeled as a closure + always-run cleanup, per-rel
  VACUUM-then-ANALYZE loop, final vac_update_datfrozenxid. MATCH.
- **`vacuum_rel`** — lock-mode selection, open/skip, privilege + relkind +
  other-temp + partitioned guards, index_cleanup/truncate reloption resolution,
  injection points, TOAST recursion, session lock, FULL→cluster_rel vs lazy→
  table_relation_vacuum, GUC nestlevel + userid save/restore. MATCH.
- **`vac_truncate_clog` / `vac_update_datfrozenxid`** — bogus/future-XID guards,
  frozenAlreadyWrapped warning, async-freeze + commit-ts advance + CLOG/multixact/
  commit-ts truncation + wrap-limit updates. MATCH.

## 3. Seam wiring

**INWARD installed** (via `init_seams()`, declared in
`backend-access-heap-vacuumlazy-seams` — a merged owner, so the inverse
seams-init guard requires + verifies these installs):
`vacuum_get_cutoffs`, `vacuum_xid_failsafe_check`, `vac_open_indexes`
(RowExclusiveLock-bound adapter), `vac_close_indexes` (NoLock adapter),
`vac_update_relstats` (UpdateRelStatsArgs→(bool,bool) adapter),
`vac_estimate_reltuples`, `vac_bulkdel_one_index`, `vac_cleanup_one_index`,
`vacuum_delay_point`, plus the cost-state globals (`set_vacuum_failsafe_active`,
`set_vacuum_cost_active`, `set_vacuum_cost_balance`, `vacuum_failsafe_active`).

**INWARD installed** (declared in `backend-commands-vacuum-seams`, consumed by
index AMs nbtree/hash/gin/gist/spgist): no-arg `vacuum_delay_point()` (==
`vacuum_delay_point(false)`), and `vacuum_tid_is_dead(tid, callback_state: u64)`
— a thread_local `u64→TidStore` registry routing to `tidstore_is_member`.

**`tidstore_is_member`** — new decl in `vacuumlazy-seams`; its real owner
`backend-access-common-tidstore` (which owns `TidStoreIsMember` and already
installs every other `tidstore_*` seam) now installs it from its `init_seams()`.

**OUTWARD seam-and-panic** (declared in `backend-commands-vacuum-seams`, owners
unported/cyclic — panic-until-owner): xact (start/commit transaction command,
prevent/is-in-transaction-block), snapmgr (push/pop active snapshot), lmgr
(session locks, conditional/unlock relation oid), namespace
(RangeVarGetRelidExtended), syscache (search class), aclchk (database_ownercheck,
pg_class_aclcheck MAINTAIN), partition (find_all_inheritors), clog/multixact/
commit-ts truncation + xlog SetTransactionIdLimit + async ForceFreezeXids,
GetAccessStrategyWithSize, GUC (at_eoxact_guc, restrict_search_path, vacuum_*
GUCs), pgstat progress, catalog inplace-update (vac_update_relstats_apply /
vac_update_datfrozenxid_apply / pg_class+pg_database scans), index AM
bulk_delete/vacuum_cleanup, cluster_rel (VACUUM FULL). These crates' real owners
install them when they land; the owner of `backend-commands-vacuum-seams`
(this crate) is `ported` not `merged`, so the inverse guard does not yet demand
this crate install them — which is correct (they are outward).

**`analyze_rel`** — declared in new `backend-commands-analyze-seams`, called from
`vacuum()`, intentionally NOT installed: analyze.c is keystone-blocked
(tableam-analyze-scan K1 + VacAttrStats K2).

## 4. Decisions / divergences

- **Plain owned `Vec`** for the relation/index working lists (dropped the
  src-idiomatic charged-`PgVec`/`vac_context` MemoryContext accounting — this
  repo's vacuum model has no `MemoryContextHandle`). Behavior-equivalent (the
  charge accounting was cosmetic); list contents + order identical to C.
- **Cost-state globals** (`VacuumFailsafeActive`/`VacuumCostActive`/
  `VacuumCostBalance`/`VacuumCostBalanceLocal`) owned here as `thread_local`
  cells — vacuum.c declares them as backend-local C globals; no autovacuum-ext
  setter exists to delegate to. `vacuum_cost_delay`/`vacuum_cost_limit` are GUC
  reads through vacuum-seams.

## 5. Gate

`cargo check --workspace` clean (0 errors); `seams-init` both guards green
(every_seam_installing_crate_is_wired_into_init_all +
every_declared_seam_is_installed_by_its_owner); `no-todo-guard` green; new crate
compiles with 0 warnings. CONTRACT_RECONCILE_PENDING count unchanged (132).
