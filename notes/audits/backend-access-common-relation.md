# Audit: backend-access-common-relation

- **Verdict: PASS**
- Date: 2026-06-13
- Model: claude-opus-4-8[1m]
- Unit: `backend-access-common-relation` (`src/backend/access/common/relation.c`)
- Branch: `port/backend-access-common-relation`

Independent function-by-function audit re-derived from the C source
(`postgres-18.3/src/backend/access/common/relation.c`), the c2rust rendering
(`c2rust-runs/backend-access-common-small/src/relation.rs`), and the Rust port
(`crates/backend-access-common-relation/src/lib.rs`).

## 1. Function inventory

`relation.c` defines exactly 5 functions; the c2rust rendering of the same file
contains the same 5 (`relation_open`, `try_relation_open`, `relation_openrv`,
`relation_openrv_extended`, `relation_close`), no statics, no inline helpers.
The port adds two private helpers (`relation_closer`, `finish_open`) that
factor shared logic — neither introduces or omits behavior.

| # | C function | C loc | Port loc | Verdict | Notes |
|---|------------|-------|----------|---------|-------|
| 1 | `relation_open` | relation.c:46-78 | lib.rs:105-131 | MATCH | lock-then-open; lockmode!=NoLock guard; `RelationIdGetRelation` -> `relation_id_get_relation` seam; `!RelationIsValid` -> `None` -> `elog(ERROR,"could not open relation with OID %u")` as `PgError::error` + `ERRCODE_INTERNAL_ERROR`; bootstrap-OR'd locked-by-me Assert -> `debug_assert!` (`check_bootstrap=true`); temp-namespace flag + `pgstat_init_relation`. |
| 2 | `try_relation_open` | relation.c:87-128 | lib.rs:138-174 | MATCH | lock first; `SearchSysCacheExists1(RELOID,...)` -> `search_syscache_exists_reloid`; on miss release useless lock (only if lockmode!=NoLock) and return `None`; then relcache load + same error; locked-by-me Assert WITHOUT bootstrap branch (`check_bootstrap=false`, faithful to C). |
| 3 | `relation_openrv` | relation.c:136-160 | lib.rs:179-201 | MATCH | inval only if lockmode!=NoLock; `RangeVarGetRelid(relation,lockmode,false)` (macro -> `RangeVarGetRelidExtended(...,0,NULL,NULL)`) -> `range_var_get_relid(mcx,relation,lockmode,false)`; then `relation_open(relOid, NoLock)`. |
| 4 | `relation_openrv_extended` | relation.c:171-193 | lib.rs:210-234 | MATCH | as openrv with `missing_ok` threaded into the seam; `!OidIsValid(relOid)` -> `Ok(None)`; else `relation_open(relOid, NoLock)`. |
| 5 | `relation_close` | relation.c:204-216 | lib.rs:46-55 (`relation_closer`) + types-rel `Relation::close`/`Drop` | MATCH | capture relid, `RelationClose` -> `relation_close` seam, then `UnlockRelationId(&relid,lockmode)` -> `unlock_relation_oid(relid,lockmode)` only if lockmode!=NoLock. Armed onto every handle via `Relation::open(data, Some(relation_closer))`; `Relation::close(lockmode)` is the explicit close, `Drop` is the abort path (`NoLock`, refcount-only release). The OID-keyed unlock re-derives the same lock tag the C `lockRelId` carried — faithful. |

### Constant / predicate spot-checks (against headers, not memory)

- `RelationUsesLocalBuffers(rel)` = `rd_rel->relpersistence == RELPERSISTENCE_TEMP`
  (rel.h:648) -> `RelationData::uses_local_buffers()` checks the same field
  against `RELPERSISTENCE_TEMP`. MATCH (c2rust confirms `relpersistence == 't'`).
- `XACT_FLAGS_ACCESSEDTEMPNAMESPACE` set -> `set_xact_accessed_temp_namespace`
  seam (the OR-into-`MyXactFlags` lives in the xact owner). MATCH.
- `elog(ERROR, ...)` severity ERROR, SQLSTATE XX000 (`ERRCODE_INTERNAL_ERROR`).
  Port uses `ERRCODE_INTERNAL_ERROR`. MATCH.
- `RangeVarGetRelid` macro `missing_ok ? RVR_MISSING_OK : 0` collapses to the
  `missing_ok: bool` seam arg; callback `NULL,NULL` -> no callback param. MATCH.

## 2. Seam audit

Owned seam crate (by C-source coverage): `backend-access-common-relation-seams`
declares exactly the open family — `relation_open`, `try_relation_open`,
`relation_openrv`, `relation_openrv_extended`. All four are installed by this
crate's `init_seams()` (lib.rs:32-37), which contains only `set()` calls, and
`seams-init::init_all()` calls `backend_access_common_relation::init_seams()`.
No `relation_close` seam exists — close is the handle closer (`Relation::open`
closer + `Drop`), which is the repo's relation-handle model, not absent logic.

Outward seam calls, each a real cross-unit dependency, thin marshal+delegate,
no branching/computation/node-construction in any seam path:

- relcache: `relation_id_get_relation` (typed `Option<RelationData<'mcx>>`, no
  invented opacity), `relation_close` — both added by this branch as
  declarations in `backend-utils-cache-relcache-seams` (owner installs).
- lmgr: `lock_relation_oid` (returns `LockGuard`, `.keep()` for xact-scoped
  hold — no lock held across `?` without a guard), `unlock_relation_oid`,
  `check_relation_locked_by_me` (Oid-keyed, infallible).
- syscache: `search_syscache_exists_reloid`.
- namespace: `range_var_get_relid`.
- inval: `accept_invalidation_messages`.
- miscinit: `is_bootstrap_processing_mode`.
- xact: `set_xact_accessed_temp_namespace`.
- pgstat: `pgstat_init_relation(relid)` — declaration added by this branch in
  `backend-utils-activity-pgstat-seams` (owner installs).

The two declarations this branch adds to neighbor seam crates (relcache,
pgstat) are pure `seam!` decls installed by their respective unported owners;
they panic loudly until those owners land (Mirror-PG-and-panic), and are NOT
installed by this unit. No `set()` outside the owner. No uninstalled owned seam.

## 3b. Design conformance

- Opacity inherited, never introduced: `relation_id_get_relation` hands back a
  real `RelationData<'mcx>`, not an opaque handle. PASS.
- Allocating seams carry `Mcx` + `PgResult`: `relation_id_get_relation` and the
  open family take `Mcx<'mcx>` and return `PgResult`. PASS.
- Locks held across `?`: `lock_relation_oid` returns a `LockGuard`; the unit
  `.keep()`s it for the transaction-scoped lifetime that mirrors C (released by
  `relation_close`/`unlock_relation_oid` or xact abort), so there is no guard
  silently dropped at end of scope. PASS.
- No shared statics for per-backend globals, no ambient-global seams, no
  registry-shaped side tables, no unledgered divergence markers. PASS.
- Seam signatures mirror the C failure surface (`PgResult` where the C can
  `ereport(ERROR+)`; infallible `bool`/getter seams where the C cannot). PASS.

## 4. Build / test

- `cargo build -p backend-access-common-relation`: clean (unrelated
  pre-existing unused-import warnings in two neighbor seam crates only).
- `cargo test -p backend-access-common-relation`: 10/10 pass. Tests pin
  lock-then-open ordering, useless-lock release, RangeVar inval handling,
  missing_ok short-circuit, the could-not-open error, and the relation_close
  (RelationClose-then-unlock) ordering.

## Verdict

**PASS.** All 5 C functions MATCH (relation_close realized as the armed handle
closer). Zero seam findings, zero design-conformance findings.
