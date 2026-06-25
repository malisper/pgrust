# Audit: backend-utils-adt-ri-triggers

- Date: 2026-06-12
- Model: Opus 4.8 [1m]
- C source: `../pgrust/postgres-18.3/src/backend/utils/adt/ri_triggers.c` (3233 lines, 42 functions)
- Crate: `crates/backend-utils-adt-ri-triggers` (lib.rs, cache.rs, checks.rs, querybuild.rs, triggers.rs)
- Owned inward seam crate: `crates/backend-utils-adt-ri-triggers-seams`
- Branch: `port/backend-utils-adt-ri-triggers`

## Top-line verdict: **PASS**

Independently re-derived the full function inventory from the C (42 definitions,
including `RI_FKey_trigger_type` at line 3210 which the prior audit's count of 41
omitted but which is in fact ported). All 42 are present (0 missing = no
divergence). The two prior `DIVERGES` findings (`ri_GenerateQualCollation`
cache-miss error path, `ri_ReportViolation` `has_perm` RLS+aclcheck) are now
fixed in-crate with faithful C logic. Build green (`cargo check --workspace`);
touched crates test green (the unrelated `backend-postmaster-launch-backend`
test-build failure on `BackendType` variant names predates this branch and is
out of scope).

## Verified constants (against headers, not memory)

- RI proc OIDs `F_RI_FKEY_*` 1644–1655 — exact (pg_proc.dat lines 4066–4110).
- `ANYMULTIRANGEOID = 4537` — exact (pg_type.dat).
- `SECURITY_LOCAL_USERID_CHANGE=0x0001`, `SECURITY_NOFORCE_RLS=0x0004` — exact (miscadmin.h).
- `SPI_OK_FINISH=2`, `SELECT=5`, `DELETE=8`, `UPDATE=9` — exact (spi.h).
- `RI_TRIGGER_PK=1`, `FK=2`, `NONE=0` — exact (trigger.h).
- `FKCONSTR_MATCH_FULL/PARTIAL/SIMPLE = 'f'/'p'/'s'` — exact (parsenodes.h).
- `TRIGGER_EVENT_*` bits, masks — exact (trigger.h).
- `CONSTRAINT_FOREIGN='f'`, `NAMEDATALEN=64`, `INDEX_MAX_KEYS=32` — exact.
- `RI_PLAN_*` 1–10, `RI_KEYS_*` 0/1/2, `MAX_QUOTED_*` derivations — exact.
- `ACL_SELECT = 1<<1` (parsenodes.h) — exact (types-acl acl.rs).
- `ACLCHECK_OK = 0` (acl.h) — exact (types-acl AclResult).
- `RLS_NONE=0`, `RLS_NONE_ENV=1`, `RLS_ENABLED=2` (rls.h) — exact (types-acl CheckEnableRlsResult).

## Per-function table

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | RI_FKey_check (250) | triggers.rs `ri_fkey_check`/`finish_fkey_check` | MATCH | SnapshotSelf skip, RowShareLock, NullCheck switch, temporal query, detectNewRows=is_partitioned, SPI_finish. |
| 2 | RI_FKey_check_ins (474) | `ri_fkey_check_ins` | MATCH | CheckTrigger INSERT then shared. |
| 3 | RI_FKey_check_upd (490) | `ri_fkey_check_upd` | MATCH | CheckTrigger UPDATE then shared. |
| 4 | ri_Check_Pk_Match (511) | checks.rs `ri_check_pk_match` | MATCH | LOOKUPPK_FROM_PK, pp_eq_oprs, temporal HAVING, SPI_OK_SELECT. |
| 5 | RI_FKey_noaction_del (639) | `ri_fkey_noaction_del` | MATCH | DELETE, ri_restrict(true). |
| 6 | RI_FKey_restrict_del (659) | `ri_fkey_restrict_del` | MATCH | DELETE, ri_restrict(false). |
| 7 | RI_FKey_noaction_upd (676) | `ri_fkey_noaction_upd` | MATCH | UPDATE, ri_restrict(true). |
| 8 | RI_FKey_restrict_upd (696) | `ri_fkey_restrict_upd` | MATCH | UPDATE, ri_restrict(false). |
| 9 | ri_restrict (712) | triggers.rs `ri_restrict` | MATCH | NO ACTION Pk_Match short-circuit; temporal subquery byte-identical; is_restrict=!is_no_action; detectNewRows=true. |
| 10 | RI_FKey_cascade_del (915) | `ri_fkey_cascade_del` | MATCH | RowExclusiveLock, DELETE FROM, SPI_OK_DELETE. |
| 11 | RI_FKey_cascade_upd (1017) | `ri_fkey_cascade_upd` | MATCH | UPDATE SET + WHERE, queryoids dup, nkeys*2 args, SPI_OK_UPDATE. |
| 12 | RI_FKey_setnull_del (1134) | `ri_fkey_setnull_del` | MATCH | ri_set(true, DELETE). |
| 13 | RI_FKey_setnull_upd (1149) | `ri_fkey_setnull_upd` | MATCH | ri_set(true, UPDATE). |
| 14 | RI_FKey_setdefault_del (1164) | `ri_fkey_setdefault_del` | MATCH | ri_set(false, DELETE). |
| 15 | RI_FKey_setdefault_upd (1179) | `ri_fkey_setdefault_upd` | MATCH | ri_set(false, UPDATE). |
| 16 | ri_set (1195) | triggers.rs `ri_set` | MATCH | queryno switch; confdelsetcols-vs-fk_attnums; SET DEFAULT recheck via ri_restrict(true). |
| 17 | RI_FKey_pk_upd_check_required (1386) | `ri_fkey_pk_upd_check_required` | MATCH | NullCheck != NONE_NULL→false; newslot KeysEqual→false. |
| 18 | RI_FKey_fk_upd_check_required (1418) | `ri_fkey_fk_upd_check_required` | MATCH | ALL_NULL→false; SOME_NULL match-type switch; current-xact-tuple→true; KeysEqual→false. |
| 19 | RI_Initial_Check (1519) | triggers.rs `ri_initial_check` + `build_join_check_query` | MATCH | ExecCheckPermissions gate; bypassrls/ownercheck RLS gate; LEFT OUTER JOIN; work_mem bump; snapshot limit 1; MATCH FULL null recheck; AtEOXact_GUC. |
| 20 | RI_PartitionRemove_Check (1813) | triggers.rs `ri_partition_remove_check` + builder | MATCH | partition-constraint preamble; pk_attnums=i+1; partgone=true. |
| 21 | quoteOneName (2031) | querybuild.rs `append_quoted_name` | MATCH | `"`-doubling, byte-exact. |
| 22 | quoteRelationName (2051) | querybuild.rs `append_quoted_relation` | MATCH | `"nsp"."rel"`. |
| 23 | ri_GenerateQual (2068) | querybuild.rs `ri_generate_qual` | MATCH | " sep " + generate_operator_clause (seam). |
| 24 | ri_GenerateQualCollation (2095) | querybuild.rs `ri_generate_qual_collation` | **MATCH (fixed)** | OidIsValid early-return; on syscache `None` for a valid collation OID now raises `PgError::error("cache lookup failed for collation {collation}")` — mirrors C `elog(ERROR, "cache lookup failed for collation %u")`. No longer a silent `Ok(())`. COLLATE qualification byte-identical. |
| 25 | ri_BuildQueryKey (2136) | cache.rs `ri_build_query_key` | MATCH | root_id except LOOKUPPK_FROM_PK. |
| 26 | ri_CheckTrigger (2168) | checks.rs `ri_check_trigger` | MATCH | CALLED_AS_TRIGGER, AFTER ROW, per-event; TRIGGER_PROTOCOL_VIOLATED. |
| 27 | ri_FetchConstraintInfo (2214) | cache.rs `ri_fetch_constraint_info` | MATCH | InvalidOid hint; pk/fk cross-checks; confmatchtype; MATCH PARTIAL FEATURE_NOT_SUPPORTED. |
| 28 | ri_LoadConstraintInfo (2268) | cache.rs `ri_load_constraint_info` | MATCH | cache enter/valid; DeconstructFkConstraintRow; root_id; hashes; temporal FindFKPeriodOpers; valid push. |
| 29 | get_ri_constraint_root (2366) | (seam: pg_constraint `get_ri_constraint_root`) | SEAMED | Catalog walk; thin delegate (syscache owner). Justified. |
| 30 | InvalidateConstraintCacheCallBack (2400) | cache.rs `invalidate_constraint_cache_callback` | MATCH | >1000→reset; retain marks valid=false + removes matched. Installed via owned `-seams`. |
| 31 | ri_PlanCheck (2441) | checks.rs `ri_plan_check` | MATCH | query_rel by queryno; UID swap with restore on both paths; SPI_prepare/keepplan/hash. |
| 32 | ri_PerformCheck (2484) | checks.rs `ri_perform_check` | MATCH | source_rel/source_is_pk; ExtractValues; xact-snapshot detectNewRows; UID swap; expect_OK; violation predicate. |
| 33 | ri_ExtractValues (2622) | checks.rs `ri_extract_values` | MATCH | per-key slot_getattr; nulls bool == C 'n'/' '. |
| 34 | ri_ReportViolation (2651) | checks.rs `ri_report_violation` + `report_has_perm` | **MATCH (fixed)** | Message/detail text + SQLSTATEs byte-identical. `onfk`/`rel_oid`/`attnums` selection matches C 2667-2682 (fk side: fk_attnums + fk_rel.oid; else pk). `has_perm` now in-crate per C 2697-2762: `partgone`→true; else `check_enable_rls(rel_oid, InvalidOid, true) != RLS_ENABLED`→ `pg_class_aclcheck(rel_oid, GetUserId(), ACL_SELECT)`, on miss per-column `pg_attribute_aclcheck` loop (any !=ACLCHECK_OK → false); RLS_ENABLED → false. No longer routes through `exec_check_permissions_select` (which omitted the RLS gate and could leak key values). |
| 35 | ri_NullCheck (2823) | checks.rs `ri_null_check` | MATCH | allnull/nonenull → ALL/NONE/SOME. |
| 36 | ri_InitHashTables (2860) | cache.rs `ri_init_hash_tables` | MATCH | 3 thread_local maps; CacheRegisterSyscacheCallback(CONSTROID) via seam, once. |
| 37 | ri_FetchPreparedPlan (2896) | cache.rs `ri_fetch_prepared_plan` | MATCH | find; SPI_plan_is_valid; else clear + SPI_freeplan. |
| 38 | ri_HashPreparedPlan (2948) | cache.rs `ri_hash_prepared_plan` | MATCH | enter; Assert as debug_assert. |
| 39 | ri_KeysEqual (2985) | checks.rs `ri_keys_equal` | MATCH | NULL→false; PK datum_image_eq; FK contained-by for period last key else ff_eq_oprs via ri_CompareWithCast. |
| 40 | ri_CompareWithCast (3071) | checks.rs `ri_compare_with_cast` | MATCH | cast via FunctionCall3 (typmod -1, implicit false) on both; FunctionCall2Coll. |
| 41 | ri_HashCompareOp (3117) | cache.rs `ri_hash_compare_op` | MATCH | get_opcode + fmgr_info_check; op_input_types; find_coercion_pathway_implicit; FUNC/RELABEL vs IsBinaryCoercible else elog; caches eq/cast OIDs. |
| 42 | RI_FKey_trigger_type (3210) | checks.rs `ri_fkey_trigger_type` | MATCH | tgfoid switch: 10 PK procs→RI_TRIGGER_PK, 2 FK procs→RI_TRIGGER_FK, else NONE. (Prior audit omitted from count; present and correct.) |

## Seam audit

- Owned inward seam crate: `backend-utils-adt-ri-triggers-seams` — single decl
  `invalidate_constraint_cache_callback`, installed by `init_seams()`. OK.
- Outward seams are thin marshal+delegate for cross-unit deps.
- **Fix for #34:** `report_has_perm` now performs the full RLS→table-aclcheck→
  per-column-aclcheck decision in-crate, calling per-primitive seams:
  `check_enable_rls` (declared in `backend-utils-misc-more-seams`, owner rls.c),
  `pg_class_aclcheck` / `pg_attribute_aclcheck` (declared in
  `backend-catalog-aclchk-seams`, owner aclchk.c), and the existing
  `get_user_id` (miscinit-seams). The owner crates for these primitives are not
  yet ported in this repo — none of the aclchk/rls seams are `set()` anywhere —
  so they are correct seam-and-panic outward dependencies (mirror PG and panic),
  not silent stubs. The previously-misused `exec_check_permissions_select` is no
  longer on this path.
- **Fix for #24:** the collation cache-miss error is raised in-crate as
  `elog(ERROR)`-equivalent; the syscache `None` is no longer swallowed.

## Build / test

- `cargo check --workspace`: green (pre-existing doc/unused-import warnings only).
- `cargo test -p backend-utils-adt-ri-triggers -p backend-catalog-aclchk-seams
  -p backend-utils-misc-more-seams -p types-acl`: green.
- `backend-postmaster-launch-backend` lib-test fails to compile on `BackendType`
  variant names (`AutoVacWorker`/`SlotSyncWorker`); reproduced with this branch's
  changes stashed → predates this work, out of scope for this unit.

No outstanding MISSING or DIVERGES. Verdict: **PASS**.
