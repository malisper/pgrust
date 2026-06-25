# Audit: backend-executor-execUtils

- **Unit:** `backend-executor-execUtils`
- **C source:** `src/backend/executor/execUtils.c` (1499 lines, PostgreSQL 18.3)
- **c2rust rendering:** `../pgrust/c2rust-runs/backend-executor-execUtils/src/execUtils.rs`
- **Port:** `crates/backend-executor-execUtils/src/lib.rs`
- **Supporting crates audited:** `backend-executor-execUtils-seams` (the unit's
  own seam crate), and the new per-owner seam crates introduced by this port:
  `backend-nodes-core-seams`, `backend-access-common-next-seams`,
  `backend-access-tableam-seams`, `backend-parser-relation-seams`,
  `backend-partitioning-core-seams`, `backend-storage-lmgr-lmgr-seams`,
  `backend-utils-cache-typcache-seams`, `backend-utils-init-miscinit-seams`,
  `backend-utils-mb-mbutils-seams`, `backend-jit-jit-seams`,
  `backend-executor-execExpr-seams`, `backend-executor-execMain-seams`,
  `backend-executor-nodeModifyTable-seams`; plus the extensions to
  `backend-executor-execTuples-seams` and `backend-utils-cache-relcache-seams`.
- **Auditor:** independent re-derivation from the C sources and headers
  (`executor.h`, `memutils.h`, `htup_details.h`, `tuptable.h`, `nodes.h`,
  `parsenodes.h`, `lockdefs.h`, `pg_bitutils.h`, `errcodes.txt`,
  `itemptr.h`/`block.h`, `rel.h`), 2026-06-12

## Function inventory (every definition in execUtils.c)

execUtils.c defines exactly 45 functions (42 extern, 3 static:
`CreateExprContextInternal`, `tlist_matches_tupdesc`, `ShutdownExprContext`,
`GetResultRTEPermissionInfo` — the first is also static, making 4 static and 41
extern). The c2rust rendering additionally localizes post-preprocessor header
inlines used by this translation unit: `heap_getattr` / `fastgetattr` /
`HeapTupleNoNulls` / `att_isnull` / `fetch_att` (htup_details.h /
tupmacs.h), `exec_rt_fetch` (executor.h), `pg_prevpower2_64` /
`pg_leftmost_one_pos64` (pg_bitutils.h), `HeapTupleHeaderGet{TypeId,TypMod,
DatumLength,Natts}` (htup_details.h), `ItemPointerSetInvalid` (itemptr.h),
`MemoryContextSwitchTo`, `newNode`, list inlines, and `TupleDescAttr`. The ones
whose logic this unit depends on are ported locally (rows 46–49 below); the
header getters live in `types-tuple` and were re-verified against the headers.

| # | C function | C location | Port location | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `CreateExecutorState` | execUtils.c:88 | `lib.rs::CreateExecutorState` | MATCH | `AllocSetContextCreate(CurrentMemoryContext, "ExecutorState", ALLOCSET_DEFAULT_SIZES)` ≡ `parent.new_child("ExecutorState")` (explicit-parent model; mcx has no block-size knobs, sanctioned divergence). `makeNode(EState)` inside qcontext + the full field-initialization block ≡ `McxOwned::try_new(..., EStateData::new_in)`. `EStateData::new_in` re-checked field by field against execUtils.c:112–166: every assignment present with the exact C initial value (ForwardScanDirection, InvalidSnapshot→`Opaque(None)`, NIL→empty `PgVec`, NULL→`None`, 0/false; `es_query_cxt = qcontext` ≡ `mcx`). The `MemoryContextSwitchTo` pair is the owned-allocation model. |
| 2 | `FreeExecutorState` | execUtils.c:192 | `lib.rs::FreeExecutorState` | MATCH | C drains `es_exprcontexts` via `FreeExprContext(linitial(...), true)`; `lcons` in CreateExprContextInternal prepends, so C frees newest-first. The pool appends, so the port's highest-index-first loop over `Some` entries frees in the identical (reverse-creation) order with `isCommit=true`. JIT release: `if (es_jit) jit_release_context; es_jit = NULL` ≡ `Opaque.take()` + seam. Partition directory likewise. `MemoryContextDelete(es_query_cxt)` (frees the EState node too) ≡ `drop(estate)` of the `McxOwned` bundle. |
| 3 | `CreateExprContextInternal` (static) | execUtils.c:237 | `lib.rs::CreateExprContextInternal` | MATCH | Node created in `es_query_cxt`; all ExprContext fields initialized to the C values (`NULL` slots→`None`, per-query memory = `es_query_cxt`, fresh "ExprContext" child for `ecxt_per_tuple_memory`, aggvalues/aggnulls NULL→empty, `caseValue`/`domainValue` = (0, true), callbacks NULL). `ecxt_param_exec_vals`/`ecxt_param_list_info`/`ecxt_estate` are pointer aliases of EState fields; the owned model threads the EState explicitly instead of aliasing (documented; readers obtain them from the threaded EState, observationally identical). `lcons` linkage ≡ `add_expr_context` pool append, with reverse-order shutdown preserved in FreeExecutorState (row 2). The size parameters are accepted and ignored — mcx divergence table, same as nodeMaterial's tuplestore sizes. |
| 4 | `CreateExprContext` | execUtils.c:307 | `lib.rs::CreateExprContext` | MATCH | `CreateExprContextInternal(estate, ALLOCSET_DEFAULT_SIZES)`; the three constants verified (memutils.h:157–159 = 0 / 8*1024 / 8*1024*1024). |
| 5 | `CreateWorkExprContext` | execUtils.c:322 | `lib.rs::CreateWorkExprContext` | MATCH | `pg_prevpower2_size_t(work_mem * (Size)1024 / 16)` with `work_mem` read through the globals seam (`i32`, KB — same widening to `usize` before the multiply as the C `(Size)` cast); `Min(.., MAXSIZE)` then `Max(.., INITSIZE)` in the same order; passes MINSIZE/INITSIZE/computed max to the internal ctor. |
| 6 | `CreateStandaloneExprContext` | execUtils.c:357 | `lib.rs::CreateStandaloneExprContext` | MATCH | Node in caller's context (`mcx` = the C `CurrentMemoryContext`), which is also `ecxt_per_query_memory`; fresh "ExprContext" per-tuple child; params NULL (no EState); `ecxt_estate = NULL` ≡ the standalone value not living in any EState pool; all other fields as row 3. |
| 7 | `FreeExprContext` | execUtils.c:416 | `lib.rs::FreeExprContext` (EState-owned) + `lib.rs::FreeStandaloneExprContext` | MATCH | C: `ShutdownExprContext(econtext, isCommit)`, `MemoryContextDelete(per_tuple)`, unlink from `es_exprcontexts` if owned, `pfree(node)`. EState-owned path: shutdown then tombstone (`es_exprcontexts[i] = None`) — the drop deletes the per-tuple child context and the node, and the tombstone is the `list_delete_ptr`; other `EcxtId`s stay valid exactly as other C pointers do. Standalone path (`ecxt_estate == NULL`): shutdown then consume/drop, no unlink — split into `FreeStandaloneExprContext` because the two ownership shapes are distinct types in the owned model; together they cover both C branches. |
| 8 | `ReScanExprContext` | execUtils.c:443 | `lib.rs::ReScanExprContext` | MATCH | `ShutdownExprContext(econtext, true)` then `MemoryContextReset(per_tuple)` ≡ `.reset()`. |
| 9 | `MakePerTupleExprContext` | execUtils.c:458 | `lib.rs::MakePerTupleExprContext` | MATCH | Lazy create-once, returns the cached id thereafter. |
| 10 | `ExecAssignExprContext` | execUtils.c:485 | `lib.rs::ExecAssignExprContext` | MATCH | `ps_ExprContext = CreateExprContext(estate)`. |
| 11 | `ExecGetResultType` | execUtils.c:495 | `lib.rs::ExecGetResultType` | MATCH | Field read. |
| 12 | `ExecGetResultSlotOps` | execUtils.c:504 | `lib.rs::ExecGetResultSlotOps` | MATCH | All four branches re-derived: `resultopsset && resultops` (non-NULL ≡ `is_some`) early return with optional `*isfixed = resultopsfixed`; otherwise the isfixed cascade (`resultopsset` → `resultopsfixed`; else slot present → `TTS_FIXED(slot)` ≡ `is_fixed()` (TTS_FLAG_FIXED = 1<<4 verified tuptable.h / c2rust); else false); no slot → `&TTSOpsVirtual` ≡ `TupleSlotKind::Virtual`; else `slot->tts_ops`. The `&TTSOps*` singleton identity carries as the `TupleSlotKind` token (established model). |
| 13 | `ExecGetCommonSlotOps` | execUtils.c:536 | `lib.rs::ExecGetCommonSlotOps` | MATCH | `nplans <= 0` ≡ `is_empty()` (slice length cannot be negative); first element probed, `!isfixed` → NULL; loop over the rest with the same two early-NULL conditions; pointer equality of ops singletons ≡ token `!=`. |
| 14 | `ExecGetCommonChildSlotOps` | execUtils.c:563 | `lib.rs::ExecGetCommonChildSlotOps` | MATCH | `{outer, inner}` in that order. C passes possibly-NULL children into `ExecGetResultSlotOps`, which would deref NULL → crash; the port's `.expect` is the same impossible state, loudly. |
| 15 | `ExecAssignProjectionInfo` | execUtils.c:583 | `lib.rs::ExecAssignProjectionInfo` | MATCH/SEAMED | `ps_ProjInfo = ExecBuildProjectionInfo(plan->targetlist, ps_ExprContext, ps_ResultTupleSlot, planstate, inputDesc)` via the execExpr owner's seam; the seam takes the node and extracts those same fields in the owner (the owned tree cannot lend the target list and the node mutably at once) — thin marshal, no logic at the call site. |
| 16 | `ExecConditionalAssignProjectionInfo` | execUtils.c:603 | `lib.rs::ExecConditionalAssignProjectionInfo` | MATCH | Match branch: `ps_ProjInfo = NULL` and the three `resultops* = scanops*` copies, same order. Else branch: lazily `ExecInitResultSlot(planstate, &TTSOpsVirtual)` (seam) + the three resultops writes (Virtual/true/true), then `ExecAssignProjectionInfo(planstate, inputDesc)`. NIL targetlist ≡ `unwrap_or(&[])`. |
| 17 | `tlist_matches_tupdesc` (static) | execUtils.c:630 | `lib.rs::tlist_matches_tupdesc` | MATCH | Loop `attrno in 1..=numattrs` over paired tlist iterator; all six bail-outs in the C order: tlist too short; non-Var expr (`!var || !IsA(var, Var)` ≡ the `Some(Expr::Var)` pattern); `varattno != attrno`; `attisdropped`; `atthasmissing`; type/typmod mismatch with the `vartypmod == -1` escape. Planner-invariant `Assert`s → `debug_assert_eq!`. Trailing-item check (`tlist too long`). The unused `PlanState *ps` parameter is dropped (unused in C too). |
| 18 | `ExecAssignScanType` | execUtils.c:692 | `lib.rs::ExecAssignScanType` | SEAMED | `ExecSetSlotDescriptor(ss_ScanTupleSlot, tupDesc)` via execTuples seam; NULL slot would crash in C ≡ `.expect`. |
| 19 | `ExecCreateScanSlotFromOuterPlan` | execUtils.c:704 | `lib.rs::ExecCreateScanSlotFromOuterPlan` | MATCH | `outerPlanState` → `ExecGetResultType` → `ExecInitScanTupleSlot(estate, scanstate, tupDesc, tts_ops)` (seam). C shares the outer node's descriptor pointer; the port clones it into the per-query context — same lifetime (both live exactly as long as the per-query context), observationally identical. This is the unit's own pending seam: signature confirmed against the C, installed by `init_seams()`. |
| 20 | `ExecRelationIsTargetRelation` | execUtils.c:729 | `lib.rs::ExecRelationIsTargetRelation` | MATCH | `list_member_int(es_plannedstmt->resultRelations, scanrelid)` ≡ `contains(&(scanrelid as i32))`; NULL plannedstmt would crash in C ≡ `.expect`. |
| 21 | `ExecOpenScanRelation` | execUtils.c:742 | `lib.rs::ExecOpenScanRelation` | MATCH | `ExecGetRangeTableRelation(estate, scanrelid, false)`; unscannable-matview check under the identical predicate `(eflags & (EXEC_FLAG_EXPLAIN_ONLY \| EXEC_FLAG_WITH_NO_DATA)) == 0 && !RelationIsScannable(rel)` — flags 0x0001/0x0040 verified (executor.h:65,71); `RelationIsScannable` ≡ `rd_rel->relispopulated` (rel.h) via relcache seam. `ereport(ERROR, errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)=55000 (errcodes.txt:424), errmsg("materialized view \"%s\" has not been populated"), errhint("Use the REFRESH MATERIALIZED VIEW command."))` — message, sqlstate, and hint all match. |
| 22 | `ExecInitRangeTable` | execUtils.c:773 | `lib.rs::ExecInitRangeTable` | MATCH | All six assignments in order: range table, perminfos, `es_range_table_size = list_length`, `es_unpruned_relids`, `palloc0`'d NULL-initialized `es_relations` array (≡ `resize(.., None)` in the per-query context), `es_result_relations`/`es_rowmarks` reset to NULL (≡ empty). |
| 23 | `ExecGetRangeTableRelation` | execUtils.c:825 | `lib.rs::ExecGetRangeTableRelation` | MATCH | `Assert(rti > 0 && rti <= size)` → `debug_assert!`. Pruned-relation check: `!isResultRel && !bms_is_member(rti, es_unpruned_relids)` → `elog(ERROR, "trying to open a pruned relation")` (internal-error sqlstate by default — matches elog). First-open path: `exec_rt_fetch(rti)`, `Assert(rtekind == RTE_RELATION)` (RTE_RELATION = 0 verified), then `IsParallelWorker()` split: normal → `table_open(relid, NoLock)` + `Assert(rellockmode == AccessShareLock \|\| CheckRelationLockedByMe(rel, rellockmode, false))` (NoLock=0 / AccessShareLock=1 verified lockdefs.h; the lock check sits inside `debug_assert!` so, like the C Assert, it is not evaluated in release); worker → `table_open(relid, rellockmode)`. Result cached in `es_relations[rti-1]`. |
| 24 | `ExecInitResultRelation` | execUtils.c:880 | `lib.rs::ExecInitResultRelation` | MATCH | Open via row 23 with `isResultRel=true`; `InitResultRelInfo(rri, rel, rti, NULL, es_instrument)` via execMain seam (the caller-allocated node becomes a pool entry, id returned — pointer↔id model); lazy `palloc0` of `es_result_relations` sized `es_range_table_size`; store at `[rti-1]`; `lappend` onto `es_opened_result_relations`. |
| 25 | `UpdateChangedParamSet` | execUtils.c:910 | `lib.rs::UpdateChangedParamSet` | SEAMED | `bms_intersect(node->plan->allParam, newchg)` then `node->chgParam = bms_join(node->chgParam, parmset)` — two thin seam calls, the consuming/recycling semantics of `bms_join` carried by `take()` + move. Allocation context made explicit (`mcx` = the C CurrentMemoryContext). |
| 26 | `executor_errposition` | execUtils.c:936 | `lib.rs::executor_errposition` | MATCH | `location < 0` → 0; NULL estate or NULL sourceText → 0; `pg_mbstrlen_with_len(es_sourceText, location) + 1` via mbutils seam; `errposition(pos)` via the ported errdata mechanism; returns 0 (the C return value of errposition is also 0). |
| 27 | `RegisterExprContextCallback` | execUtils.c:963 | `lib.rs::RegisterExprContextCallback` | MATCH | Node allocated in `ecxt_per_query_memory`; pushed at the front of `ecxt_callbacks`. |
| 28 | `UnregisterExprContextCallback` | execUtils.c:989 | `lib.rs::UnregisterExprContextCallback` | MATCH | C splices out **every** entry matching `function && arg` (fn-pointer address + Datum equality) preserving the order of survivors; the port drains the chain and relinks non-matches in order — identical multiset/order semantics; removed entries drop (pfree). Covered by a unit test (`unregister_removes_all_matches...`). |
| 29 | `ShutdownExprContext` (static) | execUtils.c:1020 | `lib.rs::ShutdownExprContext` | MATCH | Fast-path empty return; pop-from-head loop calls `function(arg)` only when `isCommit`, freeing each node; list left empty either way. The C runs callbacks inside `ecxt_per_tuple_memory` purely to mop leaks — no ambient context in the owned model (documented; callbacks allocate via captured handles). |
| 30 | `GetAttributeByName` | execUtils.c:1061 | `lib.rs::GetAttributeByName` | MATCH | The `attname == NULL` / `isNull == NULL` elogs are unrepresentable (`&str`, returned tuple). NULL tuple → `(0, true)` "kinda bogus" path. `HeapTupleHeaderGetTypeId/TypMod` → `lookup_rowtype_tupdesc` (typcache seam; the refcount/`ReleaseTupleDesc` pairing becomes clone+drop). Name scan over `0..natts` with `namestrcmp == 0` ≡ NUL-padded `name_str()` byte equality (equal iff strncmp-equal within NAMEDATALEN); first match breaks with `att->attnum`. `InvalidAttrNumber` → `elog(ERROR, "attribute \"%s\" does not exist")`, same message. tmptup construction row 49; `heap_getattr` row 46. |
| 31 | `GetAttributeByNum` | execUtils.c:1124 | `lib.rs::GetAttributeByNum` | MATCH | `!AttributeNumberIsValid(attrno)` (== InvalidAttrNumber == 0) → `elog(ERROR, "invalid attribute number %d")` **before** the NULL-tuple check, same order; then identical to row 30 minus the name scan. |
| 32 | `ExecTargetListLength` | execUtils.c:1175 | `lib.rs::ExecTargetListLength` | MATCH | `list_length`. |
| 33 | `ExecCleanTargetListLength` | execUtils.c:1185 | `lib.rs::ExecCleanTargetListLength` | MATCH | Count of `!resjunk` entries. |
| 34 | `ExecGetTriggerOldSlot` | execUtils.c:1204 | `lib.rs::ExecGetTriggerOldSlot` (+ `make_rel_extra_slot`) | MATCH | Lazy: `ExecInitExtraTupleSlot(estate, RelationGetDescr(rel), table_slot_callbacks(rel))` in `es_query_cxt` (the `MemoryContextSwitchTo` wrapper ≡ allocating through the estate's own context); cached in `ri_TrigOldSlot`. The shared helper `make_rel_extra_slot` is a verbatim factoring of the four identical C bodies. |
| 35 | `ExecGetTriggerNewSlot` | execUtils.c:1226 | `lib.rs::ExecGetTriggerNewSlot` | MATCH | As row 34 for `ri_TrigNewSlot`. |
| 36 | `ExecGetReturningSlot` | execUtils.c:1248 | `lib.rs::ExecGetReturningSlot` | MATCH | As row 34 for `ri_ReturningSlot`. |
| 37 | `ExecGetAllNullSlot` | execUtils.c:1273 | `lib.rs::ExecGetAllNullSlot` | MATCH | As row 34 plus `ExecStoreAllNullTuple(slot)` (seam) before caching in `ri_AllNullSlot`. |
| 38 | `ExecGetChildToRootMap` | execUtils.c:1300 | `lib.rs::ExecGetChildToRootMap` | MATCH | Valid-flag gate; root present → `convert_tuples_by_name(RelationGetDescr(child), RelationGetDescr(root))` (descriptors cloned out of the relcache into the per-query context — the C map is built there too); root absent → NULL; flag set true in both arms; returns the cached map. |
| 39 | `ExecGetRootToChildMap` | execUtils.c:1326 | `lib.rs::ExecGetRootToChildMap` | MATCH | `Assert(ri_RootResultRelInfo)` → `.expect` (unconditional, as in C). Valid-flag gate; `indesc` = root's descr, `outdesc` = child's; `build_attrmap_by_name_if_req(indesc, outdesc, !childrel->rd_rel->relispartition)` in `es_query_cxt`; map built only when an attrMap is returned; flag set true regardless. |
| 40 | `ExecGetInsertedCols` | execUtils.c:1361 | `lib.rs::ExecGetInsertedCols` | MATCH | `GetResultRTEPermissionInfo` NULL → NULL; child rel (`ri_RootResultRelInfo`) → `ExecGetRootToChildMap`, and if a map exists `execute_attr_map_cols(map->attrMap, perminfo->insertedCols)`; otherwise the perminfo set itself (returned as a copy in the caller's `mcx` — the C lends a pointer into the perminfo; the owned tree cannot lend across the `&mut EStateData`, documented, contents identical). |
| 41 | `ExecGetUpdatedCols` | execUtils.c:1382 | `lib.rs::ExecGetUpdatedCols` | MATCH | As row 40 over `updatedCols`. |
| 42 | `ExecGetExtraUpdatedCols` | execUtils.c:1403 | `lib.rs::ExecGetExtraUpdatedCols` | MATCH | `!ri_extraUpdatedCols_valid` → `ExecInitGenerated(relinfo, estate, CMD_UPDATE)` (nodeModifyTable seam; CMD_UPDATE = 2 verified nodes.h); returns `ri_extraUpdatedCols` (copied, as row 40). |
| 43 | `ExecGetAllUpdatedCols` | execUtils.c:1418 | `lib.rs::ExecGetAllUpdatedCols` | MATCH | `bms_union(ExecGetUpdatedCols(..), ExecGetExtraUpdatedCols(..))`. The C switches to the per-tuple context and tells the caller to copy if it needs a longer lifespan; the port takes the target `mcx` explicitly — the same contract made explicit. |
| 44 | `GetResultRTEPermissionInfo` (static) | execUtils.c:1438 | `lib.rs::GetResultRTEPermissionInfo` | MATCH | Three-way rti choice (root's `ri_RangeTableIndex`; own non-zero `ri_RangeTableIndex`; else 0) and the `rti > 0` gate around `exec_rt_fetch` + `getRTEPermissionInfo(es_rteperminfos, rte)` (parse_relation seam; node pointer ≡ 0-based index into the perminfo list). |
| 45 | `ExecGetResultRelCheckAsUser` | execUtils.c:1489 | `lib.rs::ExecGetResultRelCheckAsUser` | MATCH | NULL perminfo → `elog(ERROR, "no RTEPermissionInfo found for result relation with OID %u")` (the C derefs `ri_RelationDesc` for the OID; the port prints `InvalidOid` if absent rather than crashing while building the message — same error either way); `checkAsUser ? checkAsUser : GetUserId()` via miscinit seam. |
| 46 | `heap_getattr` (htup_details.h inline, localized) | c2rust execUtils.rs:3356 | `lib.rs::heap_getattr` | MATCH | `attnum > 0`: beyond `HeapTupleHeaderGetNatts` (t_infomask2 & HEAP_NATTS_MASK = 0x07FF, verified) → `getmissingattr`; else `fastgetattr` folded in: `HeapTupleNoNulls` (≡ `!(t_infomask & HEAP_HASNULL)`, 0x0001 verified) or bit not set → fetch (`nocachegetattr`; the C `attcacheoff >= 0` `fetch_att` shortcut is a pure cache optimization with identical results — `heap_attisnull` under the guard reduces exactly to `att_isnull(attnum-1, t_bits)`); null bit set → `(0, true)`. `attnum <= 0` → `heap_getsysattr`. |
| 47 | `exec_rt_fetch` (executor.h inline) | c2rust execUtils.rs:3393 | `lib.rs::exec_rt_fetch` | MATCH | `list_nth(es_range_table, rti - 1)`. |
| 48 | `pg_prevpower2_size_t` (pg_bitutils.h) | c2rust execUtils.rs:3382 | `lib.rs::pg_prevpower2_size_t` | MATCH | `1 << pg_leftmost_one_pos(num)` ≡ `1 << (BITS-1-leading_zeros)`; UB-for-zero mirrored as a debug assertion. Unit-tested. |
| 49 | `tmptup` construction (shared C stanza in rows 30/31) | execUtils.c:1108–1111 | `lib.rs::tmptup_from_composite` | MATCH | `t_len = HeapTupleHeaderGetDatumLength` (va_header>>2 & 0x3FFFFFFF); `ItemPointerSetInvalid` ≡ `ItemPointerData::new(0xFFFF_FFFF, 0)` (InvalidBlockNumber, InvalidOffsetNumber — verified block.h/off.h via c2rust `BlockIdSet(.., 4294967295)`); `t_tableOid = InvalidOid`; `t_data = tuple` (small header cloned into `mcx`). |
| 50 | `ResetExprContext` / `ResetPerTupleExprContext` (executor.h macros) | executor.h | `lib.rs::ResetExprContext` / `ResetPerTupleExprContext` | MATCH | Extra macro ports used by callers: per-tuple-memory reset only (no callbacks), and the estate-level conditional variant. Verified against executor.h. |

## Constants verified against headers (not from memory)

| Constant | Port value | Header | OK |
|---|---|---|---|
| `EXEC_FLAG_EXPLAIN_ONLY` | 0x0001 | executor.h:65 | yes |
| `EXEC_FLAG_WITH_NO_DATA` | 0x0040 | executor.h:71 | yes |
| `ALLOCSET_DEFAULT_MINSIZE` | 0 | memutils.h:157 | yes |
| `ALLOCSET_DEFAULT_INITSIZE` | 8*1024 | memutils.h:158 | yes |
| `ALLOCSET_DEFAULT_MAXSIZE` | 8*1024*1024 | memutils.h:159 | yes |
| `ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE` | 55000 | errcodes.txt:424 | yes |
| `RTE_RELATION` | 0 | parsenodes.h (first enumerator) | yes |
| `CMD_UPDATE` | 2 | nodes.h (CMD_UNKNOWN=0, CMD_SELECT=1, CMD_UPDATE=2) | yes |
| `NoLock` / `AccessShareLock` | 0 / 1 | lockdefs.h:34,36 | yes |
| `HEAP_HASNULL` | 0x0001 | htup_details.h (via types-tuple:159) | yes |
| `HEAP_NATTS_MASK` | 0x07FF | htup_details.h (via types-tuple:179) | yes |
| `TTS_FLAG_FIXED` | 1<<4 | tuptable.h (c2rust execUtils.rs:3375) | yes |
| `InvalidBlockNumber` | 0xFFFFFFFF | block.h (c2rust `BlockIdSet`) | yes |
| `InvalidAttrNumber` | 0 | attnum.h (types-core) | yes |

## Seam audit

- **This unit's seam crate** (`backend-executor-execUtils-seams`):
  `exec_create_scan_slot_from_outer_plan` — declared by the earlier
  nodeMaterial port; signature re-confirmed against the C; installed by this
  crate's `init_seams()`, which contains nothing but the one `set()` call.
  `seams-init::init_all()` calls `backend_executor_execUtils::init_seams()`. ✔
- **Outward seams** all target unported owners (their crates do not exist
  yet, so a direct dep is impossible — calls panic loudly until the owners
  land, per repo convention). Every declaration audited as thin marshal +
  delegate; none contains branching, node construction, or computation:
  - `backend-nodes-core-seams`: `bms_is_member`, `bms_intersect`, `bms_join`,
    `bms_union` (bitmapset.c).
  - `backend-access-common-next-seams`: `build_attrmap_by_name_if_req`,
    `convert_tuples_by_name`, `convert_tuples_by_name_attrmap`,
    `execute_attr_map_cols` (attmap.c/tupconvert.c).
  - `backend-access-tableam-seams`: `table_slot_callbacks`.
  - `backend-parser-relation-seams`: `get_rte_permission_info`.
  - `backend-partitioning-core-seams`: `destroy_partition_directory`.
  - `backend-storage-lmgr-lmgr-seams`: `check_relation_locked_by_me`.
  - `backend-utils-cache-typcache-seams`: `lookup_rowtype_tupdesc`.
  - `backend-utils-init-miscinit-seams`: `get_user_id`.
  - `backend-utils-mb-mbutils-seams`: `pg_mbstrlen_with_len`.
  - `backend-jit-jit-seams`: `jit_release_context`.
  - `backend-executor-execExpr-seams`: `exec_build_projection_info` (the
    "extract fields from the node" wording places the extraction in the
    owner's implementation; the call site is one call).
  - `backend-executor-execMain-seams`: `init_result_rel_info`.
  - `backend-executor-nodeModifyTable-seams`: `exec_init_generated`.
  - Extensions to existing seam crates: `backend-executor-execTuples-seams`
    (`exec_init_result_slot`, `exec_init_scan_tuple_slot`,
    `exec_init_extra_tuple_slot`, `exec_set_slot_descriptor`,
    `exec_store_all_null_tuple`) and `backend-utils-cache-relcache-seams`
    (`relation_rd_att`, `relation_rd_rel_relispopulated`,
    `relation_rd_rel_relispartition`, `relation_get_relation_name`); plus
    reuse of `backend-access-table-table-seams::table_open`,
    `backend-access-transam-parallel-seams::is_parallel_worker`,
    `backend-utils-init-small-seams::work_mem`.
- **No `set()` outside owners**: the only non-owner `set()` calls are test
  mocks inside `#[cfg(test)]` modules (established repo pattern). ✔
- **No function body replaced by a self-seam**: every C function's logic
  lives in this crate; seams carry only other units' functions. ✔
- Direct deps (`backend-access-common-heaptuple` for
  getmissingattr/heap_attisnull/nocachegetattr/heap_getsysattr,
  `backend-utils-error` for errposition) are real already-merged crates —
  correctly direct, not seamed. ✔

## Build & tests

- `cargo build --workspace`: clean (three cosmetic non_snake_case crate-name
  warnings in the new camelCase seam crates fixed during this audit by adding
  the same `#![allow(non_snake_case)]` the sibling seam crates carry).
- `cargo test --workspace`: all green, including the unit's 13 tests
  (callback ordering/unregister semantics, expr-context pool lifecycle,
  per-tuple laziness, `pg_prevpower2_size_t`, tlist matching).

## Spot-check of the auditor

Re-derived in full detail (C → c2rust → port, line by line): rows 2, 5, 12,
17, 23, 28, 30, 39, 44, 46 — no discrepancies beyond the documented owned-model
translations (pool ids for `ExprContext*`/`ResultRelInfo*`, Oid-crossing
relations, explicit `Mcx` threading, copy-out instead of borrowed bitmapsets).

## Verdict

**PASS** — every function `MATCH` (or `SEAMED` within the rules); zero seam
findings. `CATALOG.tsv` row set to `audited`.
