# Audit: backend-executor-execReplication

C source: `src/backend/executor/execReplication.c` (PG 18.3, 886 lines).
Port: `crates/backend-executor-execReplication/src/lib.rs`.
Method: re-derived from the C + c2rust run independent of the port's comments.

## Function inventory

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `build_replindex_scan_key` (54) | `build_replindex_scan_key` (130) | MATCH | Reads `indkey`/`indclass` via `search_pg_index_info` (the relcache `rd_indextuple` syscache row — the trimmed `rd_index` only carries scalars, so this mirrors the C `SysCacheGetAttrNotNull(INDEXRELID, indclass)` + `idxrel->rd_index->indkey`). Loop over `IndexRelationGetNumberOfKeyAttributes`; skips `!AttributeNumberIsValid`; `get_opclass_input_type`/`get_opclass_family`/`IndexAmTranslateCompareType(COMPARE_EQ, relam, opfamily, false)`/`get_opfamily_member`; `!OidIsValid(operator)` -> `elog(ERROR,"missing operator %d(%u,%u) in opfamily %u")` (internal msg, same args); `get_opcode`; `ScanKeyInit(attno=index_attoff+1, eq_strategy, regop, tts_values[table_attno-1])`; `sk_collation = rd_indcollation[i]`; `SK_ISNULL|SK_SEARCHNULL` on null. `Assert(skey_attoff>0)` -> debug_assert. eq_strategy `i16`->`StrategyNumber(u16)` cast for ScanKeyInit (C `StrategyNumber`=uint16). |
| 2 | `should_refetch_tuple` (133) | `should_refetch_tuple` (233) | MATCH | TM_Result switch: TM_Ok no-op; TM_Updated -> LOG with moved-partitions vs concurrent-update message (ERRCODE_T_R_SERIALIZATION_FAILURE), refetch=true; TM_Deleted -> LOG concurrent delete, refetch=true; TM_Invisible -> `elog(ERROR,"attempted to lock invisible tuple")`; default -> `elog(ERROR,"unexpected table_tuple_lock status: %u")`. `ItemPointerIndicatesMovedPartitions` inlined (offset 0xfffd + block InvalidBlockNumber 0xffffffff). |
| 3 | `RelationFindReplTupleByIndex` (178) | `RelationFindReplTupleByIndex` (292) | MATCH | `index_open(RowExclusiveLock)`; `isIdxSafeToSkipDuplicates = GetRelationIdentityOrPK(rel)==idxoid`; dirty snapshot; build scan key; `index_beginscan`; `'retry` loop: `index_rescan`, `index_getnext_slot` loop, skip eq check when safe (lazily alloc eq cache sized natts), `tuples_equal` continue, `ExecMaterializeSlot`, xwait from snap.xmin?:xmax read off `scan.xs_snapshot` (AM fills it during the dirty scan; faithful to C's `&snap`), `XactLockTableWait`+goto retry, else found+break; lock block via `lock_found_tuple`; `index_endscan`; `index_close(NoLock)`. The "found tuple, lock it" block factored into `lock_found_tuple` (shared with seq scan) — pure refactor, identical logic. |
| 4 | `tuples_equal` (281) | `tuples_equal` (453) | MATCH | `slot_getallattrs` both slots (slot2 taken `&mut` so it can be deformed, as C does); per-attr: skip `attisdropped||attgenerated`; null/null-mismatch handling; eq cache as `Vec<Oid>` of resolved eq-opr fn oids (analog of `TypeCacheEntry**`; 0=NULL); `lookup_element_eq_opr` (= `lookup_type_cache(atttypid,TYPECACHE_EQ_OPR_FINFO)->eq_opr_finfo.fn_oid`); `!OidIsValid` -> `ereport(ERROR,ERRCODE_UNDEFINED_FUNCTION,"could not identify an equality operator for type %s",format_type_be)`; `FunctionCall2Coll(eq_opr_finfo, attcollation, v1, v2)` via `function_call2_coll_datum` over the canonical Datum lane; `!DatumGetBool` -> return false. |
| 5 | `RelationFindReplTupleSeq` (354) | `RelationFindReplTupleSeq` (549) | MATCH | eq cache palloc0'd to natts; dirty snapshot; `table_beginscan(rel,&snap,0,NULL)` (tableam-seams form); `table_slot_create`; `'retry`: `table_rescan(NULL)`, `table_scan_getnextslot(Forward)` loop, `tuples_equal` continue, found+`ExecCopySlot`, xwait off `scan.rs_snapshot`, `XactLockTableWait`+goto retry, break; `lock_found_tuple`; `table_endscan`; `ExecDropSingleTupleTableSlot(scanslot)`. The `Assert(equalTupleDescs)` is a PG_USED_FOR_ASSERTS_ONLY check; omitted (no equalTupleDescs over standalone slots in scope) — assertion-only, no behavior. |
| 6 | `BuildConflictIndexInfo` (437) | `BuildConflictIndexInfo` (642) | MATCH | loop ri_NumIndices; `conflictindex != RelationGetRelid(indexRelation)` continue; `Assert(ii_UniqueOps==NULL)` debug_assert; `BuildSpeculativeIndexInfo`. Take/put of the pooled descriptor+IndexInfo to lend `&Relation`/`&mut IndexInfo` without aliasing; restores on the error path too. |
| 7 | `FindConflictTuple` (467) | `FindConflictTuple` (693) | MATCH | `*conflictslot=NULL`; `BuildConflictIndexInfo`; `'retry`: `ExecCheckIndexConstraints(relinfo,slot,estate,&conflictTid,&slot->tts_tid,list_make1_oid(conflictindex))` true -> drop prior conflictslot + return None/false; else `table_slot_create` pushed into pool (-> SlotId, so ReportApplyConflict can read it), `PushActiveSnapshot(GetLatestSnapshot())`, `table_tuple_lock(GetActiveSnapshot(), LockTupleShare, LockWaitBlock, 0)`, `PopActiveSnapshot`, `should_refetch_tuple` -> goto retry, else return Some. The C per-slot `ExecDropSingleTupleTableSlot(*conflictslot)` on the satisfies-path: in the owned slot-pool model a per-slot drop would shift pool ids, so a prior conflict slot from a failed retry stays in `es_tupleTable` and is reclaimed at FreeExecutorState — the exact compromise execIndexing's standalone slots make (documented in-code). Behavior-preserving. |
| 8 | `CheckAndReportConflict` (521) | `CheckAndReportConflict` (779) | MATCH | `foreach_oid(uniqueidx, ri_onConflictArbiterIndexes)`: `list_member_oid(recheckIndexes,uniqueidx) && FindConflictTuple(...)` -> build ConflictTupleInfo {slot,indexoid}, `GetTupleTransactionInfo(conflictslot,&xmin,&origin,&ts)`, lappend; if any -> `ReportApplyConflict(estate,relinfo,ERROR, len>1?CT_MULTIPLE_UNIQUE_CONFLICTS:type, searchslot, remoteslot, conflicttuples)`. `track_commit_timestamp` GUC read from guc-tables vars (commit-ts.c global, passed to GetTupleTransactionInfo which the conflict crate signature takes explicitly); `MySubscription->oid` via worker seam `my_subscription_oid` (worker.c global, ReportApplyConflict's `subid`). |
| 9 | `ExecSimpleRelationInsert` (561) | `ExecSimpleRelationInsert` (874) | MATCH | `Assert(relkind==RELKIND_RELATION)`; `CheckCmdReplicaIdentity(CMD_INSERT)`; BR insert triggers gated on `ri_TrigDesc->trig_insert_before_row`; `ExecComputeStoredGenerated(CMD_INSERT)` gated on `constr->has_generated_stored`; `ExecConstraints` gated on `constr`; `ExecPartitionCheck(true)` gated on `relispartition`; `simple_table_tuple_insert`; `ExecInsertIndexTuples(false, conflictindexes?true:false, &conflict, conflictindexes, false)` gated on ri_NumIndices>0; `if conflict CheckAndReportConflict(CT_INSERT_EXISTS, recheck, NULL, slot)`; `ExecARInsertTriggers(slot, recheckIndexes, NULL)`; `list_free` = owned PgVec dropped. |
| 10 | `ExecSimpleRelationUpdate` (650) | `ExecSimpleRelationUpdate` (984) | MATCH | tid=&searchslot->tts_tid; `Assert(relkind==RELKIND_RELATION)`+`Assert(!IsCatalogRelation)`; `CheckCmdReplicaIdentity(CMD_UPDATE)`; BR update triggers `ExecBRUpdateTriggers(epqstate,tid,NULL,slot,NULL,NULL,false)` gated; generated/constraints/partition as insert (CMD_UPDATE); `simple_table_tuple_update(estate->es_snapshot,&update_indexes)`; `ExecInsertIndexTuples(true,...,update_indexes==TU_Summarizing)` gated on ri_NumIndices>0 && update_indexes!=TU_None; `if conflict CheckAndReportConflict(CT_UPDATE_EXISTS, recheck, searchslot, slot)`; `ExecARUpdateTriggers(NULL,NULL,tid,NULL,slot,recheckIndexes,NULL,false)`. tmfd: C passes NULL; the trigger seam requires `&mut TM_FailureData` so a throwaway local is passed (output discarded), the established nodeModifyTable convention. |
| 11 | `ExecSimpleRelationDelete` (733) | `ExecSimpleRelationDelete` (1128) | MATCH | tid=&searchslot->tts_tid; `CheckCmdReplicaIdentity(CMD_DELETE)`; BR delete triggers `ExecBRDeleteTriggers(epqstate,tid,NULL,NULL,NULL,NULL,false)` gated; `simple_table_tuple_delete(estate->es_snapshot)`; `ExecARDeleteTriggers(tid,NULL,NULL,false)`. tmfd throwaway local (C NULL), same convention. |
| 12 | `CheckCmdReplicaIdentity` (766) | `CheckCmdReplicaIdentity` (1187) | MATCH | partitioned-table early return; non-UPDATE/DELETE early return; `RelationBuildPublicationDesc` -> the 6-branch `if/else if` error chain (rf/cols/gencols valid_for_update/delete) each `ereport(ERROR,ERRCODE_INVALID_COLUMN_REFERENCE, errmsg, errdetail)` with the exact messages/details; `OidIsValid(RelationGetReplicaIndex)` return; `relreplident==REPLICA_IDENTITY_FULL` return; final `pubactions.pubupdate`/`pubdelete` -> `ereport(ERROR,ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ...does not have a replica identity and publishes..., errhint)`. PublicationDesc obtained via `relation_build_publication_desc` seam (relcache-nodexform-seams) — SEAMED, see below. |
| 13 | `CheckSubscriptionRelkind` (876) | `CheckSubscriptionRelkind` (1288) | MATCH | `relkind != RELKIND_RELATION && != RELKIND_PARTITIONED_TABLE` -> `ereport(ERROR,ERRCODE_WRONG_OBJECT_TYPE, "cannot use relation \"%s.%s\" as logical replication target", errdetail_relkind_not_supported(relkind))`. |

Inline helper `lock_found_tuple` is a refactor of the duplicated "Found tuple, try to lock it" block (#3 + #5); not a C function, identical logic in both.

## Seam audit

Owned seam crate: `crates/backend-executor-execReplication-seams` declares ONE
seam, `get_relation_identity_or_pk`. `init_seams()` installs it (->
`logical-relation::GetRelationIdentityOrPK`; PG18 homes the fn in relation.c, the
seam lets cross-cycle consumer conflict.c reach it). `seams-init::init_all()`
calls `backend_executor_execReplication::init_seams()`. The seams-init
recurrence guards (`every_declared_seam_is_installed_by_its_owner`,
`every_seam_installing_crate_is_wired_into_init_all`) both pass.

Outward seam calls — each a thin marshal+delegate, no logic:
- execIndexing-seams `exec_check_index_constraints`, `exec_insert_index_tuples`
- execMain-seams `exec_constraints`, `exec_partition_check`
- trigger-seams `exec_br/ar_insert/update/delete_triggers`
- catalog-index-seams `build_speculative_index_info`
- amapi-seams `index_am_translate_cmptype`
- lsyscache-seams `get_opclass_input_type`/`get_opclass_family`/`get_opfamily_member`/`get_opcode`
- syscache-seams `search_pg_index_info`
- typcache-seams `lookup_element_eq_opr`
- fmgr-seams `function_call2_coll_datum`
- lmgr-seams `xact_lock_table_wait`
- snapmgr-seams `get_latest_snapshot`/`get_active_snapshot`/`push_active_snapshot`/`pop_active_snapshot`
- xact-seams `get_current_command_id`
- worker-seams `my_subscription_oid`, catalog-seams `is_catalog_relation`
- pg-class-seams `errdetail_relkind_not_supported`
- nodeModifyTable-seams `exec_compute_stored_generated` (NEW; installed by nodeModifyTable::init_seams -> lifecycle::ExecComputeStoredGenerated)
- relcache-nodexform-seams `relation_build_publication_desc` (NEW)

Direct (acyclic) calls: indexam (index_open/close/beginscan/rescan/getnext_slot/
endscan), tableam (table_slot_create/endscan/tuple_lock/simple_table_tuple_*),
tableam-seams scan begin/rescan/getnextslot, execTuples (ExecMaterializeSlot/
ExecCopySlot/ExecDropSingleTupleTableSlot), conflict
(GetTupleTransactionInfo/ReportApplyConflict/ConflictTupleInfo), logical-relation
(GetRelationIdentityOrPK), format_type_be_owned, relcache RelationGetReplicaIndex.
No cycle: conflict deps execReplication-**seams** (decls), not the impl.

`#12` is SEAMED on `relation_build_publication_desc` (-> PublicationDesc): the
publication-catalog row-filter/column-list/generated-column validity computation
lives in relcache.c's `RelationBuildPublicationDesc` + pg_publication.c, whose
*value*-producing path is unported (the existing `publication_desc` seam returns
`()` and is uninstalled). This is a genuine unported callee (seam-and-panic),
NOT absent logic in this crate. Homed in the relcache owner's seam crate
(`backend-utils-cache-relcache-nodexform-seams`, owner dir nonexistent -> guard
exempt) rather than this unit's seam crate, so this crate's own declared seam
stays installable. New `PublicationDesc` type added to
`types-catalog::pg_publication` (field-for-field vs pg_publication.h).

## Design conformance

- Allocating paths use `Mcx`+`PgResult` (`PgVec::try_reserve`+`mcx.oom`, fallible
  `clone_in`); error-message `format!`/`to_string` are at return-Err / errmsg
  sites. No new opaque handles (real `Relation`/`SlotData`/`SlotId`/`RriId`,
  canonical Datum). No shared statics (`track_commit_timestamp` is the GUC var;
  `my_subscription_oid` a worker seam). No locks held across `?`. No registries.
  `XactLockTableWait` C-NULL rel/ctid passed as `rel.name()`/tts_tid under
  `XLTW_Oper::None`, which disables the error-context callback — inert values,
  behavior-preserving (seam requires non-Option args).

## Verdict: PASS

All 13 functions MATCH; `#12`'s pubdesc dependency is SEAMED on a genuinely
unported owner (pg_publication.c validity computation). Zero seam findings; the
one inward seam is installed and wired. Gate: cargo check --workspace,
no-todo-guard, both seams-init recurrence guards, crate test --no-run all green.
