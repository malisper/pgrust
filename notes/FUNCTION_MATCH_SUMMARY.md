# Function-Match Parity Summary

Total functions audited: **2872**

## Counts per verdict

| Verdict | Count |
|---|---|
| MATCH | 2438 |
| SUPERSEDED | 170 |
| SEAMED | 98 |
| PARTIAL | 97 |
| MISSING | 63 |
| DIVERGES | 6 |

## Genuine parity gaps (MISSING / PARTIAL / DIVERGES): 166

These are the real "not at parity with c2rust + og PG" items, grouped by crate.

### backend-access-transam-commit-ts (3)

- **pg_last_committed_xact** [PARTIAL] (src/backend/access/transam/commit_ts.c): GetLatestCommitTsData + non-normal-xid->all-NULL preserved, but get_call_result_type composite check + heap_form_tuple row construction dropped; returns typed Option tuple.
- **pg_xact_commit_timestamp** [PARTIAL] (src/backend/access/transam/commit_ts.c): core lookup + NULL-on-not-found matches; returns Option<TimestampTz> rather than Datum/PG_RETURN; fmgr ABI wrapping dropped.
- **pg_xact_commit_timestamp_origin** [PARTIAL] (src/backend/access/transam/commit_ts.c): lookup + found->all-NULL preserved; tuple/Datum construction + get_call_result_type error path dropped; returns Option<(ts,nodeid)>.

### backend-access-transam-twophase (1)

- **pg_prepared_xact** [PARTIAL] (twophase.c): lib.rs:919 ports per-row projection; SRF FuncCallContext/heap_form_tuple plumbing is the funcapi boundary, left to caller.

### backend-access-transam-xlog (24)

- **CheckPointGuts** [PARTIAL] (xlog.c): checkpoint.rs:335: SLRU/buffer/replication checkpoint callback sequence; all callees are panicking ext stand-ins, not wired seams.
- **CheckXLogRemoved/XLogGetLastRemovedSegno/UpdateLastRemovedPtr/RemoveTempXlogFiles/RemoveOldXlogFiles/RemoveNonParentXlogFiles/RemoveXlogFile/ValidateXLOGDirectoryStructure** [MISSING] (xlog.c): WAL file removal/recycling. Absent (RemoveOldXlogFiles only a panicking ext).
- **CreateCheckPoint** [PARTIAL] (xlog.c): checkpoint.rs:149 ports full algorithm over owned CheckpointState, but EVERY cross-subsystem call goes through mod ext (checkpoint.rs:505) which is a bare panic! stand-in. lib.rs:734 process-singleton entry also panics. xlog-checkpoint-deps debt.
- **CreateRestartPoint** [PARTIAL] (xlog.c): checkpoint.rs:353: restartpoint algorithm over owned state; ext callees panic; lib.rs:740 entry panics. Driver debt.
- **GetOldestRestartPoint/XLogShutdownWalRcv/.../assign_wal_consistency_checking** [MISSING] (xlog.c): Misc XLogCtl/GUC accessors + WAL-consistency-checking setup. Absent.
- **GetRedoRecPtr/GetFullPageWriteInfo/GetInsertRecPtr/GetFlushRecPtr/.../GetXLogWriteRecPtr** [MISSING] (xlog.c): XLogCtl position accessors. Declared as panicking driver stubs (lib.rs ~642).
- **GetSystemIdentifier/GetMockAuthenticationNonce/DataChecksumsEnabled/GetDefaultCharSignedness/GetFakeLSNForUnloggedRel** [MISSING] (xlog.c): ControlFile accessors. Absent.
- **GetWALAvailability(driver)/SetConfigOptionInternal** [MISSING] (xlog.c): lib.rs:718,725: process-singleton entries are explicit panic! stubs (xlog-driver debt). The pure retention::GetWALAvailability exists; the XLogCtl/GUC-posture-reading entry does not.
- **GetXLogBuffer/WALReadFromBuffers/AdvanceXLInsertBuffer** [MISSING] (xlog.c): WAL buffer cache. Absent.
- **InitControlFile/WriteControlFile/ReadControlFile/UpdateControlFile/LocalProcessControlFile** [MISSING] (xlog.c): pg_control disk codec. Absent.
- **RecoveryInProgress/GetRecoveryState/XLogInsertAllowed/.../ReachedEndOfBackup** [MISSING] (xlog.c): Recovery-state machinery. Absent (RecoveryInProgress only a panicking ext).
- **ReserveXLogInsertLocation/ReserveXLogSwitch/CopyXLogRecordToWAL** [MISSING] (xlog.c): WAL-insert reservation/copy. Absent.
- **ShutdownXLOG/LogCheckpointStart/LogCheckpointEnd/.../RecoveryRestartPoint** [MISSING] (xlog.c): Checkpoint-driver/logging helpers needing XLogCtl. Absent or panicking ext stand-ins.
- **WALInsertLockAcquire/AcquireExclusive/Release/UpdateInsertingAt/WaitXLogInsertionsToFinish** [MISSING] (xlog.c): WAL insert-lock machinery. Absent (declared only as panicking ext in checkpoint).
- **XLOGShmemSize/XLOGShmemInit/BootStrapXLOG/StartupXLOG** [MISSING] (xlog.c): XLogCtl shmem stand-up + bootstrap + recovery startup. Declared as panicking driver stubs (lib.rs:608-699); xlog-driver debt (task #111).
- **XLogFileInitInternal/XLogFileInit/XLogFileCopy/InstallXLogFileSegment/XLogFileOpen/XLogFileClose/PreallocXlogFiles** [MISSING] (xlog.c): WAL segment file lifecycle. Absent.
- **XLogGetOldestSegno** [PARTIAL] (xlog.c): retention.rs:173 ports the segno-min over a name iterator; the AllocateDir(XLOGDIR) scan is lifted to the caller.
- **XLogInsertRecord** [MISSING] (xlog.c): WAL-insert engine (ReserveXLogInsertLocation/CopyXLogRecordToWAL/WALInsertLock*) entirely absent. Core of xlog runtime; task #111 pending.
- **XLogPutNextOid/RequestXLogSwitch/XLogRestorePoint/XLogReportParameters/UpdateFullPageWrites** [MISSING] (xlog.c): WAL-record emitters. XLogPutNextOid declared as panicking driver stub (lib.rs:622); consumed by varsup via seam but unimplemented here.
- **XLogSetAsyncXactLSN/XLogSetReplicationSlotMinimumLSN/XLogGetReplicationSlotMinimumLSN/UpdateMinRecoveryPoint** [MISSING] (xlog.c): XLogCtl LSN bookkeeping. Absent.
- **XLogWrite/XLogFlush/XLogBackgroundFlush/XLogNeedsFlush** [MISSING] (xlog.c): WAL write/flush engine. XLogFlush declared as panicking driver stub (lib.rs:612); not implemented.
- **issue_xlog_fsync/assign_wal_sync_method/get_backup_status/do_pg_backup_start/do_pg_backup_stop/do_pg_abort_backup/register_persistent_abort_backup_handler** [MISSING] (xlog.c): WAL fsync + base-backup machinery. Absent.
- **str_time/XLogInitNewTimeline/CleanupAfterArchiveRecovery/CleanupBackupHistory/UpdateCheckPointDistanceEstimate(driver)/KeepLogSeg(driver)** [MISSING] (xlog.c): Recovery/timeline/cleanup helpers needing XLogCtl or filesystem driver. Absent (pure cores exist in retention.rs; the driver-side callers do not).
- **xlog_redo** [PARTIAL] (xlog.c): redo.rs:51: info-opcode dispatch flow is MATCH and the no-op arms return Ok faithfully; but substantive arms (xlog_redo_nextoid, xlog_redo_fpi, xlog_redo_control_file_arm) are bare panic! stand-ins in mod ext, NOT seamed despite varsup already exposing XLogRedoNextOid. Installed as rm_redo seam.

### backend-bootstrap-bootstrap (1)

- **index_register** [PARTIAL] (src/backend/bootstrap/bootstrap.c): Control flow matches C. But shared IndexInfo vocabulary trimmed to ii_Unique only; the C copyObject of expressions/predicate + state resets + exclusion-assert reduce to value Copy. Opacity inherited from shared types crate.

### backend-executor-execExpr (6)

- **ExecCheck** [MISSING] (execExpr.c): No exec_check; null-state short-circuit + IS_QUAL assert + evaluator absent (mentioned only in a doc comment).
- **ExecInitCheck** [MISSING] (execExpr.c): No exec_init_check; the make_ands_explicit(qual)->ExecInitExpr wrapper is absent.
- **ExecPrepareCheck** [MISSING] (execExpr.c): No exec_prepare_check; depends on absent ExecInitCheck + unported expression_planner.
- **ExecPrepareExpr** [PARTIAL] (execExpr.c): standalone ExecInitExpr compile implemented, but leading expression_planner (optimizer/planner.c, unported, no reachable seam) is a loud mirror-PG-and-panic; whole fn unreachable until planner lands.
- **ExecPrepareExprList** [PARTIAL] (execExpr.c): present and loops calling exec_prepare_expr, but each call panics on the unported expression_planner.
- **ExecPrepareQual** [MISSING] (execExpr.c): No exec_prepare_qual; expression_planner+ExecInitQual wrapper absent.

### backend-executor-execExprInterp (36)

- **ExecAggCopyTransValue** [PARTIAL] (execExprInterp.c): by-val/by-ref copy-or-free; by-ref datumCopy/pfree legs blocked on trans-value owner model.
- **ExecAggInitGroup** [PARTIAL] (execExprInterp.c): first-input group init; by-ref datumCopy into agg context documented panic on trimmed transfn frame / composite owner.
- **ExecAggPlainTransByRef** [PARTIAL] (execExprInterp.c): pointer-compare + newval datumCopy present, but transfn invoke is same trimmed-frame panic.
- **ExecAggPlainTransByVal** [PARTIAL] (execExprInterp.c): transfn FunctionCallInvoke is documented panic on nodeAgg-parked fmgr call frame.
- **ExecEvalAggOrderedTransDatum** [PARTIAL] (execExprInterp.c): tuplesort_putdatum; sort-state put leg via owner.
- **ExecEvalAggOrderedTransTuple** [PARTIAL] (execExprInterp.c): tuplesort_puttupleslot; slot-payload + sort-state blockers.
- **ExecEvalArrayCoerce** [PARTIAL] (execExprInterp.c): NULL fast path present; array_map per-element coerce via arrayfuncs/fmgr owner.
- **ExecEvalArrayExpr** [PARTIAL] (execExprInterp.c): element gather + multidim concat control flow present; construct_md_array/construct_empty_array belong to arrayfuncs owner (panic where unported).
- **ExecEvalCoerceViaIOSafe** [PARTIAL] (execExprInterp.c): documented panic on the I/O fn dispatch via fmgr call frame; blocked until fmgr widens the frame.
- **ExecEvalConvertRowtype** [PARTIAL] (execExprInterp.c): map build present, tuple convert via composite-datum owner.
- **ExecEvalFieldSelect** [PARTIAL] (execExprInterp.c): rowtype tupdesc lookup present; field fetch composite-datum-owner / slot-payload bound.
- **ExecEvalFieldStoreDeForm** [PARTIAL] (execExprInterp.c): composite deform into per-field cells; blocked on composite-datum owner.
- **ExecEvalFieldStoreForm** [PARTIAL] (execExprInterp.c): re-form composite from cells; blocked on composite-datum owner.
- **ExecEvalFuncExprFusage** [PARTIAL] (execExprInterp.c): documented panic — invoking op.d.func.fn_addr(fcinfo) + pgstat usage needs fmgr-widened FunctionCallInfoBaseData + pgstat owner.
- **ExecEvalFuncExprStrictFusage** [PARTIAL] (execExprInterp.c): same fmgr-call-frame + pgstat blocker; strict-NULL scan reads fcinfo->args[i].isnull (absent in trimmed frame).
- **ExecEvalJsonCoercion** [PARTIAL] (execExprInterp.c): present; coercion fn dispatch blocked on fmgr/json owner.
- **ExecEvalJsonConstructor** [PARTIAL] (execExprInterp.c): present; json[b]_build_*_worker/JsonbValueToJsonb legs route to json ADT owner; panics where unported.
- **ExecEvalJsonExprPath** [PARTIAL] (execExprInterp.c): present; JsonPathExists/Query/Value execution belongs to jsonpath owner.
- **ExecEvalJsonIsPredicate** [PARTIAL] (execExprInterp.c): present; json_validate/json type-check legs depend on json ADT owner.
- **ExecEvalMinMax** [PARTIAL] (execExprInterp.c): GREATEST/LEAST compare loop; per-pair compare fn dispatch needs fmgr call frame.
- **ExecEvalPreOrderedDistinctMulti** [PARTIAL] (execExprInterp.c): multi-col equalfn via ExecQual on pertrans; slot-payload / fmgr blockers.
- **ExecEvalPreOrderedDistinctSingle** [PARTIAL] (execExprInterp.c): equalfn compare + last-datum copy; per-arg fmgr compare / datumCopy blocked on trimmed transfn frame.
- **ExecEvalRow** [PARTIAL] (execExprInterp.c): heap_form_tuple from element cells; tuple form/bless legs via composite-datum owner.
- **ExecEvalRowNullInt** [PARTIAL] (execExprInterp.c): per-attr IS [NOT] NULL over composite; deform/expanded-record fetch reaches composite-datum owner (stand-in where record ADT unported).
- **ExecEvalScalarArrayOp** [PARTIAL] (execExprInterp.c): NULL-array guard + empty-array fast path expressible, per-element loop is documented panic — fmgr call frame + ArrayType detoast/deconstruct + get_typlenbyvalalign not reachable.
- **ExecEvalWholeRowVar** [PARTIAL] (execExprInterp.c): present; slot->tuple materialize + rowtype bless legs touch execTuples slot payload / composite-datum owner (panic stand-in where unported).
- **ExecEvalXmlExpr** [PARTIAL] (execExprInterp.c): present; XML op legs that need the xml ADT owner are seam-or-panic per the unported xml support.
- **ExecInterpExpr** [PARTIAL] (execExprInterp.c): full opcode dispatch present and most arms faithful, BUT EEOP_*_FETCHSOME/EEOP_*_VAR/EEOP_ASSIGN_*_VAR arms loud-panic 'blocked until execTuples lands slot payload'; types-nodes now carries tts_values/tts_isnull, so panics are stale/unwired, not faithful.
- **ExecJustApplyFuncToCase** [PARTIAL] (execExprInterp.c): func-dispatch leg blocked on fmgr-widened FunctionCallInfo call frame (panic naming owner).
- **ExecJustAssignInnerVar/OuterVar/ScanVar (ExecJustAssignVarImpl)** [PARTIAL] (execExprInterp.c): reads step payload + CheckOpSlotCompatibility, then panics writing resultslot->tts_values/tts_isnull 'blocked until execTuples'; slot value-array model has since landed, panic stale/unwired.
- **ExecJustAssignInnerVarVirt/OuterVarVirt/ScanVarVirt (ExecJustAssignVarVirtImpl)** [PARTIAL] (execExprInterp.c): same slot-payload blocker.
- **ExecJustHashInnerVar/OuterVar (ExecJustHashVarImpl)** [PARTIAL] (execExprInterp.c): hash leg modeled, blocked on slot payload deform/fmgr call frame.
- **ExecJustHashInnerVarVirt/OuterVarVirt (ExecJustHashVarVirtImpl)** [PARTIAL] (execExprInterp.c): same blockers.
- **ExecJustHashInnerVarWithIV** [PARTIAL] (execExprInterp.c): init-value hash + slot deform blocked on slot payload / fmgr frame.
- **ExecJustHashOuterVarStrict** [PARTIAL] (execExprInterp.c): same blockers.
- **ExecJustInnerVarVirt/OuterVarVirt/ScanVarVirt (ExecJustVarVirtImpl)** [PARTIAL] (execExprInterp.c): virtual-slot direct tts_values read blocked on execTuples slot value-array model (now landed; stale panic).

### backend-executor-execTuples (12)

- **ExecAllocTableSlot** [PARTIAL] (src/backend/executor/execTuples.c): builds only trimmed TupleTableSlot pool header (no tts_values/tts_isnull); EState pool payload convergence still pending.
- **ExecFetchSlotHeapTupleDatum** [PARTIAL] (src/backend/executor/execTuples.c): heap_copy_tuple_as_datum ported but heap_copy_tuple_as_datum_carrier panics minting composite Datum word (HeapTupleGetDatum) — bridge unported workspace-wide.
- **ExecInitExtraTupleSlot** [PARTIAL] (src/backend/executor/execTuples.c): ExecAllocTableSlot pool-header only (payload convergence pending).
- **ExecInitNullTupleSlot** [PARTIAL] (src/backend/executor/execTuples.c): ExecInitExtraTupleSlot then ExecStoreAllNullTuple seam (which panics pending payload model).
- **ExecInitResultSlot** [PARTIAL] (src/backend/executor/execTuples.c): sets ps_ResultTupleSlot + resultops faithfully, but underlying slot is payload-less pool header (ExecAllocTableSlot PARTIAL).
- **ExecInitResultTupleSlotTL** [PARTIAL] (src/backend/executor/execTuples.c): wired; calls ExecInitResultTypeTL (MISSING/unset) then ExecInitResultSlot (PARTIAL) — runtime-broken until those land.
- **ExecInitResultTypeTL** [MISSING] (src/backend/executor/execTuples.c): seam exec_init_result_type_tl declared but NEVER set, and no impl in-crate; callers would hit unset-seam panic. Declared-unset-seam gap.
- **ExecInitScanTupleSlot** [PARTIAL] (src/backend/executor/execTuples.c): sets ss_ScanTupleSlot + scanops; scandesc not carried on trimmed scanstate; slot is payload-less pool header.
- **ExecResetTupleTable** [MISSING] (src/backend/executor/execTuples.c): no implementation anywhere in crate; only named in doc comments. Per-query tuple-table teardown unwritten.
- **ExecSetSlotDescriptor** [PARTIAL] (src/backend/executor/execTuples.c): pool-seam seam_exec_set_slot_descriptor panics: installing a descriptor allocates tts_values/tts_isnull arrays the trimmed pool header lacks; payload-model convergence pending.
- **ExecStoreAllNullTuple** [PARTIAL] (src/backend/executor/execTuples.c): slot_store_fetch.rs version real over SlotData, but EState-pool seam seam_exec_store_all_null_tuple (what execUtils calls) panics pending pool payload model.
- **ExecStoreHeapTupleDatum** [PARTIAL] (src/backend/executor/execTuples.c): body present but deform_composite_datum_into_slot panics: DatumGetHeapTupleHeader composite-Datum decode unported workspace-wide.

### backend-executor-nodeBitmapHeapscan (2)

- **ExecBitmapHeapInitializeDSM** [PARTIAL] (nodeBitmapHeapscan.c): Sizing/allocate/insert + in-place pstate init faithful, but pstate_over_chunk/sinstrument_over_chunk (typed shared-object placement over DSM byte cursor) panic until execParallel typed-shared-object resolution lands.
- **ExecBitmapHeapInitializeWorker** [PARTIAL] (nodeBitmapHeapscan.c): shm_toc_lookup faithful but pstate_over_chunk/sinstrument_over_chunk panic (same typed-shared-object resolution gap).

### backend-executor-nodeHash (4)

- **ExecHashIncreaseNumBatches** [PARTIAL] (nodeHash.c): Rebatch walk + growEnabled shutoff faithful; first-time file-array-creation branch panics on PrepareTempTablespaces. repalloc branch fully implemented.
- **ExecHashTableCreate** [PARTIAL] (nodeHash.c): Sizing + serial bucket alloc + skew setup faithful; multi-batch serial spill path panics on PrepareTempTablespaces (tablespace unported); parallel branch defers to DSA-seamed setup.
- **MultiExecHash** [PARTIAL] (nodeHash.c): Dispatch faithful, but instrument!=NULL (EXPLAIN ANALYZE) InstrStartNode/InstrStopNode paths panic — instrument not reachable via seam crate yet. Common non-instrumented path works.
- **exec_build_hash32_expr (adapter for nodeHashjoin)** [MATCH] (nodeHash.c): Adapter reads each side's result desc/ops off its PlanState and delegates compilation to the real ported execExpr ExecBuildHash32Expr via the backend-executor-execExpr-seams::exec_build_hash32_expr seam (installed in execExpr init_seams). nodeHashjoin's build_hash_exprs threads node->hashkeys/hash->hashkeys + per-key op_strict, stores outer on hj_OuterHash and inner on the inner HashState hash_expr. Empty-default stand-ins removed.

### backend-executor-nodeHashjoin (4)

- **ExecHashJoinEstimate** [PARTIAL] (nodeHashjoin.c): DSM ParallelHashJoinState estimation panics — requires execParallel (not yet ported); no in-crate part to do first.
- **ExecHashJoinInitializeDSM** [PARTIAL] (nodeHashjoin.c): In-crate part done (ExecSetExecProcNode), then panics on DSM alloc/pstate-init/BarrierInit/SharedFileSetInit tail (execParallel unported).
- **ExecHashJoinInitializeWorker** [PARTIAL] (nodeHashjoin.c): In-crate part done (ExecSetExecProcNode), then panics on SharedFileSetAttach + shared-state lookup tail (execParallel unported).
- **ExecHashJoinReInitializeDSM** [PARTIAL] (nodeHashjoin.c): In-crate part done (Detach/DetachBatch), then panics on shm_toc_lookup/SharedFileSetDeleteAll/BarrierInit tail (execParallel unported).

### backend-executor-nodeModifyTable (2)

- **ExecInitModifyTable** [PARTIAL] (src/backend/executor/nodeModifyTable.c): init.rs — bulk faithful; FDW paths DIVERGE: ri_usesFdwDirectModify on non-empty fdwDirectModifyPlans, BeginForeignModify per-rel init, GetForeignModifyBatchSize/ri_BatchSize all return Err(unported) instead of C action — the FdwRoutine vtable is genuinely unported.
- **ExecInsert** [PARTIAL] (src/backend/executor/nodeModifyTable.c): heap path faithful; foreign-table leg (ri_FdwRoutine->ExecForeignInsert) mirror-PG-and-panic into unported fdwapi vtable, and ri_BatchSize>1 batch-pending path errors out. FDW-only paths unported.

### backend-executor-nodeSeqscan (3)

- **ExecSeqScanInitializeDSM** [DIVERGES] (src/backend/executor/nodeSeqscan.c): Body mirrors C but materializing typed ParallelTableScanDescData over raw shm_toc DSM byte cursor goes through local panicking stand-in pscan_over_chunk + store_parallel_scandesc panic — the DSM typed-shared-object model (execParallel) is unported, so this path panics rather than completing. Owner-stand-in, not own-logic stub.
- **ExecSeqScanInitializeWorker** [DIVERGES] (src/backend/executor/nodeSeqscan.c): Mirrors C but pscan_over_chunk + store_parallel_scandesc panic — unported DSM cursor model. Panics before completing.
- **ExecSeqScanReInitializeDSM** [DIVERGES] (src/backend/executor/nodeSeqscan.c): Mirrors C but parallel_scandesc_rs_parallel and pscan_arc_get_mut panic — same unported DSM interior-mutability model. Panics before the seam call.

### backend-nodes-core (24)

- **makeA_Expr** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 30); no impl anywhere in repo. Raw-parser A_Expr constructor not needed by current consumers, omitted.
- **makeAlias** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 438); no impl in repo.
- **makeColumnDef** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 565); no impl in repo.
- **makeDefElem** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 637); no impl in repo.
- **makeDefElemExtended** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 655); no impl in repo.
- **makeFromExpr** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 336); no impl in repo.
- **makeFuncCall** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 676); no impl in repo.
- **makeGroupingSet** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 892); no impl in repo.
- **makeJsonKeyValue** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 971); no impl in repo.
- **makeJsonTablePath** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 1026); no impl in repo.
- **makeJsonTablePathSpec** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 1005); no impl in repo.
- **makeNotNullConstraint** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 493); no impl in repo.
- **makeNullConst** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 388); no impl in repo (make_const exists, makeNullConst does not).
- **makeSimpleA_Expr** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 48); no impl in repo.
- **makeStringConst** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 618); no impl in repo.
- **makeVacuumRelation** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 907); no impl in repo.
- **makeVarFromTargetEntry** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 107); no impl in repo.
- **makeWholeRowVar** [MISSING] (backend/nodes/makefuncs.c): Real makefuncs.c func (line 137); no impl in repo.
- **query_tree_walker_impl / range_table_walker_impl / range_table_entry_walker_impl / query_tree_mutator_impl / range_table_mutator_impl / query_or_expression_tree_walker_impl / query_or_expression_tree_mutator_impl / raw_expression_tree_walker_impl / planstate_tree_walker_impl / planstate_walk_subplans / planstate_walk_members** [MISSING] (backend/nodes/nodeFuncs.c): These Query/RangeTblEntry/PlanState walker+mutator entry points present in c2rust but port models only expression_tree_walker/mutator + check_functions; Query/RTE/PlanState tree variants not implemented (carriers live in unported parsetree/execnodes surfaces).
- **tbm_attach_shared_iterate** [PARTIAL] (backend/nodes/tidbitmap.c): routes to dsa_shared_tbm_attach which is a bare panic — dsa_get_address of shared state/arrays is own logic not implemented.
- **tbm_end_shared_iterate** [MISSING] (backend/nodes/tidbitmap.c): Real tidbitmap.c func (line 1162) present in c2rust; no counterpart in port (only provide_tbm_end_iterate for private path). Shared-iterator teardown absent.
- **tbm_free_shared_area** [PARTIAL] (backend/nodes/tidbitmap.c): routes to dsa_shared_tbm_free bare panic — refcount sub-fetch + dsa_free own logic not implemented.
- **tbm_prepare_shared_iterate** [PARTIAL] (backend/nodes/tidbitmap.c): In-crate walk/sort faithful but DSA-allocation tail (dsa_allocate of state/arrays, refcount atomics, LWLockInitialize) is a bare panic — own tidbitmap.c logic not yet implemented (seam-and-panic for DSA/LWLock provider).
- **tbm_shared_iterate** [PARTIAL] (backend/nodes/tidbitmap.c): bare panic — LWLock-guarded shared-state walk not implemented; only private iterate path real.

### backend-postmaster-autovacuum (1)

- **AutoVacuumShmemSize** [PARTIAL] (autovacuum.c): returns autovacuum_worker_slots count (drives sizing) rather than literal sizeof byte layout; the real sizeof byte count is owned by substrate/shmem allocator. Behaviour-equivalent for allocation but not literal C byte count.

### backend-storage-file-buffile (6)

- **FileSetPath / FilePath / ChooseTablespace / FileSetDeleteAll** [MISSING] (fileset.c): fileset.c unported; no seam consumer in this crate.
- **ResetUnloggedRelations / ResetUnloggedRelationsInTablespaceDir / ResetUnloggedRelationsInDbspaceDir / parse_filename_for_nontemp_relation** [MISSING] (reinit.c): reinit.c is part of the unit but unported (CATALOG: STILL TODO).
- **SharedFileSetInit / SharedFileSetAttach / SharedFileSetDeleteAll / SharedFileSetOnDetach** [MISSING] (sharedfileset.c): sharedfileset.c is part of the unit but unported (CATALOG: STILL TODO).
- **clone_file** [MISSING] (copydir.c): copydir.c unported.
- **copy_file** [MISSING] (copydir.c): copydir.c unported.
- **copydir** [MISSING] (copydir.c): copydir.c is part of the unit but NOT ported (CATALOG: STILL TODO for unit: copydir.c). No counterpart.

### backend-storage-ipc-dsm-core (5)

- **dsm_backend_startup** [PARTIAL] (dsm.c): Only the dsm_init_done flip ported; EXEC_BACKEND control-segment re-mapping branch intentionally dropped (inherited-fork model). Documented; matches non-EXEC_BACKEND build.
- **dsm_impl_pin_segment** [PARTIAL] (dsm_impl.c): On POSIX/SysV/mmap the C body is empty (pin work is WIN32 DuplicateHandle); port is no-op returning 0 pm_handle, matching non-WIN32 build. WIN32 logic not ported.
- **dsm_impl_unpin_segment** [PARTIAL] (dsm_impl.c): non-WIN32 body empty; port is no-op. WIN32 logic not ported.
- **dsm_impl_windows** [MISSING] (dsm_impl.c): WIN32-only; intentionally not ported (matches non-WIN32 build). dsm_impl_op windows case unreachable here.
- **dsm_set_control_handle** [MISSING] (dsm.c): EXEC_BACKEND-only; intentionally not ported (no EXEC_BACKEND in this build).

### backend-storage-lmgr-proc (8)

- **AuxiliaryProcKill** [PARTIAL] (proc.c): faithful slot release + sema; DisownLatch panic-through same as ProcKill.
- **CheckDeadLock** [PARTIAL] (proc.c): faithful: acquire all NUM_LOCK_PARTITIONS partition LWLocks, unlinked-recheck, HardDeadLock RemoveFromWaitQueue, release reverse; BUT seam::deadlock_check(myproc) is a panic-through (DeadLockCheck over lock.c LockSpace arena: lock.c not yet ported) even though deadlock crate is merged — proc.c not yet wired, so deadlock-check call aborts at runtime.
- **InitAuxiliaryProcess** [PARTIAL] (proc.c): faithful AuxiliaryProcs free-slot scan/MyProc init/semaphore reset/on_shmem_exit; same OwnLatch/SwitchToSharedLatch panic-through for PGPROC-embedded latch.
- **InitProcess** [PARTIAL] (proc.c): faithful freelist pop/MyProc setup/too-many-clients & too-many-wal-senders FATAL/semaphore reset/ProcArrayAdd; BUT OwnLatch(&MyProc->procLatch) and SwitchToSharedLatch route to seam panic-throughs (latch<->proc PGPROC-latch bridge not wired), so the latch-ownership step aborts at runtime. Non-latch logic matches C.
- **JoinWaitQueue** [PARTIAL] (proc.c): faithful priority-insertion scan (group locking, conflict_tab/LockCheckConflicts/GrantLock, insert) via lock seams; BUT the early-deadlock branch calls seam::remember_simple_deadlock which is a panic-through (RememberSimpleDeadLock over lock.c LockSpace arena: lock.c not yet ported). Non-deadlock paths match C.
- **ProcKill** [PARTIAL] (proc.c): faithful: clear lock-group leadership/membership, syncrep cleanup, return PGPROC to freelist, ProcGlobal bookkeeping; but DisownLatch(&MyProc->procLatch) and SwitchBackToLocalLatch route to latch-bridge panic-throughs, aborting the latch-disown step at runtime.
- **ProcLockWakeup** [PARTIAL] (proc.c): faithful waiter walk with conflict_tab/ahead_requests + GrantLock + wakeup; wakeup_waiter helper hits the same SetLatch panic-through. The separate inward proc_lock_wakeup over a deadlock LockSpace arena is also a panic-through.
- **ProcWakeup** [PARTIAL] (proc.c): faithful: detached-link guard, dclist_delete via lock seam, clear wait state; BUT final SetLatch(&proc->procLatch) routes to set_proc_latch panic-through (PGPROC-latch bridge), aborting wake-signal at runtime.

### backend-storage-page (1)

- **compactify_tuples** [PARTIAL] (backend/storage/page/bufpage.c): Implements one faithful core loop packing kept tuples down from pd_special; produces identical final page. C's two hot-path optimizations (presorted memmove-only + scratch-buffer qsort) collapsed (behaviour-equivalent). Result-identical, but algorithm simplified vs C source.

### backend-tcop-backend-startup (2)

- **StartupPacketTimeoutHandler** [DIVERGES] (backend/tcop/backend_startup.c): Same as process_startup_packet_die: C _exit(1) vs port std::process::exit(1) (runs atexit). Should be libc::_exit(1).
- **process_startup_packet_die** [DIVERGES] (backend/tcop/backend_startup.c): C uses _exit(1) (immediate, skips atexit/on_exit, deliberate for pre-shmem signal handler). Port uses std::process::exit(1) which runs Rust destructors and libc atexit. Behavioural difference: should be libc::_exit(1).

### backend-utils-adt-acl (4)

- **aclcontains** [PARTIAL] (acl.c): argless entry panics at unported fmgr PG_GETARG marshaling boundary; faithful body in aclcontains_impl (not yet reachable via SQL).
- **aclexplode** [PARTIAL] (acl.c): SRF entry panics at unported FuncCallContext/SRF_* machinery; per-row expansion body present.
- **convert_any_priv_string** [PARTIAL] (acl.c): argless scaffold panics (needs priv text); real logic in convert_any_priv_string_str, used by callers.
- **makeaclitem** [PARTIAL] (acl.c): argless entry panics at fmgr marshaling boundary; full body incl any_priv_map in makeaclitem_impl.

### backend-utils-adt-misc2 (2)

- **DatumGetExpandedRecord** [PARTIAL] (expandedrecord.c): reachable expand-the-hard-way branch faithfully calls make_expanded_record_from_datum; already-R/W-expanded-pointer branch is mirror-and-panic (owned model can't materialize live header from datum-pointer handle). Keystone boundary.
- **domain_check_safe** [MISSING] (domains.c): Genuinely absent: no PgResult-side soft-error model in this crate. Real C function with no implementation.

### backend-utils-adt-multirangetypes (5)

- **multirange_adjacent_multirange** [MISSING] (multirangetypes.c): C SQL entry (c:2535) has its OWN two-ended adjacency logic over two multiranges (bounds_adjacent at both edges) distinct from range_adjacent_multirange_internal; no _internal exists; absent.
- **multirange_overleft_multirange** [MISSING] (multirangetypes.c): C SQL entry (c:2134) inline: compare last-bound uppers of mr1,mr2; no _internal; absent.
- **multirange_overleft_range** [MISSING] (multirangetypes.c): C SQL entry (multirangetypes.c:2109) has its OWN inline logic (last bound of mr, deserialize r, range_cmp_bounds(upper1,upper2)<=0) with NO *_internal to delegate to; absent from port.
- **multirange_overright_multirange** [MISSING] (multirangetypes.c): C SQL entry (c:2216) inline: compare first-bound lowers of mr1,mr2; no _internal; absent.
- **multirange_overright_range** [MISSING] (multirangetypes.c): C SQL entry (c:2192) inline: first-bound lower of mr vs deserialized r lower, range_cmp_bounds>=0; no _internal; absent.

### backend-utils-adt-rangetypes (4)

- **range_in** [PARTIAL] (rangetypes.c): own parse/flag/make_range logic present, but element InputFunctionCallSafe on cache.typioproc is a bare panic! not a real seam ::call into fmgr — fmgr element-IO dispatch unported. Bare panic for dispatch leg is not SEAMED; surrounding own logic complete.
- **range_out** [PARTIAL] (rangetypes.c): deserialize + range_deparse own logic present; element OutputFunctionCall is bare panic!, fmgr unported.
- **range_recv** [PARTIAL] (rangetypes.c): flag-mask + serialize own logic present but pq_getmsg* (pqformat) AND element recv fmgr are bare panic!. pqformat+fmgr unported; not real seams.
- **range_send** [PARTIAL] (rangetypes.c): pq_beginmessage/sendbyte/sendbytes + element send fmgr are bare panic!; pqformat+fmgr unported.

### backend-utils-cache-catcache (1)

- **PrepareToInvalidateCacheTuple** [PARTIAL] (catcache.c): inval_support.rs: hash/list-walk + newtuple paths ported, BUT tuple_data_area panics — deforming a bare HeapTupleData needs the user-data area owned by the not-yet-landed heaptuple tuple-carrier substrate, so the deform path is a panic stand-in.

### backend-utils-cache-lsyscache (1)

- **get_typalign** [MISSING] (lsyscache.c): No standalone get_typalign in port (SearchSysCache1(TYPEOID)->typalign else TYPALIGN_INT). get_typlenbyvalalign exists but the standalone single-field accessor is absent; no caller in repo yet.

### backend-utils-cache-relcache (4)

- **RelationBuildRuleLock** [PARTIAL] (relcache.c): derived.rs present but rules are presence-only in owned mirror (rd_has_rules); full RewriteRule node payload not materialized (rewrite vocabulary owner not ported).
- **equalPolicy** [MISSING] (relcache.c): deep policy equality not implemented (policy vocabulary unported).
- **equalRSDesc** [MISSING] (relcache.c): row-security desc equality not implemented; keep_policies approximated by rd_has_rsdesc flag compare in RelationRebuildRelation.
- **equalRuleLocks** [MISSING] (relcache.c): deep rule-content equality not implemented; RelationRebuildRelation keep_rules approximated by rd_has_rules flag compare.

