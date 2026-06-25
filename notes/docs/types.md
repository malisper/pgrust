# The types-* crate stack

Shared type definitions live in small, layered `types-*` crates, populated
incrementally as ports need them — never in one shared "types" god crate.
src-idiomatic's single `types` crate grew to 618 modules / ~95k lines; do not
recreate it.

## Why incremental-and-trimmed is load-bearing

Analysis of src-idiomatic's full types crate (code references only, comments
stripped):

- The module graph is almost a DAG: one irreducible 49-module knot
  (execnodes/pathnodes/plan-and-prim-nodes/executor/fmgr/storage/cache/tcop —
  the PlanState/EState/Expr tangle, mirroring Postgres's header cycle), plus
  one trivial 3-module pgstat knot. Everything else layers cleanly.
- But **every coarse subsystem partition of the full crate is cyclic**
  (xact→wal, commands↔nodes↔catalog, cache→everything). Copying src-idiomatic
  type modules wholesale therefore forces a god crate.
- Copying only the items a port actually consumes drops most of those edges
  (e.g. our `heaptuple` module carries none of the entangling fields the
  src-idiomatic one does). The split below only stays acyclic if ports keep
  trimming to what they use.

## Current crates (bottom-up)

Regenerate this table (crate list, modules, dependency edges) in the same
change that adds or rewires a `types-*` crate.

| Crate | Modules | Contents | Depends on (types-* / mcx) |
|---|---|---|---|
| `types-core` | `primitive`, `xact`, `fmgr`, `geo`, `init`, `instrument`, `timeline` | scalar aliases (`c.h`), Oid/TransactionId/BlockNumber, CommandId, `bits32`, `ForkNumber`, compile-time limits (`pg_config_manual.h`), trimmed `FmgrInfo` (`fmgr.h`, lookup key only), instrumentation counters (`executor/instrument.h`: `instr_time`/`BufferUsage`/`WalUsage`/`Instrumentation`), timeline-history vocabulary (`access/timeline.h`/`access/xlog_internal.h`: `TimeLineHistoryEntry`, `XLOGDIR`) | nothing |
| `types-error` | `error`, `pg_error` | error vocabulary: `ErrorLevel`, `SqlState` + complete `ERRCODE_*` table (`elog.h`/`errcodes.h`), `PGErrorVerbosity`, `PgError`/`PgResult`, `SoftErrorContext` | nothing (alloc only) |
| `types-guc` | `guc` | GUC vocabulary (`utils/guc.h`/`utils/guc_tables.h`): `GucContext`, `GucSource`, `config_type`/`config_group`, flag bits, `config_enum_entry` | nothing |
| `types-dest` | `dest` | `CommandDest` codes (`tcop/dest.h`) | nothing |
| `types-explain` | (lib) | EXPLAIN output-state vocabulary (`commands/explain_state.h`), trimmed: `ExplainFormat`, `ExplainSerializeOption`, and `ExplainState<'mcx>` (output buffer `PgString`, option flags, format, `grouping_stack: PgVec<i32>`; plan-tree/per-worker fields deferred to their owners) | mcx |
| `types-pgtime` | `pgtime` | broken-down time vocabulary (`pgtime.h`) plus the `private.h` time-unit constants (SECSPERMIN, TM_YEAR_BASE, ...) and the `pgtz.h`/`tzfile.h` loaded-zone vocabulary (`pg_tz`, parsed transition `state`) | types-core |
| `types-signal` | `legacy_pqsignal` | `pqsigfunc` stand-in + `SIG_*` sentinels | nothing |
| `types-startup` | `backend_startup` | child-process startup vocabulary (`tcop/backend_startup.h`): real `CAC_state` enum, `BackendStartupData` (the `canAcceptConnections`/`socket_created`/`fork_started` fields C spells out), and the `StartupData` currency for `postmaster_child_launch`'s `void *startup_data` | types-core |
| `types-snapshot` | `snapshot` | trimmed snapshot vocabulary (`utils/snapshot.h`): `SnapshotType`, `SnapshotData` (type tag only), `IsMVCCSnapshot` | nothing |
| `types-timeout` | (lib) | timeout-manager vocabulary (`utils/timeout.h`): `TimeoutId`/`TimeoutType` enums, `EnableTimeoutParams`, `TimeoutHandlerProc` | types-core |
| `types-condvar` | `condition_variable` | `ConditionVariable` (`storage/condition_variable.h`) | nothing |
| `types-bgworker` | (lib) | background-worker vocabulary (`postmaster/bgworker.h`/`bgworker.c`): `BgwHandleStatus`, `BackgroundWorkerHandle` (slot + generation) | nothing |
| `types-stringinfo` | (lib) | `StringInfoData` (`lib/stringinfo.h`) over context-allocated storage | mcx |
| `types-datum` | `datum`, `expandeddatum`, `varlena`, `array_build` | `Datum`, the pass-by-value `*GetDatum`/`DatumGet*` codec family, `ExpandedObjectRef`, owned `Varlena`/`Bytea` images + the `SET_VARSIZE`/`VARSIZE` header encoding (`varatt.h`); `array_build` = the trimmed-opaque `ArrayBuildStateAny` carrier (`utils/array.h`) threaded by the array-accumulation seams | types-core, mcx |
| `types-wchar` | `wchar` | multibyte/wide-char vocabulary (`mb/pg_wchar.h`) | types-core |
| `types-net` | `net` | connection/socket vocabulary (`libpq/pqcomm.h`, `libpq/libpq-be.h`, `libpq/hba.h`, `common/ip.h`): `SockAddr`/`ClientSocket`, the full `Port` (+`HbaLine`), addrinfo shapes | types-core |
| `types-walreceiver` | (lib) | walreceiver/walreceiverfuncs/libpqwalreceiver vocabulary (`replication/walreceiver.h`): `WalRcvState`, `WalRcvWakeupReason`, `WalRcvStreamOptions`, `WalRcvStartupInfo`/`WalRcvStatSnapshot` spinlocked snapshots, `WalReceiverActivity` result row, opaque `WalReceiverConn` handle | types-core |
| `types-amvalidate` | `backend_access_index_amvalidate` | row/group records for `identify_opfamily_groups` (`access/amvalidate.h`), shared by all AM validators — incl. the canonical `OpFamilyOpFuncGroup` | types-core |
| `types-storage` | `storage`, `lock`, `sync`, `waiteventset`, `buf`, `bufpage` | storage/lmgr vocabulary (`storage/lwlock.h`, `storage/proclist_types.h`, `storage/lockdefs.h`, `storage/s_lock.h`, `storage/sync.h`, `storage/buf.h`, `storage/pg_shmem.h`): `LWLock`/`LWLockMode`, `pg_atomic_uint32`/`64` and `Spinlock` (`slock_t`; real atomics — the contended-acquire backoff lives in `backend-storage-lmgr-s-lock`), `LOCKMODE` + lock-level constants, `FileTag`/`SyncRequestType`/`SyncRequestHandler`, wait-event-set vocabulary, the `Buffer` handle (`buf.h`: `InvalidBuffer`/`BufferIsValid`), `HugePagesStatus`; `bufpage` = page/line-pointer sizing (`storage/off.h`/`bufpage.h`/`itemid.h`/`htup_details.h`): `ItemIdData`, `MaxOffsetNumber`, `SizeOfPageHeaderData`, `MaxHeapTuplesPerPage` | types-core |
| `types-condvar` | `condition_variable` | `ConditionVariable` + `ConditionVariableMinimallyPadded`/`CV_MINIMAL_SIZE` (`storage/condition_variable.h`): `slock_t` mutex + `proclist_head` wakeup list | types-storage |
| `types-reloptions` | `attoptcache`, `tablespace`, `local` | parsed relation-option structs (`access/common/reloptions.c` consumers); `local` adds the local/opclass reloption vocabulary (`local_relopts`, `relopt_gen`, `relopt_type`, `local_relopt`, `relopts_validator`) for index-AM `options` support functions | nothing |
| `types-regex` | `regex` | regex-engine vocabulary (`regex/regex.h`, trimmed): `REG_*` compile flags, `pg_regoff_t`/`RegMatch` (`regmatch_t`), `RegexCompiled` (consumed `re_nsub` + opaque engine `RegexHandle`), the `pg_regcomp`/`pg_regexec`/`pg_regprefix` outcome enums carrying `pg_regerror` text | types-core, mcx |
| `types-replication` | `conflict` | logical-replication conflict vocabulary (`replication/conflict.h`): `ConflictType`, `CONFLICT_NUM_TYPES` | nothing |
| `types-tsearch` | `tsearch`, `gin`, `tsgistidx` | full-text-search vocabulary consumed by the `tsvector_ops` index/rank ports (`tsearch/ts_type.h`, `tsearch/ts_utils.h`, `access/gin.h`): `WordEntry`/`WordEntryPos`+`WEP_*`, `QueryItem`/`QueryOperator`/`QueryOperand`, `TSTernaryValue`, `ExecPhraseData`, `CheckCondition` callback, OP_*/QI_*/TS_EXEC_* consts, `GinTernaryValue`+GIN consts, and the GiST `SignTsVector`/`PickSplitResult` key vocabulary | types-core |
| `types-scan` | `scankey`, `sdir`, `genam`, `snapshot` | scan vocabulary: `ScanKeyData`/`StrategyNumber` (`access/skey.h`, `access/stratnum.h`), the canonical `ScanDirection` enum (`access/sdir.h`), opaque `SysScanHandle`/`SnapshotHandle` tokens | types-core, types-datum |
| `types-sortsupport` | (lib) | sort-support comparison vocabulary (`utils/sortsupport.h`, trimmed): `SortSupportData` (collation/reverse/nulls-first data fields + the opaque `SortComparatorId` comparator token the comparator owner installs), `BTORDER_PROC`/`BTSORTSUPPORT_PROC`/`COMPARE_EQ` | types-core, mcx |
| `types-hash` | `hash`, `hsearch`, `backend_access_hash_hashvalidate` | hash AM constants (`access/hash.h`, trimmed), dynahash consumer vocabulary (`utils/hsearch.h`: trimmed `HASHCTL`, inherited-opaque `HTAB`/`HASHHDR`, `HASHELEMENT`, `HASH_SEQ_STATUS`, `HASH_*` flags, `HASHACTION`), and the hashvalidate unit's owned catalog-row mirrors (re-exports types-amvalidate's `OpFamilyOpFuncGroup`) | types-core, types-amvalidate, mcx |
| `types-cache` | `syscache`, `inval`, `skey`, `deflist` | catalog-cache access vocabulary (`utils/catcache.h`, `utils/inval.h`, `ScanKeyInit` value form) | types-core, types-datum, mcx |
| `types-catalog` | `catalog`, `catalog_dependency`, `pg_publication`, `pg_database` | catalog vocabulary for the pg_depend port (`catalog/dependency.h`, `catalog/pg_depend.h`): `DependencyType`, `ObjectAddress`, `FormData_pg_depend`, catalog/index OIDs and `Anum_*`/`Natts_*` column numbers; publication vocabulary (`catalog/pg_publication.h`: `PublishGencolsType`); the pg_database/pg_authid/pg_db_role_setting relation+index OIDs, `Template1DbOid`/`DEFAULTTABLESPACE_OID`/`ROLE_PG_USE_RESERVED_CONNECTIONS`, and the decoded `FormPgDatabase` (`catalog/pg_database.h`) the backend-startup path reads | types-core, types-datum, mcx |
| `types-wal` | `wal`, `rmgr`, `rmgrdesc`, `reorderbuffer`, `xact`, `xact_records`, `xloginsert`, `xlog_consts`, `xlogutils` | WAL record vocabulary (`access/xlogrecord.h`, `access/xlogreader.h`, `access/rmgr.h`): `XLogRecord`, trimmed decoded-record shapes (`DecodedXLogRecord` incl. `max_block_id`/`has_block_ref`), rmgr constants (`RM_*_ID`), the transaction-record vocabulary, trimmed `ReorderBufferTXN` (`replication/reorderbuffer.h`), `xloginsert` — the `REGBUF_*` buffer-registration flags (`access/xloginsert.h`), `xlogutils` — `HotStandbyState` + the `XLogRedoAction` enum (`access/xlogutils.h`), and `xlog_consts` — the WAL-engine config enums (`WalLevel`/`WalCompression`/`WalSyncMethod`/`ArchiveMode`/`RecoveryState`/`WALAvailability`, all `repr(i32)`) plus segment/page/checkpoint/`DELAY_CHKPT` constants (`access/xlog.h`, `access/xlogdefs.h`, `access/xlog_internal.h`, `storage/proc.h`) | types-core, types-storage, mcx |
| `types-control` | (lib) | control-file + checkpoint vocabulary (`catalog/pg_control.h`, `access/transam.h`): `CheckPoint`, `ControlFileData`, `DBState` (`repr(u32)`) and the pg_control constants; the on-disk codec lives in the xlog crate | types-core |
| `types-xlog-records` | one module per `*_xlog.h` header (`heapam_xlog`, `brin_xlog`, `ginxlog`, `gistxlog`, `hash_xlog`, `nbtxlog`, `spgxlog`, `multixact`, `standbydefs`) plus `arrays` | per-rmgr `xl_*` record-body structs with checked `from_bytes` constructors and typed views of `FLEXIBLE_ARRAY_MEMBER` tails, shared by the desc routines and future redo ports | types-core, types-storage, types-tuple |
| `types-freepage` | (lib) | free-page-map vocabulary (`utils/freepage.h`, `utils/relptr.h`): the full `repr(C)` `FreePageManager` layout (consumers size shmem reservations with `size_of`), `FPM_PAGE_SIZE`/`FPM_NUM_FREELISTS`, `RelPtr` | types-core |
| `types-dsa` | (lib) | dynamic-shared-area vocabulary (`utils/dsa.h` + `dsa.c` file-private constants): `DsaPointer`/`DsaHandle`/`DsaSegmentIndex`, area-geometry constants (`DSA_OFFSET_WIDTH`, `DSA_MAX_SEGMENTS`, segment bins / fullness classes / superblock pages), the `DSA_ALLOC_*` flags, and the `dsa_size_classes`/`dsa_size_class_map` lookup tables (the `dsa_area_control`/`dsa_area_span` aggregates stay file-private in the allocator crate as in-segment `repr(C)` mirrors) | types-core |
| `types-tuple` | `heaptuple`, `heap`, `access`, `attmap`, `parse`, `tupconvert`, `backend_access_common_heaptuple`, `toast_helper` | tuple layouts, tuple descriptors, the formed/deformed tuple model, attribute maps/conversion, and the owned `ToastTupleContext` (`access/toast_helper.h`) | types-core, types-datum, types-error, mcx |
| `types-rel` | (lib) | relation-descriptor vocabulary (`utils/rel.h` / `catalog/pg_class.h`, trimmed): `RelationData` (consumed `rd_*`/`rd_rel` fields + `rd_att`), `StdRdOptions`, and the `Relation` open-handle (alias = C pointer alias; `close(lockmode)`/Drop = `relation_close`); uses `std` (`Rc` for the alias sharing) | types-core, types-error, types-storage, types-tuple, mcx |
| `types-pgstat` | `activity_pgstat`, `backend_progress`, `wait_event`, `backend_utils_activity_pgstat_bgwriter` | cumulative-statistics vocabulary (`pgstat.h`, `utils/pgstat_internal.h`, `utils/backend_progress.h`) | types-core, types-storage |
| `types-nodes` | `nodes`, `execnodes`, `executor`, `planstate`, `pathnodes`, `primnodes`, `parsenodes`, `bitmapset`, `execexpr`, `instrument`, `funcapi`, `nodeforeigncustom`, `nodeindexscan`, `jointype`, `nodemergejoin`, `nodetablefuncscan`, `queryenvironment`, `copy_query` | the designated node/executor knot crate (rule 4): plan-node/plan-state/slot/tuplestore-carrier vocabulary; `NodeTag` lives here. `jointype` = `JoinType`/`Join`/`JoinStateData`; `nodemergejoin` = `MergeJoin`/`MergeJoinClauseData`/`MergeJoinStateData` + `EXEC_MJ_*`; `nodetablefuncscan` = `TableFuncScan`/`TableFuncScanState`/`TableFuncRoutineKind` (`TableFunc`/`TableFuncType` live in `primnodes`); `nodeindexscan` hosts the `Scan`/`TidScan` plan-node bases; `primnodes` carries `SubPlan`/`SubLinkType` + `Expr::{ScalarArrayOpExpr,CurrentOfExpr}` and `execexpr` the full `SubPlanState` (nodeSubplan) with execGrouping/execExpr-owned slots as `Opaque`; `execnodes` adds `ScanStateData.ss_currentRelation`, `EStateData.es_snapshot`/`es_epq_active`; `executor` adds `TupleTableSlot.tts_tid`; `copy_query` = the COPY-(query)-TO consumed shapes (`ParseState`/`Query`/`RawStmt`/`QueryDesc`/`QuerySource`/`CURSOR_OPT_PARALLEL_OK`/`T_CreateTableAsStmt`) | types-core, types-error, types-scan, types-datum, types-tuple, types-sortsupport, types-rel, types-snapshot, types-storage, mcx |
| `types-copy` | (lib) | COPY-command option vocabulary (`commands/copy.h`, trimmed to the driver-consumed members): `CopyFormatOptions` (encoding/format/header/null-print/delim/quote/escape/force-quote) + `CopyHeaderChoice`, over context-allocated owned storage | types-core, mcx |
| `types-extensible` | (lib) | extensible-node / custom-scan registration vocabulary (`nodes/extensible.h`): `ExtensibleNodeMethods`/`CustomScanMethods` (C-ABI fn-pointer method tables), the file-local `ExtensibleNodeEntry` registry row (`char[EXTNODENAME_MAX_LEN]` key + `const void *`), `EXTNODENAME_MAX_LEN`, the `CUSTOMPATH_SUPPORT_*` flags, and the forward-declared opaque `ExtensibleNode`/`StringInfoData`/`CustomScan`/`Node` the callbacks operate over (inherited opacity, collapse onto the owners' layouts when they land) | types-core |
| `types-tidbitmap` | (lib) | TID-bitmap vocabulary (`nodes/tidbitmap.h`): opaque `TIDBitmap`/`TBMPrivateIterator`/`TBMSharedIterator` (semantic opacity, owned by `tidbitmap.c`), the public `TBMIterator`, `dsa_pointer`/`InvalidDsaPointer` | types-core |
| `types-tableam` | `relscan`, `scankey`, `tableam` | table-AM dispatch vocabulary (`access/tableam.h`, `access/relscan.h`, `access/skey.h`): the `TableAmRoutine` fn-pointer vtable (trimmed; incl. `scan_end`/`scan_rescan`/`tuple_fetch_row_version` slots), scan/parallel-scan/index-fetch descriptors (incl. `TableScanDescData.rs_tbmiterator`, the bitmap-scan `st` union member), `TM_*` result types; uses `std` (Mutex/atomics for the DSM-shared parallel descriptor) | types-core, types-error, types-tuple, types-nodes, types-snapshot, types-storage, types-tidbitmap |
| `types-applyparallel` | (lib) | parallel-apply coordinator seam vocabulary: `ShmMqResult` (`storage/shm_mq.h`), `ShmMqReceived`, `ParsedErrorNotice`, `DsmSetupResult`, and the `worker_internal.h` `ParallelTransState` / `PartialFileSetState` enums (discriminant order verified — `pa_wait_for_xact_state` compares `>=`) | types-core |
| `types-dfmgr` | (lib) | dynamic-loader carriers (`dfmgr.c` / `fmgr.h`): `Pg_magic_struct`, the loaded-files-list entry `LoadedModule` (`DynamicFileList`), `FileIdentity` (`SAME_INODE`), the integer `LibraryHandle` token, `LibraryOpen`, `LoadedModuleDetails` (uses `libc::dev_t`/`ino_t`; ABI constants + `PgAbiValues` live in types-core::fmgr) | types-core |
| `types-parsenodes` | (lib) | raw-parser parse-tree vocabulary (`nodes/parsenodes.h`, `nodes/value.h`, `nodes/primnodes.h`), trimmed: the owned `Node` enum over Value/`TypeName`/`DefElem`/`ObjectWithArgs`/`FunctionParameter`/`RoleSpec`/`AccessPriv`, the function/cast/transform/DO/CALL statement structs, the role-command statement structs (`CreateRoleStmt`/`AlterRoleStmt`/`AlterRoleSetStmt`/`DropRoleStmt`/`GrantRoleStmt`/`DropOwnedStmt`/`ReassignOwnedStmt`) + `RoleSpecType`/`RoleStmtType` + trimmed `ParseState`, and the pg_proc/pg_cast/pg_type code constants (`PROKIND_*`/`PROVOLATILE_*`/`PROPARALLEL_*`/`COERCION_*`/`TYPTYPE_*`) the command drivers consume. Distinct from the executor/plan `Node` in `types-nodes` | types-core, types-nodes |
| `types-execparallel` | (lib) | parallel-executor vocabulary (`executor/execParallel.c` + `jit/jit.h`): the DSM ABI structs (`FixedParallelExecutorState`, `SharedExecutorInstrumentation`), `JitInstrumentation`/`SharedJitInstrumentation` + `PGJIT_*`, `dsa_pointer`, the `ParamExecValue`/`RestoredParam` serialize records, `ParallelExecutorInfo<'mcx>`, and the `Copy` handle newtypes naming sibling-subsystem objects not yet ported (`PlanStateHandle`, `EStateHandle`, `ParallelContextHandle`, `Dsm`/`ShmToc`/`ShmMq`/`DsaArea`/`TupleQueueReader`/`QueryDesc`/… ). Inherited opacity that collapses onto the owners' real types when they land | types-core, types-datum, mcx |
| `types-vacuum` | `vacuum`, `vacuumparallel`, `vacuumlazy` | VACUUM vocabulary trimmed to the lazy-vacuum driver's needs: `vacuum` = `VacuumParams`/`VacOptValue`/`VacuumCutoffs`/`PruneFreezeResult` + the `VACOPT_*` option flag bits (`commands/vacuum.h`, `access/heapam.h`); `vacuumparallel` = `VacDeadItemsInfo`/`IndexBulkDeleteResult`/`IndexVacuumInfo`/`BufferAccessStrategyHandle` (`access/genam.h`, `commands/vacuumparallel.c`); `vacuumlazy` = the driver's substrate id-handles (`StrategyHandle`/`TidStore`/`TidStoreIterHandle`/`ParallelVacuumStateHandle`/`GlobalVisStateHandle`/`ReadStreamHandle`) + seam DTOs (`PruneAndFreezeArgs`/`Out`, `VmSetArgs`, `UpdateRelStatsArgs`, `ParallelVacuumInit{,Args}`, `ScanCallback`, `ReapBlockInfo`, `LinePointerState`) | types-core, types-storage |
| `types-autovacuum` | (lib) | `backend/postmaster/autovacuum.c` vocabulary: the process-local list/mapping/recheck structs (`AvlDbase`, `AvRelation`, `AutovacTable`) and the catalog/pgstat-reader by-value carriers (`AvwDbase`, `DbStatEntry`, `PgClassScanRow`, `TabStatEntry`, `RecheckClassRow`) | types-core, types-reloptions, types-vacuum |
| `types-authid` | (lib) | `pg_authid`/`pg_auth_members` catalog-row vocabulary consumed by `commands/user.c`: `AuthIdForm`/`AuthMemForm` (`GETSTRUCT` views), `New*Record`, `*Update` deltas, opaque `TupleHandle`/`CatCListHandle`, and `PasswordType` (`libpq/crypt.h`) | types-core |
| `types-deadlock` | (lib) | deadlock-detector vocabulary (`storage/lmgr/deadlock.c`, `storage/lock.h`): `DeadLockState`, `LockMethodData` (conflict/name table), the `LockSpace` shared-lock-table arena modeling the `LOCK`/`PROCLOCK`/`PGPROC` graph as fixed-identity slots (`ProcId`/`LockId`/`ProcLockId` = absolute-shmem-address analogues), `Edge`/`WaitOrder`/`DeadlockInfo` workspace records, `DeadlockReport`. `LOCKMASK`/`LOCKMETHODID`/`MAX_LOCKMODES`/`PROC_IS_AUTOVACUUM` were added to types-storage | types-core, types-storage |
| `types-replication-slot` | (lib) | replication-slot data vocabulary (`replication/slot.h` + the `slot.c` on-disk format): `ReplicationSlotPersistency`, `ReplicationSlotInvalidationCause`, `ReplicationSlotPersistentData`, `ReplicationSlotOnDisk`, `SlotInvalidationCauseMap`, `PG_REPLSLOT_DIR`, `RS_INVAL_MAX_CAUSES`, the `SlotIs{Physical,Logical}` predicates. The live shmem `ReplicationSlot` struct (with embedded real lock primitives) stays in the `backend-replication-slot` crate. | types-core, types-tuple |
| `types-tsearch` | (lib) | tsearch dictionary vocabulary (`tsearch/ts_public.h`): `TSLexeme` (owned `lexeme`, `TSL_*` flags), `StopList`, the `ispell` template's `DictISpell`, and the `SpellHandle` opaque token naming the not-yet-ported `spell.c` `IspellDict` (collapses onto the real `IspellDict` when that unit lands) | mcx |
| `types-ri-triggers` | (lib) | RI-trigger seam vocabulary (`utils/adt/ri_triggers.c`): the foreign-owned handles its seams pass (`TupleTableSlotRef`/`TriggerRef`/`TriggerDataRef`/`SpiPlanPtr` — objects RI only forwards, never dereferences; SPIPlanPtr is opaque in C too) plus the `Mcx`-allocated projected rows (`FkConstraintRow`, `PeriodOpers`, `SpiExecResult`, `ResultColumn`, `UserContext`) | types-core, types-datum, mcx |

## Placement rules for new type modules

1. Keep the src-idiomatic module name; place the module in the lowest existing
   `types-*` crate that can hold it without a cyclic crate edge.
2. No fit → create a new small `types-<subsystem>` crate at the right layer.
   Prefer more small crates over widening an existing one.
3. Copy only the items the port consumes. If a copied type's src-idiomatic
   definition references something above its layer, that's a flag: trim the
   field, box/indirect it, or the type actually belongs to the consuming
   unit's crate — decide per port, like a seam.
   - **Exception — `ParseLoc location` on primnodes.** Earlier ports trimmed
     `location` from the `types-nodes::primnodes` Expr structs as a
     "purely-positional, no reader" field. That was wrong: the parser
     (`parse_func`/`parse_oper`/`parse_expr`/`parse_target`/`parse_clause`)
     SETS `location` on every node it builds, and
     `parser_errposition`/`exprLocation` (nodeFuncs.c) read it back to point
     error messages at the offending token. Dropping it diverges from C error
     output. `location` is therefore carried field-for-field on every primnode
     that has it in C (`nodes/primnodes.h`): Var, Const, Param, Aggref,
     GroupingFunc, WindowFunc, MergeSupportFunc, FuncExpr, NamedArgExpr,
     OpExpr, ScalarArrayOpExpr, BoolExpr, SubLink, RelabelType, CoerceViaIO,
     ArrayCoerceExpr, ConvertRowtypeExpr, CollateExpr, CaseExpr, CaseWhen,
     ArrayExpr, RowExpr, CoalesceExpr, MinMaxExpr, SQLValueFunction, XmlExpr,
     JsonFormat, JsonConstructorExpr, JsonIsPredicate, JsonBehavior, JsonExpr,
     NullTest, BooleanTest, CoerceToDomain, CoerceToDomainValue, SetToDefault
     (and Aggref also keeps `aggpresorted`). Nodes C does not give a
     `location` (e.g. SubPlan, FieldSelect, RowCompareExpr, CurrentOfExpr,
     InferenceElem, TargetEntry, SubscriptingRef, FieldStore, CaseTestExpr,
     NextValueExpr, ReturningExpr) stay without one.
4. The 49-module node/executor knot is irreducible: when ports reach it, those
   modules go into a single designated `types-nodes` crate. Don't try to split
   it; don't let anything else fall into it by convenience.
5. Seam-crate signatures may reference any `types-*` crate (plus `std` and
   primitives); a seam crate depends only on the `types-*` crates its
   signatures need and `seam-core`.
6. **Opacity is inherited, never introduced.** Where C has a typed pointer
   (`RelationData *`), the Rust type is the real struct (populated
   incrementally with the fields consumers use) — no handle-newtypes or
   stand-in unit structs for types C spells out. Where C uses `void *` to
   dodge a header dependency or missing generics, Rust resolves it to the
   real type too — restructure the crate layering (move the type up to where
   its payload lives) rather than encoding the layering problem into a fake
   type. The only opacity that stays is semantic opacity C means (genuinely
   heterogeneous extension slots like `fdw_private`).
   **Precedent**: src-idiomatic let consumers invent stand-ins and deferred
   reconciliation to the wiring phase; `Relation` ended up as three
   incompatible representations at once (`usize` in copyfrom/copyto, `Oid` in
   heapam_handler, the real `RelationData` in cache.rs) and the wiring phase
   never came. Every stand-in is a signature break billed to whoever finally
   defines the real type.
7. **C enums become Rust enums (or newtypes), never bare integer aliases.**
   `pub type TimeoutId = i32` plus positional constants ("the 9th
   enumerator") is the transcribed-constant-table bug class with no type
   safety between namespaces. Small `#[repr(i32)]` enums or newtype wrappers,
   values verified against the C header, used in seam signatures from the
   first declaration.
