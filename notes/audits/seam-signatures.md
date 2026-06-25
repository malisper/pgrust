# Seam signature audit — failure surface vs PostgreSQL 18.3 C

Audit of every seam declaration in every `crates/*-seams` crate against the
original C under `../pgrust/postgres-18.3`. Rule applied (now in AGENTS.md):
a seam returns `types_error::PgResult<T>` iff the C function (or any path it
takes) can `ereport`/`elog` at ERROR or higher; infallible C functions return
bare values; seams never return `&'static mut` (shared-state access goes
through a `&mut dyn FnMut(&mut T)` callback instead).

Notes on borderline calls:
- `palloc`/`repalloc` count as fallible (`ereport(ERROR, "out of memory")`).
- `ereport(COMMERROR)` does **not** count (below ERROR; no longjmp).
- `Assert` does not count (no ereport in production builds).

Branch: `seam-signature-audit`. 79 seams audited; 23 fixed; 56 already
correct (17 of those touched only to normalize the `PgResult` import from the
`types_core` re-export to its canonical home `types_error`).

| Seam | C function (file) | Verdict | Action |
|---|---|---|---|
| heaptoast-seams::toast_flatten_tuple_to_datum | toast_flatten_tuple_to_datum (access/heap/heaptoast.c) | FALLIBLE — detoast_attr → toast_fetch_datum ereports (`missing chunk number ...`), palloc | **fixed**: → `PgResult<FormedTuple>` |
| amvalidate-seams::check_amproc_signature | check_amproc_signature (access/index/amvalidate.c) | FALLIBLE — `elog(ERROR, "cache lookup failed for function %u")` | ok (already `PgResult`) |
| amvalidate-seams::check_amoptsproc_signature | check_amoptsproc_signature (amvalidate.c) | FALLIBLE — same syscache path | ok |
| amvalidate-seams::check_amop_signature | check_amop_signature (amvalidate.c) | FALLIBLE — `elog(ERROR, "cache lookup failed for operator %u")` | ok |
| amvalidate-seams::opclass_for_family_datatype | opclass_for_family_datatype (amvalidate.c) | FALLIBLE — `SearchSysCacheList1(CLAAMNAMENSP, ...)` catalog read can ereport | **fixed**: → `PgResult<Oid>` |
| amvalidate-seams::identify_opfamily_groups | identify_opfamily_groups (amvalidate.c) | FALLIBLE — `elog(ERROR, "cannot validate operator family without ordered data")`, palloc | **fixed**: → `PgResult<Vec<OpFamilyOpFuncGroup>>` |
| table-seams::table_open | table_open (access/table/table.c) | FALLIBLE — relation_open ereports (`could not open relation with OID %u`), validate_relation_kind ereports, lock acquisition | **fixed**: → `PgResult<Oid>` (handle model kept) |
| table-seams::table_close | table_close → relation_close → UnlockRelationId → LockRelease (storage/lmgr/lock.c) | FALLIBLE — `elog(ERROR, "unrecognized lock method/mode")`, `elog(ERROR, "failed to re-find shared lock/proclock object")` | **fixed**: → `PgResult<()>` |
| multixact-seams::multixact_twophase_recover | multixact_twophase_recover (access/transam/multixact.c) | FALLIBLE — TwoPhaseGetDummyProcNumber → TwoPhaseGetGXact `elog(ERROR, "failed to find GlobalTransaction for xid %u")` | ok (already `PgResult`) |
| multixact-seams::multixact_twophase_postcommit | multixact_twophase_postcommit (multixact.c) | FALLIBLE — same TwoPhaseGetGXact path | ok |
| multixact-seams::multixact_twophase_postabort | multixact_twophase_postabort (multixact.c) | FALLIBLE — calls postcommit | ok |
| parallel-seams::is_parallel_worker | IsParallelWorker() (access/parallel.h) | INFALLIBLE — global read | ok (bare) |
| xact-seams::command_counter_increment | CommandCounterIncrement (access/transam/xact.c) | FALLIBLE — `ereport(ERROR, "cannot have more than 2^32-2 commands in a transaction")` | ok |
| execAmi-seams::exec_re_scan | ExecReScan (executor/execAmi.c) | FALLIBLE — arbitrary node rescan code | ok; PgResult home normalized |
| execProcnode-seams::exec_init_node | ExecInitNode (executor/execProcnode.c) | FALLIBLE — arbitrary node init, palloc | ok; normalized |
| execProcnode-seams::exec_proc_node | ExecProcNode (executor.h) | FALLIBLE — arbitrary node code | ok; normalized |
| execProcnode-seams::exec_end_node | ExecEndNode (execProcnode.c) | FALLIBLE | ok; normalized |
| execTuples-seams::exec_init_result_tuple_slot_tl | ExecInitResultTupleSlotTL (executor/execTuples.c) | FALLIBLE — ExecTypeFromTL/MakeTupleTableSlot palloc | ok; normalized |
| execTuples-seams::exec_clear_tuple | ExecClearTuple (tuptable.h) | FALLIBLE — buffer-heap clear → ReleaseBuffer `elog(ERROR, "bad buffer ID")` | ok; normalized |
| execTuples-seams::exec_copy_slot | ExecCopySlot (tuptable.h) | FALLIBLE — copyslot allocates / may detoast | ok; normalized |
| execUtils-seams::exec_create_scan_slot_from_outer_plan | ExecCreateScanSlotFromOuterPlan (executor/execUtils.c) | FALLIBLE — slot creation pallocs | ok; normalized |
| pqformat-seams::pq_beginmessage | pq_beginmessage (libpq/pqformat.c) | FALLIBLE — initStringInfo → palloc(1024) OOM ereport | **fixed**: → `PgResult<()>` |
| pqformat-seams::pq_sendint32 | pq_sendint32 (pqformat.h) | FALLIBLE — enlargeStringInfo `ereport(ERROR)` (OOM / 1GB cap) | **fixed**: → `PgResult<()>` |
| pqformat-seams::pq_sendint64 | pq_sendint64 (pqformat.h) | FALLIBLE — same | **fixed**: → `PgResult<()>` |
| pqformat-seams::pq_endmessage | pq_endmessage (pqformat.c) | INFALLIBLE — pq_putmessage failures are `ereport(COMMERROR)` (no longjmp), result ignored; pfree | ok (bare) |
| lock-seams::lock_twophase_recover | lock_twophase_recover (storage/lmgr/lock.c) | FALLIBLE — `ereport(ERROR, out of shared memory)`, `elog(ERROR, "lock ... already held")` | ok |
| lock-seams::lock_twophase_postcommit | lock_twophase_postcommit → LockRefindAndRelease (lock.c) | FALLIBLE — `elog(ERROR, "failed to re-find shared lock object")` | ok |
| lock-seams::lock_twophase_postabort | lock_twophase_postabort (lock.c) | FALLIBLE — calls postcommit | ok |
| lock-seams::lock_twophase_standby_recover | lock_twophase_standby_recover (lock.c) | FALLIBLE — StandbyAcquireAccessExclusiveLock can ereport | ok |
| lwlock-seams::lwlock_initialize | LWLockInitialize (storage/lmgr/lwlock.c) | INFALLIBLE — atomic init + field writes | ok (bare) |
| lwlock-seams::lwlock_acquire | LWLockAcquire (lwlock.c) | FALLIBLE — `elog(ERROR, "too many LWLocks taken")` | **fixed**: → `PgResult<bool>` |
| lwlock-seams::lwlock_release | LWLockRelease (lwlock.c) | FALLIBLE — `elog(ERROR, "lock %s is not held")` | **fixed**: → `PgResult<()>` |
| predicate-seams::predicatelock_twophase_recover | predicatelock_twophase_recover (storage/lmgr/predicate.c) | FALLIBLE — `ereport(ERROR, out of shared memory)` | ok |
| tcop-postgres-seams::check_for_interrupts | CHECK_FOR_INTERRUPTS → ProcessInterrupts (tcop/postgres.c) | FALLIBLE — `ereport(ERROR/FATAL)` for cancel/termination | ok; normalized |
| pgstat-seams::shmem_archiver | `&pgStatLocal.shmem->archiver` | returned `&'static mut` — unsound | **fixed**: → `with_shmem_archiver(f: &mut dyn FnMut(&mut PgStatShared_Archiver))` |
| pgstat-seams::snapshot_archiver | `&pgStatLocal.snapshot.archiver` | same | **fixed**: → `with_snapshot_archiver(...)` |
| pgstat-seams::shmem_bgwriter | `&pgStatLocal.shmem->bgwriter` | same | **fixed**: → `with_shmem_bgwriter(...)` |
| pgstat-seams::snapshot_bgwriter | `&pgStatLocal.snapshot.bgwriter` | same | **fixed**: → `with_snapshot_bgwriter(...)` |
| pgstat-seams::shmem_checkpointer | `&pgStatLocal.shmem->checkpointer` | same | **fixed**: → `with_shmem_checkpointer(...)` |
| pgstat-seams::snapshot_checkpointer | `&pgStatLocal.snapshot.checkpointer` | same | **fixed**: → `with_snapshot_checkpointer(...)` |
| pgstat-seams::shmem_is_shutdown | `pgStatLocal.shmem->is_shutdown` read | INFALLIBLE — field read | ok (bare) |
| pgstat-seams::assert_is_up | pgstat_assert_is_up() (utils/pgstat_internal.h) | INFALLIBLE — `((void)true)` outside assert builds | ok (bare) |
| pgstat-seams::snapshot_fixed | pgstat_snapshot_fixed (utils/activity/pgstat.c) | FALLIBLE — pgstat_build_snapshot pallocs/dsa; per-kind snapshot_cb takes LWLockAcquire | **fixed**: → `PgResult<()>` |
| stat-seams::pgstat_flush_io | pgstat_flush_io → pgstat_io_flush_cb (utils/activity/pgstat_io.c) | FALLIBLE — `LWLockAcquire` on the blocking path | **fixed**: → `PgResult<bool>` |
| stat-seams::pgstat_twophase_postcommit | pgstat_twophase_postcommit (utils/activity/pgstat_relation.c) | FALLIBLE — pgstat_prep_relation_pending → entry-ref creation can ereport (dsa/palloc OOM) | ok |
| stat-seams::pgstat_twophase_postabort | pgstat_twophase_postabort (pgstat_relation.c) | FALLIBLE — same | ok |
| status-seams::my_be_entry_present | `MyBEEntry != NULL` | INFALLIBLE | ok (bare) |
| status-seams::track_activities | `pgstat_track_activities` GUC read | INFALLIBLE | ok |
| status-seams::begin_write_activity | PGSTAT_BEGIN_WRITE_ACTIVITY (utils/backend_status.h) | INFALLIBLE — changecount bump + barrier | ok |
| status-seams::end_write_activity | PGSTAT_END_WRITE_ACTIVITY | INFALLIBLE | ok |
| status-seams::set_progress_command | `st_progress_command = ...` | INFALLIBLE — field write | ok |
| status-seams::set_progress_command_target | `st_progress_command_target = ...` | INFALLIBLE | ok |
| status-seams::progress_command | `st_progress_command` read | INFALLIBLE | ok |
| status-seams::zero_progress_param | `MemSet(st_progress_param, 0, ...)` | INFALLIBLE | ok |
| status-seams::set_progress_param | `st_progress_param[i] = v` | INFALLIBLE | ok |
| status-seams::incr_progress_param | `st_progress_param[i] += v` | INFALLIBLE | ok |
| format-type-seams::format_type_be | format_type_be (utils/adt/format_type.c) | FALLIBLE — cache-lookup `elog(ERROR)` | ok |
| misc2-seams::eoh_get_flat_size | EOH_get_flat_size (utils/adt/expandeddatum.c) | FALLIBLE — method dispatch; e.g. expanded-array `ereport(ERROR, "array size exceeds the maximum allowed")` | **fixed**: → `PgResult<usize>` |
| misc2-seams::eoh_flatten_into | EOH_flatten_into (expandeddatum.c) | FALLIBLE — method dispatch can ereport | **fixed**: → `PgResult<()>` |
| regproc-seams::format_procedure | format_procedure (utils/adt/regproc.c) | FALLIBLE — catalog lookup `elog(ERROR)` | ok |
| regproc-seams::format_operator | format_operator (regproc.c) | FALLIBLE — `elog(ERROR, "cache lookup failed for operator %u")` | ok |
| timestamp-seams::get_current_timestamp | GetCurrentTimestamp (utils/adt/timestamp.c) | INFALLIBLE — gettimeofday + arithmetic | ok (bare) |
| lsyscache-seams::get_opfamily_name | get_opfamily_name (utils/cache/lsyscache.c) | FALLIBLE — syscache + `missing_ok=false` ereport | ok |
| lsyscache-seams::get_opclass_input_type | get_opclass_input_type (lsyscache.c) | FALLIBLE — `elog(ERROR, "cache lookup failed for opclass %u")` | ok |
| relcache-seams::relation_rd_att | RelationGetDescr / `relation->rd_att` (utils/rel.h) | INFALLIBLE — field read (clone is marshaling) | ok (bare) |
| syscache-seams::search_opclass | SearchSysCache1(CLAOID) (utils/cache/syscache.c) | FALLIBLE — catcache fill can ereport | ok |
| syscache-seams::search_amop_list | SearchSysCacheList1(AMOPSTRATEGY) | FALLIBLE | ok |
| syscache-seams::search_amproc_list | SearchSysCacheList1(AMPROCNUM) | FALLIBLE | ok |
| error-seams::ereport | errstart/errfinish (utils/error/elog.c) | FALLIBLE by design — ERROR+ comes back as `Err` | ok |
| init-small-seams::work_mem | `work_mem` GUC read (utils/init/globals.c) | INFALLIBLE | ok (bare) |
| sort-storage-seams::tuplestore_begin_heap | tuplestore_begin_heap (utils/sort/tuplestore.c) | FALLIBLE — pallocs | ok; normalized |
| sort-storage-seams::tuplestore_set_eflags | tuplestore_set_eflags | FALLIBLE — `elog(ERROR, "too late to call tuplestore_set_eflags")` | ok; normalized |
| sort-storage-seams::tuplestore_alloc_read_pointer | tuplestore_alloc_read_pointer | FALLIBLE — `elog(ERROR, "too late to require new tuplestore eflags")`, repalloc | ok; normalized |
| sort-storage-seams::tuplestore_ateof | tuplestore_ateof | INFALLIBLE — pure field read | **fixed**: → bare `bool` |
| sort-storage-seams::tuplestore_advance | tuplestore_advance → tuplestore_gettuple | FALLIBLE — temp-file read `ereport(ERROR)`s | ok; normalized |
| sort-storage-seams::tuplestore_gettupleslot | tuplestore_gettupleslot | FALLIBLE — same | ok; normalized |
| sort-storage-seams::tuplestore_puttupleslot | tuplestore_puttupleslot | FALLIBLE — `elog(ERROR, "unexpected out-of-memory situation in tuplestore")`, BufFileCreateTemp/write ereports | ok; normalized |
| sort-storage-seams::tuplestore_copy_read_pointer | tuplestore_copy_read_pointer | FALLIBLE — BufFileSeek/Tell `ereport(ERROR)`s | ok; normalized |
| sort-storage-seams::tuplestore_trim | tuplestore_trim | INFALLIBLE — pfree/memmove only | **fixed**: → bare `()` |
| sort-storage-seams::tuplestore_rescan | tuplestore_rescan | FALLIBLE — `ereport(ERROR, "could not seek in temporary file")` | ok; normalized |
| sort-storage-seams::tuplestore_end | tuplestore_end | INFALLIBLE — BufFileClose/MemoryContextDelete/pfree have no ERROR path | **fixed**: → bare `()` |
