// ANALYZE end-to-end over fixture storage: skewed heap -> analyze_rel ->
// real pg_statistic rows -> syscache bundle read-back -> planner estimates,
// asserted against live PostgreSQL 18.3 on the identical dataset (the sample
// covers the whole table, so the statistics are deterministic).
use std::cell::Cell;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Mutex;

use commands_analyze::VacuumParams;
use datum::Datum;
use mcx::{Mcx, MemoryContext, PgVec};
use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{XLogRecPtrToBytePos, DB_IN_PRODUCTION, RECOVERY_STATE_DONE};
use types_core::{
    BackendType, BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, Oid, XLogRecPtr,
    BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
};
use types_nodes::list::NodeList;
use types_rel::{
    FormData_pg_class, LockInfoData, LockRelId, Relation, RelationData, RELKIND_RELATION,
};
use types_storage::bufpage::PageRef;
use types_tuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5544_3322_1100_AB01;
const T_OID: Oid = 61010;
const STAT_OID: Oid = 2619;
const STATEXT_OID: Oid = 3381;
const INT4OID: Oid = 23;
const INT4_EQ_OP: Oid = 96;
const INT4_LT_OP: Oid = 97;
const INT4_GT_OP: Oid = 521;
const INT4_BTREE_FAM: Oid = 1976;
const INT4_BTREE_OPCLASS: Oid = 1978;
const BTINT4CMP: Oid = 351;

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

struct Fake {
    pages: Vec<usize>,
    pins: Vec<i32>,
    locks: Vec<i32>,
    buf_rel: Vec<(Oid, BlockNumber)>,
    rels: Vec<(Oid, Vec<usize>)>,
}

static FAKE: Mutex<Fake> = Mutex::new(Fake {
    pages: Vec::new(),
    pins: Vec::new(),
    locks: Vec::new(),
    buf_rel: Vec::new(),
    rels: Vec::new(),
});

static RELSTATS: Mutex<Vec<(Oid, BlockNumber, f64, BlockNumber)>> = Mutex::new(Vec::new());

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    f(&mut FAKE.lock().unwrap_or_else(|e| e.into_inner()))
}

fn rel_buffers(f: &mut Fake, relid: Oid) -> &mut Vec<usize> {
    if !f.rels.iter().any(|(r, _)| *r == relid) {
        f.rels.push((relid, Vec::new()));
    }
    &mut f.rels.iter_mut().find(|(r, _)| *r == relid).unwrap().1
}

fn install_bufmgr_seams() {
    bufmgr_seams::read_buffer::set(|rel, block| {
        with_fake(|f| {
            let relid = rel.rd_id;
            let bufs = rel_buffers(f, relid).clone();
            assert!(
                (block as usize) < bufs.len(),
                "read past end: rel {relid} block {block}"
            );
            let idx = bufs[block as usize];
            f.pins[idx] += 1;
            Ok(idx as Buffer + 1)
        })
    });
    bufmgr_seams::read_buffer_strategy::set(|rel, block, _strategy| {
        bufmgr_seams::read_buffer::call(rel, block)
    });
    bufmgr_seams::buffer_get_block_number::set(|buf| {
        with_fake(|f| f.buf_rel[(buf - 1) as usize].1)
    });
    bufmgr_seams::buffer_get_page::set(|buf| {
        let addr = with_fake(|f| {
            assert!(f.pins[(buf - 1) as usize] > 0, "page access without pin");
            f.pages[(buf - 1) as usize]
        });
        NonNull::new(addr as *mut u8).unwrap()
    });
    bufmgr_seams::release_buffer::set(|buf| {
        with_fake(|f| {
            let p = &mut f.pins[(buf - 1) as usize];
            assert!(*p > 0, "double release of buffer {buf}");
            *p -= 1;
        });
        Ok(())
    });
    bufmgr_seams::incr_buffer_ref_count::set(|buf| {
        with_fake(|f| f.pins[(buf - 1) as usize] += 1);
    });
    bufmgr_seams::lock_buffer::set(|buf, mode| {
        with_fake(|f| {
            let l = &mut f.locks[(buf - 1) as usize];
            match mode {
                bufmgr_seams::BUFFER_LOCK_UNLOCK => {
                    assert!(*l > 0, "unlock without lock");
                    *l -= 1;
                }
                _ => {
                    assert_eq!(*l, 0, "double content lock");
                    *l += 1;
                }
            }
        });
        Ok(())
    });
    bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
    bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| Ok(()));
    bufmgr_seams::buffer_is_permanent::set(|_buf| true);
    bufmgr_seams::buffer_get_lsn_atomic::set(|buf| {
        let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
        // SAFETY: leaked test page, always live.
        unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.lsn()
    });
    bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|rel, fork| {
        if fork != ForkNumber::MAIN_FORKNUM {
            return Ok(0);
        }
        let relid = rel.rd_id;
        Ok(with_fake(|f| rel_buffers(f, relid).len() as BlockNumber))
    });
    bufmgr_seams::extend_buffered_rel_by::set(|rel, _fork, _strategy, flags, extend_by| {
        assert_eq!(extend_by, 1);
        assert!(flags & bufmgr_seams::EB_LOCK_FIRST != 0);
        let relid = rel.rd_id;
        Ok(with_fake(|f| {
            let addr = Box::leak(Box::new(TestPage([0u8; BLCKSZ]))).0.as_mut_ptr() as usize;
            f.pages.push(addr);
            f.pins.push(1);
            f.locks.push(1);
            let idx = f.pages.len() - 1;
            let block = rel_buffers(f, relid).len() as BlockNumber;
            rel_buffers(f, relid).push(idx);
            f.buf_rel.push((relid, block));
            (idx as Buffer + 1, 1)
        }))
    });
    bufmgr_seams::relation_smgr_locator::set(|rel| types_storage::RelFileLocatorBackend {
        locator: types_storage::RelFileLocator::new(1663, 5, rel.rd_id),
        backend: INVALID_PROC_NUMBER,
    });

    xloginsert_seams::xlog_insert::set(|rmid, info, fragments| {
        xloginsert::insert_record(rmid, info, 0, fragments, &[])
    });
    xloginsert_seams::xlog_insert_with_flags::set(|rmid, info, _flags, fragments| {
        xloginsert::insert_record(rmid, info, 0, fragments, &[])
    });
    xloginsert_seams::xlog_insert_record::set(|rmid, info, flags, main_data, bufs| {
        let mut blocks: Vec<xloginsert::RegBlock<'_>> = Vec::with_capacity(bufs.len());
        for b in bufs {
            let (addr, (relid, block)) =
                with_fake(|f| (f.pages[(b.buffer - 1) as usize], f.buf_rel[(b.buffer - 1) as usize]));
            blocks.push(xloginsert::RegBlock {
                block_id: b.block_id,
                rlocator: types_storage::RelFileLocator::new(1663, 5, relid),
                forknum: ForkNumber::MAIN_FORKNUM,
                block,
                // SAFETY: leaked test page, BLCKSZ, pinned by the caller.
                page: unsafe { core::slice::from_raw_parts(addr as *const u8, BLCKSZ) },
                flags: b.flags,
                bufdata: b.bufdata,
            });
        }
        xloginsert::insert_record(rmid, info, flags, main_data, &blocks)
    });

    smgr_seams::smgr_cached_nblocks::set(|_loc, _fork| InvalidBlockNumber);
    smgr_seams::smgr_exists::set(|_loc, _fork| Ok(false));
    smgr_seams::smgr_set_cached_nblocks::set(|_loc, _fork, _n| Ok(()));
}

fn install_proc_boot_seams() {
    use init_small::globals as g;
    g::SetMaxConnections(16);
    g::set_max_worker_processes(2);
    g::SetMaxBackends(16 + 3 + 2 + 2 + 2);
    g::SetMyProcPid(781);
    g::SetMyDatabaseId(5);
    g::set_transaction_buffers(64);
    g::set_subtransaction_buffers(64);

    pg_sema_seams::pg_semaphore_create::set(|_| {});
    pg_sema_seams::pg_semaphore_reset::set(|_| {});
    pg_sema_seams::pg_semaphore_lock::set(|_| {});
    pg_sema_seams::pg_semaphore_unlock::set(|_| {});
    s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
    s_lock_seams::finish_spin_delay::set(|_| {});
    s_lock_seams::set_spins_per_delay::set(|_| {});
    s_lock_seams::update_spins_per_delay::set(|v| v);
    latch_seams::own_latch::set(|_| {});
    latch_seams::disown_latch::set(|_| {});
    latch_seams::set_latch::set(|_| {});
    latch_seams::set_latch_my_latch::set(|| {});
    latch_seams::wait_latch_my_latch::set(|_, _, _| 0);
    latch_seams::reset_latch_my_latch::set(|| {});
    miscinit_seams::switch_to_shared_latch::set(|| {});
    miscinit_seams::switch_back_to_local_latch::set(|| {});
    miscinit_seams::get_user_id::set(|| 10);
    // vacuum_is_permitted_for_relation calls miscinit::GetUserId() directly
    // (not the seam); arm the thread-local too.
    miscinit::SetUserIdAndSecContext(10, 0);
    miscinit_seams::is_bootstrap_processing_mode::set(|| false);
    waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
    waitevent_seams::pgstat_report_wait_start::set(|_| {});
    waitevent_seams::pgstat_report_wait_end::set(|| {});
    waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
    ipc_seams::on_shmem_exit::set(|_, _| {});
    deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
    pmsignal_seams::register_postmaster_child_active::set(|| {});
    syncrep_seams::sync_rep_cleanup_at_proc_exit::set(|| {});
    condition_variable_seams::condition_variable_cancel_sleep::set(|| false);
    autovacuum_seams::wake_autovacuum_launcher::set(|| {});
    lock_seams::abort_strong_lock_acquire::set(|| {});
    lock_seams::get_awaited_lock_hashcode::set(|| None);
    lock_seams::lock_release_all::set(|_, _| lock::VirtualXactLockTableCleanup());
    lock_seams::lock_acquire_extended::set(|_, _, _, _, _, _| {
        Ok(types_storage::lock::LOCKACQUIRE_OK)
    });
    lmgr_seams::lock_relation_oid::set(|_, _| Ok(()));
    lmgr_seams::unlock_relation_oid::set(|_, _| Ok(()));
    timeout_seams::disable_timeouts::set(|_| {});
    aio_seams::pgaio_closing_fd::set(|_| {});
    sync_seams::register_sync_request::set(|_, _, _| Ok(true));
    sinval_seams::receive_shared_invalid_messages::set(|_, _| Ok(()));
    sinval_seams::send_shared_invalid_messages::set(|_| Ok(()));
    aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
    aio_seams::at_eoxact_aio::set(|_| {});
    aio_seams::pgaio_error_cleanup::set(|| {});
    logical_worker_seams::at_eoxact_logical_rep_workers::set(|_| {});
}

fn install_xact_periphery_seams() {
    timestamp_seams::get_current_timestamp::set(|| 777_000_000);
    trigger_seams::after_trigger_begin_xact::set(|| Ok(()));
    trigger_seams::after_trigger_end_xact::set(|_| Ok(()));
    trigger_seams::after_trigger_fire_deferred::set(|| Ok(()));
    async_seams::pre_commit_notify::set(|| Ok(()));
    async_seams::at_commit_notify::set(|| Ok(()));
    async_seams::at_abort_notify::set(|| {});
    tablecmds_seams::pre_commit_on_commit_actions::set(|| Ok(()));
    tablecmds_seams::at_eoxact_on_commit_actions::set(|_| {});
    spi_seams::at_eoxact_spi::set(|_| Ok(()));
    spi_seams::spi_inside_nonatomic_context::set(|| false);
    be_fsstubs_seams::at_eoxact_large_object::set(|_| Ok(()));
    namespace_seams::at_eoxact_namespace::set(|_, _| {});
    catalog_index_seams::reset_reindex_state::set(|_| {});
    catalog_storage_seams::smgr_get_pending_deletes::set(|mcx, _for_commit| {
        Ok(PgVec::new_in(mcx))
    });
    catalog_storage_seams::smgr_do_pending_deletes::set(|_| Ok(()));
    catalog_storage_seams::smgr_do_pending_syncs::set(|_, _| Ok(()));
    combocid_seams::at_eoxact_combocid::set(|| {});
    combocid_seams::heap_tuple_header_adjust_cmax::set(|_hdr, cid| Ok((cid, false)));
    combocid_seams::heap_tuple_header_get_cmax::set(|hdr| hdr.raw_command_id());
    combocid_seams::heap_tuple_header_get_cmin::set(|hdr| hdr.raw_command_id());
    multixact_seams::at_eoxact_multixact::set(|| {});
    multixact_seams::multi_xact_id_set_oldest_member::set(|| Ok(()));
    multixact_seams::multi_xact_id_is_running::set(|_, _| Ok(false));
    pg_enum_seams::at_eoxact_enum::set(|| {});
    relcache_seams::at_eoxact_relation_cache::set(|_| Ok(()));
    typcache_seams::at_eoxact_type_cache::set(|| {});
    logical_seams::reset_logical_streaming_state::set(|| {});
    snapbuild_seams::snap_build_reset_exported_snapshot_state::set(|| {});
    parallel_seams::is_parallel_worker::set(|| false);
    parallel_seams::at_eoxact_parallel::set(|_| Ok(()));
    origin_seams::replorigin_session_origin::set(|| types_core::InvalidRepOriginId);
    origin_seams::replorigin_session_origin_lsn::set(|| 0);
    origin_seams::replorigin_session_origin_timestamp::set(|| 0);
    origin_seams::set_replorigin_session_origin_timestamp::set(|_| {});
    commit_ts_seams::transaction_tree_set_commit_ts_data::set(|_, _, _, _| Ok(()));
    commit_ts_seams::extend_commit_ts::set(|_| Ok(()));
    syncrep_seams::sync_rep_wait_for_lsn::set(|_, _| Ok(()));
    backend_status_seams::pgstat_report_xact_timestamp::set(|_| {});
    backend_status_seams::pgstat_report_query_id::set(|_, _| {});
    backend_status_seams::pgstat_report_plan_id::set(|_, _| {});
    backend_status_seams::pgstat_clear_backend_status_snapshot::set(|| {});
    backend_progress_seams::pgstat_progress_end_command::set(|| {});
    predicate_seams::pre_commit_check_for_serialization_failure::set(|| Ok(()));
    predicate_seams::release_predicate_locks::set(|_, _| Ok(()));
    predicate_seams::register_predicate_locking_xid::set(|_| Ok(()));
    predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
    predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
    predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
    predicate_seams::check_for_serializable_conflict_out_needed::set(|_r, _s| Ok(false));
    predicate_seams::predicate_lock_relation::set(|_r, _s| Ok(()));
    predicate_seams::predicate_lock_tid::set(|_r, _t, _s, _x| Ok(()));
    pruneheap_seams::heap_page_prune_opt::set(|_r, _b| Ok(()));
    freespace_seams::get_page_with_free_space::set(|_rel, _need| Ok(InvalidBlockNumber));
    freespace_seams::record_and_get_page_with_free_space::set(|_rel, _old, _avail, _need| {
        Ok(InvalidBlockNumber)
    });
    catalog_seams::is_catalog_relation::set(|rel| rel.rd_id == STAT_OID);
    catalog_seams::is_catalog_relation_oid::set(|relid| relid == STAT_OID);
    catalog_seams::is_toast_relation::set(|_rel| false);
    catalog_seams::is_shared_relation::set(|_relid| false);
    dbcommands_seams::get_database_name::set(|_| Ok(Some("testdb".to_string())));
    syscache_seams::search_syscache_exists_databaseoid::set(|_| Ok(true));
    syscache_seams::relation_invalidates_snapshots_only::set(|_| false);
    syscache_seams::relation_has_sys_cache::set(|relid| relid == STAT_OID);
    syscache_seams::search_syscache_exists_reloid::set(|_| Ok(true));
    syscache_seams::sys_cache_invalidate::set(|_, _| Ok(()));
    aclchk_seams::pg_class_aclmask::set(|_relid, _roleid, mask, _how_all| Ok(mask));
    aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));
    lmgr_seams::check_relation_locked_by_me::set(|_, _, _| true);
}

fn user_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for (i, name) in ["v", "u"].iter().enumerate() {
        let mut att = FormData_pg_attribute {
            attrelid: T_OID,
            attnum: i as i16 + 1,
            atttypid: INT4OID,
            atttypmod: -1,
            attlen: 4,
            attbyval: true,
            attalign: types_tuple::TYPALIGN_INT,
            attstorage: types_tuple::TYPSTORAGE_PLAIN,
            attislocal: true,
            attnotnull: false,
            ..Default::default()
        };
        att.attname.namestrcpy(name);
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts: 2,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn stat_col(attnum: i16, name: &str, typid: Oid, len: i16, byval: bool, align: i8, storage: i8, notnull: bool) -> FormData_pg_attribute {
    let mut att = FormData_pg_attribute {
        attrelid: STAT_OID,
        attnum,
        atttypid: typid,
        atttypmod: -1,
        attlen: len,
        attbyval: byval,
        attalign: align,
        attstorage: storage,
        attislocal: true,
        attnotnull: notnull,
        ..Default::default()
    };
    att.attname.namestrcpy(name);
    att
}

fn pg_statistic_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
    const I: i8 = types_tuple::TYPALIGN_INT;
    const P: i8 = types_tuple::TYPSTORAGE_PLAIN;
    let mut cols: Vec<FormData_pg_attribute> = vec![
        stat_col(1, "starelid", 26, 4, true, I, P, true),
        stat_col(2, "staattnum", 21, 2, true, b's' as i8, P, true),
        stat_col(3, "stainherit", 16, 1, true, b'c' as i8, P, true),
        stat_col(4, "stanullfrac", 700, 4, true, I, P, true),
        stat_col(5, "stawidth", 23, 4, true, I, P, true),
        stat_col(6, "stadistinct", 700, 4, true, I, P, true),
    ];
    for k in 0..5i16 {
        cols.push(stat_col(7 + k, &format!("stakind{}", k + 1), 21, 2, true, b's' as i8, P, true));
    }
    for k in 0..5i16 {
        cols.push(stat_col(12 + k, &format!("staop{}", k + 1), 26, 4, true, I, P, true));
    }
    for k in 0..5i16 {
        cols.push(stat_col(17 + k, &format!("stacoll{}", k + 1), 26, 4, true, I, P, true));
    }
    for k in 0..5i16 {
        cols.push(stat_col(22 + k, &format!("stanumbers{}", k + 1), 1021, -1, false, I, b'x' as i8, false));
    }
    for k in 0..5i16 {
        cols.push(stat_col(27 + k, &format!("stavalues{}", k + 1), 2277, -1, false, b'd' as i8, b'x' as i8, false));
    }
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for att in cols {
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts: 31,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

static T_RELPAGES: Mutex<(i32, f32)> = Mutex::new((0, -1.0));

fn make_relation<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> RelationData<'mcx> {
    let (name, att, isstat): (&str, Rc<TupleDescData<'mcx>>, bool) = if relid == T_OID {
        ("t", user_tupdesc(mcx), false)
    } else if relid == STATEXT_OID {
        // Empty catalog: scans see 0 blocks; the tupdesc is never deformed.
        ("pg_statistic_ext", pg_statistic_tupdesc(mcx), true)
    } else {
        ("pg_statistic", pg_statistic_tupdesc(mcx), true)
    };
    let mut relname = NameData::default();
    relname.namestrcpy(name);
    let (relpages, reltuples) = if relid == T_OID {
        *T_RELPAGES.lock().unwrap()
    } else {
        (0, -1.0)
    };
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: if isstat { 11 } else { 2200 },
        reltype: 0,
        relowner: 10,
        relam: tableam_vocab::HEAP_TABLE_AM_OID,
        relfilenode: relid,
        reltablespace: 0,
        relpages,
        reltuples,
        relallvisible: 0,
        reltoastrelid: 0,
        relhasindex: false,
        relisshared: false,
        relpersistence: RELPERSISTENCE_PERMANENT,
        relkind: RELKIND_RELATION,
        relhassubclass: false,
        relrowsecurity: false,
        relispopulated: true,
        relreplident: b'd',
        relispartition: false,
        relfrozenxid: 3,
        relminmxid: 1,
    };
    RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: relid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: relid, dbId: 5 } },
        rd_rel,
        rd_att: att,
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    }
}

// The planner probes reach all_rows_selectable -> pg_class_aclcheck ->
// SearchSysCache1(RELOID); a real miss would phase-2-open pg_class, so the
// registered cache is force-initialized and seeded with the test table's row.
fn seed_reloid_cache() {
    use cache_syscache::{SysCacheKey, RELOID};
    use catcache::CCFastKind;
    use types_tuple::ATTNULLABLE_UNRESTRICTED;

    let cx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("test-pgclass")));
    let mcx = cx.mcx();

    let attr = |attlen: i16, attbyval: bool, attalignby: u8| CompactAttribute {
        attcacheoff: Cell::new(-1),
        attlen,
        attbyval,
        attispackable: attlen == -1,
        atthasmissing: false,
        attisdropped: false,
        attgenerated: false,
        attnullability: ATTNULLABLE_UNRESTRICTED,
        attalignby,
    };
    let n1 = || attr(1, true, 1);
    let o4 = || attr(4, true, 4);
    let i2 = || attr(2, true, 2);
    let cols = [
        o4(),
        attr(64, false, 1),
        o4(), o4(), o4(), o4(), o4(), o4(), o4(),
        o4(), o4(), o4(), o4(), o4(),
        n1(), n1(), n1(),
        n1(),
        i2(), i2(),
        n1(), n1(), n1(), n1(), n1(), n1(), n1(), n1(),
        o4(), o4(), o4(),
        attr(-1, false, 4),
        attr(-1, false, 4),
        attr(-1, false, 4),
    ];
    let mut compact: PgVec<'_, CompactAttribute> = PgVec::new_in(mcx);
    let mut attrs: PgVec<'_, FormData_pg_attribute> = PgVec::new_in(mcx);
    for c in &cols {
        compact.push(c.clone());
        attrs.push(FormData_pg_attribute::default());
    }
    let td: &'static TupleDescData<'static> = Box::leak(Box::new(TupleDescData {
        natts: cols.len() as i32,
        tdtypeid: 83,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    }));
    catcache::testing::force_initialized(RELOID, [CCFastKind::Int4; 4]);
    catcache::testing::set_tupdesc(RELOID, td);

    let mut nd = NameData::default();
    nd.namestrcpy("t");
    let mut name_buf: PgVec<'_, u8> = PgVec::new_in(mcx);
    mcx::vec_append_bytes(&mut name_buf, &nd.data).unwrap();
    let mut values = [Datum::null(); 34];
    let mut nulls = [false; 34];
    values[0] = Datum::from_oid(T_OID);
    values[1] = Datum::from_usize(name_buf.as_ptr() as usize);
    values[2] = Datum::from_oid(2200);
    values[5] = Datum::from_oid(10);
    values[16] = Datum::from_u8(b'p');
    values[17] = Datum::from_u8(b'r');
    values[18] = Datum::from_i16(2);
    values[25] = Datum::from_bool(true);
    values[26] = Datum::from_u8(b'd');
    nulls[31] = true;
    nulls[32] = true;
    nulls[33] = true;
    let tup = heaptuple::heap_form_tuple(mcx, td, &values, &nulls).unwrap();
    let t = tup.as_tuple();
    // SAFETY: contiguous formed image, t_len bytes from the header.
    let image = unsafe { core::slice::from_raw_parts(t.header_ptr(), t.t_len as usize) };
    catcache::testing::insert_positive(
        RELOID,
        &[
            SysCacheKey::Value(Datum::from_oid(T_OID)),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        ],
        image,
    );
}

fn install_relation_seams() {
    relation_seams::relation_open::set(|mcx, relid, _lockmode| {
        assert!(
            relid == T_OID || relid == STAT_OID || relid == STATEXT_OID,
            "unknown relation oid {relid}"
        );
        Ok(Relation::open(make_relation(mcx, relid), None))
    });
    relcache_seams::relation_id_get_relation::set(|relid| {
        assert!(
            relid == T_OID || relid == STAT_OID || relid == STATEXT_OID,
            "unknown relation oid {relid}"
        );
        let cx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("test-relcache")));
        Ok(Some(Rc::new(make_relation(cx.mcx(), relid))))
    });
    relcache_seams::relation_get_index_list::set(|mcx, _relid| Ok(PgVec::new_in(mcx)));
    relcache_seams::relation_get_stat_ext_list::set(|mcx, _relid| Ok(PgVec::new_in(mcx)));
}

fn install_syscache_fixture_overrides() {
    // Real projections stay installed for pg_statistic (the flip under test);
    // pg_type/pg_attribute/pg_opclass live behind mocks (no such catalogs here).
    // Fixture columns carry no attoptions: valid tuple, null column.
    syscache_seams::pg_attribute_attoptions::set(|_mcx, _relid, _attnum| Ok(Some(None)));
    syscache_seams::lookup_pg_type_shape::set(|typid| {
        Ok(match typid {
            INT4OID => Some(types_tuple::PgTypeShape {
                typlen: 4,
                typbyval: true,
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typcollation: 0,
            }),
            _ => None,
        })
    });
    syscache_seams::lookup_pg_attribute_stattarget::set(|_, _| Ok(None));
    // pg_statistic seams (incl. stawidth) install via
    // init_seams_pg_statistic_only below; set-once seams forbid a second set.
    syscache_seams::pg_type_typanalyze::set(|_| Ok(0));
    syscache_seams::syscache_hash_value_typeoid::set(|typid| Ok(typid));
    syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
        let mut typname = NameData::default();
        typname.namestrcpy("int4");
        Ok((typid == INT4OID).then_some(syscache_seams::PgTypeTypcacheShape {
            typname,
            typlen: 4,
            typbyval: true,
            typalign: b'i' as i8,
            typstorage: b'p' as i8,
            typtype: b'b' as i8,
            typisdefined: true,
            typrelid: 0,
            typsubscript: 0,
            typelem: 0,
            typarray: 0,
            typcollation: 0,
        }))
    });
    indexcmds_seams::get_default_opclass::set(|typid, am| {
        Ok(if typid == INT4OID && am == 403 { INT4_BTREE_OPCLASS } else { 0 })
    });
    syscache_seams::lookup_pg_opclass_shape::set(|opclass| {
        Ok((opclass == INT4_BTREE_OPCLASS).then_some(syscache_seams::PgOpclassShape {
            opcmethod: 403,
            opcfamily: INT4_BTREE_FAM,
            opcintype: INT4OID,
            // int4 opclasses store no separate key type (pg_opclass: 0).
            opckeytype: ::types_core::InvalidOid,
        }))
    });
    syscache_seams::lookup_pg_amop_by_strategy::set(|opfamily, left, right, strategy| {
        Ok(match (opfamily, left, right, strategy) {
            (INT4_BTREE_FAM, INT4OID, INT4OID, 3) => INT4_EQ_OP,
            (INT4_BTREE_FAM, INT4OID, INT4OID, 1) => INT4_LT_OP,
            (INT4_BTREE_FAM, INT4OID, INT4OID, 5) => INT4_GT_OP,
            _ => 0,
        })
    });
    syscache_seams::lookup_pg_amproc::set(|opfamily, left, right, procnum| {
        Ok(match (opfamily, left, right, procnum) {
            (INT4_BTREE_FAM, INT4OID, INT4OID, 1) => BTINT4CMP,
            _ => 0,
        })
    });
    syscache_seams::lookup_pg_operator_shape::set(|opno| {
        let mk = |code, com, neg, rest| syscache_seams::PgOperatorShape { oprnamespace: 11,
            oprleft: INT4OID,
            oprright: INT4OID,
            oprresult: 16,
            oprcom: com,
            oprnegate: neg,
            oprcode: code,
            oprrest: rest,
            oprjoin: 0,
            oprcanmerge: true,
            oprcanhash: true,
        };
        Ok(match opno {
            INT4_EQ_OP => Some(mk(65, INT4_EQ_OP, 518, 101)),
            INT4_LT_OP => Some(mk(66, INT4_GT_OP, 525, 103)),
            INT4_GT_OP => Some(mk(147, INT4_LT_OP, 523, 104)),
            _ => None,
        })
    });
    syscache_seams::lookup_pg_proc_shape::set(|funcid| {
        let shape = |rettype, nargs| syscache_seams::PgProcShape {
            prolang: 12,
            prosecdef: false,
            proconfig_isnull: true,
            pronamespace: 11,
            prorettype: rettype,
            provariadic: 0,
            prosupport: 0,
            pronargs: nargs,
            prokind: b'f' as i8,
            provolatile: b'i' as i8,
            proparallel: b's' as i8,
            proretset: false,
            proisstrict: true,
            proleakproof: false,
        };
        Ok(match funcid {
            65 | 66 | 147 => Some(shape(16, 2)),
            _ => None,
        })
    });
    syscache_seams::pg_proc_cost_shape::set(|funcid| {
        Ok(match funcid {
            65 | 66 | 147 => Some(syscache_seams::PgProcCostShape { procost: 1.0, prorows: 0.0, prosupport: 0 }),
            _ => None,
        })
    });
    syscache_seams::lookup_pg_amop_by_operator::set(|opno, purpose, opfamily| {
        let strat = match opno {
            INT4_EQ_OP => 3,
            INT4_LT_OP => 1,
            INT4_GT_OP => 5,
            _ => return Ok(None),
        };
        Ok((purpose == b's' && (opfamily == 0 || opfamily == INT4_BTREE_FAM)).then_some(
            syscache_seams::PgAmopShape {
                amopstrategy: strat,
                amopsortfamily: 0,
                amoplefttype: INT4OID,
                amoprighttype: INT4OID,
            },
        ))
    });
    syscache_seams::lookup_pg_amop_members_by_operator::set(|mcx, opno| {
        let mut v = mcx::PgVec::new_in(mcx);
        let strat = match opno {
            INT4_EQ_OP => Some(3),
            INT4_LT_OP => Some(1),
            INT4_GT_OP => Some(5),
            _ => None,
        };
        if let Some(strat) = strat {
            v.push(syscache_seams::PgAmopMemberShape {
                amopfamily: INT4_BTREE_FAM,
                amoplefttype: INT4OID,
                amoprighttype: INT4OID,
                amopstrategy: strat,
                amopmethod: 403,
            });
        }
        Ok(v)
    });
    syscache_seams::lookup_pg_opfamily_shape::set(|opfid| {
        Ok((opfid == INT4_BTREE_FAM).then_some(syscache_seams::PgOpfamilyShape {
            opfmethod: 403,
            opfname: NameData::default(),
        }))
    });
}

fn write_control_file(dir: &std::path::Path) {
    let mut cf = controldata_utils::ControlFileData::ZEROED;
    cf.system_identifier = SYS_ID;
    cf.pg_control_version = PG_CONTROL_VERSION;
    cf.catalog_version_no = controldata_utils::CATALOG_VERSION_NO;
    cf.state = DB_IN_PRODUCTION;
    cf.checkPoint = SEG as u64 + 40;
    cf.checkPointCopy.redo = SEG as u64 + 40;
    cf.checkPointCopy.ThisTimeLineID = 1;
    cf.checkPointCopy.PrevTimeLineID = 1;
    cf.checkPointCopy.nextXid = types_core::FullTransactionId::from_epoch_and_xid(0, 3);
    cf.unloggedLSN = FirstNormalUnloggedLSN;
    cf.maxAlign = 8;
    cf.floatFormat = FLOATFORMAT_VALUE;
    cf.blcksz = 8192;
    cf.relseg_size = 131072;
    cf.xlog_blcksz = 8192;
    cf.xlog_seg_size = SEG as u32;
    cf.nameDataLen = 64;
    cf.indexMaxKeys = 32;
    cf.toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE;
    cf.loblksize = 2048;
    cf.float8ByVal = true;
    cf.crc = controldata_utils::crc_of_image(&cf.to_disk_bytes());
    let mut image = vec![0u8; PG_CONTROL_FILE_SIZE];
    image[..controldata_utils::SIZEOF_CONTROL_FILE_DATA].copy_from_slice(&cf.to_disk_bytes());
    std::fs::write(dir.join("global/pg_control"), &image).unwrap();
}

fn boot() {
    let dir = std::env::temp_dir().join(format!("pgrust_analyze_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for sub in ["global", "pg_wal", "pg_xact", "pg_subtrans"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    std::env::set_current_dir(&dir).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    init_small::globals::set_enableFsync(false);

    install_proc_boot_seams();
    shmem::init_seams();
    fd::init_seams();
    guc_tables::init_seams();
    guc::init_seams();
    commands_analyze::init_seams();
    adt_bool::init_seams();
    adt_float::init_seams();
    fmgr_core::init_seams();
    transam_xlog::init_seams();
    xlogutils::init_seams();
    heapam_visibility::init_seams();
    clog::init_seams();
    subtrans::init_seams();
    transam::init_seams();
    varsup::init_seams();
    xact::init_seams();
    snapmgr::init_seams();
    resowner::init_seams();
    procarray::init_seams();
    inval::init_seams();
    pgstat::init_seams();
    table::init_seams();
    tableam::init_seams();
    genam::init_seams();
    catcache::init_seams();
    cache_syscache::init_seams_pg_statistic_only();
    planner::init_seams();
    install_bufmgr_seams();
    install_relation_seams();
    install_syscache_fixture_overrides();
    install_xact_periphery_seams();
    guc::store::initialize_guc_options().unwrap();

    miscinit::SetIgnoreSystemIndexes(true);
    cache_syscache::InitCatalogCache().unwrap();
    seed_reloid_cache();

    RELSTATS.lock().unwrap().clear();
    vacuum_seams::vac_update_relstats::set(|rel, num_pages, num_tuples, allvis, _allfroz, _hasindex, _frozenxid, _minmulti, _in_outer| {
        RELSTATS.lock().unwrap().push((rel.rd_id, num_pages, num_tuples, allvis));
        if rel.rd_id == T_OID {
            *T_RELPAGES.lock().unwrap() = (num_pages as i32, num_tuples as f32);
        }
        Ok((false, false))
    });

    fd::InitFileAccess();
    lwlock::CreateLWLocks(false).unwrap();
    lmgr_proc::init_seams();
    lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
        autovacuum_worker_slots: 3,
        max_wal_senders: 2,
        max_prepared_xacts: 2,
        fastpath_lock_groups_per_backend: 1,
    });
    varsup::VarsupShmemInit();
    procarray::ProcArrayShmemInit();
    clog::CLOGShmemInit().unwrap();
    clog::BootStrapCLOG().unwrap();
    subtrans::SUBTRANSShmemInit().unwrap();
    subtrans::BootStrapSUBTRANS().unwrap();
    lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).unwrap();

    write_control_file(&dir);
    transam_xlog::ReadControlFile().unwrap();
    transam_xlog::XLOGShmemInit();

    let end_of_log: XLogRecPtr = 2 * SEG as u64;
    let prev_rec: XLogRecPtr = SEG as u64 + 40;
    let ctl = transam_xlog::ctl::XLogCtl();
    ctl.InsertTimeLineID.store(1, Relaxed);
    ctl.PrevTimeLineID.store(1, Relaxed);
    ctl.Insert.CurrBytePos.store(XLogRecPtrToBytePos(end_of_log), Relaxed);
    ctl.Insert.PrevBytePos.store(XLogRecPtrToBytePos(prev_rec), Relaxed);
    ctl.Insert.fullPageWrites.store(true, Relaxed);
    ctl.Insert.RedoRecPtr.store(prev_rec, Relaxed);
    ctl.RedoRecPtr.store(prev_rec, Relaxed);
    ctl.InitializedUpTo.store(end_of_log, Relaxed);
    ctl.logInsertResult.store(end_of_log, Relaxed);
    ctl.logWriteResult.store(end_of_log, Relaxed);
    ctl.logFlushResult.store(end_of_log, Relaxed);
    ctl.LogwrtRqstWrite.store(end_of_log, Relaxed);
    ctl.LogwrtRqstFlush.store(end_of_log, Relaxed);
    ctl.SharedRecoveryState.store(RECOVERY_STATE_DONE, Relaxed);
    ctl.InstallXLogFileSegmentActive.store(true, Relaxed);
    xlogutils::set_in_recovery(false);
    subtrans::StartupSUBTRANS(3).unwrap();
    assert!(transam_xlog::XLogInsertAllowed());
}

fn insert_rows() {
    let cx = MemoryContext::new("bulk-insert");
    let mcx = cx.mcx();
    xact::StartTransactionCommand().unwrap();
    let rel = table::table_open(mcx, T_OID, 3).unwrap();
    for g in 1..=10000i32 {
        let v = if g <= 5000 { 1 } else if g <= 8000 { 2 } else { 3 };
        let values = [Datum::from_i32(v), Datum::from_i32(g)];
        let nulls = [false, false];
        let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls).unwrap();
        heapam::simple_heap_insert(&rel, tup.as_tuple_mut()).unwrap();
    }
    table::table_close(rel, 3).unwrap();
    xact::CommitTransactionCommand().unwrap();
}

fn count_pg_statistic_rows() -> usize {
    let cx = MemoryContext::new("count");
    let mcx = cx.mcx();
    xact::StartTransactionCommand().unwrap();
    let rel = table::table_open(mcx, STAT_OID, 1).unwrap();
    let mut scan = genam::systable_beginscan(mcx, &rel, 0, false, None, &[]).unwrap();
    let mut n = 0;
    while genam::systable_getnext(mcx, &mut scan).unwrap().is_some() {
        n += 1;
    }
    genam::systable_endscan(mcx, scan).unwrap();
    table::table_close(rel, 1).unwrap();
    xact::CommitTransactionCommand().unwrap();
    n
}

fn run_analyze() {
    let cx = MemoryContext::new("analyze");
    let mcx = cx.mcx();
    xact::StartTransactionCommand().unwrap();
    commands_analyze::analyze_rel(mcx, T_OID, None, &NodeList::nil(), &VacuumParams { options: 0x02 }, false)
        .unwrap();
    xact::CommitTransactionCommand().unwrap();
}

fn plan_rows(qual: PlanQual) -> f64 {
    use types_nodes::nodes_enums::CmdType;
    use types_nodes::parsenodes::{Query, RTEKind};
    use types_nodes::primnodes::FromExpr;
    use types_nodes::Node;

    let cx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("plan")));
    let mcx = cx.mcx();
    xact::StartTransactionCommand().unwrap();

    let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
    rte.rtekind = RTEKind::RTE_RELATION;
    rte.relid = T_OID;
    rte.relkind = b'r';
    rte.rellockmode = 1;
    rte.inh = false;
    let rtable = NodeList::make1(mcx, rte.seal()).unwrap();
    let rtr = Node::mk_range_tbl_ref(mcx, 1).unwrap();

    let var = Node::mk_var(mcx, 1, qual.attno, INT4OID, -1, 0, 0).unwrap();
    let konst =
        Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(qual.value), false, true).unwrap();
    let (opno, opfuncid) = match qual.op {
        Op::Eq => (INT4_EQ_OP, 65),
        Op::Lt => (INT4_LT_OP, 66),
        Op::Gt => (INT4_GT_OP, 147),
    };
    let opexpr = Node::mk(
        mcx,
        types_nodes::primnodes::OpExpr {
            opno,
            opfuncid,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, var, konst).unwrap(),
            location: -1,
        },
    )
    .unwrap();

    let jointree = mcx::alloc_leak_in(
        mcx,
        FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals: Some(opexpr) },
    )
    .unwrap();
    let tvar = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, tvar, 1, Some("v"), false).unwrap();
    let parse = Query {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        jointree: Some(jointree),
        rtable,
        targetList: NodeList::make1(mcx, tle).unwrap(),
        stmt_location: 0,
        stmt_len: 30,
        ..Query::default()
    };
    let stmt = planner::planner(mcx, mcx::leak_in(mcx::alloc_in(mcx, parse).unwrap()), "test", 0, types_portal::ParamListHandle::NULL).unwrap();
    let plan = stmt.planTree.unwrap();
    let rows = plan.as_seq_scan().map(|s| s.scan.plan.plan_rows).unwrap_or_else(|| {
        panic!("expected SeqScan plan, got {:?}", plan.node_tag())
    });
    xact::CommitTransactionCommand().unwrap();
    rows
}

enum Op {
    Eq,
    Lt,
    Gt,
}

struct PlanQual {
    attno: i16,
    op: Op,
    value: i32,
}

#[test]
fn analyze_end_to_end_matches_live_postgres() {
    boot();
    insert_rows();

    let nblocks = with_fake(|f| rel_buffers(f, T_OID).len() as BlockNumber);
    assert!(nblocks > 10, "10000 rows spread over multiple pages");

    run_analyze();

    // pg_class update args (vac_update_relstats is the vacuum unit's; the
    // recorder stands in): live PG 18.3 reported reltuples=10000.
    {
        let recs = RELSTATS.lock().unwrap();
        let (relid, pages, tuples, _allvis) = recs[0];
        assert_eq!(relid, T_OID);
        assert_eq!(pages, nblocks);
        assert_eq!(tuples, 10000.0);
    }

    // Read back through the real syscache -> heap path (the consumer flip).
    let cx = MemoryContext::new("bundle");
    let mcx = cx.mcx();
    xact::StartTransactionCommand().unwrap();

    // Column v: live PG 18.3: stanullfrac 0, stawidth 4, stadistinct 3,
    // MCV {1,2,3}/{0.5,0.3,0.2}, correlation {1}, no histogram.
    let b = syscache_seams::lookup_pg_statistic_bundle::call(mcx, T_OID, 1, false)
        .unwrap()
        .expect("stats for t.v");
    assert_eq!(b.stanullfrac, 0.0);
    assert_eq!(b.stawidth, 4);
    assert_eq!(b.stadistinct, 3.0);
    assert_eq!(b.slots.len(), 2);
    let mcv = &b.slots[0];
    assert_eq!(mcv.kind, 1);
    assert_eq!(mcv.staop, INT4_EQ_OP);
    assert_eq!(mcv.valuetype().unwrap(), INT4OID);
    let mcv_vals: Vec<i32> = mcv.values().unwrap().iter().map(|d| d.as_i32()).collect();
    assert_eq!(mcv_vals, [1, 2, 3]);
    let freqs: Vec<f32> = mcv.numbers().unwrap().iter().copied().collect();
    assert_eq!(freqs, [0.5, 0.3, 0.2]);
    let corr = &b.slots[1];
    assert_eq!(corr.kind, 3);
    assert_eq!(corr.numbers().unwrap().iter().copied().collect::<Vec<_>>(), [1.0]);

    // Column u: stadistinct -1, histogram of 101 values 1..10000, correlation 1.
    let b = syscache_seams::lookup_pg_statistic_bundle::call(mcx, T_OID, 2, false)
        .unwrap()
        .expect("stats for t.u");
    assert_eq!(b.stadistinct, -1.0);
    assert_eq!(b.slots.len(), 2);
    let hist = &b.slots[0];
    assert_eq!(hist.kind, 2);
    assert_eq!(hist.staop, INT4_LT_OP);
    assert_eq!(hist.values().unwrap().len(), 101);
    assert_eq!(hist.values().unwrap()[0].as_i32(), 1);
    assert_eq!(hist.values().unwrap()[100].as_i32(), 10000);
    let corr = &b.slots[1];
    assert_eq!(corr.kind, 3);
    assert_eq!(corr.numbers().unwrap().iter().copied().collect::<Vec<_>>(), [1.0]);
    xact::CommitTransactionCommand().unwrap();

    assert_eq!(count_pg_statistic_rows(), 2);

    // Re-ANALYZE takes the update path: still exactly one row per column.
    run_analyze();
    assert_eq!(count_pg_statistic_rows(), 2);

    // Planner estimates vs live PostgreSQL 18.3 EXPLAIN on this dataset.
    assert_eq!(plan_rows(PlanQual { attno: 1, op: Op::Eq, value: 1 }), 5000.0);
    assert_eq!(plan_rows(PlanQual { attno: 1, op: Op::Eq, value: 2 }), 3000.0);
    assert_eq!(plan_rows(PlanQual { attno: 1, op: Op::Eq, value: 4 }), 1.0);
    assert_eq!(plan_rows(PlanQual { attno: 1, op: Op::Lt, value: 2 }), 5000.0);
    assert_eq!(plan_rows(PlanQual { attno: 2, op: Op::Eq, value: 77 }), 1.0);
    assert_eq!(plan_rows(PlanQual { attno: 2, op: Op::Lt, value: 2500 }), 2499.0);
    assert_eq!(plan_rows(PlanQual { attno: 2, op: Op::Gt, value: 9000 }), 1000.0);

    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        assert!(f.locks.iter().all(|l| *l == 0), "leaked locks: {:?}", f.locks);
    });
}
